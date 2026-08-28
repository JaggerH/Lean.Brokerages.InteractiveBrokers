/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Ledger;

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Order ledger wiring: the write-ahead intent record that lets a restarted engine ask IB
    /// "did this order of mine actually make it?" and attribute inbound fills to the strategy
    /// group that placed them. Design and decision rules live in the main repository at
    /// docs/superpowers/specs/2026-08-04-order-ledger-design.md and
    /// docs/brokerage-architecture.md ("订单账本：券商侧只碰四个方法").
    ///
    /// IB's carrier for the ledger key is <c>IBApi.Order.OrderRef</c>; it comes back on the
    /// <c>openOrder</c> callback and on every <c>execDetails</c> (already read into
    /// <c>ExecutionRecord.Tag</c>).
    /// </summary>
    public sealed partial class InteractiveBrokersBrokerage
    {
        /// <summary>
        /// IB's constraint on <c>OrderRef</c>. UNLIKE the crypto venues, IB documents NO limit at
        /// all: the TWS API Order class reference says only "The order reference. Intended for
        /// institutional customers only, although all customers may use it to identify the API
        /// client that sent the order when multiple API clients are running"
        /// (https://interactivebrokers.github.io/tws-api/classIBApi_1_1Order.html) — no length, no
        /// charset. The Web API's equivalent field (<c>cOID</c>) is likewise documented as "an
        /// arbitrary string", and CSharpAPI.dll declares <c>OrderRef</c> as a plain <c>string</c>
        /// with no client-side validation, so nothing in the shipped API tells us where the server
        /// draws the line.
        ///
        /// THESE NUMBERS ARE THEREFORE A CONSERVATIVE ESTIMATE, NOT A DOCUMENTED LIMIT:
        /// - 32 chars: comfortably above the 18-char key <see cref="OrderKeyGenerator"/> mints, and
        ///   at or below the tightest client-order-id budget among venues LEAN already integrates
        ///   (OKX clOrdId ≤ 32, Binance ≤ 36) — if IB has an undocumented cap it is unlikely to be
        ///   tighter than the industry-common 32.
        /// - Charset: ASCII alphanumerics plus <c>_ . - : /</c>. Excludes whitespace, commas and
        ///   quotes because OrderRef is echoed into IB's CSV/Flex activity reports, where those
        ///   characters are at best awkward and at worst field-splitting.
        ///
        /// Both bounds are far outside what we actually emit (18 digits), so tightening them costs
        /// nothing today; if a future key format grows past them
        /// <see cref="OrderKeyConstraint.Validate"/> throws rather than letting IB silently truncate.
        /// </summary>
        internal static readonly OrderKeyConstraint OrderRefConstraint = new OrderKeyConstraint(32, "^[A-Za-z0-9_.:/-]+$");

        /// <summary>
        /// IB error codes that mean "this order request was rejected and nothing is live at IB",
        /// i.e. the only ones that may write a ledger tombstone. Everything else — timeouts,
        /// disconnects, and every code not listed here — is "don't know", and MUST leave the Intent
        /// open: that open Intent is the only evidence a crash window ever existed.
        ///
        /// Message texts quoted verbatim from https://interactivebrokers.github.io/tws-api/message_codes.html
        ///
        /// | code | message                                                                        | why it is a confirmed rejection |
        /// |------|--------------------------------------------------------------------------------|---------------------------------|
        /// | 110  | "The price does not conform to the minimum price variation for this contract."  | price validation fails before the order is accepted |
        /// | 201  | "Order rejected - Reason:"                                                      | IB's canonical rejection code; the order is not working |
        /// | 203  | "The security &lt;security&gt; is not available or allowed for this account."   | entitlement check fails before acceptance |
        /// | 382  | "The price specified violates the number of ticks constraint specified in the default order settings." | TWS precautionary setting blocks transmission |
        /// | 383  | "The size specified violates the size constraint specified in the default order settings."             | same, on size |
        /// | 388  | "Order size is smaller than the minimum requirement."                           | size validation fails before acceptance |
        /// | 434  | "The order size cannot be zero."                                                | size validation fails before acceptance |
        ///
        /// Deliberately EXCLUDED, with the reason — each of these proves or permits the opposite:
        /// - 104 "Can't modify a filled order." / 105 "Order being modified does not match original
        ///   order." / 10148 "OrderId ... that needs to be cancelled can not be cancelled, state:" —
        ///   these prove the order EXISTS at IB.
        /// - 202 "Order cancelled - Reason:" — the order existed and was cancelled; that is a
        ///   Closed, not a Dead, and it arrives on the orderStatus path anyway.
        /// - 321 "Server error when validating an API client request." — reads as a server-side
        ///   failure of unknown effect, not a decided rejection.
        /// - 399 "Order message error" — generic wrapper, says nothing about acceptance.
        /// - 1100/1101/1102, 2103-2106, 502/504 and every transport failure — the definition of
        ///   "don't know whether it arrived".
        ///
        /// The list is a whitelist rather than the "reject by default, enumerate the ambiguous
        /// shapes" contract Binance uses, because IB's error callback shares one id sequence with
        /// market-data and contract requests and carries ~500 codes that have nothing to do with
        /// order acceptance. Missing a tombstone costs a stale open Intent (a startup warning);
        /// writing one wrongly erases the evidence of a live unacknowledged order.
        /// </summary>
        internal static readonly HashSet<int> ConfirmedRejectionCodes = new HashSet<int>
        {
            110, 201, 203, 382, 383, 388, 434
        };

        /// <summary>
        /// Maps IB order id -> the ledger key we sent as OrderRef, for the callbacks that do not
        /// echo OrderRef back (<c>orderStatus</c> and <c>error</c> carry only the id). Only ever
        /// populated for orders this run placed, which is what makes it safe to treat "id is in this
        /// map" as "this id is one of our orders": <c>_nextValidId</c> is monotonic and shared with
        /// market-data/contract request ids, so an id spent on an order is never handed out again.
        /// </summary>
        private readonly ConcurrentDictionary<int, string> _ledgerKeysByIbOrderId = new ConcurrentDictionary<int, string>();

        /// <summary>
        /// One-time-per-run: not having a ledger is a static fact about how this run was wired
        /// (the algorithm is not an <see cref="AIAlgorithm"/>), not a per-order event. Logging it on
        /// every order would bury real errors.
        /// </summary>
        private bool _ledgerWarningLogged;

        private IOrderLedger _ledger;
        private bool _ledgerResolved;

        /// <summary>
        /// This run's order ledger, or null when <see cref="_algorithm"/> is not an
        /// <see cref="AIAlgorithm"/> — the ledger is opt-in infrastructure. Cached because the
        /// getter on <see cref="AIAlgorithm"/> is not free (it lazily creates the ledger directory
        /// and takes an exclusive file lock) and the placement path hits it on every order.
        /// </summary>
        private IOrderLedger Ledger
        {
            get
            {
                // The _algorithm != null test guards the latch itself: touching this getter once
                // before _algorithm is assigned would pin _ledgerResolved to true for the whole run
                // and the ledger would silently never wire up.
                if (!_ledgerResolved && _algorithm != null)
                {
                    _ledger = (_algorithm as AIAlgorithm)?.OrderLedger;
                    _ledgerResolved = true;
                }
                return _ledger;
            }
        }

        /// <summary>
        /// Resolves the <c>OrderRef</c> to put on an outbound IB order.
        ///
        /// New orders register a write-ahead intent and get a fresh key. Updates reuse the key the
        /// original placement sent: IB modifies an order by resending it under the same order id,
        /// and minting a second key would open a second ledger entry that nothing ever acks.
        /// </summary>
        /// <param name="orders">The Lean orders behind this IB order. A combo arrives here as every
        /// leg at once, and is <b>refused</b> when a ledger is wired: the ledger's unit of identity
        /// is the exchange order, so a combo's legs cannot be reconciled individually. Combos are
        /// only placeable from a strategy with no ledger.</param>
        /// <param name="ibOrderId">The IB order id this request will use.</param>
        /// <param name="needsNewId">True for a placement, false for a modification.</param>
        /// <returns>The key, or null when there is no ledger to register with.</returns>
        internal string ResolveOrderRef(List<Order> orders, int ibOrderId, bool needsNewId)
        {
            if (!needsNewId)
            {
                _ledgerKeysByIbOrderId.TryGetValue(ibOrderId, out var existing);
                return existing;
            }

            var ledger = Ledger;
            if (ledger == null)
            {
                if (!_ledgerWarningLogged)
                {
                    _ledgerWarningLogged = true;
                    Log.Error($"InteractiveBrokersBrokerage.ResolveOrderRef(): no OrderLedger wired for order " +
                              $"{orders[0].Id} — no OrderRef will be sent. Fills for orders placed this run cannot " +
                              "be attributed to a strategy group after a restart, and a crash mid-flight leaves no " +
                              "evidence for OrderLedgerRecovery to reconcile against. This is a one-time warning; " +
                              "every subsequent order this run hits the same fallback.");
                }
                return null;
            }

            if (orders.Count > 1)
            {
                // 明确不支持：IB 给整个 combo 一个订单号、一个 OrderRef，而账本的身份单位是
                // 「一张场所订单一条意图」，只登记得下 orders[0]，其余腿共用钥匙、不被逐腿核对。
                // 接了账本就是套利策略，而套利策略不下 combo；要下 combo 的非套利策略不接账本，
                // 走的是上面 ledger == null 那条路，不受这里影响。所以这里拒绝，而不是带着
                // 半覆盖的账本下单——半覆盖看起来和全覆盖一模一样。
                var message = $"combo order (lean order {orders[0].Id} + {orders.Count - 1} more legs) refused: " +
                    "the order ledger accounts for one intent per exchange order, so a combo's legs cannot be " +
                    "reconciled individually. Per-leg ledger accounting is deliberately not implemented — " +
                    "place combos from a strategy that has no order ledger wired, or implement per-leg support " +
                    "first.";
                Log.Error($"InteractiveBrokersBrokerage.ResolveOrderRef(): {message}");
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, "ORDER_LEDGER_COMBO_UNSUPPORTED", message));
                throw new NotSupportedException(message);
            }

            // Symbol.ID.Market, not a hardcoded constant and not the brokerage name: IB trades
            // usa/cme/nymex/oanda/... and the ledger's venue comparison is ordinal and
            // case-sensitive, so a wrong label does not throw — it just makes every later lookup
            // miss in silence. OrderLedgerRecovery derives the venue the same way
            // (order.Symbol?.ID.Market), so this is the label recovery will look for.
            var venue = orders[0].Symbol.ID.Market;

            // Throws (and so aborts the placement, which is the contract) if the key does not fit
            // the venue constraint or the ledger cannot be written.
            var key = ledger.RegisterIntent(orders[0], venue, OrderRefConstraint);
            _ledgerKeysByIbOrderId[ibOrderId] = key;
            return key;
        }

        /// <summary>
        /// Test seam: wires a ledger directly, bypassing the <see cref="_algorithm"/>-based
        /// resolution — reaching that path requires the full constructor, which starts an IB
        /// Gateway. Production wiring goes through the <see cref="Ledger"/> getter only.
        /// </summary>
        internal void WireOrderLedgerForTesting(IOrderLedger ledger)
        {
            _ledger = ledger;
            _ledgerResolved = true;
        }

        /// <summary>
        /// Records IB's acknowledgement of an order. Called from the <c>openOrder</c> callback
        /// (which echoes OrderRef, so it also works for orders adopted from a previous run) and from
        /// <c>orderStatus</c> (which does not, hence the id map).
        ///
        /// For IB the exchange order id is the order id we ourselves allocated, so the ack does not
        /// teach us a new identifier — it is the proof IB has the order, which is what turns an
        /// Intent (a possible crash window) into a settled fact.
        /// </summary>
        private void RecordLedgerAck(int ibOrderId, string orderRef)
        {
            var ledger = Ledger;
            if (ledger == null)
            {
                return;
            }

            var key = !string.IsNullOrEmpty(orderRef)
                ? orderRef
                : (_ledgerKeysByIbOrderId.TryGetValue(ibOrderId, out var mapped) ? mapped : null);

            if (string.IsNullOrEmpty(key))
            {
                return;
            }

            if (!ledger.TryResolve(null, null, key, out var entry))
            {
                if (OrderKeyGenerator.IsOurs(key))
                {
                    // Our key shape, unknown to the ledger: the ledger was lost, rolled back, or a
                    // second writer exists. The startup gate treats this as a halt condition; from
                    // here the loudest available channel is a brokerage message event.
                    var message = $"IB order {ibOrderId} carries OrderRef {key}, which has our client order id shape, " +
                        "but the OrderLedger has no record of it — the ledger may be lost, rolled back, or a second " +
                        "writer exists. Fills for this order cannot be attributed.";
                    Log.Error($"InteractiveBrokersBrokerage.RecordLedgerAck(): {message}");
                    _algorithm?.Error(message);
                    OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, "ORDER_LEDGER_UNKNOWN_KEY", message));
                }
                // Anything else is simply not ours: an order placed by hand in TWS, or by another
                // API client. Adopting it is the transaction handler's business, not the ledger's.
                return;
            }

            if (entry.State != LedgerEntryState.Intent)
            {
                // IB repeats openOrder/orderStatus freely; only the first one has work to do.
                return;
            }

            if (!ledger.RecordAck(key, ibOrderId.ToStringInvariant()))
            {
                var message = $"OrderLedger.RecordAck() rejected OrderRef {key} (IB order {ibOrderId}) — the ledger " +
                    "has no matching intent. Every inbound report for this order will fail to resolve against it.";
                Log.Error($"InteractiveBrokersBrokerage.RecordLedgerAck(): {message}");
                _algorithm?.Error(message);
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, "ORDER_LEDGER_ACK_FAILED", message));
            }
        }

        /// <summary>
        /// Writes the ledger's terminal state when IB reports the order finished (filled or
        /// cancelled). Without this the ledger never closes: acked entries pile up forever and
        /// compaction, which only discards terminal entries, can never shrink the file.
        ///
        /// Only IB's own <c>orderStatus</c> terminal states drive this. "The order is gone" inferred
        /// from an error code is not the same claim, and getting it wrong closes an entry whose
        /// order is still working.
        /// </summary>
        private void RecordLedgerClosed(int ibOrderId)
        {
            var ledger = Ledger;
            if (ledger == null || !_ledgerKeysByIbOrderId.TryGetValue(ibOrderId, out var key))
            {
                return;
            }

            if (!ledger.TryResolve(null, null, key, out var entry)
                || entry.State == LedgerEntryState.Closed
                || entry.State == LedgerEntryState.Dead)
            {
                // Already terminal: IB duplicates terminal status reports, and OrderLedger.Amend
                // always appends, so only this check makes repeated calls idempotent.
                return;
            }

            if (!ledger.RecordClosed(key))
            {
                var message = $"OrderLedger.RecordClosed() rejected OrderRef {key} (IB order {ibOrderId}) — the " +
                    "ledger has no matching intent.";
                Log.Error($"InteractiveBrokersBrokerage.RecordLedgerClosed(): {message}");
                _algorithm?.Error(message);
            }
        }

        /// <summary>
        /// Writes a ledger tombstone, but ONLY when the error code proves IB never took the order.
        /// See <see cref="ConfirmedRejectionCodes"/> for the table and its sources.
        ///
        /// Two guards beyond the code itself:
        /// - the entry must still be in <see cref="LedgerEntryState.Intent"/>. If IB ever sent an
        ///   openOrder/orderStatus for it, IB has the order and no rejection code can un-prove that.
        ///   This is also what keeps modification rejections out: an order can only be modified once
        ///   it is working, i.e. once it is acked, so a modify error never sees an Intent.
        /// - the message must not talk about modifying or cancelling, which would mean the error is
        ///   about a follow-up request rather than about the original order's acceptance.
        /// </summary>
        private void TryRecordLedgerDead(int ibOrderId, int errorCode, string errorMessage)
        {
            var ledger = Ledger;
            if (ledger == null
                || !ConfirmedRejectionCodes.Contains(errorCode)
                || !_ledgerKeysByIbOrderId.TryGetValue(ibOrderId, out var key))
            {
                return;
            }

            if (errorMessage != null
                && (errorMessage.Contains("modif", System.StringComparison.OrdinalIgnoreCase)
                    || errorMessage.Contains("cancel", System.StringComparison.OrdinalIgnoreCase)))
            {
                Log.Trace($"InteractiveBrokersBrokerage.TryRecordLedgerDead(): not tombstoning OrderRef {key} " +
                          $"(IB order {ibOrderId}): code {errorCode} arrived with a modify/cancel message, which " +
                          "says nothing about whether the original order is live.");
                return;
            }

            if (!ledger.TryResolve(null, null, key, out var entry) || entry.State != LedgerEntryState.Intent)
            {
                return;
            }

            if (!ledger.RecordDead(key, $"IB error {errorCode}: {errorMessage}"))
            {
                var message = $"OrderLedger.RecordDead() rejected OrderRef {key} (IB order {ibOrderId}) — the " +
                    "ledger has no matching intent.";
                Log.Error($"InteractiveBrokersBrokerage.TryRecordLedgerDead(): {message}");
                _algorithm?.Error(message);
            }
        }
    }
}
