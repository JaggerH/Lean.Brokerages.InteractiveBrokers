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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using IBApi;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders.Ledger;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// <see cref="IClientOrderIdQuery"/> for IB: take a ledger key we wrote before sending
    /// (<c>OrderRef</c>) and ask IB whether it ever got that order. Backs order-ledger convergence —
    /// intents that were written locally but never acked, because the send timed out or the process
    /// died mid-request.
    ///
    /// IB HAS NO LOOKUP BY OrderRef. Nothing in the TWS API takes a client order id and answers
    /// "do you have it"; the field only comes back attached to orders IB volunteers. So the query is
    /// assembled from the three requests that between them enumerate every order IB would still
    /// admit to having, and the answer is only as good as the weakest of the three:
    ///
    /// | request                              | covers                                       | id we can use |
    /// |--------------------------------------|----------------------------------------------|---------------|
    /// | <c>reqAllOpenOrders</c>              | orders still resting on the book             | order.OrderId |
    /// | <c>reqExecutions</c>                 | anything that traded — LAST 24 HOURS ONLY    | execution.OrderId |
    /// | <c>reqCompletedOrders</c>            | today's finished orders (filled or cancelled)| order.OrderId, 0 for prior sessions |
    ///
    /// The 24-hour bound on executions is IB's, documented on the <c>execDetails</c> callback
    /// ("Returns executions from the last 24 hours as a response to reqExecutions()") and repeated in
    /// this repository's own client at Client/InteractiveBrokersClient.cs. It is why a silent
    /// <c>NotFound</c> is NOT available for an old intent: see <see cref="ExecutionLookbackWindow"/>.
    /// </summary>
    public sealed partial class InteractiveBrokersBrokerage : IClientOrderIdQuery
    {
        /// <summary>
        /// How far back <c>reqExecutions</c> can see. IB's own words on the <c>execDetails</c>
        /// callback: "Returns executions from the last 24 hours as a response to reqExecutions()".
        ///
        /// This is the hard edge of the whole query. Past it, an order that filled and finished is
        /// invisible to all three sources — executions have aged out, it is not open, and
        /// <c>reqCompletedOrders</c> only reports the current trading day — and "none of my sources
        /// mentions it" stops meaning "IB never had it". So an intent older than this window can
        /// only ever produce <see cref="ClientOrderIdQueryOutcome.QueryFailed"/>, never
        /// <see cref="ClientOrderIdQueryOutcome.NotFound"/>.
        ///
        /// Note this window is if anything generous: <c>reqCompletedOrders</c> is scoped to the
        /// current trading day, so a 20-hour-old intent from yesterday's session is already outside
        /// that source even though it is inside this one. Executions is the only one of the three
        /// with a number IB commits to, and its NotFound is the one that would matter (a completed
        /// order that never traded also never moved money), so 24h is the bound used here — but see
        /// the residual risk noted on <see cref="CombineSourceLookups"/>.
        /// </summary>
        internal static readonly TimeSpan ExecutionLookbackWindow = TimeSpan.FromHours(24);

        /// <summary>
        /// <c>OrderState.Status</c> values that mean the order stopped moving. Source:
        /// https://interactivebrokers.github.io/tws-api/order_submission.html ("Order Status") —
        /// Cancelled / ApiCancelled / Filled are the terminal ones there. Inactive is deliberately
        /// NOT here: IB documents it as an order that "may be submitted at a later time", which is
        /// the opposite of done.
        /// </summary>
        private static readonly HashSet<string> TerminalOrderStates =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Filled", "Cancelled", "ApiCancelled" };

        /// <summary>How long to wait for each source's end-of-stream callback before calling it a failure.</summary>
        private static readonly TimeSpan SourceTimeout = TimeSpan.FromSeconds(15);

        /// <summary>
        /// See <see cref="IClientOrderIdQuery.QueryByClientOrderId"/>. NEVER THROWS: every source is
        /// wrapped individually (a source that throws becomes a failed source, not a lost query) and
        /// the whole method is wrapped again, because the callers are startup recovery and a
        /// once-a-minute reconcile loop where one escaping exception would abort convergence for
        /// every other pending intent in the batch.
        /// </summary>
        /// <param name="symbol">
        /// NOT used to filter any of the three requests, on purpose. IB's execution filter can take a
        /// symbol, but a filter that does not match — a mis-mapped ticker, an option/future whose IB
        /// local symbol differs from the LEAN one — turns into an empty answer that is
        /// indistinguishable from "IB does not have this order", i.e. exactly the NotFound /
        /// QueryFailed confusion this interface exists to prevent. Matching is done on OrderRef
        /// alone, which is unique per intent. The parameter is kept for the interface and for log
        /// context.
        /// </param>
        public ClientOrderIdQueryResult QueryByClientOrderId(Symbol symbol, string clientOrderId)
        {
            if (string.IsNullOrEmpty(clientOrderId))
            {
                return ClientOrderIdQueryResult.QueryFailed("clientOrderId must not be null or empty");
            }

            try
            {
                if (!IsConnected)
                {
                    // Not "IB does not have it" — we did not get to ask. Reconnecting from here would
                    // block the reconcile loop behind an IB Gateway restart; the caller retries.
                    return ClientOrderIdQueryResult.QueryFailed(
                        $"not connected to IB, so OrderRef {clientOrderId} could not be looked up");
                }

                var openOrders = LookupOrderRefInOpenOrders(clientOrderId);
                var executions = LookupOrderRefInExecutions(clientOrderId);
                var completedOrders = LookupOrderRefInCompletedOrders(clientOrderId);

                var result = CombineSourceLookups(clientOrderId, TryGetIntentAge(clientOrderId),
                    openOrders, completedOrders, executions);

                if (result.Outcome == ClientOrderIdQueryOutcome.QueryFailed)
                {
                    // Logged here rather than left to the caller: the reason names which of the three
                    // sources went dark, and that is the only place that information exists.
                    Log.Error($"InteractiveBrokersBrokerage.QueryByClientOrderId({symbol}, {clientOrderId}): " +
                              result.FailureReason);
                }
                return result;
            }
            catch (Exception ex)
            {
                var reason = $"Exception querying IB for OrderRef {clientOrderId}: {ex.Message}";
                Log.Error($"InteractiveBrokersBrokerage.QueryByClientOrderId(): {reason}");
                return ClientOrderIdQueryResult.QueryFailed(reason);
            }
        }

        /// <summary>
        /// How old the intent is, from OUR OWN write-ahead record, or null when we cannot tell.
        ///
        /// This is a local fact, not a claim about the venue: the ledger line was written just before
        /// the send, so it dates the send. It is used for one thing only — deciding whether
        /// <see cref="ExecutionLookbackWindow"/> ever covered this order — and never as evidence
        /// about whether IB has it.
        ///
        /// Null (no ledger wired, or no entry under this key) is the conservative input: without an
        /// age we cannot show the sources covered the order, so "none of them mentions it" cannot be
        /// promoted to NotFound.
        /// </summary>
        private TimeSpan? TryGetIntentAge(string clientOrderId)
        {
            try
            {
                var ledger = Ledger;
                if (ledger == null || !ledger.TryResolve(null, null, clientOrderId, out var entry) || entry == null)
                {
                    return null;
                }
                return DateTime.UtcNow - entry.CreatedOrFallbackUtc;
            }
            catch (Exception ex)
            {
                // Same direction as a missing entry: unknown age, never a fabricated one.
                Log.Error($"InteractiveBrokersBrokerage.TryGetIntentAge({clientOrderId}): {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// The whole three-state decision, pure so it can be tested without an IB Gateway.
        /// </summary>
        /// <param name="lookups">
        /// The sources, in the order their exchange order id should be preferred. Every one of them
        /// must be present — a source that was skipped has to arrive here as a FAILED lookup, never
        /// be omitted, or its absence silently reads as "answered, no match".
        /// </param>
        /// <remarks>
        /// The three rules, in the order they are applied:
        ///
        /// 1. ANY source matched => Found. A positive answer is authoritative on its own; the other
        ///    sources failing does not weaken it, because they could only have added detail.
        /// 2. ANY source failed and none matched => QueryFailed, naming which source and why. This is
        ///    the rule the whole file exists for: a timed-out or throwing source makes the answer
        ///    unknown, and reporting unknown as NotFound lets convergence write a terminal state for
        ///    an order that may be resting at IB right now.
        /// 3. All three answered, none matched => NotFound, but ONLY if the intent is young enough
        ///    that <see cref="ExecutionLookbackWindow"/> covered it. Unknown age, or an age past the
        ///    window, is QueryFailed for the same reason as rule 2 — the sources cannot see that far
        ///    back, so their silence is not evidence.
        ///
        /// RESIDUAL RISK, stated rather than hidden: inside the 24h window this NotFound is still
        /// slightly optimistic, because <c>reqCompletedOrders</c> is scoped to the current trading
        /// day. An order cancelled without a fill during yesterday's session, 20 hours ago, is past
        /// completed-orders and was never in executions, and would be reported NotFound. Two things
        /// bound the damage: an order that never filled never moved money, and the Engine's
        /// convergence only trusts a NotFound for venues listed in its own
        /// <c>VenueNotFoundRetention</c> table (Engine/TransactionHandlers/OrderLedgerConvergence.cs),
        /// which today lists okx/binance/binanceus and none of IB's markets (usa/cme/nymex/oanda/...)
        /// — so a NotFound from here currently converges to nothing but an alarm. ANYONE ADDING AN IB
        /// MARKET TO THAT TABLE must first decide what retention value is honest given the day-scoped
        /// source; it is smaller than 24h, not larger.
        /// </remarks>
        internal static ClientOrderIdQueryResult CombineSourceLookups(string clientOrderId, TimeSpan? intentAge,
            params OrderRefLookup[] lookups)
        {
            var matches = lookups.Where(lookup => lookup.Matched).ToList();
            if (matches.Count > 0)
            {
                // First non-zero IB order id, in the caller's preference order. Only the IB order id
                // is usable: the ledger's ack path writes exactly this number
                // (RecordLedgerAck -> ibOrderId.ToStringInvariant()) and inbound fills resolve
                // against it, so handing back a permId here would produce an entry that no fill can
                // ever match — silently.
                var ibOrderId = matches.Select(match => match.IbOrderId).FirstOrDefault(id => id != 0);

                // The largest reported fill wins: both numbers estimate the same quantity, and the
                // larger one is the source that has seen more of it. Under-reporting a fill is the
                // dangerous direction.
                var filledQuantity = matches.Max(match => match.FilledQuantity);

                // Terminal if ANY source says so — the sources are read one after another, so a race
                // where the order finishes between two of them shows up exactly like this.
                var isTerminal = matches.Any(match => match.IsTerminal);

                if (ibOrderId == 0)
                {
                    // IB has the order but gave no usable order id: completedOrder leaves OrderId 0
                    // for orders placed in a previous session. Deliberately NOT Found — a Found with
                    // no exchange order id is treated by convergence as a malformed brokerage
                    // implementation, and that is the wrong story. What actually happened is that we
                    // cannot produce the identifier the ledger needs, i.e. the query did not answer.
                    var permIds = string.Join(", ", matches.Where(match => match.PermId != 0)
                        .Select(match => $"{match.Source}: permId {match.PermId}"));
                    return ClientOrderIdQueryResult.QueryFailed(
                        $"IB has an order under OrderRef {clientOrderId} ({permIds}) but reported IB order id 0 for " +
                        "it — that happens for orders placed in a previous session, and a permId cannot be recorded " +
                        "as the exchange order id because inbound fills resolve by IB order id. Resolve this intent " +
                        "by hand.");
                }

                return ClientOrderIdQueryResult.Found(
                    ibOrderId.ToString(CultureInfo.InvariantCulture), isTerminal, filledQuantity);
            }

            var failures = lookups.Where(lookup => !lookup.Answered).ToList();
            if (failures.Count > 0)
            {
                var detail = string.Join("; ", failures.Select(failure => $"{failure.Source}: {failure.FailureReason}"));
                var answered = lookups.Where(lookup => lookup.Answered).Select(lookup => lookup.Source).ToList();
                return ClientOrderIdQueryResult.QueryFailed(
                    $"OrderRef {clientOrderId} was not found by [{string.Join(", ", answered)}], but " +
                    $"{failures.Count} of {lookups.Length} sources could not answer — {detail}. " +
                    "This is NOT 'IB does not have this order': the order may be live at IB right now.");
            }

            if (intentAge == null)
            {
                return ClientOrderIdQueryResult.QueryFailed(
                    $"All {lookups.Length} IB sources answered and none has OrderRef {clientOrderId}, but the " +
                    "intent's age is unknown (no ledger entry for this key), so it cannot be shown that " +
                    $"reqExecutions' {ExecutionLookbackWindow.TotalHours}h window ever covered this order. " +
                    "Reporting NotFound off a query that may simply be looking too late is how a live order gets " +
                    "written off.");
            }

            if (intentAge > ExecutionLookbackWindow)
            {
                return ClientOrderIdQueryResult.QueryFailed(
                    $"All {lookups.Length} IB sources answered and none has OrderRef {clientOrderId}, but the intent " +
                    $"is {intentAge} old — past reqExecutions' {ExecutionLookbackWindow.TotalHours}h window, and " +
                    "reqCompletedOrders only covers the current trading day. An order that filled and finished that " +
                    "long ago is invisible to all three sources, so their silence proves nothing. A human must look " +
                    "this OrderRef up in IB's activity report and resolve the ledger entry by hand.");
            }

            return ClientOrderIdQueryResult.NotFound();
        }

        /// <summary>
        /// Source 1: <c>reqAllOpenOrders</c> — every order still resting at IB, whichever API client
        /// or TWS put it there. <c>reqOpenOrders</c> (this client only) is not enough: after a crash
        /// the order we are asking about was placed by a process that no longer exists.
        /// </summary>
        private OrderRefLookup LookupOrderRefInOpenOrders(string clientOrderId)
        {
            const string source = "reqAllOpenOrders";
            try
            {
                OrderRefLookup match = null;
                var completed = new ManualResetEvent(false);
                var lastOrderId = 0;
                Exception handlerException = null;

                EventHandler<IB.OpenOrderEventArgs> onOpenOrder = (sender, args) =>
                {
                    try
                    {
                        if (args.OrderId > lastOrderId)
                        {
                            lastOrderId = args.OrderId;
                        }
                        if (match == null && args.Order != null && args.Order.OrderRef == clientOrderId)
                        {
                            match = OrderRefLookup.Match(source, args.Order.OrderId, args.Order.PermId,
                                IsTerminalOrderState(args.OrderState?.Status), args.Order.FilledQuantity);
                        }
                    }
                    catch (Exception e)
                    {
                        handlerException = e;
                    }
                };
                EventHandler onOpenOrderEnd = (sender, args) => completed.Set();

                // reqAllOpenOrders makes IB require that subsequent order ids exceed every id it just
                // reported, so the id bump has to happen under the same lock the placement path takes
                // — otherwise this read-only query can make the next PlaceOrder be rejected.
                lock (_nextValidIdLocker)
                {
                    _client.OpenOrder += onOpenOrder;
                    _client.OpenOrderEnd += onOpenOrderEnd;
                    try
                    {
                        CheckRateLimiting();
                        _client.ClientSocket.reqAllOpenOrders();

                        if (!completed.WaitOne(SourceTimeout))
                        {
                            return OrderRefLookup.Failed(source,
                                $"no openOrderEnd within {SourceTimeout.TotalSeconds}s");
                        }
                    }
                    finally
                    {
                        _client.OpenOrder -= onOpenOrder;
                        _client.OpenOrderEnd -= onOpenOrderEnd;
                    }

                    if (lastOrderId >= _nextValidId)
                    {
                        Log.Trace($"InteractiveBrokersBrokerage.LookupOrderRefInOpenOrders(): Updating nextValidId " +
                                  $"from {_nextValidId} to {lastOrderId + 1}");
                        _nextValidId = lastOrderId + 1;
                    }
                }

                if (handlerException != null)
                {
                    // A throw while reading one order means we may have skipped the very order we are
                    // looking for. Unless we already matched, this source did not answer.
                    return match ?? OrderRefLookup.Failed(source,
                        $"failed while reading an open order: {handlerException.Message}");
                }

                return match ?? OrderRefLookup.NoMatch(source);
            }
            catch (Exception ex)
            {
                return OrderRefLookup.Failed(source, ex.Message);
            }
        }

        /// <summary>
        /// Source 2: <c>reqExecutions</c> — anything that traded in the last 24 hours. This is the
        /// only source that can prove an unacked intent moved money.
        /// </summary>
        private OrderRefLookup LookupOrderRefInExecutions(string clientOrderId)
        {
            const string source = "reqExecutions";
            try
            {
                var filter = new ExecutionFilter
                {
                    AcctCode = _account,
                    ClientId = ClientId,
                    // Time left empty: IB then returns everything it still has, i.e. the full 24h
                    // window. A start time here could only ever narrow that.
                };

                var requestId = GetNextId();
                _requestInformation[requestId] = new RequestInformation
                {
                    RequestId = requestId,
                    RequestType = RequestType.Executions,
                    Message = $"[Id={requestId}] QueryByClientOrderId: OrderRef {clientOrderId}"
                };

                OrderRefLookup match = null;
                var completed = new ManualResetEvent(false);
                Exception handlerException = null;

                EventHandler<IB.ExecutionDetailsEventArgs> onExecDetails = (sender, args) =>
                {
                    try
                    {
                        if (args.RequestId != requestId || args.Execution == null
                            || args.Execution.OrderRef != clientOrderId)
                        {
                            return;
                        }

                        // CumQty is the running total for the order, so the largest one seen is the
                        // filled quantity; summing Shares across reports would double count a
                        // corrected execution. An execution alone does NOT make the order terminal —
                        // a partial fill leaves it working — so terminality is left to the open /
                        // completed sources.
                        var filled = Math.Max(match?.FilledQuantity ?? 0m, args.Execution.CumQty);
                        match = OrderRefLookup.Match(source, args.Execution.OrderId, args.Execution.PermId,
                            isTerminal: false, filledQuantity: filled);
                    }
                    catch (Exception e)
                    {
                        handlerException = e;
                    }
                };
                EventHandler<IB.RequestEndEventArgs> onExecDetailsEnd = (sender, args) =>
                {
                    if (args.RequestId == requestId)
                    {
                        completed.Set();
                    }
                };

                _client.ExecutionDetails += onExecDetails;
                _client.ExecutionDetailsEnd += onExecDetailsEnd;
                try
                {
                    CheckRateLimiting();
                    _client.ClientSocket.reqExecutions(requestId, filter);

                    if (!completed.WaitOne(SourceTimeout))
                    {
                        // The exact defect this repository already shipped once on the execution path:
                        // a timeout that returns "nothing" reads downstream as "nothing happened".
                        return OrderRefLookup.Failed(source,
                            $"no execDetailsEnd within {SourceTimeout.TotalSeconds}s");
                    }
                }
                finally
                {
                    _client.ExecutionDetails -= onExecDetails;
                    _client.ExecutionDetailsEnd -= onExecDetailsEnd;
                }

                if (handlerException != null)
                {
                    return match ?? OrderRefLookup.Failed(source,
                        $"failed while reading an execution: {handlerException.Message}");
                }

                return match ?? OrderRefLookup.NoMatch(source);
            }
            catch (Exception ex)
            {
                return OrderRefLookup.Failed(source, ex.Message);
            }
        }

        /// <summary>
        /// Source 3: <c>reqCompletedOrders</c> — today's orders that are done, including the ones
        /// cancelled without ever trading, which neither of the other two sources can see.
        /// </summary>
        private OrderRefLookup LookupOrderRefInCompletedOrders(string clientOrderId)
        {
            const string source = "reqCompletedOrders";
            try
            {
                OrderRefLookup match = null;
                var completed = new ManualResetEvent(false);
                Exception handlerException = null;

                EventHandler<IB.CompletedOrderEventArgs> onCompletedOrder = (sender, args) =>
                {
                    try
                    {
                        if (match == null && args.Order != null && args.Order.OrderRef == clientOrderId)
                        {
                            // "Completed" is IB's own word for done — no status parsing needed, and
                            // the status string here ("Filled", "Cancelled", ...) only says which
                            // flavour of done.
                            match = OrderRefLookup.Match(source, args.Order.OrderId, args.Order.PermId,
                                isTerminal: true, filledQuantity: args.Order.FilledQuantity);
                        }
                    }
                    catch (Exception e)
                    {
                        handlerException = e;
                    }
                };
                EventHandler onCompletedOrdersEnd = (sender, args) => completed.Set();

                _client.CompletedOrder += onCompletedOrder;
                _client.CompletedOrdersEnd += onCompletedOrdersEnd;
                try
                {
                    CheckRateLimiting();

                    // apiOnly: false — manually placed TWS orders cannot carry our OrderRef, but the
                    // narrower request would also drop orders placed by a previous run's API client,
                    // which is exactly the population this query is about.
                    _client.ClientSocket.reqCompletedOrders(false);

                    if (!completed.WaitOne(SourceTimeout))
                    {
                        return OrderRefLookup.Failed(source,
                            $"no completedOrdersEnd within {SourceTimeout.TotalSeconds}s (note: this request needs " +
                            "TWS 9.71+ / API 973.07+; an older gateway simply never answers)");
                    }
                }
                finally
                {
                    _client.CompletedOrder -= onCompletedOrder;
                    _client.CompletedOrdersEnd -= onCompletedOrdersEnd;
                }

                if (handlerException != null)
                {
                    return match ?? OrderRefLookup.Failed(source,
                        $"failed while reading a completed order: {handlerException.Message}");
                }

                return match ?? OrderRefLookup.NoMatch(source);
            }
            catch (Exception ex)
            {
                return OrderRefLookup.Failed(source, ex.Message);
            }
        }

        private static bool IsTerminalOrderState(string status)
        {
            return !string.IsNullOrEmpty(status) && TerminalOrderStates.Contains(status);
        }

        /// <summary>
        /// What one of the three IB sources had to say about an OrderRef. Three states, matching the
        /// three the interface has to distinguish: FAILED (this source could not answer — it must
        /// never be read as an absence), matched, or answered-with-no-match.
        /// </summary>
        internal sealed class OrderRefLookup
        {
            /// <summary>Which IB request this came from. Appears verbatim in QueryFailed reasons.</summary>
            public string Source { get; private init; }

            /// <summary>Non-null exactly when this source could not answer.</summary>
            public string FailureReason { get; private init; }

            /// <summary>True when this source has an order under the OrderRef.</summary>
            public bool Matched { get; private init; }

            /// <summary>IB's order id, 0 when IB did not report one (completed orders from a previous session).</summary>
            public int IbOrderId { get; private init; }

            /// <summary>IB's permanent id — durable across sessions, but NOT usable as the ledger's exchange order id.</summary>
            public long PermId { get; private init; }

            /// <summary>True when this source says the order is done moving.</summary>
            public bool IsTerminal { get; private init; }

            /// <summary>Quantity filled as this source reports it.</summary>
            public decimal FilledQuantity { get; private init; }

            /// <summary>
            /// Whether this source produced an answer at all. A source that did not answer can never
            /// contribute to a NotFound.
            /// </summary>
            public bool Answered => FailureReason == null;

            private OrderRefLookup() { }

            public static OrderRefLookup Failed(string source, string reason) =>
                new() { Source = source, FailureReason = reason ?? "unspecified failure" };

            public static OrderRefLookup NoMatch(string source) =>
                new() { Source = source };

            public static OrderRefLookup Match(string source, int ibOrderId, long permId, bool isTerminal,
                decimal filledQuantity) =>
                new()
                {
                    Source = source,
                    Matched = true,
                    IbOrderId = ibOrderId,
                    PermId = permId,
                    IsTerminal = isTerminal,
                    FilledQuantity = filledQuantity
                };
        }
    }
}
