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
using QuantConnect.Logging;
using QuantConnect.Securities.UnifiedMargin;

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Data-plane writes. One IB connection serves several markets, and the data plane keys by
    /// <see cref="SecurityIdentifier.Market"/>, so every account-level signal is written once per
    /// registered market: the connection dying darkens all of them together.
    /// <para>Heartbeats, positions and account margin, no per-currency cash. Every <c>updatePortfolio</c> row IB pushes is
    /// written as a venue-reported position; <c>accountDownloadEnd</c> closes the batch and stamps
    /// <c>positions-snapshot</c> — but only when no contract in that batch failed to convert. That
    /// channel means "every holding is already written, absence from the list now means flat", so a
    /// batch missing a row must not carry it: it would turn the missing leg into a BALANCED backed
    /// by no venue evidence. The account values IB pushes alongside become the margin slot on that
    /// same boundary, so the buying-power reader gets IB's own available funds instead of a
    /// one-times-leverage guess. Per-currency cash is not written here (see
    /// <c>docs/superpowers/specs/2026-08-28-binance-ibkr-position-writes-design.md</c> and
    /// <c>docs/superpowers/specs/2026-08-28-binance-ibkr-margin-slot-design.md</c>).</para>
    /// </summary>
    public sealed partial class InteractiveBrokersBrokerage : IMultiMarketVenue
    {
        private const string BrokerTimeChannel = "broker-time";
        private const string PositionsSnapshotChannel = "positions-snapshot";

        // Standalone (not composed) IB runs are US-only in this fork; the composite overrides this.
        // volatile: written once by the composite on the wiring thread, read from the IB client thread
        // (StampBrokerTime and StampPositionsSnapshot). Whole-list atomic swap.
        private volatile IReadOnlyList<string> _venueMarkets = new[] { Market.USA };

        // Both only ever touched from the IB client thread, which delivers updatePortfolio and
        // accountDownloadEnd in order - so no lock. _sweptMarkets holds the markets whose positions
        // showed up in the batch now in flight; _sweepIncomplete records that at least one row of it
        // never made it into the data plane.
        private bool _sweepIncomplete;
        private readonly HashSet<string> _sweptMarkets = new();

        // Everything this connection has ever written a position for, and the subset of it seen
        // since the last stamp attempt. The difference is what has to be zeroed - see
        // ZeroOutSymbolsAbsentFromBatch.
        private readonly HashSet<Symbol> _writtenSymbols = new();
        private readonly HashSet<Symbol> _batchSymbols = new();

        private const string BaseCurrencyRow = "BASE";

        /// <summary>The five account values the margin slot is made of. Everything else IB pushes is ignored.</summary>
        private static readonly HashSet<string> MarginAccountValueKeys = new()
        {
            AccountValueKeys.NetLiquidation,
            AccountValueKeys.AvailableFunds,
            AccountValueKeys.ExcessLiquidity,
            AccountValueKeys.InitMarginReq,
            AccountValueKeys.MaintMarginReq
        };

        // Filled by updateAccountValue and drained by accountDownloadEnd, both on the IB client
        // thread and in order - same reason the sweep fields above take no lock. _marginCurrency is
        // the currency the accumulated rows are denominated in; see RecordAccountValue for why it is
        // not simply AccountBaseCurrency.
        private readonly Dictionary<string, decimal> _marginAccountValues = new();
        private string _marginCurrency;

        internal IReadOnlyList<string> VenueMarketsForTesting => _venueMarkets;

        /// <summary>
        /// Told by the composite which market keys this connection was registered under. Refuses an
        /// empty or blank list: an unusable registration is a wiring error, and a venue that quietly
        /// stamps nothing looks exactly like a venue that is dead.
        /// </summary>
        public void SetVenueMarkets(IReadOnlyList<string> markets)
        {
            if (markets == null || markets.Count == 0 || markets.Any(string.IsNullOrWhiteSpace))
            {
                throw new ArgumentException("A multi-market venue needs at least one non-empty market key.", nameof(markets));
            }

            _venueMarkets = markets.Select(market => market.ToLowerInvariant()).Distinct().ToList();
        }

        /// <summary>reqCurrentTime answered: the only periodic liveness probe IB gives us (every 2 minutes).</summary>
        internal void StampBrokerTime()
        {
            foreach (var market in _venueMarkets)
            {
                BrokerageDataService.Instance.RecordChannelHeartbeat(market, BrokerTimeChannel);
            }
        }

        /// <summary>
        /// One <c>updatePortfolio</c> row, as the venue reports it. IB pushes a row for every
        /// position it holds and a zero-quantity row for one it just closed, so writing each row
        /// verbatim is what makes the snapshot stamp truthful.
        /// </summary>
        /// <param name="symbol">The Lean symbol the contract mapped to; its market is the data-plane key.</param>
        /// <param name="quantity">Signed position size, IB's own unit.</param>
        /// <param name="averagePrice">Average cost per unit, already divided by the contract multiplier.</param>
        internal void RecordVenuePosition(Symbol symbol, decimal quantity, decimal averagePrice)
        {
            BrokerageDataService.Instance.UpdateSecurityPosition(symbol, new BrokerageDataService.SecurityPositionData
            {
                Quantity = quantity,
                AveragePrice = averagePrice,
                ChangeReason = BrokerageDataService.PositionChangeReason.Snapshot,
                VenueTimeUtc = DateTime.UtcNow
            });

            _sweptMarkets.Add(symbol.ID.Market);
            _writtenSymbols.Add(symbol);
            _batchSymbols.Add(symbol);
        }

        /// <summary>
        /// A row of the batch in flight never reached the data plane, so the batch can no longer
        /// endorse "absence means flat". Voids the stamp for this batch only.
        /// </summary>
        /// <remarks>
        /// Called for every failing row, logged at trace: the error-level line naming the contract is
        /// de-duplicated by the caller, and <see cref="StampPositionsSnapshot"/> reports the withheld
        /// stamp once per batch. Repeating it per row would only bury both.
        /// </remarks>
        internal void MarkSweepIncomplete(string why)
        {
            _sweepIncomplete = true;
            Log.Trace($"InteractiveBrokersBrokerage.MarkSweepIncomplete(): {PositionsSnapshotChannel} withheld for this batch: {why}");
        }

        /// <summary>
        /// <c>accountDownloadEnd</c>: the batch is complete. Stamps every registered market plus any
        /// market that reported a position in this batch - a registered market with no positions
        /// still needs the stamp, because that is exactly the case the reader has to read as flat.
        /// </summary>
        internal void StampPositionsSnapshot()
        {
            if (_sweepIncomplete)
            {
                Log.Error($"InteractiveBrokersBrokerage.StampPositionsSnapshot(): batch incomplete, not stamping {PositionsSnapshotChannel}.");
            }
            else
            {
                ZeroOutSymbolsAbsentFromBatch();

                foreach (var market in _venueMarkets.Concat(_sweptMarkets).Distinct())
                {
                    BrokerageDataService.Instance.RecordChannelHeartbeat(market, PositionsSnapshotChannel);
                }
            }

            // All three belong to the batch that just ended, never to the next one.
            _sweepIncomplete = false;
            _sweptMarkets.Clear();
            _batchSymbols.Clear();
        }

        /// <summary>
        /// One <c>updateAccountValue</c> row. Keeps the five keys the margin slot needs, denominated
        /// in the account's base currency, and drops everything else.
        /// </summary>
        /// <remarks>
        /// IB pushes each key once per currency the account holds, plus a <c>BASE</c> summary row;
        /// mixing those would add up to a number denominated in nothing. The base currency itself is
        /// only learned from the first <c>ExchangeRate == 1</c> row, which can arrive after these, so
        /// until it does the first non-<c>BASE</c> currency seen is accepted and everything else in
        /// that batch is measured against it. If the real base currency then turns out to be another
        /// one, the rows accumulated under the guess are dropped rather than blended.
        /// </remarks>
        internal void RecordAccountValue(string key, string value, string currency)
        {
            if (!MarginAccountValueKeys.Contains(key) || string.IsNullOrEmpty(currency) || currency == BaseCurrencyRow)
            {
                return;
            }

            var baseCurrencyKnown = !string.IsNullOrEmpty(AccountBaseCurrency);
            var expected = baseCurrencyKnown ? AccountBaseCurrency : _marginCurrency ?? currency;
            if (currency != expected)
            {
                return;
            }

            if (_marginCurrency != expected)
            {
                if (_marginCurrency != null)
                {
                    Log.Trace($"InteractiveBrokersBrokerage.RecordAccountValue(): account base currency is {expected}, dropping margin rows accumulated under {_marginCurrency}.");
                    _marginAccountValues.Clear();
                }
                else if (!baseCurrencyKnown)
                {
                    Log.Trace($"InteractiveBrokersBrokerage.RecordAccountValue(): account base currency not known yet, taking margin rows in {currency}.");
                }

                _marginCurrency = expected;
            }

            if (!decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
            {
                // Dropped, never defaulted to zero: a zero AvailableMargin reads as "no buying power",
                // and a zero TotalEquity reads as an empty account. Absence is the honest answer.
                Log.Error($"InteractiveBrokersBrokerage.RecordAccountValue(): {key} ({currency}) value '{value}' is not a number, ignoring this key.");
                return;
            }

            _marginAccountValues[key] = parsed;
        }

        /// <summary>
        /// <c>accountDownloadEnd</c>: writes the accumulated account values into the data plane's
        /// margin slot, once per registered market. Needs both <c>NetLiquidation</c> and
        /// <c>AvailableFunds</c> - equity without available margin is not a slot the buying-power
        /// reader can use, and a partial one written anyway would look exactly like a complete one.
        /// </summary>
        /// <remarks>
        /// The accumulated values are deliberately NOT cleared: IB streams incremental
        /// <c>updateAccountValue</c> rows and only re-pushes what changed, so clearing would make
        /// every later batch look incomplete. The next value for a key overwrites the last.
        /// Rate fields and <c>TotalLiability</c> stay at their defaults - IB does not report them,
        /// and deriving them here would put a made-up number where a reported one belongs.
        /// </remarks>
        internal void WriteAccountMargin()
        {
            if (!_marginAccountValues.TryGetValue(AccountValueKeys.NetLiquidation, out var netLiquidation) ||
                !_marginAccountValues.TryGetValue(AccountValueKeys.AvailableFunds, out var availableFunds))
            {
                Log.Trace("InteractiveBrokersBrokerage.WriteAccountMargin(): NetLiquidation and/or AvailableFunds missing, margin not written.");
                return;
            }

            var margin = new BrokerageDataService.AccountMarginData
            {
                TotalEquity = netLiquidation,
                AvailableMargin = availableFunds,
                MarginBalance = ValueOrDefault(AccountValueKeys.ExcessLiquidity),
                InitialMarginUsed = ValueOrDefault(AccountValueKeys.InitMarginReq),
                MaintenanceMarginUsed = ValueOrDefault(AccountValueKeys.MaintMarginReq),
                // One IB account backs every registered market, so the copies below are the SAME
                // pool. The pool id lets a consumer that enumerates venues (monitoring) merge them
                // instead of double-counting; per-market readers ignore it.
                MarginPool = "ibkr",
                LastUpdated = DateTime.UtcNow
            };

            foreach (var market in _venueMarkets)
            {
                BrokerageDataService.Instance.UpdateMargin(market, margin);
            }
        }

        private decimal ValueOrDefault(string key)
        {
            return _marginAccountValues.TryGetValue(key, out var value) ? value : default;
        }

        /// <summary>
        /// A position closed while we were away leaves its last non-zero row sitting in the data
        /// plane: the full download that follows a reconnect carries only positions still open, so
        /// nothing overwrites it, and the stamp would then endorse stale evidence as a complete
        /// picture. Anything we have written before and did not hear about in this batch is
        /// therefore written flat before the stamp goes on.
        /// </summary>
        /// <remarks>
        /// Only safe on a complete batch: a batch missing a row cannot prove absence, so an
        /// incomplete one zeroes nothing.
        ///
        /// After the initial download IB keeps streaming incremental <c>updatePortfolio</c> rows, so
        /// this window is "everything since the last stamp", not just the download itself. That is
        /// the right window rather than a loose one: a symbol that moved at all since the last stamp
        /// is by definition in it, so what the difference selects is symbols absent from the whole
        /// window - and every such symbol was necessarily absent from the full download that ends it.
        /// </remarks>
        private void ZeroOutSymbolsAbsentFromBatch()
        {
            foreach (var symbol in _writtenSymbols.Where(symbol => !_batchSymbols.Contains(symbol)).ToList())
            {
                BrokerageDataService.Instance.UpdateSecurityPosition(symbol, new BrokerageDataService.SecurityPositionData
                {
                    Quantity = 0m,
                    AveragePrice = 0m,
                    ChangeReason = BrokerageDataService.PositionChangeReason.Snapshot,
                    VenueTimeUtc = DateTime.UtcNow
                });

                // Written flat once, and then forgotten: a symbol IB no longer reports must not be
                // re-zeroed on every later batch, which would keep refreshing its LastUpdated and
                // dress a long-dead row up as fresh venue evidence.
                _writtenSymbols.Remove(symbol);

                Log.Trace($"InteractiveBrokersBrokerage.ZeroOutSymbolsAbsentFromBatch(): {symbol} absent from this batch, written flat.");
            }
        }
    }
}
