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
using System.Linq;
using QuantConnect.Logging;
using QuantConnect.Securities.UnifiedMargin;

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Data-plane writes. One IB connection serves several markets, and the data plane keys by
    /// <see cref="SecurityIdentifier.Market"/>, so every account-level signal is written once per
    /// registered market: the connection dying darkens all of them together.
    /// <para>Heartbeats and positions, no balances. Every <c>updatePortfolio</c> row IB pushes is
    /// written as a venue-reported position; <c>accountDownloadEnd</c> closes the batch and stamps
    /// <c>positions-snapshot</c> — but only when no contract in that batch failed to convert. That
    /// channel means "every holding is already written, absence from the list now means flat", so a
    /// batch missing a row must not carry it: it would turn the missing leg into a BALANCED backed
    /// by no venue evidence. Per-currency cash and margin are not written here (see
    /// <c>docs/superpowers/specs/2026-08-28-binance-ibkr-position-writes-design.md</c>).</para>
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
        }

        /// <summary>
        /// A row of the batch in flight never reached the data plane, so the batch can no longer
        /// endorse "absence means flat". Voids the stamp for this batch only.
        /// </summary>
        internal void MarkSweepIncomplete(string why)
        {
            _sweepIncomplete = true;
            Log.Error($"InteractiveBrokersBrokerage.MarkSweepIncomplete(): {PositionsSnapshotChannel} withheld for this batch: {why}");
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
                foreach (var market in _venueMarkets.Concat(_sweptMarkets).Distinct())
                {
                    BrokerageDataService.Instance.RecordChannelHeartbeat(market, PositionsSnapshotChannel);
                }
            }

            // Both flags belong to the batch that just ended, never to the next one.
            _sweepIncomplete = false;
            _sweptMarkets.Clear();
        }
    }
}
