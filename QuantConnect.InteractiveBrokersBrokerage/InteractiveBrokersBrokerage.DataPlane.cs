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
using QuantConnect.Securities.UnifiedMargin;

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Data-plane writes. One IB connection serves several markets, and the data plane keys by
    /// <see cref="SecurityIdentifier.Market"/>, so every account-level signal is written once per
    /// registered market: the connection dying darkens all of them together.
    /// <para>Liveness only. This brokerage never writes a position or a balance into the data plane,
    /// so it must not stamp <c>positions-snapshot</c> either: that channel means "every holding is
    /// already written, absence from the list now means flat", and stamping it without the writes
    /// would turn every open IB leg into a false urgent alarm and every flat one into a BALANCED
    /// backed by no venue evidence. Leg reconciliation for an IB leg stays 没验到 until the sweep
    /// itself writes the rows.</para>
    /// </summary>
    public sealed partial class InteractiveBrokersBrokerage : IMultiMarketVenue
    {
        private const string BrokerTimeChannel = "broker-time";

        // Standalone (not composed) IB runs are US-only in this fork; the composite overrides this.
        // volatile: written once by the composite on the wiring thread, read from the IB client thread
        // (HandleBrokerTime) and the algorithm thread (GetAccountHoldings). Whole-list atomic swap.
        private volatile IReadOnlyList<string> _venueMarkets = new[] { Market.USA };

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
    }
}
