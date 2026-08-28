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
    /// </summary>
    public sealed partial class InteractiveBrokersBrokerage : IMultiMarketVenue
    {
        private const string BrokerTimeChannel = "broker-time";
        private const string PositionsSnapshotChannel = "positions-snapshot";

        // Standalone (not composed) IB runs are US-only in this fork; the composite overrides this.
        private IReadOnlyList<string> _venueMarkets = new[] { Market.USA };

        // Set by DownloadAccount: true only when AccountDownloadEnd arrived inside its window, no
        // holding failed to convert, AND this download actually filled the holdings dictionary.
        // Anything else means the list may be partial, and a partial list must not vouch for absence.
        private volatile bool _accountSweepComplete;

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

        /// <summary>
        /// Records whether the account download that just finished may vouch for the holdings list.
        /// Three conditions, all necessary:
        /// the download ended on its own (<paramref name="downloadSucceeded"/>); nothing failed to
        /// convert; and <c>_loadExistingHoldings</c> is still set, because that flag gates the only
        /// writer of <c>_accountData.AccountHoldings</c> (HandlePortfolioUpdates). After the first
        /// <see cref="GetAccountHoldings"/> clears it, a reconnect's download - which starts from an
        /// <c>_accountData</c> that Connect() cleared - refills nothing, and a vouch for an empty
        /// dictionary is exactly the "everything is flat" falsehood this channel exists to prevent.
        /// </summary>
        private bool MarkAccountSweepComplete(bool downloadSucceeded)
        {
            _accountSweepComplete = downloadSucceeded
                && _accountHoldingsLastException == null
                && _loadExistingHoldings;
            return _accountSweepComplete;
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
        /// Vouches "I swept the whole account" under every registered market - but only if the sweep
        /// really completed. Called from <see cref="GetAccountHoldings"/>.
        /// </summary>
        internal void StampPositionsSnapshotIfSweepComplete()
        {
            // _accountHoldingsLastException is re-checked here, not just at the end of the download:
            // a contract that fails to convert during the continuous portfolio updates drops a
            // holding out of the list long after DownloadAccount returned true.
            if (!_accountSweepComplete || _accountHoldingsLastException != null)
            {
                Logging.Log.Error("InteractiveBrokersBrokerage.StampPositionsSnapshotIfSweepComplete(): account download did not complete cleanly, positions-snapshot not stamped - leg reconciliation stays unverified until a clean sweep");
                return;
            }

            foreach (var market in _venueMarkets)
            {
                BrokerageDataService.Instance.RecordChannelHeartbeat(market, PositionsSnapshotChannel);
            }

            // One stamp per clean download. GetAccountHoldings is called repeatedly, but only the
            // first call reads a dictionary this download filled - it then clears
            // _loadExistingHoldings and the dictionary freezes. A second stamp would report a fresh
            // sweep that never happened.
            _accountSweepComplete = false;
        }

        /// <summary>Drives the real three-way condition of <see cref="MarkAccountSweepComplete"/>
        /// without a gateway: <paramref name="downloadSucceeded"/> stands in for DownloadAccount's
        /// result, the other two conditions are read from the live fields.</summary>
        internal bool MarkAccountSweepForTesting(bool downloadSucceeded) => MarkAccountSweepComplete(downloadSucceeded);

        internal void MarkLoadExistingHoldingsForTesting(bool loadExistingHoldings) => _loadExistingHoldings = loadExistingHoldings;

        internal void MarkAccountHoldingsExceptionForTesting(Exception exception) => _accountHoldingsLastException = exception;
    }
}
