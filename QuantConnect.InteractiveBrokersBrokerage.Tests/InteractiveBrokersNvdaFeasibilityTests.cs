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
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using QuantConnect.Algorithm;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Securities;
using QuantConnect.Util;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Can the IBKR leg of the cross-venue pair actually run? Answers the two questions that
    /// nothing else in this repo answers: does the paper account produce a NVDA quote at all,
    /// and does the account report itself as ready to trade.
    /// </summary>
    /// <remarks>
    /// Explicit and hand-run: it launches IB Gateway and talks to the live paper account, so it
    /// belongs nowhere near an automated suite. What it is guarding against is starting a long
    /// paper run whose IBKR leg is silently dataless — a subscription that never ticks looks
    /// exactly like a quiet market, and the strategy would simply never trade while appearing
    /// healthy.
    /// </remarks>
    [TestFixture, Explicit("Launches IB Gateway and connects to the live paper account.")]
    public class InteractiveBrokersNvdaFeasibilityTests
    {
        private static readonly Symbol Nvda = Symbol.Create("NVDA", SecurityType.Equity, Market.USA);

        [Test]
        public void ConnectsAndReportsAccountState()
        {
            using var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider());
            ib.Connect();

            Assert.IsTrue(ib.IsConnected, "brokerage reported itself not connected after Connect()");

            var balances = ib.GetCashBalance();
            Console.WriteLine($"cash balances: {balances.Count}");
            foreach (var balance in balances)
            {
                Console.WriteLine($"  {balance.Currency} {balance.Amount}");
            }

            var holdings = ib.GetAccountHoldings();
            Console.WriteLine($"holdings: {holdings.Count}");
            foreach (var holding in holdings)
            {
                Console.WriteLine($"  {holding.Symbol.Value} qty={holding.Quantity} avg={holding.AveragePrice}");
            }

            Assert.IsNotEmpty(balances, "paper account reported no cash balance at all");
        }

        [Test]
        public void ProducesNvdaQuotes()
        {
            using var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(), new SecurityProvider());
            ib.Connect();
            Assert.IsTrue(ib.IsConnected, "brokerage reported itself not connected after Connect()");

            var received = new List<BaseData>();
            var cancellation = new CancellationTokenSource();

            var entry = MarketHoursDatabase.FromDataFolder().GetEntry(Nvda.ID.Market, Nvda, Nvda.SecurityType);
            var config = new SubscriptionDataConfig(typeof(Tick), Nvda, Resolution.Tick,
                entry.DataTimeZone, entry.ExchangeHours.TimeZone, true, true, false);

            var enumerator = ib.Subscribe(config, (s, e) => { });
            Assert.IsNotNull(enumerator, "Subscribe() returned no enumerator for NVDA");

            Task.Run(() =>
            {
                while (!cancellation.IsCancellationRequested && enumerator.MoveNext())
                {
                    var data = enumerator.Current;
                    if (data == null)
                    {
                        continue;
                    }
                    lock (received)
                    {
                        received.Add(data);
                    }
                }
            }, cancellation.Token);

            // Generous: an unsubscribed account fails fast with an error, so this window is about
            // giving a working subscription time to tick, not about waiting out a failure.
            Thread.Sleep(TimeSpan.FromSeconds(30));
            cancellation.Cancel();

            int count;
            lock (received)
            {
                count = received.Count;
                Console.WriteLine($"NVDA data points in 30s: {count}");
                for (var i = 0; i < Math.Min(5, received.Count); i++)
                {
                    var d = received[i];
                    Console.WriteLine($"  {d.GetType().Name} {d.EndTime:HH:mm:ss.fff} price={d.Price}");
                }
            }

            // The whole point of the probe: say which of the two it was, rather than a bare failure.
            Assert.Greater(count, 0,
                "no NVDA data arrived in 30s. Either the market is closed (check the clock against " +
                "09:30-16:00 ET) or this paper account has no US equity market data entitlement — " +
                "'ib-enable-delayed-streaming-data' is currently true, so delayed data should have " +
                "arrived if any entitlement exists at all.");
        }
    }
}
