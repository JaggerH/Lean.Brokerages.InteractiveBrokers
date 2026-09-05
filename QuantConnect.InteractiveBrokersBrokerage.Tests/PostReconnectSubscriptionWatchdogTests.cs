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
using System.Threading;
using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Securities.UnifiedMargin;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// The one-shot resubscribe after a 1102 that lied. Gateway evidence: a re-login under a
    /// competing session desubscribes all farm market data, then reports 1102 "data maintained";
    /// nothing ticks again until the next full connect. No gateway is involved here - the pure
    /// class is driven with explicit clocks, and the brokerage wiring is driven through the
    /// internal error / tick handlers on a parameterless instance.
    /// </summary>
    [TestFixture]
    [NonParallelizable] // BrokerageDataService.Instance is process-wide state these tests reset.
    public class PostReconnectSubscriptionWatchdogTests
    {
        private static readonly Symbol Nvda = Symbol.Create("NVDA", SecurityType.Equity, Market.USA);
        private static readonly Symbol Msft = Symbol.Create("MSFT", SecurityType.Equity, Market.USA);
        private static readonly Symbol EurUsd = Symbol.Create("EURUSD", SecurityType.Forex, Market.Oanda);
        private static readonly TimeSpan Never = TimeSpan.FromHours(1); // timer must not fire on its own in these tests

        [SetUp]
        public void Reset() => BrokerageDataService.Reset();

        private static PostReconnectSubscriptionWatchdog Make(List<Symbol> subscribed, out Func<int> restores, TimeSpan? timeout = null)
        {
            var count = 0;
            restores = () => count;
            return new PostReconnectSubscriptionWatchdog(timeout ?? Never, () => subscribed, () => count++);
        }

        [Test]
        public void NotArmedUntil1102_CheckIsNoOp()
        {
            using var watchdog = Make(new List<Symbol> { Nvda }, out var restores);
            watchdog.Check(DateTime.UtcNow);
            Assert.IsFalse(watchdog.IsArmed);
            Assert.AreEqual(0, restores());
        }

        [Test]
        public void SilenceAfter1102_ResubscribesOnce()
        {
            using var watchdog = Make(new List<Symbol> { Nvda, Msft }, out var restores);
            var t0 = new DateTime(2026, 9, 4, 21, 6, 38, DateTimeKind.Utc);

            watchdog.RecordTick(Nvda, t0.AddSeconds(-16)); // last tick before the reconnect
            watchdog.OnReconnectedDataMaintained(t0);
            Assert.IsTrue(watchdog.IsArmed);

            watchdog.Check(t0.AddSeconds(120));
            Assert.AreEqual(1, restores());
            Assert.IsFalse(watchdog.IsArmed);

            // idempotent: the timer path and the 2108 path cannot double-fire for the same 1102
            watchdog.Check(t0.AddSeconds(121));
            watchdog.OnFarmInactive(t0.AddSeconds(141));
            Assert.AreEqual(1, restores());
        }

        [Test]
        public void AnySubscribedSymbolTicking_ConfirmsDataMaintained()
        {
            using var watchdog = Make(new List<Symbol> { Nvda, Msft }, out var restores);
            var t0 = DateTime.UtcNow;

            watchdog.OnReconnectedDataMaintained(t0);
            watchdog.RecordTick(Msft, t0.AddSeconds(5)); // one symbol is enough - a dropped set drops all

            watchdog.Check(t0.AddSeconds(120));
            Assert.AreEqual(0, restores());
            Assert.IsFalse(watchdog.IsArmed, "a confirmed-alive 1102 is settled, not left armed");
        }

        [Test]
        public void TickBeforeThe1102_DoesNotCount()
        {
            using var watchdog = Make(new List<Symbol> { Nvda }, out var restores);
            var t0 = DateTime.UtcNow;

            watchdog.RecordTick(Nvda, t0.AddSeconds(-1));
            watchdog.OnReconnectedDataMaintained(t0);

            watchdog.Check(t0.AddSeconds(120));
            Assert.AreEqual(1, restores());
        }

        [Test]
        public void FarmInactive2108WithinWindow_ResubscribesEarly()
        {
            using var watchdog = Make(new List<Symbol> { Nvda }, out var restores);
            var t0 = DateTime.UtcNow;

            watchdog.OnReconnectedDataMaintained(t0);
            watchdog.OnFarmInactive(t0.AddSeconds(30)); // before the silence timeout would have elapsed

            Assert.AreEqual(1, restores());
            Assert.IsFalse(watchdog.IsArmed);
        }

        [Test]
        public void FarmInactive2108_OutsideWindowOrWhileTicking_IsIgnored()
        {
            using var watchdog = Make(new List<Symbol> { Nvda }, out var restores);
            var t0 = DateTime.UtcNow;

            // 2108 also appears in healthy reconnects: outside the window it says nothing about this 1102
            watchdog.OnReconnectedDataMaintained(t0);
            watchdog.OnFarmInactive(t0 + PostReconnectSubscriptionWatchdog.FarmInactiveWindow + TimeSpan.FromSeconds(1));
            Assert.AreEqual(0, restores());
            Assert.IsTrue(watchdog.IsArmed, "an out-of-window 2108 neither fires nor settles the check");

            // and while the symbol is ticking, an in-window 2108 is some other farm's business
            watchdog.RecordTick(Nvda, t0.AddSeconds(10));
            watchdog.OnFarmInactive(t0.AddSeconds(20));
            Assert.AreEqual(0, restores());
            Assert.IsFalse(watchdog.IsArmed);
        }

        [Test]
        public void FarmInactive2108WithoutA1102_IsIgnored()
        {
            using var watchdog = Make(new List<Symbol> { Nvda }, out var restores);
            watchdog.OnFarmInactive(DateTime.UtcNow);
            Assert.AreEqual(0, restores());
        }

        [Test]
        public void Disarm_CancelsThePendingCheck()
        {
            using var watchdog = Make(new List<Symbol> { Nvda }, out var restores);
            var t0 = DateTime.UtcNow;

            watchdog.OnReconnectedDataMaintained(t0);
            watchdog.Disarm(); // a 1100 or 1101 followed
            watchdog.Check(t0.AddSeconds(120));

            Assert.AreEqual(0, restores());
        }

        [Test]
        public void NothingSubscribed_NothingToRestore()
        {
            using var watchdog = Make(new List<Symbol>(), out var restores);
            var t0 = DateTime.UtcNow;

            watchdog.OnReconnectedDataMaintained(t0);
            watchdog.Check(t0.AddSeconds(120));

            Assert.AreEqual(0, restores());
            Assert.IsFalse(watchdog.IsArmed);
        }

        [Test]
        public void ResubscribeThrowing_DoesNotEscapeAndStillCountsAsFired()
        {
            var calls = 0;
            using var watchdog = new PostReconnectSubscriptionWatchdog(Never, () => new[] { Nvda }, () => { calls++; throw new InvalidOperationException("socket gone"); });
            var t0 = DateTime.UtcNow;

            watchdog.OnReconnectedDataMaintained(t0);
            Assert.DoesNotThrow(() => watchdog.Check(t0.AddSeconds(120)));
            Assert.AreEqual(1, calls);
            Assert.AreEqual(1, watchdog.RestoreCount);
        }

        [Test]
        public void TimerFiresOnItsOwnAfterTheSilenceTimeout()
        {
            using var watchdog = Make(new List<Symbol> { Nvda }, out var restores, TimeSpan.FromMilliseconds(50));
            watchdog.OnReconnectedDataMaintained(DateTime.UtcNow);

            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (restores() == 0 && DateTime.UtcNow < deadline)
            {
                Thread.Sleep(10);
            }
            Assert.AreEqual(1, restores(), "the timer callback must drive Check without anyone calling it");
        }

        [Test]
        public void NonPositiveTimeout_IsRefused()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new PostReconnectSubscriptionWatchdog(TimeSpan.Zero, () => Array.Empty<Symbol>(), () => { }));
        }

        // ---- brokerage wiring: HandleError / HandleTickPrice / HandleTickSize on a gateway-less instance ----

        private static IB.ErrorEventArgs GatewayMessage(int code, string text) => new IB.ErrorEventArgs(id: -1, time: 0, code: code, message: text);

        [Test]
        public void Brokerage_1102ArmsTheWatchdog_AndA1101RestoreSettlesIt()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            brokerage.RegisterSubscriptionForTesting(1, Nvda);

            brokerage.HandleError(this, GatewayMessage(1102, "Connectivity between IB and Trader Workstation has been restored - data maintained."));
            Assert.IsTrue(brokerage.ReconnectWatchdog.IsArmed);

            // a later 1101 resubscribes outright, which supersedes the pending check
            brokerage.HandleError(this, GatewayMessage(1101, "Connectivity between IB and Trader Workstation has been restored - data lost."));
            Assert.IsFalse(brokerage.ReconnectWatchdog.IsArmed);
            Assert.AreEqual(0, brokerage.ReconnectWatchdog.RestoreCount, "the 1101 restore is the brokerage's own, not the watchdog's");
        }

        [Test]
        public void Brokerage_1101IsNotArmed_ItRestoresOutright()
        {
            // the documented "data lost" code already resubscribes; the watchdog is for the other one
            var brokerage = new InteractiveBrokersBrokerage();
            brokerage.HandleError(this, GatewayMessage(1101, "Connectivity between IB and Trader Workstation has been restored - data lost."));
            Assert.IsFalse(brokerage.ReconnectWatchdog.IsArmed);
        }

        [Test]
        public void Brokerage_2108AfterA1102WithNoTicks_Resubscribes()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            brokerage.RegisterSubscriptionForTesting(1, Nvda);

            brokerage.HandleError(this, GatewayMessage(1102, "restored - data maintained"));
            brokerage.HandleError(this, GatewayMessage(2108, "Market data farm connection is inactive but should be available upon demand.usfarm"));

            Assert.AreEqual(1, brokerage.ReconnectWatchdog.RestoreCount);
            Assert.IsFalse(brokerage.ReconnectWatchdog.IsArmed);
        }

        [Test]
        public void Brokerage_2108WithoutA1102_DoesNothing()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            brokerage.RegisterSubscriptionForTesting(1, Nvda);

            brokerage.HandleError(this, GatewayMessage(2108, "Market data farm connection is inactive but should be available upon demand.usfarm"));

            Assert.AreEqual(0, brokerage.ReconnectWatchdog.RestoreCount);
        }

        [Test]
        public void Brokerage_PriceMessageAfter1102_CountsAsATick()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            brokerage.RegisterSubscriptionForTesting(7, Nvda);

            brokerage.HandleError(this, GatewayMessage(1102, "restored - data maintained"));
            var armedAt = DateTime.UtcNow;
            brokerage.HandleTickPrice(this, new IB.TickPriceEventArgs(7, IBApi.TickType.BID, 170.25, new TickAttrib()));

            var last = brokerage.ReconnectWatchdog.LastTickUtc(Nvda);
            Assert.IsNotNull(last);
            Assert.GreaterOrEqual(last.Value, armedAt.AddSeconds(-1));

            brokerage.ReconnectWatchdog.Check(DateTime.UtcNow.AddMinutes(10));
            Assert.AreEqual(0, brokerage.ReconnectWatchdog.RestoreCount);
        }

        [Test]
        public void Brokerage_UnknownTickerId_IsNotATick()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            brokerage.RegisterSubscriptionForTesting(7, Nvda);

            brokerage.HandleTickPrice(this, new IB.TickPriceEventArgs(99, IBApi.TickType.BID, 170.25, new TickAttrib()));

            Assert.IsNull(brokerage.ReconnectWatchdog.LastTickUtc(Nvda));
            CollectionAssert.IsEmpty(BrokerageDataService.Instance.GetMarketDataFeeds());
        }

        [Test]
        public void Brokerage_ForexPriceMessage_StampsFeedLivenessEvenIfNoSizeEverValidates()
        {
            // A cash leg whose size messages never pass Tick.IsValid() is emitted to nobody,
            // yet the farm is serving it. The feed registry must say so, or the adjudicator
            // cannot tell "never listed" from "alive" for that leg.
            var brokerage = new InteractiveBrokersBrokerage();
            brokerage.RegisterSubscriptionForTesting(3, EurUsd);

            brokerage.HandleTickPrice(this, new IB.TickPriceEventArgs(3, IBApi.TickType.BID, 1.1052, new TickAttrib()));
            brokerage.HandleTickSize(this, new IB.TickSizeEventArgs(3, IBApi.TickType.BID_SIZE, -1)); // "no size available"

            var feed = BrokerageDataService.Instance.GetMarketDataFeeds().SingleOrDefault(f => f.Symbol == EurUsd);
            Assert.IsNotNull(feed, "forex leg must appear in market_data_feeds off the price message alone");
        }

        [Test]
        public void Brokerage_EquityPriceThenSize_StampsOneFeedEntryNotTwo()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            brokerage.RegisterSubscriptionForTesting(5, Msft);

            brokerage.HandleTickPrice(this, new IB.TickPriceEventArgs(5, IBApi.TickType.BID, 410.10, new TickAttrib()));
            brokerage.HandleTickPrice(this, new IB.TickPriceEventArgs(5, IBApi.TickType.ASK, 410.12, new TickAttrib()));
            // the size message would emit the tick to the aggregator, which is null on a gateway-less
            // instance, so stop at the price messages: the registry is keyed by symbol and stamps a
            // timestamp, so two hooks refresh one entry rather than producing two.
            var feeds = BrokerageDataService.Instance.GetMarketDataFeeds().Where(f => f.Symbol == Msft).ToList();
            Assert.AreEqual(1, feeds.Count);
        }
    }
}
