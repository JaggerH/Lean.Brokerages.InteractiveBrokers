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

using System.Collections.Generic;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Securities.UnifiedMargin;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Data-plane writes that need no gateway: which market keys the <c>broker-time</c> heartbeat
    /// and the <c>positions-snapshot</c> stamp land under, that an unusable market registration is
    /// refused outright, and that a sweep which failed to convert a contract stamps nothing — the
    /// stamp means "every holding is written, absence now means flat", so a half-written sweep must
    /// not carry it.
    /// </summary>
    [TestFixture]
    [NonParallelizable] // BrokerageDataService.Instance is process-wide state these tests reset.
    public class InteractiveBrokersDataPlaneTests
    {
        [SetUp]
        public void Reset() => BrokerageDataService.Reset();

        [Test]
        public void DefaultsToUsaWhenNobodyInjectsMarkets()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            CollectionAssert.AreEqual(new[] { Market.USA }, brokerage.VenueMarketsForTesting);

            brokerage.StampBrokerTime();
            Assert.IsTrue(BrokerageDataService.Instance.TryGetChannelHeartbeat(Market.USA, "broker-time", out _));
        }

        [Test]
        public void BrokerTimeIsStampedUnderEveryRegisteredMarket()
        {
            // 一条连接横跨两个市场：断了一起哑，所以活着也得一起报。
            var brokerage = new InteractiveBrokersBrokerage();
            ((IMultiMarketVenue)brokerage).SetVenueMarkets(new List<string> { "usa", "krx" });

            brokerage.StampBrokerTime();

            Assert.IsTrue(BrokerageDataService.Instance.TryGetChannelHeartbeat("usa", "broker-time", out _));
            Assert.IsTrue(BrokerageDataService.Instance.TryGetChannelHeartbeat("krx", "broker-time", out _));
        }

        [Test]
        public void MarketKeysAreLowerCasedAndDeduplicated()
        {
            // 复合层传下来的键理应已小写，但数据面按键匹配，规范化只能由写入方兜底。
            var brokerage = new InteractiveBrokersBrokerage();
            ((IMultiMarketVenue)brokerage).SetVenueMarkets(new List<string> { "USA", "usa", "KRX" });

            CollectionAssert.AreEqual(new[] { "usa", "krx" }, brokerage.VenueMarketsForTesting);
        }

        [Test]
        public void EmptyMarketListIsRefused()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            Assert.Throws<System.ArgumentException>(
                () => ((IMultiMarketVenue)brokerage).SetVenueMarkets(new List<string>()));
        }

        [Test]
        public void NullOrBlankMarketKeyIsRefused()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            Assert.Throws<System.ArgumentException>(
                () => ((IMultiMarketVenue)brokerage).SetVenueMarkets(null));
            Assert.Throws<System.ArgumentException>(
                () => ((IMultiMarketVenue)brokerage).SetVenueMarkets(new List<string> { "usa", " " }));

            // 被拒之后不许留下半份状态：默认市场仍在。
            CollectionAssert.AreEqual(new[] { Market.USA }, brokerage.VenueMarketsForTesting);
        }

        [Test]
        public void RecordVenuePositionFilesUnderTheSymbolsOwnMarket()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            var symbol = Symbol.Create("SPY", SecurityType.Equity, Market.USA);

            brokerage.RecordVenuePosition(symbol, 10m, 400m);

            Assert.IsTrue(BrokerageDataService.Instance.TryGetSecurityPosition(symbol, out var position));
            Assert.AreEqual(10m, position.Quantity);
            Assert.AreEqual(400m, position.AveragePrice);
        }

        [Test]
        public void StampCoversRegisteredMarketsAndMarketsSeenThisSweep()
        {
            // 注册的市场即使这一轮一条持仓都没有也要盖——「没报过」这时才读得成「平的」。
            var brokerage = new InteractiveBrokersBrokerage();
            ((IMultiMarketVenue)brokerage).SetVenueMarkets(new List<string> { "usa", "cme" });

            brokerage.RecordVenuePosition(Symbol.Create("INFY", SecurityType.Equity, Market.India), 5m, 100m);
            brokerage.StampPositionsSnapshot();

            Assert.IsTrue(BrokerageDataService.Instance.TryGetChannelHeartbeat("usa", "positions-snapshot", out _));
            Assert.IsTrue(BrokerageDataService.Instance.TryGetChannelHeartbeat("cme", "positions-snapshot", out _));
            Assert.IsTrue(BrokerageDataService.Instance.TryGetChannelHeartbeat(Market.India, "positions-snapshot", out _));
        }

        [Test]
        public void IncompleteSweepStampsNothingThenNextCleanSweepStamps()
        {
            var brokerage = new InteractiveBrokersBrokerage();

            brokerage.MarkSweepIncomplete("contract conversion failed");
            brokerage.StampPositionsSnapshot();

            Assert.IsFalse(BrokerageDataService.Instance.TryGetChannelHeartbeat(Market.USA, "positions-snapshot", out _),
                "一条持仓没转换成功，这一轮的名单就不完整，不许背书「名单外都是平的」。");

            brokerage.RecordVenuePosition(Symbol.Create("SPY", SecurityType.Equity, Market.USA), 10m, 400m);
            brokerage.StampPositionsSnapshot();

            Assert.IsTrue(BrokerageDataService.Instance.TryGetChannelHeartbeat(Market.USA, "positions-snapshot", out _),
                "不完整只作废当轮，下一轮干净就该恢复盖戳。");
        }

        [Test]
        public void StampedMarketsResetBetweenSweeps()
        {
            // 上一轮出现过的 market 不能粘住：这一轮 IB 没推它，就没有全量名单可背书。
            var brokerage = new InteractiveBrokersBrokerage();

            brokerage.RecordVenuePosition(Symbol.Create("INFY", SecurityType.Equity, Market.India), 5m, 100m);
            brokerage.StampPositionsSnapshot();
            Assert.IsTrue(BrokerageDataService.Instance.TryGetChannelHeartbeat(Market.India, "positions-snapshot", out _));

            BrokerageDataService.Reset();
            brokerage.StampPositionsSnapshot();

            Assert.IsTrue(BrokerageDataService.Instance.TryGetChannelHeartbeat(Market.USA, "positions-snapshot", out _));
            Assert.IsFalse(BrokerageDataService.Instance.TryGetChannelHeartbeat(Market.India, "positions-snapshot", out _));
        }
    }
}
