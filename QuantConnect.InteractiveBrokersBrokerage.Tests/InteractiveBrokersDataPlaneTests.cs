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
    /// not carry it. Plus the margin slot written on that same batch boundary: which rows count as
    /// the account's own, and that an incomplete set is left unwritten rather than zero-filled.
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

        [Test]
        public void PositionMissingFromACompleteBatchIsWrittenFlat()
        {
            // 断线期间平掉的仓，重连后的全量下载里根本不出现——不写零就永远是那条旧的非零数，
            // 而这一批还会被盖上戳，等于拿陈数背书「已对平」。
            var brokerage = new InteractiveBrokersBrokerage();
            var spy = Symbol.Create("SPY", SecurityType.Equity, Market.USA);
            var aapl = Symbol.Create("AAPL", SecurityType.Equity, Market.USA);

            brokerage.RecordVenuePosition(spy, 10m, 400m);
            brokerage.StampPositionsSnapshot();

            brokerage.RecordVenuePosition(aapl, 5m, 200m);
            brokerage.StampPositionsSnapshot();

            Assert.IsTrue(BrokerageDataService.Instance.TryGetSecurityPosition(spy, out var spyPosition));
            Assert.AreEqual(0m, spyPosition.Quantity);
            Assert.IsTrue(BrokerageDataService.Instance.TryGetSecurityPosition(aapl, out var aaplPosition));
            Assert.AreEqual(5m, aaplPosition.Quantity);
        }

        [Test]
        public void IncompleteBatchZeroesNothing()
        {
            // 少了一行的一批证明不了任何东西的缺席，更不能拿它把别的仓写平。
            var brokerage = new InteractiveBrokersBrokerage();
            var spy = Symbol.Create("SPY", SecurityType.Equity, Market.USA);

            brokerage.RecordVenuePosition(spy, 10m, 400m);
            brokerage.StampPositionsSnapshot();

            brokerage.MarkSweepIncomplete("contract conversion failed");
            brokerage.StampPositionsSnapshot();

            Assert.IsTrue(BrokerageDataService.Instance.TryGetSecurityPosition(spy, out var spyPosition));
            Assert.AreEqual(10m, spyPosition.Quantity);
        }

        [Test]
        public void ASymbolIsWrittenFlatOnceNotOnEveryLaterBatch()
        {
            // 反复写零会不断刷新 LastUpdated，把一条早就没人报的仓打扮成新鲜的交易所证据。
            var brokerage = new InteractiveBrokersBrokerage();
            var spy = Symbol.Create("SPY", SecurityType.Equity, Market.USA);

            brokerage.RecordVenuePosition(spy, 10m, 400m);
            brokerage.StampPositionsSnapshot();
            brokerage.StampPositionsSnapshot();

            Assert.IsTrue(BrokerageDataService.Instance.TryGetSecurityPosition(spy, out var flattened));
            Assert.AreEqual(0m, flattened.Quantity);

            // 打上哨兵值：再写一次零会把它抹掉，不写则原样留着。
            BrokerageDataService.Instance.UpdateSecurityPosition(spy, new BrokerageDataService.SecurityPositionData
            {
                Quantity = 0m,
                AveragePrice = 123m
            });

            brokerage.StampPositionsSnapshot();

            Assert.IsTrue(BrokerageDataService.Instance.TryGetSecurityPosition(spy, out var untouched));
            Assert.AreEqual(123m, untouched.AveragePrice);
        }

        [Test]
        public void MarginIsWrittenPerRegisteredMarketWhenCoreKeysArrived()
        {
            // 一条连接横跨两个市场，账户只有一个：两个 market 各写一份同样的保证金。
            var brokerage = new InteractiveBrokersBrokerage();
            ((IMultiMarketVenue)brokerage).SetVenueMarkets(new List<string> { "usa", "cme" });

            brokerage.RecordAccountValue("NetLiquidation", "100000.5", "USD");
            brokerage.RecordAccountValue("AvailableFunds", "40000.25", "USD");
            brokerage.RecordAccountValue("ExcessLiquidity", "45000", "USD");
            brokerage.RecordAccountValue("InitMarginReq", "60000", "USD");
            brokerage.RecordAccountValue("MaintMarginReq", "55000", "USD");

            brokerage.WriteAccountMargin();

            foreach (var market in new[] { "usa", "cme" })
            {
                Assert.IsTrue(BrokerageDataService.Instance.TryGetMargin(market, out var margin), market);
                Assert.AreEqual(100000.5m, margin.TotalEquity, market);
                Assert.AreEqual(40000.25m, margin.AvailableMargin, market);
                Assert.AreEqual(45000m, margin.MarginBalance, market);
                Assert.AreEqual(60000m, margin.InitialMarginUsed, market);
                Assert.AreEqual(55000m, margin.MaintenanceMarginUsed, market);

                // IB 不报这三个，留默认——不编。
                Assert.AreEqual(0m, margin.InitialMarginRate, market);
                Assert.AreEqual(0m, margin.MaintenanceMarginRate, market);
                Assert.AreEqual(0m, margin.TotalLiability, market);

                Assert.AreNotEqual(default(System.DateTime), margin.LastUpdated, market);
            }
        }

        [Test]
        public void MarginIsNotWrittenWithoutAvailableFunds()
        {
            // 可用保证金正是买力模型要读的那个数，缺它写出去的就是一份看着完整、实则没有可用额度的槽位。
            var brokerage = new InteractiveBrokersBrokerage();

            brokerage.RecordAccountValue("NetLiquidation", "100000", "USD");
            brokerage.RecordAccountValue("ExcessLiquidity", "45000", "USD");
            brokerage.WriteAccountMargin();

            Assert.IsFalse(BrokerageDataService.Instance.TryGetMargin(Market.USA, out _));

            // 下一批补齐了就该写。
            brokerage.RecordAccountValue("AvailableFunds", "40000", "USD");
            brokerage.WriteAccountMargin();

            Assert.IsTrue(BrokerageDataService.Instance.TryGetMargin(Market.USA, out var margin));
            Assert.AreEqual(40000m, margin.AvailableMargin);
        }

        [Test]
        public void BaseCurrencyRowsAreIgnored()
        {
            // IB 每个键都推一份 "BASE" 汇总行，混进来就是把两套口径的数搅在一起。
            var brokerage = new InteractiveBrokersBrokerage();

            brokerage.RecordAccountValue("NetLiquidation", "999999", "BASE");
            brokerage.RecordAccountValue("AvailableFunds", "999999", "BASE");
            brokerage.WriteAccountMargin();

            Assert.IsFalse(BrokerageDataService.Instance.TryGetMargin(Market.USA, out _),
                "只有 BASE 行，等于本币的数一个都没到，不许写。");

            brokerage.RecordAccountValue("NetLiquidation", "100000", "USD");
            brokerage.RecordAccountValue("AvailableFunds", "40000", "USD");
            brokerage.WriteAccountMargin();

            Assert.IsTrue(BrokerageDataService.Instance.TryGetMargin(Market.USA, out var margin));
            Assert.AreEqual(100000m, margin.TotalEquity);
            Assert.AreEqual(40000m, margin.AvailableMargin);
        }

        [Test]
        public void ForeignCurrencyRowsAreIgnoredOnceACurrencyIsAccepted()
        {
            // 账户里同时有 EUR 头寸，IB 会按币种各推一份；混着累加出来的权益谁也不是。
            var brokerage = new InteractiveBrokersBrokerage();

            brokerage.RecordAccountValue("NetLiquidation", "100000", "USD");
            brokerage.RecordAccountValue("AvailableFunds", "40000", "USD");
            brokerage.RecordAccountValue("NetLiquidation", "7000", "EUR");
            brokerage.WriteAccountMargin();

            Assert.IsTrue(BrokerageDataService.Instance.TryGetMargin(Market.USA, out var margin));
            Assert.AreEqual(100000m, margin.TotalEquity);
        }

        [Test]
        public void UnparseableValueIsIgnoredNotFatal()
        {
            // IB 偶尔推空串或 "N/A"，它只该让这一个键缺席，不该掀掉整批。
            var brokerage = new InteractiveBrokersBrokerage();

            Assert.DoesNotThrow(() => brokerage.RecordAccountValue("NetLiquidation", "N/A", "USD"));
            brokerage.RecordAccountValue("AvailableFunds", "40000", "USD");
            brokerage.WriteAccountMargin();

            Assert.IsFalse(BrokerageDataService.Instance.TryGetMargin(Market.USA, out _),
                "解析失败的键就是没到，不许拿 0 顶上。");

            brokerage.RecordAccountValue("NetLiquidation", "100000", "USD");
            brokerage.WriteAccountMargin();

            Assert.IsTrue(BrokerageDataService.Instance.TryGetMargin(Market.USA, out var margin));
            Assert.AreEqual(100000m, margin.TotalEquity);
        }

        [Test]
        public void UnrelatedAccountKeysAreNotRecorded()
        {
            // 只收这五个键：账户值推送里有上百个键，全存下来只是给自己攒垃圾。
            var brokerage = new InteractiveBrokersBrokerage();

            brokerage.RecordAccountValue("CashBalance", "100000", "USD");
            brokerage.RecordAccountValue("BuyingPower", "400000", "USD");
            brokerage.WriteAccountMargin();

            Assert.IsFalse(BrokerageDataService.Instance.TryGetMargin(Market.USA, out _));
        }
    }
}
