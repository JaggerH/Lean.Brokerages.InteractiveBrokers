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
    /// Data-plane writes that need no gateway: which market keys the heartbeat lands under, and
    /// that a positions sweep is only vouched for when the account download really completed.
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

        /// <summary>A brokerage wired the way a live download leaves it: holdings loading enabled.</summary>
        private static InteractiveBrokersBrokerage CreateSweepingBrokerage()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            ((IMultiMarketVenue)brokerage).SetVenueMarkets(new List<string> { "usa", "krx" });
            brokerage.MarkLoadExistingHoldingsForTesting(true);
            return brokerage;
        }

        [Test]
        public void PositionsSnapshotRequiresACompletedAccountDownload()
        {
            var brokerage = CreateSweepingBrokerage();

            brokerage.MarkAccountSweepForTesting(downloadVerdict: false);
            brokerage.StampPositionsSnapshotIfSweepComplete();
            Assert.IsFalse(BrokerageDataService.Instance.TryGetChannelHeartbeat("usa", "positions-snapshot", out _),
                "下载超时或转换出错时持仓可能不全——此时「没有」不等于「平」");

            brokerage.MarkAccountSweepForTesting(downloadVerdict: true);
            brokerage.StampPositionsSnapshotIfSweepComplete();
            Assert.IsTrue(BrokerageDataService.Instance.TryGetChannelHeartbeat("usa", "positions-snapshot", out _));
            Assert.IsTrue(BrokerageDataService.Instance.TryGetChannelHeartbeat("krx", "positions-snapshot", out _));

            // 一次干净下载只许盖一次：第一次 GetAccountHoldings 之后 _loadExistingHoldings 归 false、
            // 持仓字典就冻住了，再盖一次等于报告一次没发生过的扫描。
            BrokerageDataService.Reset();
            brokerage.StampPositionsSnapshotIfSweepComplete();
            Assert.IsFalse(BrokerageDataService.Instance.TryGetChannelHeartbeat("usa", "positions-snapshot", out _),
                "冻住的持仓字典不该再盖出一个「刚扫过」的心跳");
        }

        [Test]
        public void SweepIsNotVouchedWhenHoldingsWereNeverLoaded()
        {
            // 重连时 Connect() 清空 _accountData，而写字典的唯一一处受 _loadExistingHoldings 把守；
            // 第一次 GetAccountHoldings 把它关掉之后，再下载一次也不会把持仓填回来——
            // 此时替一个空字典背书，正是这条通道要挡的那句假话。
            var brokerage = new InteractiveBrokersBrokerage();
            ((IMultiMarketVenue)brokerage).SetVenueMarkets(new List<string> { "usa", "krx" });
            brokerage.MarkLoadExistingHoldingsForTesting(false);

            // 注意：这里只断言"不盖戳"。DownloadAccount 的返回值是连接闸门，它不受这个条件影响。
            brokerage.MarkAccountSweepForTesting(downloadVerdict: true);
            Assert.IsFalse(brokerage.AccountSweepCompleteForTesting);

            brokerage.StampPositionsSnapshotIfSweepComplete();
            Assert.IsFalse(BrokerageDataService.Instance.TryGetChannelHeartbeat("usa", "positions-snapshot", out _));
            Assert.IsFalse(BrokerageDataService.Instance.TryGetChannelHeartbeat("krx", "positions-snapshot", out _));
        }

        [Test]
        public void ConversionExceptionAfterTheSweepSuppressesTheStamp()
        {
            // 合约转换失败可能发生在下载返回之后的持续持仓推送里，那时候标志位已经是 true 了。
            var brokerage = CreateSweepingBrokerage();
            brokerage.MarkAccountSweepForTesting(downloadVerdict: true);
            Assert.IsTrue(brokerage.AccountSweepCompleteForTesting);

            brokerage.MarkAccountHoldingsExceptionForTesting(new System.Exception("contract conversion failed"));

            brokerage.StampPositionsSnapshotIfSweepComplete();
            Assert.IsFalse(BrokerageDataService.Instance.TryGetChannelHeartbeat("usa", "positions-snapshot", out _));
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
    }
}
