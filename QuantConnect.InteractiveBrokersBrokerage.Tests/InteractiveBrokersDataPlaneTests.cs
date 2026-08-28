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
    /// lands under, and that an unusable market registration is refused outright. Liveness is all IB
    /// writes here — it reports no positions or balances into the data plane, so it stamps nothing
    /// that would let absence from a holdings list be read as flat.
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
    }
}
