using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Orders;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// 隔夜路由：IBKR 的隔夜场要求合约路由到 "OVERNIGHT" 且 TIF 为 "OVT"。
    /// 两个分支都必须只在 OvernightSession 为 true 时生效——泄漏到常规时段
    /// 会把白天的单送进一个当时没开的场。
    /// </summary>
    [TestFixture]
    public class OvernightRoutingTests
    {
        [Test]
        public void OvernightSession_RoutesToOvernightExchange()
        {
            var properties = new ArbitrageOrderProperties { OvernightSession = true };

            Assert.AreEqual("OVERNIGHT",
                InteractiveBrokersBrokerage.ResolveOvernightExchange(properties, currentExchange: null));
        }

        [Test]
        public void NotOvernight_KeepsExistingExchange()
        {
            var properties = new ArbitrageOrderProperties { OvernightSession = false };

            Assert.IsNull(
                InteractiveBrokersBrokerage.ResolveOvernightExchange(properties, currentExchange: null));
        }

        [Test]
        public void NonArbitrageProperties_KeepsExistingExchange()
        {
            var properties = new InteractiveBrokersOrderProperties();

            Assert.IsNull(
                InteractiveBrokersBrokerage.ResolveOvernightExchange(properties, currentExchange: null));
        }

        [Test]
        public void AlreadyDirectedExchange_IsNotOverridden()
        {
            // 期权 MOO/MOC 已经把 exchange 定向到 CBOE，隔夜分支不许抢它
            var properties = new ArbitrageOrderProperties { OvernightSession = true };

            Assert.AreEqual("CBOE",
                InteractiveBrokersBrokerage.ResolveOvernightExchange(properties, currentExchange: "CBOE"));
        }

        [Test]
        public void OvernightSession_UsesOvtTimeInForce()
        {
            var properties = new ArbitrageOrderProperties { OvernightSession = true };

            Assert.AreEqual("OVT", InteractiveBrokersBrokerage.ResolveOvernightTimeInForce(properties));
        }

        [Test]
        public void NotOvernight_HasNoTimeInForceOpinion()
        {
            var properties = new ArbitrageOrderProperties { OvernightSession = false };

            Assert.IsNull(InteractiveBrokersBrokerage.ResolveOvernightTimeInForce(properties));
        }
    }
}
