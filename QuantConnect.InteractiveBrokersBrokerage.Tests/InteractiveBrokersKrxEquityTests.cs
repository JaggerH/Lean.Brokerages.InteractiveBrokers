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

using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// A Korea Exchange cash equity has to survive the whole trip: the Symbol we send has to come back
    /// as the same Symbol when IB reports the fill. Testing one direction is not enough - the inbound
    /// path used to answer with the default equity market (usa) for every contract, which produces a
    /// different Symbol than the one the order was placed with, and an order ledger that never matches
    /// its own fills.
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersKrxEquityTests
    {
        private const string SkHynix = "000660";

        [Test]
        public void OutboundUsesTheNumericKrxTicker()
        {
            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);
            var symbol = Symbol.Create(SkHynix, SecurityType.Equity, Market.KRX);

            Assert.AreEqual(SkHynix, mapper.GetBrokerageSymbol(symbol));
        }

        [Test]
        public void InboundResolvesTheKoreanMarketFromTheContractCurrency()
        {
            var contract = CreateContract();

            Assert.AreEqual(Market.KRX,
                InteractiveBrokersBrokerage.GetContractMarket(contract, SecurityType.Equity));
        }

        [Test]
        public void InboundUsdEquityStillResolvesToTheUsaMarket()
        {
            var contract = new Contract
            {
                Symbol = "AAPL",
                SecType = IB.SecurityType.Stock,
                Exchange = "SMART",
                Currency = Currencies.USD
            };

            Assert.AreEqual(Market.USA,
                InteractiveBrokersBrokerage.GetContractMarket(contract, SecurityType.Equity));
        }

        [Test]
        public void SymbolRoundTripsThroughAnIbContract()
        {
            var mapper = new InteractiveBrokersSymbolMapper(TestGlobals.MapFileProvider);
            var expected = Symbol.Create(SkHynix, SecurityType.Equity, Market.KRX);

            // outbound: the Symbol becomes an IB contract
            var contract = CreateContract();
            contract.Symbol = mapper.GetBrokerageSymbol(expected);

            // inbound: the contract IB hands back becomes a Symbol again
            var market = InteractiveBrokersBrokerage.GetContractMarket(contract, SecurityType.Equity);
            var actual = mapper.GetLeanSymbol(contract.Symbol, SecurityType.Equity, market);

            Assert.AreEqual(expected, actual);
            Assert.AreEqual(expected.ID, actual.ID);
            Assert.AreEqual(expected.Value, actual.Value);
        }

        private static Contract CreateContract()
        {
            return new Contract
            {
                Symbol = SkHynix,
                SecType = IB.SecurityType.Stock,
                Exchange = "SMART",
                PrimaryExch = "KSE",
                Currency = Currencies.KRW
            };
        }
    }
}
