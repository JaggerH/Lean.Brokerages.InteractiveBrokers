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
using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Locks the judgement GetExecutionHistory applies to a record that failed conversion: an
    /// execution on an instrument the algorithm trades fails the whole query (a quietly short answer
    /// would settle reconciliation windows over a missing fill), anything else is skipped with a
    /// count. The judgement is a pure function, so none of this needs an IB Gateway.
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersExecutionHistoryJudgementTests
    {
        private static readonly Symbol Spy = Symbol.Create("SPY", SecurityType.Equity, Market.USA);
        private static readonly Symbol Aapl = Symbol.Create("AAPL", SecurityType.Equity, Market.USA);

        private static Contract IbContract(string ticker) => new Contract { Symbol = ticker };

        [Test]
        public void MappableContractOnATradedInstrumentIsOurs()
        {
            var ours = InteractiveBrokersBrokerage.ExecutionBelongsToInstruments(
                IbContract("SPY"), new List<Symbol> { Spy },
                tryMapInbound: _ => Spy, tryMapOutbound: _ => null);

            Assert.IsTrue(ours);
        }

        [Test]
        public void MappableContractOnAForeignInstrumentIsNot()
        {
            var ours = InteractiveBrokersBrokerage.ExecutionBelongsToInstruments(
                IbContract("AAPL"), new List<Symbol> { Spy },
                tryMapInbound: _ => Aapl, tryMapOutbound: _ => null);

            Assert.IsFalse(ours);
        }

        [Test]
        public void UnmappableContractIsJudgedByTheOutboundTickerCaseInsensitively()
        {
            // The typical failing record: MapSymbol threw, so the only comparison left is our
            // instruments mapped outbound against the contract's raw IB ticker.
            var ours = InteractiveBrokersBrokerage.ExecutionBelongsToInstruments(
                IbContract("spy"), new List<Symbol> { Spy },
                tryMapInbound: _ => null, tryMapOutbound: _ => "SPY");

            Assert.IsTrue(ours);
        }

        [Test]
        public void UnmappableContractWithNoOutboundMatchIsForeign()
        {
            var ours = InteractiveBrokersBrokerage.ExecutionBelongsToInstruments(
                IbContract("LEGACYFUND"), new List<Symbol> { Spy },
                tryMapInbound: _ => null, tryMapOutbound: _ => "SPY");

            Assert.IsFalse(ours);
        }

        [Test]
        public void AnOutboundMapperFailingOnOneInstrumentDoesNotHideAMatchOnAnother()
        {
            // The judgement must never throw, and a null answer for one instrument (its own mapping
            // failed) must not end the comparison before the instrument that does match.
            var ours = InteractiveBrokersBrokerage.ExecutionBelongsToInstruments(
                IbContract("AAPL"), new List<Symbol> { Spy, Aapl },
                tryMapInbound: _ => null,
                tryMapOutbound: s => s == Spy ? null : "AAPL");

            Assert.IsTrue(ours);
        }

        [Test]
        public void WithNoInstrumentsNothingIsOurs()
        {
            var ours = InteractiveBrokersBrokerage.ExecutionBelongsToInstruments(
                IbContract("SPY"), new List<Symbol>(),
                tryMapInbound: _ => Spy, tryMapOutbound: _ => "SPY");

            Assert.IsFalse(ours);
        }
    }
}
