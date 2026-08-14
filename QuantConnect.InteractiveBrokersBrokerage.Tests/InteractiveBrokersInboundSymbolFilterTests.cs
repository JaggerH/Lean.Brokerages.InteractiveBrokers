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
using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Orders;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Locks the tie-breaker every inbound IB report goes through before it is attributed to a Lean
    /// order. The lookup key is a bare IB order id, so once this brokerage shares an order provider
    /// with other venues a single id can answer with orders on several symbols; the symbol carried by
    /// the report is what separates them. The function is pure, so none of this needs an IB Gateway.
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersInboundSymbolFilterTests
    {
        private static readonly Symbol Spy = Symbol.Create("SPY", SecurityType.Equity, Market.USA);
        private static readonly Symbol Aapl = Symbol.Create("AAPL", SecurityType.Equity, Market.USA);

        private static MarketOrder NewOrder(Symbol symbol)
        {
            return new MarketOrder(symbol, 100m, new DateTime(2026, 8, 14));
        }

        private static List<Order> Filter(List<Order> orders, Symbol symbol)
        {
            return InteractiveBrokersBrokerage.FilterOrdersByLeanSymbol(orders, symbol, brokerageOrderId: 1, caller: "Test");
        }

        [Test]
        public void KeepsOnlyTheOrderOnTheReportedSymbol()
        {
            var spyOrder = NewOrder(Spy);
            var orders = new List<Order> { NewOrder(Aapl), spyOrder };

            var filtered = Filter(orders, Spy);

            Assert.AreEqual(1, filtered.Count);
            Assert.AreSame(spyOrder, filtered[0]);
        }

        [Test]
        public void WithoutASymbolNothingIsFiltered()
        {
            // A report we could not map, or an order adopted from a previous run: the pre-existing
            // unfiltered behaviour must survive, never a dropped report.
            var orders = new List<Order> { NewOrder(Aapl), NewOrder(Spy) };

            var filtered = Filter(orders, null);

            Assert.AreSame(orders, filtered);
        }

        [Test]
        public void NoMatchFallsBackToTheUnfilteredList()
        {
            // Report and order book disagree about what this id is. Losing an order event for certain
            // is worse than the pre-existing first-match guess, so the caller still gets candidates.
            var orders = new List<Order> { NewOrder(Aapl), NewOrder(Aapl) };

            var filtered = Filter(orders, Spy);

            Assert.AreSame(orders, filtered);
        }

        [Test]
        public void ALoneCandidateIsReturnedWithoutCheckingItsSymbol()
        {
            var orders = new List<Order> { NewOrder(Aapl) };

            var filtered = Filter(orders, Spy);

            Assert.AreSame(orders, filtered);
        }

        [Test]
        public void SeveralOrdersOnTheSameSymbolAreAllReturned()
        {
            // The symbol cannot separate these; the caller's own multi-order handling stays in charge
            // instead of this method picking one at random.
            var orders = new List<Order> { NewOrder(Spy), NewOrder(Spy), NewOrder(Aapl) };

            var filtered = Filter(orders, Spy);

            Assert.AreEqual(2, filtered.Count);
            CollectionAssert.AreEqual(new[] { orders[0], orders[1] }, filtered);
        }
    }
}
