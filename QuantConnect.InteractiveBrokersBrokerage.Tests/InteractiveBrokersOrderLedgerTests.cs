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
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Interfaces;
using QuantConnect.Orders;
using QuantConnect.Orders.Ledger;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Locks the judgement surfaces of the IB order-ledger wiring that can be exercised without an
    /// IB Gateway: the OrderRef constraint, the table deciding which error codes may write a ledger
    /// tombstone, and — via the parameterless (Composer) constructor, which never starts
    /// IBAutomater — the outbound placement chain itself: the key <c>ResolveOrderRef</c> registers
    /// is the key <c>ConvertOrder</c> stamps on the outbound <c>IBApi.Order.OrderRef</c>.
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersOrderLedgerTests
    {
        /// <summary>
        /// Records RegisterIntent calls and answers TryResolve with "unknown" — enough ledger for
        /// the placement chain, which only registers and maps keys.
        /// </summary>
        private sealed class RecordingLedger : IOrderLedger
        {
            public readonly List<Order> RegisteredOrders = new List<Order>();

            public string Directory { get; } = "fake-ledger-dir";

            public string RegisterIntent(Order order, string venue, OrderKeyConstraint constraint)
            {
                RegisteredOrders.Add(order);
                var key = OrderKeyGenerator.Build(order.Id, RegisteredOrders.Count);
                constraint.Validate(key, venue);
                return key;
            }

            public bool RecordAck(string clientOrderId, string exchangeOrderId) => true;
            public bool RecordClosed(string clientOrderId) => true;
            public bool RecordDead(string clientOrderId, string reason) => true;
            public bool TryResolve(string venue, string exchangeOrderId, string clientOrderId, out LedgerEntry entry)
            {
                entry = null;
                return false;
            }
            public bool Rebind(string clientOrderId, int newLeanOrderId) => true;
            public void Compact(DateTime beforeUtc) { }
            public void SyncToDisk() { }
            public string RunId => "test-run";
            public IReadOnlyList<LedgerEntry> OpenIntents => new List<LedgerEntry>();
            public void Dispose() { }
        }

        private static MarketOrder NewOrder()
        {
            // Order.Id stays 0: its setter is internal to QuantConnect.Common and the id plays no
            // part in what these tests lock.
            return new MarketOrder(Symbol.Create("SPY", SecurityType.Equity, Market.USA), 100m, new DateTime(2026, 8, 14));
        }

        [Test]
        public void PlacementChainStampsTheRegisteredKeyOnTheOutboundOrder()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            var ledger = new RecordingLedger();
            brokerage.WireOrderLedgerForTesting(ledger);
            var orders = new List<Order> { NewOrder() };

            var key = brokerage.ResolveOrderRef(orders, ibOrderId: 1, needsNewId: true);
            var ibOrder = brokerage.ConvertOrder(orders, new IBApi.Contract(), ibOrderId: 1, orderRef: key);

            // The whole point of the ledger: the key we registered is the key IB will echo back.
            Assert.IsNotNull(key);
            Assert.AreEqual(key, ibOrder.OrderRef);
            Assert.AreEqual(1, ledger.RegisteredOrders.Count);
        }

        [Test]
        public void ModificationReusesThePlacementKeyInsteadOfMintingASecondOne()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            var ledger = new RecordingLedger();
            brokerage.WireOrderLedgerForTesting(ledger);
            var orders = new List<Order> { NewOrder() };

            var placementKey = brokerage.ResolveOrderRef(orders, ibOrderId: 1, needsNewId: true);
            var modificationKey = brokerage.ResolveOrderRef(orders, ibOrderId: 1, needsNewId: false);

            // A second key would open a second ledger entry that nothing ever acks.
            Assert.AreEqual(placementKey, modificationKey);
            Assert.AreEqual(1, ledger.RegisteredOrders.Count);
        }

        [Test]
        public void NoLedgerMeansEmptyOrderRefNotNull()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            var orders = new List<Order> { NewOrder() };

            var key = brokerage.ResolveOrderRef(orders, ibOrderId: 1, needsNewId: true);
            var ibOrder = brokerage.ConvertOrder(orders, new IBApi.Contract(), ibOrderId: 1, orderRef: key);

            // IB's own convention for unset string fields is the empty string.
            Assert.IsNull(key);
            Assert.AreEqual(string.Empty, ibOrder.OrderRef);
        }

        [Test]
        public void ComboIsRefusedWhenALedgerIsWired()
        {
            // 明确不支持：账本一条记录对应一张场所订单，combo 的其余腿不可能被逐腿核对。
            // 带着半覆盖的账本把 combo 下出去，事后长得和全覆盖一模一样——所以拒绝，
            // 且必须在写入账本之前拒绝（抛出即中止下单，是这条路径的既有约定）。
            var brokerage = new InteractiveBrokersBrokerage();
            var ledger = new RecordingLedger();
            brokerage.WireOrderLedgerForTesting(ledger);
            var orders = new List<Order> { NewOrder(), NewOrder() };
            var messages = new List<BrokerageMessageEvent>();
            brokerage.Message += (_, e) => messages.Add(e);

            Assert.Throws<NotSupportedException>(
                () => brokerage.ResolveOrderRef(orders, ibOrderId: 1, needsNewId: true));

            Assert.IsEmpty(ledger.RegisteredOrders, "拒绝必须发生在写账本之前，否则留下一条永远不会被 ack 的意图");
            Assert.AreEqual(1, messages.Count);
            Assert.AreEqual("ORDER_LEDGER_COMBO_UNSUPPORTED", messages[0].Code);
            Assert.AreEqual(BrokerageMessageType.Error, messages[0].Type);
        }

        [Test]
        public void ComboIsAllowedWhenNoLedgerIsWired()
        {
            // 配对用例：非套利策略不接账本，它照常可以下 combo——上面那条拒绝只针对
            // "接了账本"这一种情形，不是把 combo 从这个券商里删掉。
            var brokerage = new InteractiveBrokersBrokerage();
            var orders = new List<Order> { NewOrder(), NewOrder() };

            Assert.DoesNotThrow(() => brokerage.ResolveOrderRef(orders, ibOrderId: 1, needsNewId: true));
        }

        [Test]
        public void OrderRefConstraintAcceptsAGeneratedLedgerKey()
        {
            var key = OrderKeyGenerator.Build(1, 42);

            Assert.DoesNotThrow(() => InteractiveBrokersBrokerage.OrderRefConstraint.Validate(key, Market.USA));
        }

        [Test]
        public void OrderRefConstraintRejectsOverlongKeys()
        {
            var tooLong = new string('7', InteractiveBrokersBrokerage.OrderRefConstraint.MaxLength + 1);

            // Never silently truncated: IB documents no length limit at all, so a key past our
            // conservative bound must surface as a refusal to place the order.
            Assert.Throws<ArgumentException>(
                () => InteractiveBrokersBrokerage.OrderRefConstraint.Validate(tooLong, Market.USA));
        }

        [TestCase("has space")]
        [TestCase("comma,separated")]
        [TestCase("quote\"d")]
        public void OrderRefConstraintRejectsReportBreakingCharacters(string key)
        {
            Assert.Throws<ArgumentException>(
                () => InteractiveBrokersBrokerage.OrderRefConstraint.Validate(key, Market.USA));
        }

        [TestCase(110)] // The price does not conform to the minimum price variation for this contract.
        [TestCase(201)] // Order rejected - Reason:
        [TestCase(203)] // The security <security> is not available or allowed for this account.
        [TestCase(382)] // The price specified violates the number of ticks constraint ...
        [TestCase(383)] // The size specified violates the size constraint ...
        [TestCase(388)] // Order size is smaller than the minimum requirement.
        [TestCase(434)] // The order size cannot be zero.
        public void ConfirmedRejectionCodesContainsOnlyProvenRejections(int errorCode)
        {
            Assert.IsTrue(InteractiveBrokersBrokerage.ConfirmedRejectionCodes.Contains(errorCode));
        }

        [TestCase(104)]   // Can't modify a filled order - proves the order EXISTS
        [TestCase(105)]   // Order being modified does not match original order - the original is live
        [TestCase(202)]   // Order cancelled - the order existed; that is Closed, not Dead
        [TestCase(321)]   // Server error when validating an API client request - effect unknown
        [TestCase(399)]   // Order message error - generic, says nothing about acceptance
        [TestCase(10148)] // OrderId ... that needs to be cancelled can not be cancelled
        [TestCase(1100)]  // Connectivity between IB and TWS has been lost - the definition of "don't know"
        [TestCase(504)]   // Not connected
        public void ConfirmedRejectionCodesExcludesAmbiguousCodes(int errorCode)
        {
            // Writing a tombstone for any of these erases the only evidence that a live,
            // unacknowledged order may exist at IB.
            Assert.IsFalse(InteractiveBrokersBrokerage.ConfirmedRejectionCodes.Contains(errorCode));
        }
    }
}
