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
using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Orders.Ledger;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Locks the two judgement surfaces of the IB order-ledger wiring that can be exercised without
    /// an IB Gateway: the OrderRef constraint, and the table deciding which error codes are allowed
    /// to write a ledger tombstone.
    ///
    /// NOT covered here, and deliberately called out rather than faked: that the key actually lands
    /// on the outbound IBApi.Order. Every path to <c>ConvertOrder</c> goes through the brokerage
    /// constructor, which starts IBAutomater and an IB Gateway process, so it cannot be reached from
    /// a unit test in this repository.
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersOrderLedgerTests
    {
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
