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

using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Interfaces;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Locks the capability interfaces this brokerage DECLARES, not the methods it happens to have.
    /// </summary>
    /// <remarks>
    /// WHY THIS EXISTS. Every consumer of an optional brokerage capability discovers it with a
    /// runtime type test (<c>brokerage is IExecutionHistoryProvider</c>), so a matching public method
    /// on an undeclared class is invisible: the capability is present, unreachable, and the failure
    /// is a polite sentence rather than an error.
    ///
    /// That is not hypothetical. <c>GetExecutionHistory</c> shipped fully implemented — the timeout
    /// throws, foreign instruments are triaged, the coverage is stated — while the class declaration
    /// listed only <c>IDataQueueHandler</c> and <c>IDataQueueUniverseProvider</c>. Cross-venue runs
    /// logged "Interactive Brokers Brokerage does not implement IExecutionHistoryProvider" once a
    /// minute, the reconciliation checkpoint never advanced, and nothing anywhere was broken enough
    /// to fail. A compile-time test would not have caught it either — the code compiled fine.
    ///
    /// So the assertion is deliberately on the declaration, and any new optional capability added to
    /// this brokerage gets a line here.
    /// </remarks>
    [TestFixture]
    public class InteractiveBrokersCapabilityDeclarationTests
    {
        [Test]
        public void DeclaresExecutionHistoryProvider()
        {
            Assert.IsTrue(typeof(IExecutionHistoryProvider).IsAssignableFrom(typeof(InteractiveBrokersBrokerage)),
                "InteractiveBrokersBrokerage has GetExecutionHistory but does not declare " +
                "IExecutionHistoryProvider, so every caller's 'is IExecutionHistoryProvider' test fails and " +
                "reconciliation silently treats this venue as unaskable.");
        }

        [Test]
        public void DeclaresClientOrderIdQuery()
        {
            Assert.IsTrue(typeof(IClientOrderIdQuery).IsAssignableFrom(typeof(InteractiveBrokersBrokerage)),
                "InteractiveBrokersBrokerage no longer declares IClientOrderIdQuery, so order-ledger " +
                "convergence can no longer ask IB whether an unacked intent reached it.");
        }
    }
}
