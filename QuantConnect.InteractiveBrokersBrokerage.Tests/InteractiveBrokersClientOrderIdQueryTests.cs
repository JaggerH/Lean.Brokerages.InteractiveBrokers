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
using System.Runtime.CompilerServices;
using NUnit.Framework;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Orders.Ledger;
using Lookup = QuantConnect.Brokerages.InteractiveBrokers.InteractiveBrokersBrokerage.OrderRefLookup;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Locks the boundary between the three outcomes of <c>IClientOrderIdQuery</c> for IB.
    ///
    /// IB has no lookup by OrderRef, so the answer is assembled from three requests
    /// (reqAllOpenOrders / reqExecutions / reqCompletedOrders). The interesting judgement is
    /// therefore not any single request but how their answers combine — above all that a source
    /// which could NOT answer never turns into "IB does not have this order". That judgement lives
    /// in <c>CombineSourceLookups</c>, which is pure, and is what these tests drive.
    ///
    /// NOT covered here, and called out rather than faked: the three requests themselves. Reaching
    /// them needs the brokerage constructor, which starts IBAutomater and an IB Gateway process.
    /// What that leaves untested is the wiring from each IB callback into a <c>Lookup</c> — the
    /// per-source try/catch and timeout paths are only exercised through their results below.
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersClientOrderIdQueryTests
    {
        private const string ClientOrderId = "712345678901234567";
        private static readonly TimeSpan YoungIntent = TimeSpan.FromMinutes(5);

        private static Lookup OpenOrdersNoMatch => Lookup.NoMatch("reqAllOpenOrders");
        private static Lookup CompletedNoMatch => Lookup.NoMatch("reqCompletedOrders");
        private static Lookup ExecutionsNoMatch => Lookup.NoMatch("reqExecutions");

        [Test]
        public void AllThreeSourcesAnsweredAndNoneHasIt_IsNotFound()
        {
            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, YoungIntent,
                OpenOrdersNoMatch, CompletedNoMatch, ExecutionsNoMatch);

            Assert.AreEqual(ClientOrderIdQueryOutcome.NotFound, result.Outcome);
        }

        [TestCase("reqAllOpenOrders")]
        [TestCase("reqCompletedOrders")]
        [TestCase("reqExecutions")]
        public void OneSourceCouldNotAnswer_IsQueryFailedNotNotFound(string failingSource)
        {
            // The whole point of the file: two of three sources saying "not here" plus one saying
            // nothing at all is NOT an absence. Collapsing this into NotFound lets convergence write
            // a terminal state for an order that may be resting at IB right now.
            var lookups = new[]
            {
                failingSource == "reqAllOpenOrders" ? Lookup.Failed(failingSource, "no openOrderEnd within 15s") : OpenOrdersNoMatch,
                failingSource == "reqCompletedOrders" ? Lookup.Failed(failingSource, "no completedOrdersEnd within 15s") : CompletedNoMatch,
                failingSource == "reqExecutions" ? Lookup.Failed(failingSource, "no execDetailsEnd within 15s") : ExecutionsNoMatch
            };

            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, YoungIntent, lookups);

            Assert.AreEqual(ClientOrderIdQueryOutcome.QueryFailed, result.Outcome);
            // The reason has to name the source: it is the only record of which one went dark.
            StringAssert.Contains(failingSource, result.FailureReason);
        }

        [Test]
        public void ASourceThatThrew_IsQueryFailedAndTheExceptionTextSurvivesIntoTheReason()
        {
            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, YoungIntent,
                OpenOrdersNoMatch, CompletedNoMatch,
                Lookup.Failed("reqExecutions", "failed while reading an execution: object reference not set"));

            Assert.AreEqual(ClientOrderIdQueryOutcome.QueryFailed, result.Outcome);
            StringAssert.Contains("object reference not set", result.FailureReason);
        }

        [Test]
        public void EveryFailingSourceIsNamed_NotJustTheFirst()
        {
            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, YoungIntent,
                Lookup.Failed("reqAllOpenOrders", "socket closed"),
                Lookup.Failed("reqCompletedOrders", "not supported by this gateway"),
                ExecutionsNoMatch);

            Assert.AreEqual(ClientOrderIdQueryOutcome.QueryFailed, result.Outcome);
            StringAssert.Contains("socket closed", result.FailureReason);
            StringAssert.Contains("not supported by this gateway", result.FailureReason);
        }

        [Test]
        public void IntentOlderThanTheExecutionWindow_IsQueryFailedEvenThoughEverySourceAnswered()
        {
            // 25 hours: reqExecutions can no longer see the fills, and reqCompletedOrders is scoped
            // to the current trading day. All three are silent because they cannot look that far
            // back, not because IB never had the order.
            var age = InteractiveBrokersBrokerage.ExecutionLookbackWindow + TimeSpan.FromHours(1);

            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, age,
                OpenOrdersNoMatch, CompletedNoMatch, ExecutionsNoMatch);

            Assert.AreEqual(ClientOrderIdQueryOutcome.QueryFailed, result.Outcome);
            StringAssert.Contains("24", result.FailureReason);
        }

        [Test]
        public void IntentAtTheExecutionWindowBoundary_IsStillNotFound()
        {
            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId,
                InteractiveBrokersBrokerage.ExecutionLookbackWindow,
                OpenOrdersNoMatch, CompletedNoMatch, ExecutionsNoMatch);

            Assert.AreEqual(ClientOrderIdQueryOutcome.NotFound, result.Outcome);
        }

        [Test]
        public void UnknownIntentAge_IsQueryFailed()
        {
            // No ledger entry for the key => we cannot show the 24h window ever covered this order,
            // so silence from the three sources proves nothing.
            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, null,
                OpenOrdersNoMatch, CompletedNoMatch, ExecutionsNoMatch);

            Assert.AreEqual(ClientOrderIdQueryOutcome.QueryFailed, result.Outcome);
        }

        [Test]
        public void RestingAtIb_IsFoundAndNotTerminal()
        {
            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, YoungIntent,
                Lookup.Match("reqAllOpenOrders", ibOrderId: 42, permId: 900, isTerminal: false, filledQuantity: 0m),
                CompletedNoMatch, ExecutionsNoMatch);

            Assert.AreEqual(ClientOrderIdQueryOutcome.Found, result.Outcome);
            Assert.AreEqual("42", result.ExchangeOrderId);
            Assert.IsFalse(result.IsTerminal);
        }

        [Test]
        public void CompletedAtIb_IsFoundAndTerminal()
        {
            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, YoungIntent,
                OpenOrdersNoMatch,
                Lookup.Match("reqCompletedOrders", ibOrderId: 42, permId: 900, isTerminal: true, filledQuantity: 10m),
                ExecutionsNoMatch);

            Assert.AreEqual(ClientOrderIdQueryOutcome.Found, result.Outcome);
            Assert.AreEqual("42", result.ExchangeOrderId);
            Assert.IsTrue(result.IsTerminal);
            Assert.AreEqual(10m, result.FilledQuantity);
        }

        [Test]
        public void OnlyExecutionsHaveIt_IsFoundWithTheFillButNotTerminal()
        {
            // A fill does not prove the order stopped working — a partial fill leaves it live. The
            // safe direction is non-terminal: the entry stays open and gets queried again.
            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, YoungIntent,
                OpenOrdersNoMatch, CompletedNoMatch,
                Lookup.Match("reqExecutions", ibOrderId: 42, permId: 900, isTerminal: false, filledQuantity: 3m));

            Assert.AreEqual(ClientOrderIdQueryOutcome.Found, result.Outcome);
            Assert.AreEqual("42", result.ExchangeOrderId);
            Assert.IsFalse(result.IsTerminal);
            Assert.AreEqual(3m, result.FilledQuantity);
        }

        [Test]
        public void AMatchBeatsAFailedSource()
        {
            // A positive answer is authoritative on its own — the failed source could only have
            // added detail, never taken the order away.
            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, null,
                Lookup.Match("reqAllOpenOrders", ibOrderId: 42, permId: 900, isTerminal: false, filledQuantity: 0m),
                Lookup.Failed("reqCompletedOrders", "no completedOrdersEnd within 15s"),
                Lookup.Failed("reqExecutions", "no execDetailsEnd within 15s"));

            Assert.AreEqual(ClientOrderIdQueryOutcome.Found, result.Outcome);
            Assert.AreEqual("42", result.ExchangeOrderId);
        }

        [Test]
        public void TheLargestReportedFillWins()
        {
            // Both numbers estimate the same quantity; the larger one has seen more of it.
            // Under-reporting a fill is the dangerous direction.
            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, YoungIntent,
                Lookup.Match("reqAllOpenOrders", ibOrderId: 42, permId: 900, isTerminal: false, filledQuantity: 3m),
                CompletedNoMatch,
                Lookup.Match("reqExecutions", ibOrderId: 42, permId: 900, isTerminal: false, filledQuantity: 7m));

            Assert.AreEqual(7m, result.FilledQuantity);
        }

        [Test]
        public void TerminalFromAnySourceMakesItTerminal()
        {
            // The sources are read one after another; an order that finishes in between shows up
            // exactly like this.
            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, YoungIntent,
                Lookup.Match("reqAllOpenOrders", ibOrderId: 42, permId: 900, isTerminal: false, filledQuantity: 0m),
                Lookup.Match("reqCompletedOrders", ibOrderId: 42, permId: 900, isTerminal: true, filledQuantity: 10m),
                ExecutionsNoMatch);

            Assert.IsTrue(result.IsTerminal);
        }

        [Test]
        public void TheFirstNonZeroIbOrderIdWins()
        {
            // completedOrder leaves OrderId 0 for orders from a previous session; another source
            // that saw the same order can still supply the usable id.
            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, YoungIntent,
                OpenOrdersNoMatch,
                Lookup.Match("reqCompletedOrders", ibOrderId: 0, permId: 900, isTerminal: true, filledQuantity: 10m),
                Lookup.Match("reqExecutions", ibOrderId: 42, permId: 900, isTerminal: false, filledQuantity: 10m));

            Assert.AreEqual(ClientOrderIdQueryOutcome.Found, result.Outcome);
            Assert.AreEqual("42", result.ExchangeOrderId);
        }

        [Test]
        public void FoundWithNoUsableIbOrderId_IsQueryFailedNotFound()
        {
            // IB has the order but only under a permId. Reporting Found with no exchange order id
            // would make convergence accuse the brokerage of a malformed answer; reporting the permId
            // as the exchange order id would write an id no inbound fill can ever resolve by. The
            // honest answer is "could not get what the ledger needs".
            var result = InteractiveBrokersBrokerage.CombineSourceLookups(ClientOrderId, YoungIntent,
                OpenOrdersNoMatch,
                Lookup.Match("reqCompletedOrders", ibOrderId: 0, permId: 987654321, isTerminal: true, filledQuantity: 10m),
                ExecutionsNoMatch);

            Assert.AreEqual(ClientOrderIdQueryOutcome.QueryFailed, result.Outcome);
            StringAssert.Contains("987654321", result.FailureReason);
        }

        [Test]
        public void EmptyClientOrderId_IsQueryFailedAndDoesNotThrow()
        {
            // Built without the constructor on purpose: every path into the real one starts
            // IBAutomater and an IB Gateway process. What is under test is the contract's hardest
            // requirement — QueryByClientOrderId MUST NOT THROW — on the two guards that run before
            // anything touches IB (empty id, and not connected, which is what an unstarted client
            // reports).
            var brokerage = (InteractiveBrokersBrokerage)RuntimeHelpers
                .GetUninitializedObject(typeof(InteractiveBrokersBrokerage));

            ClientOrderIdQueryResult result = null;
            Assert.DoesNotThrow(() => result = brokerage.QueryByClientOrderId(Symbols.SPY, string.Empty));
            Assert.AreEqual(ClientOrderIdQueryOutcome.QueryFailed, result.Outcome);

            Assert.DoesNotThrow(() => result = brokerage.QueryByClientOrderId(Symbols.SPY, ClientOrderId));
            Assert.AreEqual(ClientOrderIdQueryOutcome.QueryFailed, result.Outcome);
            StringAssert.Contains("not connected", result.FailureReason);
        }
    }
}
