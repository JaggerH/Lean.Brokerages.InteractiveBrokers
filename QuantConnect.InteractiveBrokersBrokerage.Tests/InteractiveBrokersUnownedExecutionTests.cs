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
using System.Linq;
using System.Reflection;
using IBApi;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Orders;
using QuantConnect.Orders.Ledger;
using QuantConnect.Securities;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Locks IB's half of the unowned-fill contract: what happens to an execution that resolves to
    /// no local order. The decision is a pure function, so none of this needs an IB Gateway.
    ///
    /// Spec: main repo docs/superpowers/specs/2026-08-26-unowned-fill-handling.md.
    /// Recon: docs/research/2026-08-26-ibkr-unowned-fill-recon.md.
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersUnownedExecutionTests
    {
        /// <summary>A client order id in this system's shape, minted by the real generator.</summary>
        private static readonly string OurKey = OrderKeyGenerator.Build(7, 42);

        private static UnownedExecutionDisposition Classify(bool isLiquidation, int candidateCount, string orderRef)
        {
            return InteractiveBrokersBrokerage.ClassifyUnownedExecution(isLiquidation, candidateCount, orderRef);
        }

        [Test]
        public void OurKeyShapeIsNeverAdopted()
        {
            // The reverse lock. Our shape with no local order means the ledger lost a write or a
            // second writer exists — the loudest condition this system has. Adopting it would
            // invent an order and silence the alarm forever.
            Assert.AreEqual(UnownedExecutionDisposition.LedgerAlarmOnly, Classify(false, 0, OurKey),
                "our own key shape with no local order is an alarm, not an adoption candidate");
        }

        [Test]
        public void ManualOrderIsAdopted()
        {
            // An order placed by hand in TWS: no OrderRef of ours, no local order. Before this it
            // was logged and dropped, leaving the engine account behind the real one.
            Assert.AreEqual(UnownedExecutionDisposition.Adopt, Classify(false, 0, string.Empty));
            Assert.AreEqual(UnownedExecutionDisposition.Adopt, Classify(false, 0, null));
            Assert.AreEqual(UnownedExecutionDisposition.Adopt, Classify(false, 0, "hand-typed-ref"));
        }

        [Test]
        public void LiquidationIsAlwaysAdopted()
        {
            // IB's own Liquidation flag is authoritative. A liquidation MUST be followed: refusing
            // leaves holdings behind the account with no second chance at the fill.
            Assert.AreEqual(UnownedExecutionDisposition.Adopt, Classify(true, 0, string.Empty));
            Assert.AreEqual(UnownedExecutionDisposition.Adopt, Classify(true, 3, "someone-elses-ref"));
        }

        [Test]
        public void LiquidationCarryingOurKeyIsAdoptedAndAlarmed()
        {
            // Both facts are true and neither may be dropped: follow the position, and still say
            // the ledger cannot account for the key.
            Assert.AreEqual(UnownedExecutionDisposition.AdoptAndAlarm, Classify(true, 0, OurKey));
        }

        [Test]
        public void SameIdOnAnotherSymbolIsNotAdopted()
        {
            // Local orders DO hold this IB order id, just none on this symbol. Adopting would
            // double-book against the order that really owns the id.
            Assert.AreEqual(UnownedExecutionDisposition.SymbolMismatch, Classify(false, 2, string.Empty));
            Assert.AreEqual(UnownedExecutionDisposition.SymbolMismatch, Classify(false, 1, OurKey));
        }

        /// <summary>
        /// Builds a brokerage that never touches IB. The multi-arg constructors run
        /// <c>Initialize</c>, which STARTS IBAutomater, so the offline seam is the parameterless
        /// constructor plus the two fields <see cref="InteractiveBrokersBrokerage.GetOrder"/> reads.
        /// </summary>
        private static InteractiveBrokersBrokerage NewOfflineBrokerage(out List<QuantConnect.Orders.Order> adopted)
        {
            var brokerage = new InteractiveBrokersBrokerage();

            Set(brokerage, "_orderProvider", new OrderProvider());
            Set(brokerage, "_symbolMapper",
                new InteractiveBrokersSymbolMapper(new Dictionary<SecurityType, Dictionary<string, string>>()));

            var captured = new List<QuantConnect.Orders.Order>();
            brokerage.NewBrokerageOrderNotification += (_, e) => captured.Add(e.Order);
            adopted = captured;
            return brokerage;
        }

        private static void Set(object target, string field, object value)
        {
            var info = target.GetType().GetField(field, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(info, $"field {field} not found — the offline seam moved");
            info.SetValue(target, value);
        }

        private static IB.ExecutionDetailsEventArgs NewExecution(int liquidation, string side, string orderRef)
        {
            var contract = new Contract { Symbol = "SPY", SecType = IB.SecurityType.Stock, Currency = "USD", Exchange = "SMART" };
            var execution = new Execution
            {
                OrderId = 987654,
                ExecId = "0001-liq-1",
                Shares = 100,
                Side = side,
                Liquidation = liquidation,
                OrderRef = orderRef ?? string.Empty
            };
            return new IB.ExecutionDetailsEventArgs(requestId: 0, contract, execution);
        }

        [Test]
        public void LiquidationReachesLeanThroughTheAdoptionNotification()
        {
            // The link the whole IB half rests on, and until now only a code-reading claim: a
            // forced liquidation with no local order must leave through NewBrokerageOrderNotification,
            // which is what ExternalOrderAdoptionBrokerageMessageHandler listens on. If an upstream
            // change or a refactor breaks this, the strategy silently stops following liquidations.
            var brokerage = NewOfflineBrokerage(out var adopted);

            var order = brokerage.GetOrder(NewExecution(liquidation: 1, side: "SLD", orderRef: string.Empty));

            Assert.AreEqual(1, adopted.Count, "a liquidation with no local order must be handed to Lean");
            Assert.AreSame(order, adopted[0]);
            Assert.AreEqual(-100m, order.Quantity, "SLD is a sell, so the adopted shell must be negative");
            Assert.AreEqual("Brokerage Liquidation", order.Tag);
        }

        [Test]
        public void ManualOrderReachesLeanThroughTheSameNotification()
        {
            var brokerage = NewOfflineBrokerage(out var adopted);

            var order = brokerage.GetOrder(NewExecution(liquidation: 0, side: "BOT", orderRef: "typed-in-tws"));

            Assert.AreEqual(1, adopted.Count, "a hand-placed order must be adopted, not dropped");
            Assert.AreEqual(100m, order.Quantity);
            Assert.AreEqual("External Order", order.Tag, "the shell must not claim IB liquidated us");
        }

        [Test]
        public void OurKeyShapeProducesNoAdoptionNotification()
        {
            // The reverse lock, end to end: nothing may be handed to Lean, and GetOrder returns
            // null so no fill is emitted against an invented order.
            var brokerage = NewOfflineBrokerage(out var adopted);

            var order = brokerage.GetOrder(NewExecution(liquidation: 0, side: "BOT", orderRef: OurKey));

            Assert.IsNull(order);
            Assert.IsEmpty(adopted, "our key shape with no local order is an alarm, never an adoption");
        }

        [Test]
        public void UnmappableInstrumentIsDroppedLoudly_EvenWhenItIsALiquidation()
        {
            // MapSymbol runs before every judgement. An instrument this algorithm cannot name used
            // to throw straight into HandleExecutionDetails' catch - a bare Log.Error, and a
            // liquidation on it never even reached the classification. It still cannot be adopted
            // (no Symbol to build an order on), so the contract is: dropped, but announced.
            var brokerage = NewOfflineBrokerage(out var adopted);
            var messages = new List<BrokerageMessageEvent>();
            brokerage.Message += (_, m) => messages.Add(m);

            var contract = new Contract { Symbol = "US-T", SecType = "BOND", Currency = "USD", Exchange = "SMART" };
            var execution = new Execution { OrderId = 42, ExecId = "0001-bond", Shares = 5, Side = "SLD", Liquidation = 1, OrderRef = string.Empty };

            var order = brokerage.GetOrder(new IB.ExecutionDetailsEventArgs(requestId: 0, contract, execution));

            Assert.IsNull(order);
            Assert.IsEmpty(adopted, "an order without a Symbol cannot be adopted");
            var report = messages.SingleOrDefault(m => m.Code == InteractiveBrokersBrokerage.UnmappableExecutionCode);
            Assert.IsNotNull(report, "the drop must go out as a brokerage message, not only a log line");
            Assert.AreEqual(BrokerageMessageType.Warning, report.Type, "an Error halts without flattening");
            StringAssert.Contains("LIQUIDATION", report.Message);
            StringAssert.Contains("0001-bond", report.Message);
        }

        [Test]
        public void NothingIsDroppedSilently()
        {
            // Every branch either adopts or alarms. A disposition that silently discards a real
            // fill is exactly what this change removed; keep it unreachable.
            foreach (var candidateCount in new[] { 0, 1, 2 })
            {
                foreach (var isLiquidation in new[] { true, false })
                {
                    foreach (var orderRef in new[] { null, string.Empty, "external", OurKey })
                    {
                        Assert.AreNotEqual(UnownedExecutionDisposition.Drop,
                            Classify(isLiquidation, candidateCount, orderRef),
                            $"liquidation={isLiquidation}, candidates={candidateCount}, ref={orderRef ?? "<null>"}");
                    }
                }
            }
        }
    }
}
