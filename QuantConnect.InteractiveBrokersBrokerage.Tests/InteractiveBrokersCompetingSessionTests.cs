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
using System.Linq;
using NUnit.Framework;
using QuantConnect.Brokerages;
using QuantConnect.Brokerages.InteractiveBrokers;
using IB = QuantConnect.Brokerages.InteractiveBrokers.Client;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// 10197 "No market data during competing live session": the gateway repeats it for every
    /// subscription every 30 seconds while the live username is logged in elsewhere, and during
    /// that time no tick arrives. It must reach the algorithm as a Warning - once per throttle
    /// window, not eight times a round.
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersCompetingSessionTests
    {
        private const string IbText = "No market data during competing live session";

        private static IB.ErrorEventArgs CompetingSession(int requestId) =>
            new IB.ErrorEventArgs(id: requestId, time: 0, code: IB.CompetingLiveSessionMarketDataErrorHandler.ErrorCode, message: IbText);

        [Test]
        public void ErrorCodeIs10197()
        {
            Assert.AreEqual(10197, IB.CompetingLiveSessionMarketDataErrorHandler.ErrorCode);
        }

        [Test]
        public void OneRoundOfEightSubscriptions_SurfacesOneWarning()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            var messages = new List<BrokerageMessageEvent>();
            brokerage.Message += (_, m) => messages.Add(m);

            for (var requestId = 1; requestId <= 8; requestId++)
            {
                brokerage.HandleError(this, CompetingSession(requestId));
            }

            var competing = messages.Where(m => m.Code == "10197").ToList();
            Assert.AreEqual(1, competing.Count, "one warning per throttle window, not one per subscription");
            Assert.AreEqual(BrokerageMessageType.Warning, competing[0].Type);
        }

        [Test]
        public void WarningKeepsIbTextAndExplainsTheCause()
        {
            var brokerage = new InteractiveBrokersBrokerage();
            BrokerageMessageEvent raised = null;
            brokerage.Message += (_, m) => raised = m;

            brokerage.HandleError(this, CompetingSession(5));

            Assert.IsNotNull(raised);
            StringAssert.Contains(IbText, raised.Message);
            StringAssert.Contains("competing live session: paper market data is bound to the live username", raised.Message.ToLowerInvariant());
        }

        [Test]
        public void Handler_EmitsAgainOnlyAfterFifteenMinutes()
        {
            var emitted = new List<BrokerageMessageEvent>();
            var handler = new IB.CompetingLiveSessionMarketDataErrorHandler(emitted.Add);
            var t0 = new DateTime(2026, 9, 4, 14, 52, 40, DateTimeKind.Utc);

            for (var round = 0; round < 10; round++)
            {
                // a round every 30 seconds, eight subscriptions each
                for (var requestId = 1; requestId <= 8; requestId++)
                {
                    handler.Handle(t0.AddSeconds(30 * round), IB.CompetingLiveSessionMarketDataErrorHandler.ErrorCode, IbText);
                }
            }
            Assert.AreEqual(1, emitted.Count);

            handler.Handle(t0.AddMinutes(14).AddSeconds(59), IB.CompetingLiveSessionMarketDataErrorHandler.ErrorCode, IbText);
            Assert.AreEqual(1, emitted.Count);

            handler.Handle(t0.AddMinutes(15), IB.CompetingLiveSessionMarketDataErrorHandler.ErrorCode, IbText);
            Assert.AreEqual(2, emitted.Count, "a condition that outlives the window is reported again");
            Assert.IsTrue(emitted.All(m => m.Type == BrokerageMessageType.Warning));
        }
    }
}
