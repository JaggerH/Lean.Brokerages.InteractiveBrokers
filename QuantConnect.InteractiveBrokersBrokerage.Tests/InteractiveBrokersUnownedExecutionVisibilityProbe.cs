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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using IBApi;
using NUnit.Framework;
using QuantConnect.Algorithm;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Configuration;
using QuantConnect.Securities;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Answers ONE runtime question the code cannot answer about itself: does
    /// <c>ExecutionFilter.ClientId</c> hide fills this API client did not place?
    ///
    /// Why it cannot be settled by reading: the brokerage's own source says both things.
    /// <c>InteractiveBrokersBrokerage.cs:110-111</c> pins <c>ClientId = 0</c> and notes that orders
    /// created by hand in TWS can only be cancelled/modified from client 0 — i.e. client 0 is the
    /// identity that CAN see them. <c>:1102-1125</c> asserts the opposite, that a hand-placed fill
    /// "is not in the answer". One of the two is wrong and only the gateway can say which.
    ///
    /// What this probe can and cannot establish:
    /// - CAN: whether a fill placed under a DIFFERENT api client id comes back through the
    ///   clientId-0 query, and whether it is pushed to the clientId-0 connection in real time.
    ///   Those are exactly the restart-recovery and live-adoption paths.
    /// - CANNOT: a TWS hand-placed order. IB Gateway has no trading UI, so that order cannot be
    ///   created here at all — and it is NOT equivalent to the second-client case, because TWS
    ///   orders attach to client 0, the very id the filter is set to.
    /// - CANNOT: a liquidation. IB generates those itself, under no api client, and a paper
    ///   account will not produce one on demand. Whatever client id IB stamps on a liquidation
    ///   execution stays unknown after this probe. Do not read a green result as "liquidations are
    ///   recoverable" — read it only as "the filter does not hide foreign fills".
    ///
    /// Explicit and hand-run: it launches IB Gateway, connects to the live paper account, and
    /// PLACES TWO REAL (paper) ORDERS of 1 share each, flattening both before it returns.
    /// </summary>
    [TestFixture, Explicit("Launches IB Gateway, connects to the paper account and places 1-share paper orders.")]
    public class InteractiveBrokersUnownedExecutionVisibilityProbe
    {
        private const int ForeignClientId = 2;
        private static readonly Symbol Nvda = Symbol.Create("NVDA", SecurityType.Equity, Market.USA);

        // Pre-market capable: MKT orders do not execute outside regular hours, so every order in
        // this probe is a far-through-the-market LMT with OutsideRth set. The caps are deliberately
        // wide so the order is marketable wherever NVDA opens; a 1-share paper fill at NBBO is the
        // point, not the price.
        private const decimal BuyCap = 300m;
        private const decimal SellFloor = 150m;

        [Test]
        public void ForeignClientFillVisibility()
        {
            var probeStart = DateTime.UtcNow.AddMinutes(-5);

            // The brokerage owns client id 0. IB refuses a second connection under the same id, so
            // the baseline order must go through the brokerage itself, not a second raw client.
            var orders = new List<QuantConnect.Orders.Order>();
            using var ib = new InteractiveBrokersBrokerage(new QCAlgorithm(), new OrderProvider(orders), new SecurityProvider());
            ib.Connect();
            Assert.IsTrue(ib.IsConnected, "brokerage reported itself not connected after Connect()");

            // Every order event the clientId-0 connection surfaces, so the real-time half of the
            // question is answered from the same run rather than inferred.
            var pushed = new ConcurrentBag<string>();
            ib.OrdersStatusChanged += (_, events) =>
            {
                foreach (var e in events)
                {
                    pushed.Add($"{e.Symbol.Value} {e.Status} qty={e.FillQuantity} @ {e.FillPrice}");
                }
            };

            // ---- Step 1: baseline. Our own fill must be queryable, otherwise a miss in step 2
            // means "the query does not work", not "the filter hid it".
            var ownFillSeen = PlaceAndFlattenViaBrokerage(ib, orders);
            Console.WriteLine($"[baseline] own-client fill reported: {ownFillSeen}");

            // IB batches execution reporting; querying the instant the fill lands reads as "no
            // executions" and would make the assertion below fire on a timing artefact.
            Thread.Sleep(TimeSpan.FromSeconds(10));

            var afterOwn = ib.GetExecutionHistory(probeStart, DateTime.UtcNow);
            Console.WriteLine($"[baseline] GetExecutionHistory returned {afterOwn.Count} records");
            foreach (var record in afterOwn)
            {
                Console.WriteLine($"  {record.Symbol.Value} qty={record.Quantity} id={record.ExecutionId}");
            }
            Assert.IsNotEmpty(afterOwn,
                "the clientId-0 query returned nothing even for a fill this client placed — the query " +
                "itself is broken, so nothing below can be interpreted");
            var baselineCount = afterOwn.Count;

            // ---- Step 2: the actual question. Same account, different api client id.
            var foreignFillSeen = PlaceAndFlattenViaForeignClient(ForeignClientId, out var foreignOrderId);
            Assert.IsTrue(foreignFillSeen,
                $"the client {ForeignClientId} connection never reported its own fill, so this run cannot " +
                "say anything about visibility from client 0");
            Console.WriteLine($"[foreign] client {ForeignClientId} filled, ibOrderId={foreignOrderId}");

            // IB batches execution reporting; give the report time to land before concluding absence.
            Thread.Sleep(TimeSpan.FromSeconds(10));

            var afterForeign = ib.GetExecutionHistory(probeStart, DateTime.UtcNow);
            Console.WriteLine($"[foreign] GetExecutionHistory returned {afterForeign.Count} records " +
                              $"(baseline was {baselineCount})");
            foreach (var record in afterForeign)
            {
                Console.WriteLine($"  {record.Symbol.Value} qty={record.Quantity} id={record.ExecutionId}");
            }

            Console.WriteLine("[push] order events surfaced on the clientId-0 connection:");
            foreach (var line in pushed)
            {
                Console.WriteLine($"  {line}");
            }

            // Deliberately NOT an assertion on a expected answer — this probe exists to find out
            // which answer is true, so it records the verdict and leaves the judgement to the
            // reader. It only fails when the run itself was inconclusive (handled above).
            var foreignVisible = afterForeign.Count > baselineCount;
            Console.WriteLine();
            Console.WriteLine("================ VERDICT ================");
            Console.WriteLine(foreignVisible
                ? "FOREIGN FILL IS VISIBLE through the clientId-0 execution query. The ClientId filter " +
                  "does NOT hide fills placed under another api client id, so the ':1102-1125' comment " +
                  "is wrong as written and restart recovery is not structurally blocked for them."
                : "FOREIGN FILL IS HIDDEN from the clientId-0 execution query. The ClientId filter is " +
                  "enforced against other api client ids. This still says NOTHING about a TWS hand order " +
                  "(those attach to client 0) nor about a liquidation (no api client at all).");
            Console.WriteLine($"real-time pushes observed on client 0: {pushed.Count}");
            Console.WriteLine("=========================================");

            // The baseline order left 1 share behind (single-order limitation above). Flatten it so
            // the probe is position-neutral, and do it from the raw client since client 0 is taken.
            FlattenOneShare();
        }

        /// <summary>
        /// Manufactures, from a RAW client connected as clientId 0 while no engine holds that id,
        /// the execution shape a TWS hand order leaves behind for the RESTART-RECOVERY path: an
        /// execution stamped ClientId 0 whose IB order id exists in no local order table and whose
        /// OrderRef was never set by us. It then prints every field of every execution IB returns,
        /// so the assumed shapes (Liquidation 0, empty OrderRef) are checked against real reports.
        ///
        /// What this CANNOT manufacture is the live-push half of the TWS question: an order
        /// appearing under client 0 WHILE the engine holds that connection. The API allows only one
        /// client-0 session, so that order can only come from the TWS trading UI (TWS and the
        /// gateway API are separate sessions). That half still needs a real TWS.
        ///
        /// Side effect by design: the SELL flattens the 1-share NVDA residue the 2026-08-26 run
        /// left in the paper account. Run it only when that residue exists, or it opens a short.
        /// </summary>
        [Test]
        public void ClientZeroExternalOrderShape()
        {
            using var client = new MinimalIbClient();
            client.Connect(Config.Get("ib-host", "127.0.0.1"), Config.GetInt("ib-port", 4002), 0);

            var id = client.PlaceLimitOrder(NvdaContract(), "SELL", 1, (double)SellFloor);
            Assert.IsTrue(client.WaitForFill(id, TimeSpan.FromSeconds(120)),
                "the client-0 SELL never filled — shapes below would describe nothing");

            // IB batches execution reporting; ask only after the fill has settled server-side.
            Thread.Sleep(TimeSpan.FromSeconds(5));
            client.RequestExecutions();
            Assert.IsTrue(client.WaitForExecutionsEnd(TimeSpan.FromSeconds(30)),
                "execDetailsEnd never arrived — the field-shape dump above is incomplete");
        }

        /// <summary>
        /// Sells the single share the baseline left behind, from a throwaway client id.
        /// </summary>
        private static void FlattenOneShare()
        {
            using var client = new MinimalIbClient();
            client.Connect(Config.Get("ib-host", "127.0.0.1"), Config.GetInt("ib-port", 4002), 3);
            var id = client.PlaceLimitOrder(NvdaContract(), "SELL", 1, (double)SellFloor);
            Console.WriteLine($"[cleanup] flattening baseline share, filled={client.WaitForFill(id, TimeSpan.FromSeconds(60))}");
        }

        private static Contract NvdaContract() => new()
        {
            Symbol = "NVDA",
            SecType = "STK",
            Exchange = "SMART",
            PrimaryExch = "NASDAQ",
            Currency = "USD"
        };

        /// <summary>
        /// Buys 1 NVDA through the brokerage itself (client id 0) and sells it back, so the baseline
        /// fill is genuinely OUR client's — the thing the ClientId filter is supposed to let through.
        /// </summary>
        private static bool PlaceAndFlattenViaBrokerage(InteractiveBrokersBrokerage ib,
            List<QuantConnect.Orders.Order> orders)
        {
            var filled = 0;
            var gate = new ManualResetEventSlim(false);
            EventHandler<List<QuantConnect.Orders.OrderEvent>> handler = (_, events) =>
            {
                if (events.Any(e => e.Status == QuantConnect.Orders.OrderStatus.Filled))
                {
                    Interlocked.Increment(ref filled);
                    gate.Set();
                }
            };
            ib.OrdersStatusChanged += handler;
            try
            {
                // A single order: Order.Id is read-only and defaults to 0, so two of them would
                // collide in the brokerage's id maps. The share it leaves behind is flattened by
                // the caller's cleanup, which runs on the raw client.
                var order = new QuantConnect.Orders.LimitOrder(Nvda, 1m, BuyCap, DateTime.UtcNow, "probe-baseline",
                    new QuantConnect.Orders.InteractiveBrokersOrderProperties { OutsideRegularTradingHours = true });
                orders.Add(order);
                ib.PlaceOrder(order);
                gate.Wait(TimeSpan.FromSeconds(60));
            }
            finally
            {
                ib.OrdersStatusChanged -= handler;
            }
            return filled > 0;
        }

        /// <summary>
        /// Opens a raw IB connection under <paramref name="clientId"/>, buys 1 NVDA at market, waits
        /// for the fill, then sells it back so the probe leaves no position behind.
        /// </summary>
        private static bool PlaceAndFlattenViaForeignClient(int clientId, out int ibOrderId)
        {
            using var client = new MinimalIbClient();
            client.Connect(Config.Get("ib-host", "127.0.0.1"), Config.GetInt("ib-port", 4002), clientId);

            var contract = NvdaContract();

            ibOrderId = client.PlaceLimitOrder(contract, "BUY", 1, (double)BuyCap);
            var filled = client.WaitForFill(ibOrderId, TimeSpan.FromSeconds(60));

            // Flatten regardless of what the buy reported: a fill we failed to observe is exactly
            // the case where leaving a position behind would be worst.
            var flattenId = client.PlaceLimitOrder(contract, "SELL", 1, (double)SellFloor);
            client.WaitForFill(flattenId, TimeSpan.FromSeconds(60));

            return filled;
        }

        /// <summary>
        /// The smallest IB client that can place an order and notice it filled. Inherits
        /// <see cref="DefaultEWrapper"/> so only the four callbacks that matter are overridden.
        /// </summary>
        private sealed class MinimalIbClient : DefaultEWrapper, IDisposable
        {
            private readonly EReaderMonitorSignal _signal = new();
            private readonly ManualResetEventSlim _nextIdReady = new(false);
            private readonly ConcurrentDictionary<int, ManualResetEventSlim> _fills = new();
            private readonly EClientSocket _socket;
            private EReader _reader;
            private int _nextOrderId = -1;

            public MinimalIbClient()
            {
                _socket = new EClientSocket(this, _signal);
            }

            public void Connect(string host, int port, int clientId)
            {
                _socket.eConnect(host == "LOCALHOST" ? "127.0.0.1" : host, port, clientId);
                _reader = new EReader(_socket, _signal);
                _reader.Start();
                new Thread(() =>
                {
                    while (_socket.IsConnected())
                    {
                        _signal.waitForSignal();
                        _reader.processMsgs();
                    }
                })
                { IsBackground = true }.Start();

                if (!_nextIdReady.Wait(TimeSpan.FromSeconds(30)))
                {
                    throw new TimeoutException($"client {clientId} never received nextValidId");
                }
            }

            public int PlaceLimitOrder(Contract contract, string action, decimal quantity, double limitPrice)
            {
                var orderId = Interlocked.Increment(ref _nextOrderId);
                _fills[orderId] = new ManualResetEventSlim(false);
                _socket.placeOrder(orderId, contract, new IBApi.Order
                {
                    Action = action,
                    OrderType = "LMT",
                    LmtPrice = limitPrice,
                    OutsideRth = true,
                    TotalQuantity = quantity,
                    Tif = "DAY"
                });
                return orderId;
            }

            public bool WaitForFill(int orderId, TimeSpan timeout)
                => _fills.TryGetValue(orderId, out var gate) && gate.Wait(timeout);

            private readonly ManualResetEventSlim _execEnd = new(false);

            public void RequestExecutions()
            {
                _execEnd.Reset();
                _socket.reqExecutions(9001, new ExecutionFilter());
            }

            public bool WaitForExecutionsEnd(TimeSpan timeout) => _execEnd.Wait(timeout);

            public override void execDetails(int reqId, Contract contract, Execution execution)
                => Console.WriteLine($"    [raw client] execDetails reqId={reqId} {contract.Symbol} " +
                    $"side={execution.Side} shares={execution.Shares} orderId={execution.OrderId} " +
                    $"permId={execution.PermId} clientId={execution.ClientId} " +
                    $"liquidation={execution.Liquidation} orderRef='{execution.OrderRef}' " +
                    $"execId={execution.ExecId} time={execution.Time}");

            public override void execDetailsEnd(int reqId) => _execEnd.Set();

            public override void nextValidId(int orderId)
            {
                Interlocked.Exchange(ref _nextOrderId, orderId - 1);
                _nextIdReady.Set();
            }

            public override void orderStatus(int orderId, string status, decimal filled, decimal remaining,
                double avgFillPrice, long permId, int parentId, double lastFillPrice, int clientId,
                string whyHeld, double mktCapPrice)
            {
                Console.WriteLine($"    [raw client] orderStatus id={orderId} {status} filled={filled}");
                if (status == "Filled" && _fills.TryGetValue(orderId, out var gate))
                {
                    gate.Set();
                }
            }

            public override void error(Exception e) => Console.WriteLine($"    [raw client] error: {e.Message}");

            public override void error(string str) => Console.WriteLine($"    [raw client] error: {str}");

            public override void error(int id, long errorTime, int errorCode, string errorMsg, string advancedOrderRejectJson)
                => Console.WriteLine($"    [raw client] error id={id} code={errorCode}: {errorMsg}");

            public void Dispose()
            {
                if (_socket.IsConnected())
                {
                    _socket.eDisconnect();
                }
                _signal.issueSignal();
            }
        }
    }
}
