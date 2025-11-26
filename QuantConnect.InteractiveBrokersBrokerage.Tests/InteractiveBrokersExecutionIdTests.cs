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
using QuantConnect.Algorithm;
using QuantConnect.Brokerages.InteractiveBrokers;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Securities;
using QuantConnect.Tests.Engine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace QuantConnect.Tests.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// Integration tests for ExecutionId support in Interactive Brokers brokerage
    /// Tests verify that OrderEvents contain ExecutionId and GetExecutionHistory works correctly
    /// </summary>
    [TestFixture]
    public class InteractiveBrokersExecutionIdTests
    {
        private InteractiveBrokersBrokerage _brokerage;
        private List<Order> _orders = new List<Order>();
        private List<OrderEvent> _orderEvents = new List<OrderEvent>();

        [SetUp]
        public void Setup()
        {
            Log.LogHandler = new NUnitLogHandler();

            var securityProvider = new SecurityProvider();
            // Use EURUSD forex for testing (trades 24/5, no after-hours issues)
            var eurusd = Symbols.EURUSD;
            securityProvider[eurusd] = new Security(
                SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork),
                new SubscriptionDataConfig(
                    typeof(TradeBar),
                    eurusd,
                    Resolution.Minute,
                    TimeZones.NewYork,
                    TimeZones.NewYork,
                    false,
                    false,
                    false
                ),
                new Cash(Currencies.USD, 0, 1m),
                SymbolProperties.GetDefault(Currencies.USD),
                ErrorCurrencyConverter.Instance,
                RegisteredSecurityDataTypesProvider.Null,
                new SecurityCache()
            );

            _brokerage = new InteractiveBrokersBrokerage(
                new QCAlgorithm(),
                new OrderProvider(_orders),
                securityProvider);

            // Subscribe to order events to collect ExecutionIds
            _brokerage.OrdersStatusChanged += (sender, events) =>
            {
                _orderEvents.AddRange(events);
                foreach (var e in events)
                {
                    Log.Trace($"OrderEvent: Status={e.Status}, ExecutionId={e.ExecutionId ?? "(null)"}");
                }
            };

            _brokerage.Connect();
            Assert.IsTrue(_brokerage.IsConnected, "Failed to connect to IB");
        }

        [TearDown]
        public void Teardown()
        {
            try
            {
                Log.Trace("-----");
                Log.Trace("InteractiveBrokersExecutionIdTests.Teardown(): Starting cleanup...");
                Log.Trace("-----");

                // Cancel all open orders
                var openOrders = _brokerage.GetOpenOrders();
                foreach (var order in openOrders)
                {
                    _brokerage.CancelOrder(order);
                }
                Thread.Sleep(2000);

                Log.Trace($"Teardown: Processed {_orderEvents.Count} order events");
                _orders.Clear();
                _orderEvents.Clear();
            }
            catch (Exception ex)
            {
                Log.Error($"Teardown error: {ex.Message}");
            }
            finally
            {
                _brokerage?.Dispose();
            }
        }

        /// <summary>
        /// Tests that OrderEvent for filled orders contains non-empty ExecutionId
        /// This verifies the EmitOrderFill modification is working correctly
        /// </summary>
        [Test]
        [Explicit("Requires live IB Paper Trading connection")]
        public void OrderFillEvent_ShouldContainExecutionId()
        {
            // Arrange
            var orderTag = $"Test_{DateTime.UtcNow:yyyyMMddHHmmss}";
            // Use 25000 units to meet IDEALPRO minimum requirement
            var order = new MarketOrder(Symbols.EURUSD, 25000, DateTime.UtcNow, orderTag);
            _orders.Add(order);

            var fillEvent = new ManualResetEvent(false);
            EventHandler<List<OrderEvent>> fillHandler = (sender, events) =>
            {
                if (events.Any(e => e.Status == OrderStatus.Filled || e.Status == OrderStatus.PartiallyFilled))
                {
                    fillEvent.Set();
                }
            };

            _brokerage.OrdersStatusChanged += fillHandler;

            try
            {
                // Act
                Log.Trace($"Placing market order: {order.Symbol}, Quantity={order.Quantity}, Tag={orderTag}");
                _brokerage.PlaceOrder(order);
                var filled = fillEvent.WaitOne(TimeSpan.FromSeconds(30));

                // Assert
                Assert.IsTrue(filled, "Order did not fill within 30 seconds");

                var fillEvents = _orderEvents.Where(e =>
                    e.Status == OrderStatus.Filled || e.Status == OrderStatus.PartiallyFilled).ToList();

                Assert.IsNotEmpty(fillEvents, "No fill events received");

                Log.Trace($"Received {fillEvents.Count} fill event(s)");

                foreach (var e in fillEvents)
                {
                    Assert.IsNotNull(e.ExecutionId, "ExecutionId is null");
                    Assert.IsNotEmpty(e.ExecutionId, "ExecutionId is empty");
                    Log.Trace($"✅ Fill event has ExecutionId: {e.ExecutionId}, FillPrice={e.FillPrice}, FillQuantity={e.FillQuantity}");
                }
            }
            finally
            {
                _brokerage.OrdersStatusChanged -= fillHandler;
            }
        }

        /// <summary>
        /// Tests that GetExecutionHistory returns executions within specified UTC time range
        /// and that ExecutionIds match those from OrderEvents
        /// </summary>
        [Test]
        [Explicit("Requires live IB Paper Trading connection")]
        public void GetExecutionHistory_ShouldReturnExecutionsInTimeRange()
        {
            // Arrange - Place an order first
            var orderTag = $"HistoryTest_{DateTime.UtcNow:yyyyMMddHHmmss}";
            var beforeOrderTime = DateTime.UtcNow.AddMinutes(-15);

            // Use 25000 units to meet IDEALPRO minimum requirement
            var order = new MarketOrder(Symbols.EURUSD, 25000, DateTime.UtcNow, orderTag);
            _orders.Add(order);

            var fillEvent = new ManualResetEvent(false);
            EventHandler<List<OrderEvent>> fillHandler = (sender, events) =>
            {
                if (events.Any(e => e.Status == OrderStatus.Filled))
                {
                    fillEvent.Set();
                }
            };

            _brokerage.OrdersStatusChanged += fillHandler;

            try
            {
                Log.Trace($"Placing order for history test: Tag={orderTag}, Time={DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
                _brokerage.PlaceOrder(order);
                Assert.IsTrue(fillEvent.WaitOne(TimeSpan.FromSeconds(30)), "Order did not fill");

                var afterOrderTime = DateTime.UtcNow.AddMinutes(15);
                Thread.Sleep(2000); // Wait for IB to process

                // Act - Query execution history
                Log.Trace($"Querying execution history: {beforeOrderTime:yyyy-MM-dd HH:mm:ss} to {afterOrderTime:yyyy-MM-dd HH:mm:ss} UTC");
                var executions = _brokerage.GetExecutionHistory(beforeOrderTime, afterOrderTime);

                // Assert
                Assert.IsNotNull(executions, "GetExecutionHistory returned null");
                Log.Trace($"GetExecutionHistory returned {executions.Count} execution(s)");

                Assert.IsNotEmpty(executions, "GetExecutionHistory returned no executions");

                // Get ExecutionId from OrderEvent to match with GetExecutionHistory
                var fillEvents = _orderEvents.Where(e => e.Status == OrderStatus.Filled || e.Status == OrderStatus.PartiallyFilled).ToList();
                Assert.IsNotEmpty(fillEvents, "No fill events received");

                var orderEventExecutionIds = fillEvents.Select(e => e.ExecutionId).ToList();
                Log.Trace($"OrderEvent ExecutionIds: {string.Join(", ", orderEventExecutionIds)}");

                // Find execution in history that matches our OrderEvent ExecutionId
                var matchingExecution = executions.FirstOrDefault(e => orderEventExecutionIds.Contains(e.ExecutionId));
                Assert.IsNotNull(matchingExecution,
                    $"Could not find execution with ExecutionId matching OrderEvents. " +
                    $"OrderEvent IDs: [{string.Join(", ", orderEventExecutionIds)}], " +
                    $"History IDs: [{string.Join(", ", executions.Select(e => e.ExecutionId))}]");

                Log.Trace($"✅ Found execution: Id={matchingExecution.ExecutionId}, " +
                         $"Symbol={matchingExecution.Symbol}, Time={matchingExecution.TimeUtc:yyyy-MM-dd HH:mm:ss} UTC, " +
                         $"Quantity={matchingExecution.Quantity}, Price={matchingExecution.Price}");

                // Verify time is within range
                Assert.GreaterOrEqual(matchingExecution.TimeUtc, beforeOrderTime,
                    $"Execution time {matchingExecution.TimeUtc:yyyy-MM-dd HH:mm:ss} is before start time {beforeOrderTime:yyyy-MM-dd HH:mm:ss}");
                Assert.LessOrEqual(matchingExecution.TimeUtc, afterOrderTime,
                    $"Execution time {matchingExecution.TimeUtc:yyyy-MM-dd HH:mm:ss} is after end time {afterOrderTime:yyyy-MM-dd HH:mm:ss}");

                Log.Trace("✅ ExecutionId consistency verified between OrderEvent and GetExecutionHistory");
            }
            finally
            {
                _brokerage.OrdersStatusChanged -= fillHandler;
            }
        }

        /// <summary>
        /// Tests that GetExecutionHistory correctly handles UTC time conversion
        /// All returned executions should have timestamps within the query range
        /// </summary>
        [Test]
        [Explicit("Requires live IB Paper Trading connection")]
        public void GetExecutionHistory_ShouldHandleTimezoneCorrectly()
        {
            // Test that UTC time conversion works correctly
            // Query executions from last hour
            var endTime = DateTime.UtcNow;
            var startTime = endTime.AddHours(-1);

            Log.Trace($"Querying executions from last hour: {startTime:yyyy-MM-dd HH:mm:ss} to {endTime:yyyy-MM-dd HH:mm:ss} UTC");
            var executions = _brokerage.GetExecutionHistory(startTime, endTime);

            Assert.IsNotNull(executions, "GetExecutionHistory returned null");
            Log.Trace($"✅ Retrieved {executions.Count} executions from last hour");

            foreach (var exec in executions.Take(5))
            {
                Log.Trace($"  - Execution: {exec.ExecutionId}, " +
                         $"Symbol: {exec.Symbol}, " +
                         $"Time: {exec.TimeUtc:yyyy-MM-dd HH:mm:ss} UTC, " +
                         $"Qty: {exec.Quantity}, " +
                         $"Price: {exec.Price}");

                // All times should be within the query range
                Assert.GreaterOrEqual(exec.TimeUtc, startTime,
                    $"Execution time {exec.TimeUtc:yyyy-MM-dd HH:mm:ss} is before start time {startTime:yyyy-MM-dd HH:mm:ss}");
                Assert.LessOrEqual(exec.TimeUtc, endTime,
                    $"Execution time {exec.TimeUtc:yyyy-MM-dd HH:mm:ss} is after end time {endTime:yyyy-MM-dd HH:mm:ss}");
            }

            if (executions.Count > 0)
            {
                Log.Trace("✅ All execution times are within UTC query range");
            }
        }

        /// <summary>
        /// Tests that Submit/Invalid/Cancel order events do NOT contain ExecutionId
        /// Only Fill/PartiallyFilled events should have ExecutionId
        /// </summary>
        [Test]
        [Explicit("Requires live IB Paper Trading connection")]
        public void NonFillEvents_ShouldNotContainExecutionId()
        {
            // Arrange - Place and immediately cancel an order
            // Use 25000 units to meet IDEALPRO minimum requirement
            var order = new LimitOrder(Symbols.EURUSD, 25000, 0.01m, DateTime.UtcNow); // Very low price, won't fill
            _orders.Add(order);

            var cancelEvent = new ManualResetEvent(false);
            EventHandler<List<OrderEvent>> cancelHandler = (sender, events) =>
            {
                if (events.Any(e => e.Status == OrderStatus.Canceled))
                {
                    cancelEvent.Set();
                }
            };

            _brokerage.OrdersStatusChanged += cancelHandler;

            try
            {
                Log.Trace("Placing limit order that will be canceled");
                _brokerage.PlaceOrder(order);
                Thread.Sleep(1000);

                _brokerage.CancelOrder(order);
                var canceled = cancelEvent.WaitOne(TimeSpan.FromSeconds(10));

                Assert.IsTrue(canceled, "Order was not canceled within 10 seconds");

                // Assert - Check that non-fill events don't have ExecutionId
                var submitEvents = _orderEvents.Where(e => e.Status == OrderStatus.Submitted).ToList();
                var cancelEvents = _orderEvents.Where(e => e.Status == OrderStatus.Canceled).ToList();

                Log.Trace($"Received {submitEvents.Count} Submit event(s), {cancelEvents.Count} Cancel event(s)");

                foreach (var e in submitEvents)
                {
                    Assert.IsTrue(string.IsNullOrEmpty(e.ExecutionId),
                        $"Submit event should not have ExecutionId, but got: {e.ExecutionId}");
                    Log.Trace($"✅ Submit event correctly has no ExecutionId");
                }

                foreach (var e in cancelEvents)
                {
                    Assert.IsTrue(string.IsNullOrEmpty(e.ExecutionId),
                        $"Cancel event should not have ExecutionId, but got: {e.ExecutionId}");
                    Log.Trace($"✅ Cancel event correctly has no ExecutionId");
                }
            }
            finally
            {
                _brokerage.OrdersStatusChanged -= cancelHandler;
            }
        }
    }
}
