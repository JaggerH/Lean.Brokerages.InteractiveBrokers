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
using IBApi;

namespace QuantConnect.Brokerages.InteractiveBrokers.Client
{
    /// <summary>
    /// Event arguments class for the <see cref="InteractiveBrokersClient.CompletedOrder"/> event.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="OpenOrderEventArgs"/> there is no separate order id argument: IB's
    /// <c>completedOrder</c> callback does not carry one, because a completed order may come from a
    /// previous session where this client's id sequence never applied. The id, when there is one,
    /// is <c>Order.OrderId</c>; when IB leaves that 0 the durable identifier is <c>Order.PermId</c>.
    /// </remarks>
    public sealed class CompletedOrderEventArgs : EventArgs
    {
        /// <summary>
        /// The Contract class attributes describe the contract.
        /// </summary>
        public Contract Contract { get; }

        /// <summary>
        /// The Order class attributes define the details of the order, including its OrderRef.
        /// </summary>
        public Order Order { get; }

        /// <summary>
        /// The orderState attributes; <see cref="IBApi.OrderState.Status"/> carries the completed status.
        /// </summary>
        public OrderState OrderState { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CompletedOrderEventArgs"/> class
        /// </summary>
        public CompletedOrderEventArgs(Contract contract, Order order, OrderState orderState)
        {
            Contract = contract;
            Order = order;
            OrderState = orderState;
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        public override string ToString()
        {
            return $"OrderId: {Order?.OrderId}, PermId: {Order?.PermId}, Contract: {Contract}, OrderStatus: {OrderState?.Status}";
        }
    }
}
