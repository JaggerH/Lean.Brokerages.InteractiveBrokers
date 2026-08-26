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

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// What to do with an inbound execution that resolved to no local order — IB's half of the
    /// unowned-fill contract. Decided by
    /// <see cref="InteractiveBrokersBrokerage.ClassifyUnownedExecution"/>.
    ///
    /// Spec: main repo docs/superpowers/specs/2026-08-26-unowned-fill-handling.md.
    /// </summary>
    internal enum UnownedExecutionDisposition
    {
        /// <summary>
        /// Not ours: an IB forced liquidation, or an order placed by hand in TWS. Build a shell
        /// order and hand it to Lean so the fill reaches the engine account.
        /// </summary>
        Adopt,

        /// <summary>
        /// A forced liquidation whose OrderRef nonetheless carries our client order id shape.
        /// Adopt it anyway — a liquidation must be followed or holdings silently fall behind the
        /// account — but raise the ledger alarm too, so the "ledger lost a write / second writer"
        /// condition is not buried by the adoption.
        /// </summary>
        AdoptAndAlarm,

        /// <summary>
        /// OrderRef carries our client order id shape but no local order matches, and IB is not
        /// calling it a liquidation. That is the ledger-loss alarm, not an adoption candidate:
        /// inventing an order here would silence the loudest signal this system has.
        /// </summary>
        LedgerAlarmOnly,

        /// <summary>
        /// Local orders DO carry this IB order id, but none on the execution's symbol. Adopting
        /// would double-book against the order that really owns the id. Log and drop.
        /// </summary>
        SymbolMismatch,

        /// <summary>
        /// Pre-adoption behaviour: log and drop. Retained only so the old decision table stays
        /// expressible; production no longer returns it.
        /// </summary>
        Drop
    }
}
