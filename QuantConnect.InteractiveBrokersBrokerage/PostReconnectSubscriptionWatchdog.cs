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
using QuantConnect.Logging;

namespace QuantConnect.Brokerages.InteractiveBrokers
{
    /// <summary>
    /// One-shot guard for a reconnect the gateway reports as 1102 ("restored - data maintained")
    /// while the market data subscriptions were in fact dropped.
    /// </summary>
    /// <remarks>
    /// Per the IB documentation 1102 means the subscriptions survived and only 1101 requires
    /// re-requesting market data, and that is how the brokerage treats them. Gateway logs show one
    /// case where the report is wrong: a gateway that re-authenticates while another session of the
    /// same user is online ("COMPETE: session kicked out") walks the competing-session path -
    /// "Unsubscribe MD before routing table rerequest / Desubscribing all farm mkt data / Disconnect
    /// all farms due to competing session" - forces a re-login, and afterwards reports 1102 to the
    /// API. Every reqMktData is gone, nothing ticks again until the next full connect, and the farm
    /// later reports 2108 ("inactive but should be available upon demand" = no active request on it).
    /// <para>
    /// Rather than treating every 1102 as 1101 (a resubscribe on every healthy reconnect), this
    /// watchdog arms on 1102 and resubscribes once if, within the silence timeout, none of the
    /// subscribed symbols has produced a single tick since the 1102 - or earlier, if the farm reports
    /// 2108 while still silent. Outside trading hours a legitimately quiet book can trip it; the
    /// cost is one redundant resubscribe, which is the same request set the 1101 path issues.
    /// </para>
    /// </remarks>
    public sealed class PostReconnectSubscriptionWatchdog : IDisposable
    {
        /// <summary>
        /// Default time after a 1102 before total silence is taken as lost subscriptions.
        /// </summary>
        public static readonly TimeSpan DefaultSilenceTimeout = TimeSpan.FromSeconds(120);

        /// <summary>
        /// A 2108 this long after the 1102 still counts as belonging to that reconnect. 2108 also
        /// shows up in healthy reconnects, so it is only honored inside this window and only while
        /// nothing has ticked.
        /// </summary>
        public static readonly TimeSpan FarmInactiveWindow = TimeSpan.FromMinutes(5);

        private readonly TimeSpan _silenceTimeout;
        private readonly Func<IEnumerable<Symbol>> _subscribedSymbols;
        private readonly Action _resubscribe;
        private readonly ConcurrentDictionary<Symbol, DateTime> _lastTickUtc = new();
        private readonly object _lock = new();
        private readonly Timer _timer;
        private DateTime? _armedAtUtc;
        private int _restoreCount;

        /// <summary>
        /// True between a 1102 and either a tick, a resubscribe, or an explicit disarm.
        /// </summary>
        public bool IsArmed
        {
            get { lock (_lock) { return _armedAtUtc.HasValue; } }
        }

        /// <summary>
        /// Number of resubscribes this watchdog has issued.
        /// </summary>
        public int RestoreCount => Volatile.Read(ref _restoreCount);

        /// <summary>
        /// Creates a new watchdog.
        /// </summary>
        /// <param name="silenceTimeout">Silence after a 1102 that triggers the resubscribe.</param>
        /// <param name="subscribedSymbols">Symbols currently requested from the gateway.</param>
        /// <param name="resubscribe">Restores every subscription; invoked at most once per 1102.</param>
        public PostReconnectSubscriptionWatchdog(TimeSpan silenceTimeout, Func<IEnumerable<Symbol>> subscribedSymbols, Action resubscribe)
        {
            if (silenceTimeout <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(silenceTimeout), silenceTimeout, "must be positive");
            }
            _silenceTimeout = silenceTimeout;
            _subscribedSymbols = subscribedSymbols ?? throw new ArgumentNullException(nameof(subscribedSymbols));
            _resubscribe = resubscribe ?? throw new ArgumentNullException(nameof(resubscribe));
            _timer = new Timer(_ => TimerCheck(), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        }

        /// <summary>
        /// Last time a tick arrived for the symbol, if any.
        /// </summary>
        public DateTime? LastTickUtc(Symbol symbol)
        {
            return _lastTickUtc.TryGetValue(symbol, out var time) ? time : null;
        }

        /// <summary>
        /// Records a market data message for the symbol. Called on the tick path; must stay cheap.
        /// </summary>
        public void RecordTick(Symbol symbol, DateTime utcNow)
        {
            if (symbol == null)
            {
                return;
            }
            _lastTickUtc[symbol] = utcNow;
        }

        /// <summary>
        /// Gateway reported 1102: arm the silence timer. A second 1102 restarts the window.
        /// </summary>
        public void OnReconnectedDataMaintained(DateTime utcNow)
        {
            lock (_lock)
            {
                _armedAtUtc = utcNow;
                _timer.Change(_silenceTimeout, Timeout.InfiniteTimeSpan);
            }
        }

        /// <summary>
        /// Gateway reported 2108 (farm inactive). Inside the window after a 1102 and with nothing
        /// ticked since, this is the earliest evidence the subscriptions are gone: resubscribe now.
        /// </summary>
        public void OnFarmInactive(DateTime utcNow)
        {
            DateTime armedAt;
            lock (_lock)
            {
                if (!_armedAtUtc.HasValue || utcNow - _armedAtUtc.Value > FarmInactiveWindow)
                {
                    return;
                }
                armedAt = _armedAtUtc.Value;
            }

            Evaluate(armedAt, $"farm reported 2108 (inactive) {(utcNow - armedAt).TotalSeconds:F0}s after the 1102 and no subscribed symbol has ticked since");
        }

        /// <summary>
        /// Silence timer elapsed (or a test drives it): resubscribe if nothing ticked since the 1102.
        /// </summary>
        public void Check(DateTime utcNow)
        {
            DateTime armedAt;
            lock (_lock)
            {
                if (!_armedAtUtc.HasValue)
                {
                    return;
                }
                armedAt = _armedAtUtc.Value;
            }

            Evaluate(armedAt, $"no subscribed symbol ticked within {_silenceTimeout.TotalSeconds:F0}s");
        }

        /// <summary>
        /// Something else restored or dropped the connection (1100, 1101, a full connect): this
        /// 1102 no longer describes the current state.
        /// </summary>
        public void Disarm()
        {
            lock (_lock)
            {
                _armedAtUtc = null;
                _timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            }
        }

        private void Evaluate(DateTime armedAt, string reason)
        {
            var symbols = _subscribedSymbols().ToList();
            if (symbols.Count == 0)
            {
                // nothing to restore
                Disarm();
                return;
            }

            var ticked = symbols.Where(s => _lastTickUtc.TryGetValue(s, out var t) && t >= armedAt).ToList();
            if (ticked.Count > 0)
            {
                Log.Trace($"PostReconnectSubscriptionWatchdog: gateway reported 1102 and {ticked.Count}/{symbols.Count} subscribed symbols ticked since - subscriptions confirmed alive");
                Disarm();
                return;
            }

            lock (_lock)
            {
                // another path may have disarmed or refired while we were looking
                if (!_armedAtUtc.HasValue || _armedAtUtc.Value != armedAt)
                {
                    return;
                }
                _armedAtUtc = null;
                _timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                Interlocked.Increment(ref _restoreCount);
            }

            Log.Trace($"PostReconnectSubscriptionWatchdog: gateway reported 1102 but {reason} ({symbols.Count} symbols) - resubscribing. " +
                      "A gateway that re-logged under a competing session desubscribes all farm market data yet reports 1102.");
            try
            {
                _resubscribe();
            }
            catch (Exception err)
            {
                Log.Error($"PostReconnectSubscriptionWatchdog: resubscribe failed: {err}");
            }
        }

        private void TimerCheck()
        {
            try
            {
                Check(DateTime.UtcNow);
            }
            catch (Exception err)
            {
                // timer thread: an escape here would take the process down
                Log.Error($"PostReconnectSubscriptionWatchdog.TimerCheck(): {err}");
            }
        }

        /// <summary>
        /// Stops the timer.
        /// </summary>
        public void Dispose()
        {
            _timer.Dispose();
        }
    }
}
