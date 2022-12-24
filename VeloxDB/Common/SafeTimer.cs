using System;
using System.Diagnostics;
using System.Threading;

namespace VeloxDB.Common;

internal sealed class SafeTimer : IDisposable
{
	static readonly double rFreq = 1.0 / Stopwatch.Frequency;

	readonly object sync = new object();

	Timer timer;
	TimerCallback callback;
	int period;
	bool disposed;

	public SafeTimer(TimerCallback callback, object state, int dueTime, int period, Action<SafeTimer> timerSetter)
	{
		this.callback = callback;
		timer = new Timer(Callback, state, Timeout.Infinite, Timeout.Infinite);
		timerSetter(this);
		timer.Change(dueTime, Timeout.Infinite);
	}

	public void ModifyIntervalAsync(int dueTime, int period)
	{
		Utils.RunAsObservedTask(() =>
		{
			lock (sync)
			{
				if (disposed)
					return;

				this.period = period;
				timer.Change(dueTime, period);
			}
		});
	}

	private void Callback(object state)
	{
		long t = Stopwatch.GetTimestamp();
		lock (sync)
		{
			if (disposed)
				return;

			callback(state);

			if (disposed)	// In case it was disposed during the callback
				return;

			long rem = Math.Max(0, period - (long)((Stopwatch.GetTimestamp() - t) * rFreq));
			timer.Change(rem, Timeout.Infinite);
		}
	}

	public void Dispose()
	{
		lock (sync)
		{
			if (disposed)
				return;

			disposed = true;
			timer.Dispose();
		}

		System.GC.SuppressFinalize(this);
	}
}
