using System;
using System.Diagnostics;
using System.Drawing;
using System.Runtime.InteropServices;
using VeloxDB.Common;

namespace Client;

internal class Statistics
{
	string title;
	Thread timer;
	long prevCount;
	Stopwatch totalTime;
	volatile bool disposed;

	ParallelCounter counter;

	public Statistics(string title)
	{
		this.title = title;

		counter = new ParallelCounter();
	}

	public void Start()
	{
		totalTime = Stopwatch.StartNew();
		timer = new Thread(() =>
		{
			Stopwatch stopwatch = Stopwatch.StartNew();
			List<long> hist = new List<long>();
			int interval = 1;
			int longMult = 10;

			while (true)
			{
				if (disposed)
					break;

				Thread.Sleep(interval * 1000);
				double time = stopwatch.Elapsed.TotalSeconds;
				stopwatch.Restart();
				long t = prevCount;
				prevCount = counter.Count;
				long rate = (long)((prevCount - t) / time);

				if (hist.Count == longMult)
					hist.RemoveAt(0);

				hist.Add(rate);

				Console.WriteLine("{0}: {1} T/s [{2}s], {3} T/s [{4}s]", title, rate, interval, hist.Average(), interval * longMult);
			}
		});
		timer.Priority = ThreadPriority.Highest;
		timer.Start();
	}

	public void Inc()
	{
		if (disposed)
			return;

		counter.Inc();
	}

	public void Stop()
	{
		if (disposed)
			return;

		totalTime.Stop();
		disposed = true;
		timer.Join();
	}

	public void Write()
	{
		Console.WriteLine("{0}: {1} T/s", title, counter.Count / totalTime.Elapsed.TotalSeconds);
	}
}
