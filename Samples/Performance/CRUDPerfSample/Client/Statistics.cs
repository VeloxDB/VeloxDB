using System;
using System.Diagnostics;
using System.Drawing;
using System.Runtime.InteropServices;
using VeloxDB.Common;

namespace Client;

internal class Statistics
{
	string title, format;
	Timer timer;
	Stopwatch stopwatch;
	long prevCount;
	Stopwatch totalTime;

	ParallelCounter counter;

	public Statistics(string title)
	{
		this.format = title + ": {0} T/s";
		this.title = title;

		counter = new ParallelCounter();
	}

	public void Start()
	{
		stopwatch = Stopwatch.StartNew();
		totalTime = Stopwatch.StartNew();
		timer = new Timer(p =>
		{
			double time = stopwatch.Elapsed.TotalSeconds;
			stopwatch.Restart();
			long t = prevCount;
			prevCount = counter.Count;
			long rate = (long)((prevCount - t) / time);
			Console.WriteLine(format, rate);
		}, null, 1000, 1000);
	}

	public void Inc()
	{
		counter.Inc();
	}

	public void Stop()
	{
		totalTime.Stop();
		timer.Dispose();
	}

	public void Write()
	{
		Console.WriteLine("{0}: {1} T/s", title, counter.Count / totalTime.Elapsed.TotalSeconds);
	}
}
