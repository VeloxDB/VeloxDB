using System;
using System.Threading;

namespace VeloxDB.Common;

internal sealed class CountingGate
{
	object sync = new object();

	int count;
	ManualResetEventSlim signal;
	bool permaOpen;

	public CountingGate()
	{
		signal = new ManualResetEventSlim(true);
	}

	public void Enter()
	{
		signal.Wait();
	}

	public void Open()
	{
		lock (sync)
		{
			TTTrace.Write(this.GetHashCode());

			if (permaOpen)
				return;

			Checker.AssertTrue(count > 0);
			count--;
			if (count == 0)
				signal.Set();
		}
	}

	public void Close()
	{
		lock (sync)
		{
			TTTrace.Write(this.GetHashCode());

			if (permaOpen)
				return;

			count++;
			if (count == 1)
				signal.Reset();
		}
	}

	public void PermanentlyOpen()
	{
		lock (sync)
		{
			permaOpen = true;
			signal.Set();
		}
	}
}
