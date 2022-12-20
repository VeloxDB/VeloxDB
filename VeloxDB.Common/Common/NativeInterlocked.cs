using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace VeloxDB.Common;

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal struct NativeInterlocked
{
	int state;

	public int Increment()
	{
		return Interlocked.Increment(ref state);
	}

	public int Decrement()
	{
		return Interlocked.Decrement(ref state);
	}

	public int Add(int amount)
	{
		return Interlocked.Add(ref state, amount);
	}

	public int CompareExchange(int value, int comparand)
	{
		return Interlocked.CompareExchange(ref state, value, comparand);
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal struct NativeInterlocked64
{
	long state;

	public long Increment()
	{
		return Interlocked.Increment(ref state);
	}

	public long Decrement()
	{
		return Interlocked.Decrement(ref state);
	}

	public long Add(long amount)
	{
		return Interlocked.Add(ref state, amount);
	}

	public long CompareExchange(long value, long comparand)
	{
		return Interlocked.CompareExchange(ref state, value, comparand);
	}
}
