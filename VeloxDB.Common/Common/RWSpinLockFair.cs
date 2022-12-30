using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace VeloxDB.Common;

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal struct RWSpinLockFair
{
	const int yieldAfterInit = 1024;
	const int sleepAfterYields = 600;

	const int writerBitMask = unchecked((int)0x80000000);
	const int writerWaitingBitMask = unchecked((int)0x40000000);
	const int writerBitsMask = unchecked((int)(writerBitMask | writerWaitingBitMask));
	const int readerBitsMask = unchecked((int)~writerBitsMask);

	int state;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void EnterWriteLock()
	{
		int count = 0;
		int yieldCount = 0;
		int yieldAfter = yieldAfterInit;

		while (true)
		{
			int s = state;
			if ((s == 0 || s == writerWaitingBitMask) && Interlocked.CompareExchange(ref state, writerBitMask, s) == s)
				return;

			if ((s & writerWaitingBitMask) == 0)
				Interlocked.CompareExchange(ref state, s | writerWaitingBitMask, s);

			YieldOrSleep(ref yieldAfter, sleepAfterYields, ref count, ref yieldCount);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitWriteLock()
	{
#if !X86_64
		Thread.MemoryBarrier();
#endif

		Checker.AssertTrue((state & writerBitMask) != 0);
		state = 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void EnterReadLock()
	{
		int count = 0;
		int yieldCount = 0;
		int yieldAfter = yieldAfterInit;

		while (true)
		{
			int s = state;
			if ((s & writerBitsMask) == 0 && Interlocked.CompareExchange(ref state, s + 1, s) == s)
				return;

			YieldOrSleep(ref yieldAfter, sleepAfterYields, ref count, ref yieldCount);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool TryEnterReadLock(int timeout)
	{
		int count = 0;
		int yieldCount = 0;
		int yieldAfter = yieldAfterInit;

		long tstart = Stopwatch.GetTimestamp();

		while (true)
		{
			int s = state;
			if ((s & writerBitsMask) == 0 && Interlocked.CompareExchange(ref state, s + 1, s) == s)
				return true;

			YieldOrSleep(ref yieldAfter, sleepAfterYields, ref count, ref yieldCount);

			long tend = Stopwatch.GetTimestamp();
			if ((tend - tstart) * 1000 / Stopwatch.Frequency > timeout)
				return false;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitReadLock()
	{
		do
		{
			int s = state;
			Checker.AssertFalse((s & readerBitsMask) == 0);

			if (Interlocked.CompareExchange(ref state, ((s & readerBitsMask) - 1) | (s & writerWaitingBitMask), s) == s)
				return;
		}
		while (true);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private static void YieldOrSleep(ref int yieldAfter, int sleepAfterYields, ref int count, ref int yieldCount)
	{
		count++;
		if (count == yieldAfter)
		{
			count = 0;
			yieldAfter = Math.Min(4000, yieldAfter * 2);

			yieldCount++;
			if (yieldCount >= sleepAfterYields)
			{
				Thread.Sleep(1);
			}
			else
			{
				Thread.Yield();
			}
		}
	}
}
