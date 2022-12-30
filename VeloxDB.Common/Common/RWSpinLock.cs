using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace VeloxDB.Common;

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal struct RWSpinLock
{
	const int yieldAfterInit = 1024;
	const int sleepAfterYields = 600;

	int state;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void EnterWriteLock()
	{
		int count = 0;
		int yieldCount = 0;
		int yieldAfter = yieldAfterInit;

		do
		{
			int s = state;
			if (s == 0 && Interlocked.CompareExchange(ref state, int.MaxValue, 0) == 0)
				return;

			YieldOrSleep(ref yieldAfter, sleepAfterYields, ref count, ref yieldCount);
		}
		while (true);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitWriteLock()
	{
#if !X86_64
		Thread.MemoryBarrier();
#endif

		state = 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void EnterReadLock()
	{
		int count = 0;
		int yieldCount = 0;
		int yieldAfter = yieldAfterInit;

		do
		{
			int s = state;
			if (s != int.MaxValue && Interlocked.CompareExchange(ref state, s + 1, s) == s)
				return;

			YieldOrSleep(ref yieldAfter, sleepAfterYields, ref count, ref yieldCount);
		}
		while (true);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitReadLock()
	{
		Checker.AssertFalse(state == 0 || state == int.MaxValue);
		Interlocked.Decrement(ref state);
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
