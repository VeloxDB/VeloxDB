using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace VeloxDB.Common;

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal struct SingleBitSpinLock64
{
	const int yieldAfterInit = 1024;
	const int sleepAfterYields = 200;

	const ulong invSyncMask = 0x7fffffffffffffff;
	const ulong syncMask = 0x8000000000000000;

	public const int Size = 8;

	long state;

	public SingleBitSpinLock64(bool dummy)
	{
		state = unchecked((long)syncMask);
	}

	public ulong Value { get => (ulong)state & invSyncMask; set => state = (long)(((ulong)state & syncMask) | value); }

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void EnterWriteLock()
	{
		int count = 0;
		int yieldCount = 0;
		int yieldAfter = yieldAfterInit;

		do
		{
			long s = state;
			if (((ulong)s & syncMask) != 0 && Interlocked.CompareExchange(ref state, (long)((ulong)s & invSyncMask), s) == s)
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

		Checker.AssertTrue(((ulong)state & 0x8000000000000000) == 0);
		state = (long)((ulong)state | syncMask);
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
