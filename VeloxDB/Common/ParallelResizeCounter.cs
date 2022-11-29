using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Velox.Common;

internal unsafe sealed class ParallelResizeCounter : IDisposable
{
	const int singleThreadedLimit = 1024 * 32;

	object bufferHandle;
	long* deltaCounts;
	RWSpinLockFair* syncs;

	long totalCount;
	long triggerLimit;
	long countLimit;

	bool singleThreaded;
	
	public ParallelResizeCounter(long countLimit)
	{
		this.countLimit = countLimit;
		Prepare();

		deltaCounts = (long*)CacheLineMemoryManager.Allocate(sizeof(long) + sizeof(RWSpinLockFair), out bufferHandle);
		syncs = (RWSpinLockFair*)(deltaCounts + 1);
	}

	public long Count
	{
		get
		{
			long tc = totalCount;
			for (int i = 0; i < ProcessorNumber.CoreCount; i++)
			{
				long* lp = (long*)((byte*)deltaCounts + (i << AlignedAllocator.CacheLineSizeLog));
				tc += *lp;
			}

			return tc;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int EnterReadLock()
	{
		int handle = ProcessorNumber.GetCore();
		((RWSpinLockFair*)((byte*)syncs + (handle << AlignedAllocator.CacheLineSizeLog)))->EnterReadLock();
		return handle;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitReadLock(int handle)
	{
		((RWSpinLockFair*)((byte*)syncs + (handle << AlignedAllocator.CacheLineSizeLog)))->ExitReadLock();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void EnterWriteLock()
	{
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			((RWSpinLockFair*)((byte*)syncs + (i << AlignedAllocator.CacheLineSizeLog)))->EnterWriteLock();
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitWriteLock()
	{
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			((RWSpinLockFair*)((byte*)syncs + (i << AlignedAllocator.CacheLineSizeLog)))->ExitWriteLock();
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool Inc()
	{
		return Inc(ProcessorNumber.GetCore());
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool Inc(int handle)
	{
		if (singleThreaded)
		{
			long c = Interlocked.Increment(ref totalCount);
			return c > countLimit;
		}
		else
		{
			NativeInterlocked64* lp = (NativeInterlocked64*)((byte*)deltaCounts + (handle << AlignedAllocator.CacheLineSizeLog));
			long c = lp->Increment();
			if (c <= triggerLimit)
				return false;

			return TryMerge(lp, c);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool Add(long amount)
	{
		return Add(ProcessorNumber.GetCore(), amount);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool Add(int handle, long amount)
	{
		if (singleThreaded)
		{
			long c = Interlocked.Add(ref totalCount, amount);
			return c > countLimit;
		}
		else
		{
			NativeInterlocked64* lp = (NativeInterlocked64*)((byte*)deltaCounts + (handle << AlignedAllocator.CacheLineSizeLog));
			long c = lp->Add(amount);
			if (c <= triggerLimit)
				return false;

			return TryMerge(lp, c);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Dec()
	{
		Dec(ProcessorNumber.GetCore());
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Dec(int handle)
	{
		if (singleThreaded)
		{
			Interlocked.Decrement(ref totalCount);
		}
		else
		{
			NativeInterlocked64* lp = (NativeInterlocked64*)((byte*)deltaCounts + (handle << AlignedAllocator.CacheLineSizeLog));
			long c = lp->Decrement();
			if (c <= -triggerLimit)
				return;

			TryMerge(lp, c);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Sub(long amount)
	{
		Sub(ProcessorNumber.GetCore(), amount);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Sub(int handle, long amount)
	{
		if (singleThreaded)
		{
			Interlocked.Add(ref totalCount, -amount);
		}
		else
		{
			NativeInterlocked64* lp = (NativeInterlocked64*)((byte*)deltaCounts + (handle << AlignedAllocator.CacheLineSizeLog));
			long c = lp->Add(-amount);
			if (c <= -triggerLimit)
				return;

			TryMerge(lp, c);
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool TryMerge(NativeInterlocked64* lp, long c)
	{
		if (lp->CompareExchange(0, c) != c)
			return false;

		c = Interlocked.Add(ref totalCount, c);
		return c > countLimit;
	}

	public void Resized(long countLimit)
	{
		this.countLimit = countLimit;

		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			long* lp = (long*)((byte*)deltaCounts + (i << AlignedAllocator.CacheLineSizeLog));
			totalCount += *lp;
			*lp = 0;
		}

		Prepare();
	}

	public void SetCount(long count)
	{
		totalCount = count;
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			long* lp = (long*)((byte*)deltaCounts + (i << AlignedAllocator.CacheLineSizeLog));
			*lp = 0;
		}
	}

	private void Prepare()
	{
		if (countLimit > singleThreadedLimit)
		{
			singleThreaded = false;
			triggerLimit = Math.Max(8, countLimit / (64 * ProcessorNumber.CoreCount));
		}
		else
		{
			triggerLimit = 1;
			singleThreaded = true;
		}
	}

	public void Dispose()
	{
		CacheLineMemoryManager.Free(bufferHandle);
	}
}
