using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace VeloxDB.Common;

internal unsafe sealed class ParallelResizeCounter : IDisposable
{
	public const int SingleThreadedLimit = 1024 * 2;

	object bufferHandle;
	long* deltaCounts;
	RWLock* syncs;

	long totalCount;
	long triggerLimit;
	long countLimit;

	bool singleThreaded;

	public ParallelResizeCounter(long countLimit)
	{
		this.countLimit = countLimit;
		Prepare();

		deltaCounts = (long*)CacheLineMemoryManager.Allocate(sizeof(long) + sizeof(RWLock), out bufferHandle);
		syncs = (RWLock*)(deltaCounts + 1);
	}

	public long Count
	{
		get
		{
			long tc = totalCount;
			for (int i = 0; i < ProcessorNumber.CoreCount; i++)
			{
				long* lp = (long*)CacheLineMemoryManager.GetBuffer(deltaCounts, i);
				tc += *lp;
			}

			return tc;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int EnterReadLock()
	{
		int handle = ProcessorNumber.GetCore();
		RWLock* rw = (RWLock*)CacheLineMemoryManager.GetBuffer(syncs, handle);
		rw->EnterReadLock();
		return handle;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitReadLock(int handle)
	{
		RWLock* rw = (RWLock*)CacheLineMemoryManager.GetBuffer(syncs, handle);
		rw->ExitReadLock();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void EnterWriteLock()
	{
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			RWLock* rw = (RWLock*)CacheLineMemoryManager.GetBuffer(syncs, i);
			rw->EnterWriteLock();
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitWriteLock()
	{
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			RWLock* rw = (RWLock*)CacheLineMemoryManager.GetBuffer(syncs, i);
			rw->ExitWriteLock();
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
			NativeInterlocked64* lp = (NativeInterlocked64*)CacheLineMemoryManager.GetBuffer(deltaCounts, handle);
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
			NativeInterlocked64* lp = (NativeInterlocked64*)CacheLineMemoryManager.GetBuffer(deltaCounts, handle);
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
			NativeInterlocked64* lp = (NativeInterlocked64*)CacheLineMemoryManager.GetBuffer(deltaCounts, handle);
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
			NativeInterlocked64* lp = (NativeInterlocked64*)CacheLineMemoryManager.GetBuffer(deltaCounts, handle);
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
			long* lp = (long*)CacheLineMemoryManager.GetBuffer(deltaCounts, i);
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
			long* lp = (long*)CacheLineMemoryManager.GetBuffer(deltaCounts, i);
			*lp = 0;
		}
	}

	private void Prepare()
	{
		if (countLimit > SingleThreadedLimit)
		{
			singleThreaded = false;
			triggerLimit = Math.Max(ProcessorNumber.CoreCount, countLimit / (64 * ProcessorNumber.CoreCount));
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
