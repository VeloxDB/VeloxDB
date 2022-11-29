using System;
using System.Runtime.CompilerServices;

namespace Velox.Common;

internal unsafe sealed class MultiSpinLock : IDisposable
{
	readonly object bufferHandle;
	readonly RWSpinLock* syncs;
	readonly RWSpinLock* sync;

	public MultiSpinLock(bool isMultilock)
	{
		if (isMultilock)
		{
			syncs = (RWSpinLock*)CacheLineMemoryManager.Allocate(sizeof(RWSpinLock), out bufferHandle);
		}
		else
		{
			sync = (RWSpinLock*)AlignedAllocator.Allocate(sizeof(RWSpinLock));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Enter(int index)
	{
		if (syncs != null)
		{
			((RWSpinLock*)((byte*)syncs + (index << AlignedAllocator.CacheLineSizeLog)))->EnterWriteLock();
			return;
		}

		sync->EnterWriteLock();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int Enter()
	{
		if (syncs != null)
		{
			int procNum = ProcessorNumber.GetCore();
			((RWSpinLock*)((byte*)syncs + (procNum << AlignedAllocator.CacheLineSizeLog)))->EnterWriteLock();
			return procNum;
		}

		sync->EnterWriteLock();
		return 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Exit(int index)
	{
		if (syncs != null)
		{
			((RWSpinLock*)((byte*)syncs + (index << AlignedAllocator.CacheLineSizeLog)))->ExitWriteLock();
			return;
		}

		sync->ExitWriteLock();
	}

	public void Dispose()
	{
		if (syncs != null)
		{
			CacheLineMemoryManager.Free(bufferHandle);
		}
		else
		{
			AlignedAllocator.Free((IntPtr)sync);
		}
	}
}
