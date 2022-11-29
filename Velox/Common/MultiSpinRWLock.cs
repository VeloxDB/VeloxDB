using System;
using System.Runtime.CompilerServices;

namespace Velox.Common;

internal unsafe sealed class MultiSpinRWLock : IDisposable
{
	const int singleThreadedLimit = 1024 * 32;

	object bufferHandle;
	RWSpinLockFair* syncs;

	public MultiSpinRWLock()
	{
		syncs = (RWSpinLockFair*)CacheLineMemoryManager.Allocate(sizeof(RWSpinLockFair), out bufferHandle);
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

	public void Dispose()
	{
		CacheLineMemoryManager.Free(bufferHandle);
	}
}
