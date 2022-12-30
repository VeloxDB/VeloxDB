using System;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace VeloxDB.Common;

internal unsafe sealed class MultiSpinLock : IDisposable
{
	readonly object bufferHandle;
	readonly RWSpinLock* syncs;

	public MultiSpinLock()
	{
		syncs = (RWSpinLock*)CacheLineMemoryManager.Allocate(sizeof(RWSpinLock), out bufferHandle);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Enter(int index)
	{
		RWSpinLock* rw = (RWSpinLock*)CacheLineMemoryManager.GetBuffer(syncs, index);
		rw->EnterWriteLock();
		return;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int Enter()
	{
		int procNum = ProcessorNumber.GetCore();
		RWSpinLock* rw = (RWSpinLock*)CacheLineMemoryManager.GetBuffer(syncs, procNum);
		rw->EnterWriteLock();
		return procNum;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Exit(int index)
	{
		RWSpinLock* rw = (RWSpinLock*)CacheLineMemoryManager.GetBuffer(syncs, index);
		rw->ExitWriteLock();
		return;
	}

	public void Dispose()
	{
		CacheLineMemoryManager.Free(bufferHandle);
	}
}
