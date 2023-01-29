using System;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace VeloxDB.Common;

internal unsafe sealed class MultiSpinLock : IDisposable
{
	readonly object bufferHandle;
	readonly RWLock* syncs;

	public MultiSpinLock()
	{
		syncs = (RWLock*)CacheLineMemoryManager.Allocate(sizeof(RWLock), out bufferHandle);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Enter(int index)
	{
		RWLock* rw = (RWLock*)CacheLineMemoryManager.GetBuffer(syncs, index);
		rw->EnterWriteLock();
		return;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int Enter()
	{
		int procNum = ProcessorNumber.GetCore();
		RWLock* rw = (RWLock*)CacheLineMemoryManager.GetBuffer(syncs, procNum);
		rw->EnterWriteLock();
		return procNum;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Exit(int index)
	{
		RWLock* rw = (RWLock*)CacheLineMemoryManager.GetBuffer(syncs, index);
		rw->ExitWriteLock();
		return;
	}

	public void Dispose()
	{
		CacheLineMemoryManager.Free(bufferHandle);
	}
}
