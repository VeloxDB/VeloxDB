using System;
using System.Reflection;
using System.Reflection.Metadata;
using System.Runtime.CompilerServices;

namespace VeloxDB.Common;

internal unsafe sealed class MultiSpinRWLock : IDisposable
{
	const int singleThreadedLimit = 1024 * 32;

	object bufferHandle;
	RWLock* syncs;

	public MultiSpinRWLock()
	{
		syncs = (RWLock*)CacheLineMemoryManager.Allocate(sizeof(RWLock), out bufferHandle);
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
	public void EnterReadLock(int handle)
	{
		RWLock* rw = (RWLock*)CacheLineMemoryManager.GetBuffer(syncs, handle);
		rw->EnterReadLock();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool TryEnterReadLock(int timeout, out int handle)
	{
		if (timeout == -1)
		{
			handle = EnterReadLock();
			return true;
		}

		handle = ProcessorNumber.GetCore();
		RWLock* rw = (RWLock*)CacheLineMemoryManager.GetBuffer(syncs, handle);
		return RWLock.TryEnterReadLock(rw, timeout);
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

	public void Dispose()
	{
		CacheLineMemoryManager.Free(bufferHandle);
	}
}
