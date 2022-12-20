using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace VeloxDB.Common;

internal unsafe sealed class BlobStorage
{
	MemoryManager memoryManager;

	public BlobStorage(MemoryManager memoryManager)
	{
		this.memoryManager = memoryManager;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ulong AllocBlob(bool isNull, int size, out byte* buffer)
	{
		Checker.AssertFalse(!isNull && size < 4);

		if (isNull)
		{
			buffer = null;
			return 0;
		}

		ulong handle = memoryManager.Allocate((int)size + sizeof(int) + sizeof(ulong));
		buffer = memoryManager.GetBuffer(handle);
		((int*)buffer)[0] = 1;
		buffer += sizeof(int) + sizeof(ulong);
		TTTrace.Write(handle);
		return handle;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ulong GetVersion(ulong handle)
	{
		if (handle == 0)
			return ulong.MaxValue;

		return *((ulong*)(memoryManager.GetBuffer(handle) + sizeof(int)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetVersion(ulong handle, ulong version)
	{
		if (handle == 0)
			return;

		*((ulong*)(memoryManager.GetBuffer(handle) + sizeof(int))) = version;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* RetrieveBlob(ulong handle)
	{
		if (handle == 0)
			return null;

		return memoryManager.GetBuffer(handle) + sizeof(int) + sizeof(ulong);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void IncRefCount(ulong handle)
	{
		if (handle == 0)
			return;

		TTTrace.Write(handle);
		NativeInterlocked* t = (NativeInterlocked*)memoryManager.GetBuffer(handle);
		t->Increment();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void DecRefCount(ulong handle)
	{
		if (handle == 0)
			return;

		TTTrace.Write(handle);
		NativeInterlocked* t = (NativeInterlocked*)memoryManager.GetBuffer(handle);
		if (t->Decrement() == 0)
			memoryManager.Free(handle);
	}

#if TEST_BUILD
	public void Validate(Dictionary<ulong, int> d)
	{
		foreach (KeyValuePair<ulong, int> kv in d)
		{
			if (kv.Key == 0)
				continue;

			ulong handle = kv.Key;
			int* buffer = (int*)memoryManager.GetBuffer(handle);
			if (buffer[0] != kv.Value)
				throw new InvalidOperationException();
		}
	}
#endif
}
