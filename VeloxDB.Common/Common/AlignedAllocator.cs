using System;
using VeloxDB.Common;

namespace VeloxDB.Common;

internal unsafe static class AlignedAllocator
{
	static readonly int cacheLineSize = (int)NativeProcessorInfo.MaxCacheLineSize;
	static readonly int cacheLineSizeLog = (int)Math.Round(Math.Log(cacheLineSize, 2.0));

	static AlignedAllocator()
	{
		if (!Utils.IsPowerOf2(cacheLineSize))
			throw new CriticalDatabaseException("Cache line size not a power of two on a given platform.");   // Does this exist?
	}

	public static int CacheLineSize => cacheLineSize;
	public static int CacheLineSizeLog => cacheLineSizeLog;

	public static IntPtr Allocate(long size, bool zeroedOut = true)
	{
		return Allocate(size, cacheLineSize, zeroedOut);
	}

	public static IntPtr Allocate(long size, int alignment, bool zeroedOut = true)
	{
		Checker.AssertTrue(alignment >= 8);

		IntPtr buffer = NativeAllocator.Allocate(size + 2 * alignment, zeroedOut);

		IntPtr p = buffer;
		int t = (int)((long)p % alignment);
		if (t != 0)
			p = IntPtr.Add(p, alignment - t);

		p = IntPtr.Add(p, alignment);
		((ulong*)p)[-1] = (ulong)buffer;    // The actual allocated buffer is written in -8 offset from the returned address

		return p;
	}

	public static IntPtr[] AllocateMultiple(int size, int count, bool zeroedOut = true)
	{
		Checker.AssertTrue(count > 0);

		count++;    // One additional cache line where we will write address of the actual buffer for freeing
		if (size % cacheLineSize != 0)
			size = size + cacheLineSize - size % cacheLineSize;

		IntPtr buffer = NativeAllocator.Allocate(size * count + cacheLineSize, zeroedOut);

		IntPtr p = buffer;
		int t = (int)((long)p % cacheLineSize);
		if (t != 0)
			p = IntPtr.Add(p, cacheLineSize - t);

		p = IntPtr.Add(p, cacheLineSize);
		((ulong*)p)[-1] = (ulong)buffer;    // The actual allocated buffer is written in -8 offset from the returned address

		count--;
		IntPtr[] ptrs = new IntPtr[count];
		for (int i = 0; i < count; i++)
		{
			ptrs[i] = IntPtr.Add(p, i * size);
		}

		return ptrs;
	}

	public static IntPtr[] Allocate(int size, int count, out IntPtr firstBuffer, bool zeroedOut = true)
	{
		IntPtr[] buffers = AllocateMultiple(size, count, zeroedOut);
		firstBuffer = buffers[0];
		return buffers;
	}

	public static void Free(IntPtr buffer)
	{
		IntPtr baseBuffer = (IntPtr)(((ulong*)buffer)[-1]);
		NativeAllocator.Free(baseBuffer);
	}
}
