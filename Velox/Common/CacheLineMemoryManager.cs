using System;
using System.Collections.Generic;
using Velox.Common;

namespace Velox.Common;

internal unsafe static class CacheLineMemoryManager
{
	static readonly object sync = new object();

	static Dictionary<int, SingleLineMemoryManager> perSizeMap = new Dictionary<int, SingleLineMemoryManager>(4);

	public static unsafe byte* Allocate(int size, out object handle)
	{
		Checker.AssertTrue(size > 0);

		lock (sync)
		{
			if (!perSizeMap.TryGetValue(size, out SingleLineMemoryManager lineMan))
			{
				lineMan = new SingleLineMemoryManager(size);
				perSizeMap.Add(size, lineMan);
			}

			byte* buffers = lineMan.Allocate();
			handle = lineMan;
			if (lineMan.IsUsedUp)
				perSizeMap.Remove(size);

			return buffers;
		}
	}

	public static void Free(object handle)
	{
		Checker.AssertNotNull(handle);

		SingleLineMemoryManager bc = (SingleLineMemoryManager)handle;

		lock (sync)
		{
			bc.Free();
			if (bc.IsReadyForDisposal)
				bc.Dispose();
		}
	}

	private sealed class SingleLineMemoryManager
	{
		readonly int size;
		readonly int capacity;
		readonly IntPtr firstCacheLine;
		int allocCount;
		int releasedCount;

		public SingleLineMemoryManager(int size)
		{
			Checker.AssertTrue(size <= AlignedAllocator.CacheLineSize);

			this.size = size;

			capacity = AlignedAllocator.CacheLineSize / size;
			firstCacheLine = AlignedAllocator.AllocateMultiple(AlignedAllocator.CacheLineSize, ProcessorNumber.CoreCount)[0];
		}

		public bool IsUsedUp => allocCount == capacity;

		public bool IsReadyForDisposal => releasedCount == capacity;

		public byte* Allocate()
		{
			Checker.AssertTrue(allocCount < capacity);

			byte* res = (byte*)IntPtr.Add(firstCacheLine, allocCount * size);
			allocCount++;
			return res;
		}

		public void Free()
		{
			Checker.AssertTrue(releasedCount < allocCount);
			releasedCount++;
		}

		public void Dispose()
		{
			AlignedAllocator.Free(firstCacheLine);
		}
	}
}
