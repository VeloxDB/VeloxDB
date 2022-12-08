using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Velox.Common;

internal unsafe sealed partial class MemoryManager : IDisposable
{
	const int bufferSizesCount = 14;
	const byte largeBufferSizeIndex = 0x7f;
	const ulong bufferAddressMask = 0x0000ffffffffffff;
	const int bufferSizeIndexPos = 56;
	const byte largeBufferIndex = 255;
	const int freeListLimitSize = 1024 * 256;

	static readonly short* bufferSizes;

	readonly object disposeSync = new object();

	List<IntPtr> blocks;
	PerCPUData** perCPUData;
	MemoryHeap heap;

	FreeBufferLists[] freeLists;

	bool disposed;

	static MemoryManager()
	{
		short[] sizes = new short[bufferSizesCount] { 16, 32, 64, 96, 128, 192, 256, 384, 512, 768, 1024, 2048, 4098, 8192 };
		bufferSizes = (short*)AlignedAllocator.Allocate(sizes.Length * sizeof(short));
		for (int i = 0; i < sizes.Length; i++)
		{
			bufferSizes[i] = sizes[i];
		}
	}

	public MemoryManager()
	{
		heap = new MemoryHeap();

		blocks = new List<IntPtr>();
		freeLists = new FreeBufferLists[bufferSizesCount];
		for (int i = 0; i < freeLists.Length; i++)
		{
			freeLists[i] = new FreeBufferLists(bufferSizes[i]);
		}

		IntPtr[] p = AlignedAllocator.AllocateMultiple(PerCPUData.Size, ProcessorNumber.CoreCount, true);
		perCPUData = (PerCPUData**)AlignedAllocator.Allocate(sizeof(PerCPUData*) * ProcessorNumber.CoreCount);

		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			perCPUData[i] = (PerCPUData*)p[i];
			perCPUData[i]->Init((BufferList*)(p[i] + PerCPUData.HeaderSize));
		}
	}

	public static int MaxManagedSize => bufferSizes[bufferSizesCount - 1];

	public FixedAccessor RegisterFixedConsumer(int bufferSize)
	{
#if HUNT_CORRUPT
		bufferSize += 8;
#endif

		int sizeIndex = FindFixedAllocator(bufferSize);
		if (sizeIndex == largeBufferIndex)
			throw new ArgumentException("Invalid fixed buffer size.");

#if HUNT_CORRUPT
		return new FixedAccessor(this, bufferSize - 8, bufferSizes[sizeIndex], sizeIndex);
#else
		return new FixedAccessor(this, bufferSize, bufferSizes[sizeIndex], sizeIndex);
#endif
	}

	public static StaticFixedAccessor RegisterStaticFixedConsumer(int bufferSize)
	{
#if HUNT_CORRUPT
		bufferSize += 8;
#endif

		int sizeIndex = FindFixedAllocator(bufferSize);
		if (sizeIndex == largeBufferIndex)
			throw new ArgumentException("Invalid fixed buffer size.");

#if HUNT_CORRUPT
		return new StaticFixedAccessor(bufferSize - 8, bufferSizes[sizeIndex], sizeIndex);
#else
		return new StaticFixedAccessor(bufferSize, bufferSizes[sizeIndex], sizeIndex);
#endif
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* GetBuffer(ulong handle)
	{
#if HUNT_CORRUPT
		if (handle == 0)
			return null;

		return (byte*)((handle & bufferAddressMask) + 4);
#else
		return (byte*)(handle & bufferAddressMask);
#endif
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* Allocate(int bufferSize, out ulong handle)
	{
		handle = Allocate(bufferSize);
		return GetBuffer(handle);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ulong Allocate(int bufferSize)
	{
#if HUNT_CORRUPT
		bufferSize += 8;
#endif

		int sizeIndex = FindFixedAllocator(bufferSize);
		if (sizeIndex == largeBufferIndex)
		{
			byte* buffer = (byte*)heap.Allocate(bufferSize);
			return (ulong)buffer | ((ulong)largeBufferSizeIndex << bufferSizeIndexPos);
		}

#if HUNT_CORRUPT
		return AllocateFixed(bufferSize - 8, bufferSizes[sizeIndex], sizeIndex);
#else
		return AllocateFixed(bufferSize, bufferSizes[sizeIndex], sizeIndex);
#endif
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ulong AllocateFixed(int originalSize, int bufferSize, int sizeIndex)
	{
		Checker.AssertTrue(bufferSize == bufferSizes[sizeIndex]);
		int procNum = ProcessorNumber.GetCore();
		byte* buffer = perCPUData[procNum]->Alloc(bufferSize, sizeIndex, blocks, freeLists[sizeIndex]);

#if HUNT_CORRUPT
		*((uint*)buffer) = (uint)originalSize;
		*((uint*)(buffer + 4 + originalSize)) = (uint)originalSize;
#endif

		return (ulong)buffer | ((ulong)sizeIndex << bufferSizeIndexPos);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void FreeOptional(ulong handle)
	{
		if (handle == 0)
			return;

		Free(handle);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Free(ulong handle)
	{
		int sizeIndex = GetSizeIndex(handle);
		byte* buffer = (byte*)(handle & bufferAddressMask);

		if (sizeIndex == largeBufferSizeIndex)
		{
			heap.Free((IntPtr)buffer);
			return;
		}

#if HUNT_CORRUPT
		uint originalSize = *((uint*)buffer);
		if (originalSize > bufferSizes[sizeIndex] - 8)
			throw new CriticalDatabaseException();

		uint originalSize2 = *((uint*)(buffer + 4 + originalSize));
		if (originalSize != originalSize2)
			throw new CriticalDatabaseException();
#endif

		int procNum = ProcessorNumber.GetCore();
		perCPUData[procNum]->Free(buffer, sizeIndex, freeLists[sizeIndex]);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SafeFree(Action freeAction)
	{
		lock (disposeSync)
		{
			if (disposed)
				return;

			freeAction();
		}
	}

	private static int FindFixedAllocator(int size)
	{
		if (size > bufferSizes[bufferSizesCount - 1])
			return largeBufferIndex;

		int low = 0;
		int high = bufferSizesCount;
		while (low != high)
		{
			int mid = (low + high) >> 1;
			if (bufferSizes[mid] < size)
			{
				low = mid + 1;
			}
			else
			{
				high = mid;
			}
		}

		Checker.AssertTrue(bufferSizes[high] >= size);
		return high;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static int GetSizeIndex(ulong handle)
	{
		return (int)(handle >> bufferSizeIndexPos);
	}

	public void Dispose()
	{
		lock (disposeSync)
		{
			if (disposed)
				return;

			heap.Dispose();

			for (int i = 0; i < blocks.Count; i++)
			{
				AlignedAllocator.Free(blocks[i]);
			}

			AlignedAllocator.Free((IntPtr)perCPUData[0]);
			AlignedAllocator.Free((IntPtr)perCPUData);

			perCPUData = null;
			blocks.Clear();
			disposed = true;
		}
	}

	public sealed class FixedAccessor
	{
		MemoryManager owner;
		int originalSize;
		int bufferSize;
		int sizeIndex;

		public FixedAccessor(MemoryManager owner, int originalSize, int bufferSize, int sizeIndex)
		{
			this.owner = owner;
			this.originalSize = originalSize;
			this.bufferSize = bufferSize;
			this.sizeIndex = sizeIndex;
		}

		public int BufferSize => bufferSize;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public ulong Allocate()
		{
			return owner.AllocateFixed(originalSize, bufferSize, sizeIndex);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Free(ulong handle)
		{
			owner.Free(handle);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void FreeMultiple(ulong* handles, int count)
		{
			for (int i = 0; i < count; i++)
			{
				owner.Free(handles[i]);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public byte* GetBuffer(ulong handle)
		{
			return owner.GetBuffer(handle);
		}
	}

	public class StaticFixedAccessor
	{
		int originalSize;
		int bufferSize;
		int sizeIndex;

		public StaticFixedAccessor(int originalSize, int bufferSize, int sizeIndex)
		{
			this.originalSize = originalSize;
			this.bufferSize = bufferSize;
			this.sizeIndex = sizeIndex;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public byte* Allocate(MemoryManager memoryManager)
		{
			return memoryManager.GetBuffer(memoryManager.AllocateFixed(originalSize, bufferSize, sizeIndex));
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Free(MemoryManager memoryManager, byte* buffer)
		{
#if HUNT_CORRUPT
			buffer -= 4;
#endif

			ulong handle = (ulong)buffer | ((ulong)sizeIndex << bufferSizeIndexPos);
			memoryManager.Free(handle);
		}
	}

	[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
	private unsafe struct PerCPUData
	{
		public const int BlockSize = 1024 * 1024;

		public const int HeaderSize = 24;
		public const int Size = bufferSizesCount * BufferList.Size + HeaderSize;

		byte* allocBlock;
		BufferList* perSizeFreeLists;
		int allocOffset;
		RWSpinLock sync;

		public void Init(BufferList* perSizeFreeLists)
		{
			sync = new RWSpinLock();
			allocBlock = null;
			allocOffset = BlockSize;    // So that new block is allocated the first time
			this.perSizeFreeLists = perSizeFreeLists;
		}

		public byte* Alloc(int bufferSize, int bufferSizeIndex, List<IntPtr> blocks, FreeBufferLists freeLists)
		{
			sync.EnterWriteLock();
			byte* buffer = AllocInternal(bufferSize, bufferSizeIndex, blocks, freeLists);
			sync.ExitWriteLock();
			return buffer;
		}

		public void AllocMultiple(byte** buffers, int count, int bufferSize,
			int bufferSizeIndex, List<IntPtr> blocks, FreeBufferLists sharedFreeLists)
		{
			sync.EnterWriteLock();

			for (int i = 0; i < count; i++)
			{
				buffers[i] = AllocInternal(bufferSize, bufferSizeIndex, blocks, sharedFreeLists);
			}

			sync.ExitWriteLock();
		}

		private byte* AllocInternal(int bufferSize, int bufferSizeIndex, List<IntPtr> blocks, FreeBufferLists sharedFreeLists)
		{
			BufferList* freeList = &perSizeFreeLists[bufferSizeIndex];

			if (freeList->buffer == null)
				*freeList = sharedFreeLists.Get();

			byte* buffer;
			if (freeList->buffer != null)
			{
				buffer = freeList->buffer;
				freeList->buffer = *((byte**)buffer);
				freeList->count--;
			}
			else
			{
				if (allocOffset + bufferSize > BlockSize)
					AllocateNewBlock(blocks);

				buffer = allocBlock + allocOffset;
				allocOffset += bufferSize;
			}

			return buffer;
		}

		public void Free(byte* buffer, int bufferSizeIndex, FreeBufferLists sharedFreeLists)
		{
			sync.EnterWriteLock();

			BufferList* freeList = &perSizeFreeLists[bufferSizeIndex];

			*((byte**)buffer) = freeList->buffer;
			freeList->buffer = buffer;
			freeList->count++;

			if (freeList->count == sharedFreeLists.LimitCount)
			{
				sharedFreeLists.Add(*freeList);
				*freeList = new BufferList();
			}

			sync.ExitWriteLock();
		}

		private void AllocateNewBlock(List<IntPtr> blocks)
		{
			allocBlock = (byte*)AlignedAllocator.Allocate(BlockSize, false);
			allocOffset = 0;

			lock (blocks)
			{
				blocks.Add((IntPtr)allocBlock);
			}
		}
	}

	[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
	public struct BufferList
	{
		public const int Size = 16;

		public byte* buffer;
		public long count;
	}

	private class FreeBufferLists
	{
		readonly object sync = new object();

		int limitCount;
		int count;
		BufferList[] lists;

		public FreeBufferLists(int bufferSize)
		{
			this.limitCount = freeListLimitSize / bufferSize;
			lists = new BufferList[64];
		}

		public int LimitCount => limitCount;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Add(BufferList list)
		{
			lock (sync)
			{
				if (lists.Length == count)
					Array.Resize(ref lists, lists.Length * 2);

				lists[count++] = list;
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public BufferList Get()
		{
			if (count == 0)
				return new BufferList();

			lock (sync)
			{
				if (count == 0)
					return new BufferList();

				return lists[--count];
			}
		}
	}
}
