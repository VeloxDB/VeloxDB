using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal unsafe sealed partial class ObjectStorage
{
	const int blockSize = 1024 * 256;
	const int freeListLimit = 1024;

	Class @class;
	int bufferSize;
	PerCPUData** perCPUData;
	BlockList blocks;

	FreeBufferLists freeLists;

	public ObjectStorage(Class @class, int bufferSize)
	{
		bufferSize += BufferHeader.AdditionalSize;

#if HUNT_CORRUPT
		bufferSize += 4;
#endif

		if (bufferSize % 8 != 0)
			bufferSize += 8 - bufferSize % 8;

		this.@class = @class;
		this.bufferSize = bufferSize;

		blocks = new BlockList(ProcessorNumber.CoreCount * 2);
		freeLists = new FreeBufferLists();

		IntPtr[] p = AlignedAllocator.AllocateMultiple(PerCPUData.Size, ProcessorNumber.CoreCount, true);
		perCPUData = (PerCPUData**)AlignedAllocator.Allocate(ProcessorNumber.CoreCount * sizeof(PerCPUData*));
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			perCPUData[i] = (PerCPUData*)p[i];
			perCPUData[i]->Init(bufferSize);
		}
	}

	public Class Class => @class;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static byte* GetBuffer(ulong handle)
	{
		return (byte*)handle;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool IsVersionEqual(ulong handle, ulong version)
	{
		ulong currVersion = ((BufferHeader*)(handle - BufferHeader.AdditionalSize))->version;
		return currVersion == version;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool IsBufferUsed(ulong handle, out ulong version)
	{
		version = ((BufferHeader*)(handle - BufferHeader.AdditionalSize))->version;

		// We need to make sure that version check is not reordered with other loads that follow.
		// This is needed for both x64 and ARM since both memory models allow for loads to be reordered with other loads.
		Thread.MemoryBarrier();

		return (version & 1) == 1;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool IsBufferUsed(ulong handle)
	{
		ulong version = ((BufferHeader*)(handle - BufferHeader.AdditionalSize))->version;
		return (version & 1) == 1;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool IsBufferUsed(byte* buffer)
	{
		ulong version = ((BufferHeader*)(buffer - BufferHeader.AdditionalSize))->version;
		return (version & 1) == 1;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void MarkBufferAsUsed(ulong handle)
	{
#if !X86_64
		// We want to make sure that buffer is fully initialized before increasing the version number.
		// This is not needed on x64 since stores are never reordered with other stores.
		Thread.MemoryBarrier();
#endif

		Checker.AssertFalse(IsBufferUsed(handle));
		((BufferHeader*)(handle - BufferHeader.AdditionalSize))->version++;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void MarkBufferNotUsed(ulong handle)
	{
		Checker.AssertTrue(IsBufferUsed(handle));
		((BufferHeader*)(handle - BufferHeader.AdditionalSize))->version++;

#if !X86_64
		// We want to make sure that version increased while the data in the buffer is still valid
		// (which is when this function should be called). This is not needed on x64 since stores are
		// never reordered with other stores.
		Thread.MemoryBarrier();
#endif
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ulong Allocate()
	{
		int procNum = ProcessorNumber.GetCore();
		byte* buffer = perCPUData[procNum]->Alloc(blocks, freeLists);

#if HUNT_CORRUPT
		((BufferHeader*)buffer)->sizeMarker = bufferSize;
		*((uint*)(buffer + bufferSize - 4)) = (uint)bufferSize;
#endif

		return (ulong)buffer + BufferHeader.AdditionalSize;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AllocateMultiple(ulong* handles, int count)
	{
		int procNum = ProcessorNumber.GetCore();
		perCPUData[procNum]->AllocMultiple((byte**)handles, count, blocks, freeLists);

#if HUNT_CORRUPT
		for (int i = 0; i < count; i++)
		{
			byte* buffer = (byte*)handles[i];
			((BufferHeader*)buffer)->sizeMarker = bufferSize;
			*((int*)(buffer + bufferSize - 4)) = bufferSize;

			handles[i] = (ulong)buffer + BufferHeader.AdditionalSize;
		}
#else
		for (int i = 0; i < count; i++)
		{
			handles[i] = handles[i] + BufferHeader.AdditionalSize;
		}
#endif
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Free(ulong handle)
	{
		Checker.AssertFalse(IsBufferUsed(handle));
		byte* buffer = (byte*)(handle - BufferHeader.AdditionalSize);

#if HUNT_CORRUPT
		int size = ((BufferHeader*)buffer)->sizeMarker;
		if (size != bufferSize)
			throw new CriticalDatabaseException();

		int size2 = *((int*)(buffer + size - 4));
		if (size != size2)
			throw new CriticalDatabaseException();
#endif

		int procNum = ProcessorNumber.GetCore();
		perCPUData[procNum]->Free(buffer, freeLists);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void FreeMultiple(ulong* handles, int count)
	{
		for (int i = 0; i < count; i++)
		{
			Free(handles[i]);
		}
	}

	public ScanRange[] SplitScanRange(long itemsPerRange, int workerCount, out long totalCount)
	{
		long totalPartialCount = 0;
		byte*[] partialBlocks = new byte*[ProcessorNumber.CoreCount];
		int[] partialBlockSizes = new int[ProcessorNumber.CoreCount];
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			perCPUData[i]->LockAndCollect(out byte* partialBlock, out int partialBlockSize);
			partialBlocks[i] = partialBlock;
			partialBlockSizes[i] = partialBlockSize;
			totalPartialCount += partialBlockSize / bufferSize;
		}

		blocks.GetArray(out IntPtr[] fullBlocks, out int fullBlockCount);

		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			perCPUData[i]->Unlock();
		}

		int buffersPerBlock = blockSize / bufferSize;
		totalCount = totalPartialCount + buffersPerBlock * fullBlockCount;

		Utils.Range[] indexRanges = Utils.SplitRange(totalCount, itemsPerRange, workerCount);

		int rem = 0;
		while (totalPartialCount > 0)
		{
			if (indexRanges[indexRanges.Length - 1 - rem].Count > totalPartialCount)
			{
				indexRanges[indexRanges.Length - 1 - rem].Count -= totalPartialCount;
				totalPartialCount = 0;
			}
			else
			{
				totalPartialCount -= indexRanges[indexRanges.Length - 1 - rem].Count;
				indexRanges[indexRanges.Length - 1 - rem].Count = 0;
				rem++;
			}
		}

		ScanRange[] scanRanges = new ScanRange[indexRanges.Length - rem + 1];
		scanRanges[scanRanges.Length - 1] = new PartialScanRange(@class, partialBlocks, partialBlockSizes, bufferSize);

		for (int i = 0; i < indexRanges.Length - rem; i++)
		{
			scanRanges[i] = new NormalScanRange(@class, fullBlocks, (int)indexRanges[i].Offset / buffersPerBlock,
				(int)((indexRanges[i].Offset % buffersPerBlock) * bufferSize), indexRanges[i].Count, bufferSize);
		}

		return scanRanges;
	}

	public ScanRange[] SplitDisposableScanRange(long itemsPerRange, out long totalCount)
	{
		long totalPartialCount = 0;
		int partialBlockCount = ProcessorNumber.CoreCount;
		byte*[] partialBlocks = new byte*[ProcessorNumber.CoreCount];
		int[] partialBlockSizes = new int[ProcessorNumber.CoreCount];
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			perCPUData[i]->LockAndCollect(out byte* partialBlock, out int partialBlockSize);
			partialBlocks[i] = partialBlock;
			partialBlockSizes[i] = partialBlockSize;
			totalPartialCount += partialBlockSize / bufferSize;
		}

		blocks.GetArray(out IntPtr[] fullBlocks, out int fullBlockCount);

		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			perCPUData[i]->Unlock();
		}

		int buffersPerBlock = blockSize / bufferSize;
		totalCount = totalPartialCount + buffersPerBlock * fullBlockCount;
		long blocksPerRange = (itemsPerRange + buffersPerBlock - 1) / buffersPerBlock;

		List<ScanRange> scanRanges = new List<ScanRange>((int)((partialBlockCount + fullBlockCount) / blocksPerRange) + 1);
		List<IntPtr> currBlocks = new List<IntPtr>((int)blocksPerRange);
		List<int> currSizes = new List<int>((int)blocksPerRange);

		while (partialBlockCount > 0)
		{
			if (fullBlockCount > 0)
			{
				currBlocks.Add(fullBlocks[--fullBlockCount]);
				currSizes.Add(blockSize);
			}
			else
			{
				currBlocks.Add((IntPtr)partialBlocks[partialBlockCount - 1]);
				currSizes.Add(partialBlockSizes[--partialBlockCount]);
			}

			if (currBlocks.Count == blocksPerRange || partialBlockCount == 0)
			{
				scanRanges.Add(new ScanAndDisposeRange(@class, currBlocks.ToArray(), currSizes.ToArray(), bufferSize));
				currBlocks.Clear();
				currSizes.Clear();
			}
		}

		return scanRanges.ToArray();
	}

	public void Dispose(bool disposeBlocks = true)
	{
		if (disposeBlocks)
		{
			blocks.GetArray(out IntPtr[] items, out int count);
			for (int i = 0; i < count; i++)
			{
				AlignedAllocator.Free(items[i]);
			}
		}

		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			perCPUData[i]->Dispose(disposeBlocks);
		}

		AlignedAllocator.Free((IntPtr)perCPUData[0]);
		AlignedAllocator.Free((IntPtr)perCPUData);

		perCPUData = null;
		blocks = null;
	}

	[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
	private unsafe struct PerCPUData
	{
		public const int Size = 32;

		byte* allocBlock;
		BufferHeader* freeList;
		int allocOffset;
		RWSpinLock sync;
		int bufferSize;
		int freeCount;

		public void Init(int bufferSize)
		{
			this.bufferSize = bufferSize;
			sync = new RWSpinLock();
			allocBlock = null;
			allocOffset = blockSize;    // So that new block is allocated the first time
			freeList = null;
			freeCount = 0;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public byte* Alloc(BlockList blocks, FreeBufferLists sharedFreeLists)
		{
			sync.EnterWriteLock();
			byte* buffer = AllocInternal(blocks, sharedFreeLists);
			sync.ExitWriteLock();
			return buffer;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void AllocMultiple(byte** buffers, int count, BlockList blocks, FreeBufferLists sharedFreeLists)
		{
			sync.EnterWriteLock();

			for (int i = 0; i < count; i++)
			{
				buffers[i] = AllocInternal(blocks, sharedFreeLists);
			}

			sync.ExitWriteLock();
		}

		private byte* AllocInternal(BlockList blocks, FreeBufferLists sharedFreeLists)
		{
			if (freeList == null)
				freeList = (BufferHeader*)sharedFreeLists.Get(out freeCount);

			byte* buffer;
			if (freeList != null)
			{
				buffer = (byte*)freeList;
				freeList = freeList->next;
				freeCount--;
			}
			else
			{
				if (allocOffset + bufferSize > blockSize)
				{
					if (allocBlock != null)
						blocks.Add((IntPtr)allocBlock);

					AllocateNewBlock();
				}

				buffer = allocBlock + allocOffset;
				((BufferHeader*)buffer)->version = 0;
				allocOffset += bufferSize;
			}

			Checker.AssertFalse(IsBufferUsed(buffer + BufferHeader.AdditionalSize));

			return buffer;
		}

		public void Free(byte* buffer, FreeBufferLists sharedFreeLists)
		{
			sync.EnterWriteLock();

			((BufferHeader*)buffer)->next = freeList;
			freeList = (BufferHeader*)buffer;
			freeCount++;

			if (freeCount == freeListLimit)
			{
				sharedFreeLists.Add((IntPtr)freeList);
				freeList = null;
				freeCount = 0;
			}

			sync.ExitWriteLock();
		}

		public void LockAndCollect(out byte* allocBlock, out int allocOffset)
		{
			sync.EnterWriteLock();
			allocBlock = this.allocBlock;
			allocOffset = allocBlock == null ? 0 : this.allocOffset;
		}

		public void Unlock()
		{
			sync.ExitWriteLock();
		}

		private void AllocateNewBlock()
		{
			allocBlock = (byte*)AlignedAllocator.Allocate(blockSize, false);
			allocOffset = 0;
		}

		public void Dispose(bool disposeBlocks)
		{
			if (disposeBlocks && allocBlock != null)
				AlignedAllocator.Free((IntPtr)allocBlock);
		}
	}

	private class BlockList
	{
		readonly object sync = new object();
		int count;
		IntPtr[] items;

		public BlockList(int capacity)
		{
			items = new IntPtr[capacity];
		}

		public void Add(IntPtr block)
		{
			lock (sync)
			{
				if (items.Length == count)
					Array.Resize(ref items, items.Length * 2);

				items[count++] = block;
			}
		}

		public void GetArray(out IntPtr[] items, out int count)
		{
			lock (sync)
			{
				items = this.items;
				count = this.count;
			}
		}
	}

	private class FreeBufferLists
	{
		readonly object sync = new object();

		int count;
		IntPtr[] lists;

		public FreeBufferLists()
		{
			lists = new IntPtr[64];
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Add(IntPtr list)
		{
			lock (sync)
			{
				if (lists.Length == count)
					Array.Resize(ref lists, lists.Length * 2);

				lists[count++] = list;
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public IntPtr Get(out int listCount)
		{
			if (count == 0)
			{
				listCount = 0;
				return IntPtr.Zero;
			}

			lock (sync)
			{
				if (count == 0)
				{
					listCount = 0;
					return IntPtr.Zero;
				}

				listCount = freeListLimit;
				return lists[--count];
			}
		}
	}

	public abstract class ScanContext
	{
		public abstract void Processed();
	}

	public abstract class ScanRange
	{
		Class @class;

		public ScanRange(Class @class)
		{
			this.@class = @class;
		}

		public Class Class => @class;
		public abstract ulong Next(ref ScanContext scanContext);
	}

	public sealed class NormalScanRange : ScanRange
	{
		IntPtr[] blocks;
		int bufferSize;
		byte* currBlock;
		int currBlockIndex;
		int currOffset;
		long count;

		public NormalScanRange(Class @class, IntPtr[] blocks, int startBlockIndex, int startOffset, long count, int bufferSize) :
			base(@class)
		{
			this.blocks = blocks;
			this.currBlockIndex = startBlockIndex;
			this.currOffset = startOffset;
			this.count = count;
			this.bufferSize = bufferSize;

			currBlock = (byte*)blocks[currBlockIndex];
		}

		public override ulong Next(ref ScanContext scanContext)
		{
			if (count == 0)
				return 0;

			if (currOffset + bufferSize > blockSize)
			{
				currBlock = (byte*)blocks[++currBlockIndex];
				currOffset = 0;
			}

			IntPtr res = (IntPtr)(currBlock + currOffset);
			currOffset += bufferSize;
			count--;

			return (ulong)res + BufferHeader.AdditionalSize;
		}
	}

	public sealed class PartialScanRange : ScanRange
	{
		byte*[] blocks;
		int[] sizes;
		byte* currBlock;
		int bufferSize;
		int currSize;
		int currBlockIndex;
		int currOffset;

		public PartialScanRange(Class @class, byte*[] blocks, int[] sizes, int bufferSize) :
			base(@class)
		{
			this.blocks = blocks;
			this.sizes = sizes;
			this.bufferSize = bufferSize;
			currBlock = blocks[0];
			currSize = sizes[0];
		}

		public override ulong Next(ref ScanContext scanContext)
		{
			while (currOffset + bufferSize > currSize)
			{
				if (currBlockIndex == blocks.Length - 1)
					return 0;

				currBlock = (byte*)blocks[++currBlockIndex];
				currSize = sizes[currBlockIndex];
				currOffset = 0;
			}

			IntPtr res = (IntPtr)(currBlock + currOffset);
			currOffset += bufferSize;
			return (ulong)(res + BufferHeader.AdditionalSize);
		}
	}

	/// <summary>
	/// Collects blocks that have been scanned by the disposable scan range. These blocks must not be deallocated until the processing
	/// of objects is finished. This is signaled from the outside by calling the Processed method.
	/// </summary>
	public sealed class DisposableScanContext : ScanContext
	{
		List<IntPtr> blocksToDispose = new List<IntPtr>();

		public void AddDisposableBlock(IntPtr block)
		{
			blocksToDispose.Add(block);
		}

		public override void Processed()
		{
			if (blocksToDispose.Count == 0)
				return;

			for (int i = 0; i < blocksToDispose.Count; i++)
			{
				AlignedAllocator.Free(blocksToDispose[i]);
			}

			blocksToDispose.Clear();
		}
	}

	public sealed class ScanAndDisposeRange : ScanRange
	{
		IntPtr[] blocks;
		int[] sizes;
		byte* currBlock;
		int bufferSize;
		int currSize;
		int currBlockIndex;
		int currOffset;

		public ScanAndDisposeRange(Class @class, IntPtr[] blocks, int[] sizes, int bufferSize) :
			base(@class)
		{
			this.blocks = blocks;
			this.sizes = sizes;
			this.bufferSize = bufferSize;
			currBlock = (byte*)blocks[0];
			currSize = sizes[0];
		}

		public override ulong Next(ref ScanContext scanContext)
		{
			while (currOffset + bufferSize > currSize)
			{
				// We finished a single block, so we can free it
				if (currBlock != null)
				{
					if (scanContext == null)
						scanContext = new DisposableScanContext();

					((DisposableScanContext)scanContext).AddDisposableBlock((IntPtr)currBlock);
					currBlock = null;
				}

				if (currBlockIndex == blocks.Length - 1)
					return 0;

				currBlock = (byte*)blocks[++currBlockIndex];
				currSize = sizes[currBlockIndex];
				currOffset = 0;
			}

			IntPtr res = (IntPtr)(currBlock + currOffset);
			currOffset += bufferSize;
			return (ulong)(res + BufferHeader.AdditionalSize);
		}
	}

	[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
	private struct BufferHeader
	{
#if HUNT_CORRUPT
		public const int AdditionalSize = 12;
		public int sizeMarker;
#else
		public const int AdditionalSize = 8;
#endif
		public ulong version;
		public BufferHeader* next;
	}
}
