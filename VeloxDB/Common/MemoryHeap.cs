using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace VeloxDB.Common;

internal unsafe sealed class MemoryHeap
{
	public const int BlockSize = 1024 * 1024 * 16;
	public const int MinManagedSize = 512;
	public const int MaxManagedSize = 1024 * 1024;

	readonly object sync = new object();

	List<IntPtr> blocks;
	AVLTree<long, int> byAddress;
	AVLTree<SizeAddress, int> bySize;

	public MemoryHeap()
	{
		blocks = new List<IntPtr>(512);
		byAddress = new AVLTree<long, int>(1024);
		bySize = new AVLTree<SizeAddress, int>(1024);
	}

	public int BlockCount => blocks.Count;

	public IntPtr Allocate(int size)
	{
		size += sizeof(int);

		if (size > MaxManagedSize)
		{
			IntPtr p = NativeAllocator.Allocate(size);
			*((int*)p) = int.MaxValue;
			return IntPtr.Add(p, sizeof(int));
		}

		lock (sync)
		{
			TreeItem sti = bySize.FindLargerOrEqual(new SizeAddress(size));
			if (sti.IsEmpty)
			{
				AllocateNewBlock();
				sti = bySize.FindLargerOrEqual(new SizeAddress(size));
			}

			IntPtr p = bySize.GetKey(sti).Buffer;
			int bufferSize = bySize.GetKey(sti).Size;

			TreeItem ati = byAddress.Find((long)p);
			Checker.AssertFalse(ati.IsEmpty);

			bySize.Remove(sti);
			byAddress.Remove(ati);

			if (bufferSize - size < MinManagedSize)
			{
				*((int*)p) = bufferSize;
				return IntPtr.Add(p, sizeof(int));
			}

			*((int*)p) = size;
			IntPtr tp = IntPtr.Add(p, size);
			bufferSize -= size;

			bySize.Add(new SizeAddress(bufferSize, tp), 0);
			byAddress.Add((long)tp, bufferSize);

			return IntPtr.Add(p, sizeof(int));
		}
	}

	public void Free(IntPtr buffer)
	{
		buffer = IntPtr.Add(buffer, -sizeof(int));
		int size = *((int*)buffer);

		if (size == int.MaxValue)
		{
			NativeAllocator.Free(buffer);
			return;
		}

		lock (sync)
		{
			TreeItem ati = byAddress.FindSmaller((long)buffer);
			if (!ati.IsEmpty)
			{
				IntPtr prevBuffer = (IntPtr)byAddress.GetKey(ati);
				int prevSize = byAddress.GetValue(ati);
				if (IntPtr.Add(prevBuffer, prevSize) == buffer)
				{
					TreeItem sti = bySize.Find(new SizeAddress(prevSize, prevBuffer));
					Checker.AssertFalse(sti.IsEmpty);
					byAddress.Remove(ati);
					bySize.Remove(sti);
					buffer = prevBuffer;
					size += prevSize;
				}
			}

			ati = byAddress.FindLarger((long)buffer);
			if (!ati.IsEmpty)
			{
				IntPtr nextBuffer = (IntPtr)byAddress.GetKey(ati);
				int nextSize = byAddress.GetValue(ati);
				if (IntPtr.Add(buffer, size) == nextBuffer)
				{
					TreeItem sti = bySize.Find(new SizeAddress(nextSize, nextBuffer));
					byAddress.Remove(ati);
					bySize.Remove(sti);
					size += nextSize;
				}
			}

			byAddress.Add((long)buffer, size);
			bySize.Add(new SizeAddress(size, buffer), 0);
		}
	}

	private void AllocateNewBlock()
	{
		IntPtr block = AlignedAllocator.Allocate(BlockSize, false);
		blocks.Add(block);
		byAddress.Add((long)block, BlockSize);
		bySize.Add(new SizeAddress(BlockSize, block), 0);
	}

	public void Dispose()
	{
		for (int i = 0; i < blocks.Count; i++)
		{
			AlignedAllocator.Free(blocks[i]);
		}
	}

#if TEST_BUILD
	public int GetBufferCount()
	{
		return byAddress.Count;
	}
#endif

	private struct SizeAddress : IComparable<SizeAddress>
	{
		int size;
		IntPtr buffer;

		public SizeAddress(int size, IntPtr buffer)
		{
			this.size = size;
			this.buffer = buffer;
		}

		public SizeAddress(int size)
		{
			this.size = size;
			this.buffer = IntPtr.Zero;
		}

		public int Size => size;
		public IntPtr Buffer => buffer;

		public int CompareTo(SizeAddress other)
		{
			if (size < other.Size)
				return -1;

			if (size > other.Size)
				return 1;

			if ((long)buffer < (long)other.buffer)
				return -1;

			if ((long)buffer > (long)other.buffer)
				return 1;

			return 0;
		}
	}
}
