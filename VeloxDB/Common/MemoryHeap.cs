using System;
using System.Collections.Generic;
using System.Diagnostics;
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
		size += sizeof(long);

		if (size % 8 != 0)
			size += 8 - size % 8;

		if (size > MaxManagedSize)
		{
			IntPtr p = NativeAllocator.Allocate(size);
			*((int*)p) = int.MaxValue;
			return IntPtr.Add(p, sizeof(long));
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

			IntPtr result;
			if (bufferSize - size < MinManagedSize)
			{
				*((int*)p) = bufferSize;
				result = IntPtr.Add(p, sizeof(long));
#if TEST_BUILD
				byte* limit1 = (byte*)result + (size - sizeof(long));
				TTTrace.Write((ulong)result, (ulong)limit1);
				Utils.FillMemory((byte*)result, size - sizeof(long), 0xfa);
#endif
				return result;
			}

			*((int*)p) = size;
			IntPtr tp = IntPtr.Add(p, size);
			bufferSize -= size;

			bySize.Add(new SizeAddress(bufferSize, tp), 0);
			byAddress.Add((long)tp, bufferSize);

			result = IntPtr.Add(p, sizeof(long));
#if TEST_BUILD
			byte* limit2 = (byte*)result + (size - sizeof(long));
			TTTrace.Write((ulong)result, (ulong)limit2);
			Utils.FillMemory((byte*)result, size - sizeof(long), 0xfa);
#endif
			return result;
		}
	}

	public void Free(IntPtr buffer)
	{
		buffer = IntPtr.Add(buffer, -sizeof(long));
		int size = *((int*)buffer);

		if (size == int.MaxValue)
		{
			NativeAllocator.Free(buffer);
			return;
		}

#if TEST_BUILD
		if (size < 0 || size > MaxManagedSize)
			throw new CriticalDatabaseException();

		byte* limit1 = (byte*)buffer + sizeof(long);
		byte* limit2 = (byte*)limit1 + (size - sizeof(long));
		TTTrace.Write((ulong)limit1, (ulong)limit2);
		Utils.FillMemory((byte*)buffer + sizeof(long), size - sizeof(long), 0xdf);
#endif

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
