using System;
using System.Runtime.CompilerServices;
using static System.Math;

namespace VeloxDB.Common;

internal unsafe sealed class NativeList : IDisposable
{
	int itemSize;
	long capacity;
	long count;
	long offset;
	byte* buffer;

	public NativeList(long capacity, int itemSize)
	{
		this.capacity = capacity;
		this.itemSize = itemSize;

		buffer = (byte*)AlignedAllocator.Allocate((long)(capacity * itemSize), false);
	}

	public NativeList(long capacity, int itemSize, long initCount)
	{
		this.capacity = capacity;
		this.itemSize = itemSize;
		this.count = initCount;
		this.offset = initCount * itemSize;

		buffer = (byte*)AlignedAllocator.Allocate((long)(capacity * itemSize), false);
	}

	public bool IsEmpty => count == 0;
	public long Count => count;
	public byte* Buffer => buffer;
	public int ItemSize => itemSize;
	public long Capacity => capacity;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Reset()
	{
		count = 0;
		offset = 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Reset(long capacity)
	{
		count = 0;
		offset = 0;

		if (this.capacity > capacity)
		{
			this.capacity = capacity;
			AlignedAllocator.Free((IntPtr)buffer);
			buffer = (byte*)AlignedAllocator.Allocate((long)this.capacity * itemSize, false);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* Add()
	{
		if (count == capacity)
			Resize(count + 1);

		byte* res = buffer + offset;
		offset += itemSize;
		count++;
		return res;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* AddRange(long count)
	{
		if (this.count + count > capacity)
			Resize(this.count + count);

		byte* res = buffer + offset;
		offset += itemSize * count;
		this.count += count;
		return res;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* AddRangeNoResize(long count)
	{
		Checker.AssertTrue(this.count + count <= capacity);

		byte* res = buffer + offset;
		offset += itemSize * count;
		this.count += count;
		return res;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* Append(NativeList l)
	{
		Checker.AssertTrue(l.ItemSize == itemSize);

		if (this.count + l.count > capacity)
			Resize(this.count + l.count);

		byte* res = buffer + offset;
		offset += itemSize * l.count;
		this.count += l.count;
		Utils.CopyMemory(l.buffer, res, l.count * l.itemSize);
		return res;
	}

	public void CopyContent(IntPtr p)
	{
		Utils.CopyMemory(buffer, (byte*)p, count * itemSize);
	}

	public void Resize(long count)
	{
		if (count <= capacity)
			return;

		long newCapacity = Max(capacity * 2, count);
		byte* temp = (byte*)AlignedAllocator.Allocate((long)newCapacity * itemSize, false);

		if (this.count > 0)
			Utils.CopyMemory(buffer, temp, (long)this.count * itemSize);

		AlignedAllocator.Free((IntPtr)buffer);
		buffer = temp;
		capacity = newCapacity;
	}

	public void TakeContent(NativeList list)
	{
		this.count = list.count;
		this.itemSize = list.itemSize;
		this.capacity = list.capacity;

		AlignedAllocator.Free((IntPtr)buffer);
		this.buffer = list.buffer;
		list.buffer = null;
	}

	public void Dispose()
	{
		AlignedAllocator.Free((IntPtr)buffer);
		buffer = null;
	}
}
