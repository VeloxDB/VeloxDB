using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Velox.Common;

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal unsafe struct CompactLongMap
{
	public const int BucketsOffset = 8;

	const int mapLimit = 8;

	int count;
	int capacity;

	public int Count => count;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void GetBufferRequirements(int count, out int capacity, out int byteSize)
	{
		capacity = count <= mapLimit ? count : (int)Utils.GetNextPow2((uint)count * 2);
		byteSize = BucketsOffset + capacity * sizeof(long);
	}

	public static CompactLongMap* Create(byte* buffer, int capacity)
	{
		CompactLongMap* map = (CompactLongMap*)buffer;
		long* buckets = (long*)(buffer + BucketsOffset);

		map->count = 0;
		map->capacity = capacity;

		if (capacity > mapLimit)
			Utils.ZeroMemory((byte*)buckets, capacity * sizeof(long));

		return map;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool TryRemove(long id, long* buffer)
	{
		if (capacity <= mapLimit)
		{
			for (var i = 0; i < count; i++)
			{
				if (buffer[i] == id)
				{
					buffer[i] = buffer[count - 1];
					count--;
					return true;
				}
			}

			return false;
		}

		return TryRemoveFromMap(id, buffer);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Add(long id, long* buffer)
	{
		if (capacity <= mapLimit)
		{
			buffer[count++] = id;
			return;
		}

		AddToMap(id, buffer);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool TryRemoveFromMap(long id, long* buffer)
	{
		var capacityMask = (uint)capacity - 1;
		var bucket = (int)(HashUtils.GetHash64((ulong)id, 1) & capacityMask);

		while (true)
		{
			var node = buffer[bucket];

			if (node == id)
				break;

			if (node == 0)
				return false;

			bucket = (int)(bucket + 1 & capacityMask);
		}

		buffer[bucket] = long.MinValue;
		count--;

		return true;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void AddToMap(long id, long* buffer)
	{
		var capacityMask = (uint)capacity - 1;
		var bucket = (int)(HashUtils.GetHash64((ulong)id, 1) & capacityMask);
		while (true)
		{
			if (buffer[bucket] == 0)
				break;

			bucket = (int)(bucket + 1 & capacityMask);
		}

		buffer[bucket] = id;
		count++;

		Checker.AssertFalse(count == capacity);
	}
}
