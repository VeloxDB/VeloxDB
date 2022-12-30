using System;
using System.Runtime.CompilerServices;

namespace VeloxDB.Common;

internal sealed class LongHashSet
{
	const float loadFactor = 0.7f;

	int capacityMask;
	int limitCapacity;
	int count;
	int[] buckets;
	Entry[] entries;

	public LongHashSet(int capacity)
	{
		capacity = Math.Max(4, (int)Utils.GetNextPow2((ulong)capacity));
		this.capacityMask = capacity - 1;
		this.limitCapacity = (int)(capacity * loadFactor);
		buckets = new int[capacity];
		entries = new Entry[limitCapacity + 1];
	}

	public int Count => count;

	public bool Contains(long key)
	{
		int bucket = GetBucket(key);
		int index = buckets[bucket];
		while (index != 0)
		{
			if (entries[index].key == key)
				return true;

			index = entries[index].next;
		}

		return false;
	}

	public void Add(long key)
	{
		int bucket = GetBucket(key);
		int index = buckets[bucket];

		while (index != 0)
		{
			if (entries[index].key == key)
				return;

			index = entries[index].next;
		}

		count++;
		entries[count].key = key;
		entries[count].next = buckets[bucket];
		buckets[bucket] = count;

		if (count == limitCapacity)
			Resize();
	}

	public bool Remove(long key)
	{
		int bucket = GetBucket(key);
		int index = buckets[bucket];

		if (index == 0)
			return false;

		if (entries[index].key == key)
		{
			buckets[bucket] = entries[index].next;
		}
		else
		{
			int prevIndex = index;
			index = entries[index].next;
			while (index != 0 && entries[index].key != key)
			{
				prevIndex = index;
				index = entries[index].next;
			}

			if (index == 0)
				return false;

			entries[prevIndex].next = entries[index].next;
		}

		RemoveEntry(index);
		return true;
	}

	private void RemoveEntry(int index)
	{
		if (index == count)
		{
			count--;
			return;
		}

		entries[index] = entries[count];

		int bucket = GetBucket(entries[count].key);
		if (buckets[bucket] == count)
		{
			buckets[bucket] = index;
		}
		else
		{
			int tindex = buckets[bucket];
			while (entries[tindex].next != count)
				tindex = entries[tindex].next;

			entries[tindex].next = index;
		}

		count--;
	}

	public void Clear()
	{
		if (count >= (int)limitCapacity / 32)
		{
			Array.Clear(buckets);
			Array.Clear(entries, 1, count + 1);
			count = 0;
			return;
		}

		for (int i = 1; i <= count; i++)
		{
			long key = entries[i].key;
			int bucket = GetBucket(key);
			buckets[bucket] = 0;
		}

		count = 0;
	}

	public void ForEach(Action<long> action)
	{
		for (int i = 1; i <= count; i++)
		{
			action(entries[i].key);
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void Resize()
	{
		LongHashSet temp = new LongHashSet(buckets.Length * 2);
		for (int i = 1; i <= count; i++)
		{
			temp.Add(entries[i].key);
		}

		this.capacityMask = temp.capacityMask;
		this.limitCapacity = temp.limitCapacity;
		this.count = temp.count;
		this.buckets = temp.buckets;
		this.entries = temp.entries;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private int GetBucket(long key)
	{
		return ((int)key ^ (int)(key >> 32)) & capacityMask;
	}

	public struct Entry
	{
		public long key;
		public int next;
	}
}
