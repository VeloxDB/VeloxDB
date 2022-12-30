using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace VeloxDB.Common;

internal sealed class FastHashSet<TKey>
{
	const float loadFactor = 0.7f;

	int capacityMask;
	int limitCapacity;
	int count;
	int[] buckets;
	Entry<TKey>[] entries;
	IEqualityComparer<TKey> comparer;

	public FastHashSet(int capacity, IEqualityComparer<TKey> comparer = null)
	{
		capacity = Math.Max(4, (int)Utils.GetNextPow2((ulong)capacity));
		this.capacityMask = capacity - 1;
		this.limitCapacity = (int)(capacity * loadFactor);
		buckets = new int[capacity];
		entries = new Entry<TKey>[limitCapacity + 1];
		this.comparer = comparer == null ? EqualityComparer<TKey>.Default : comparer;
	}

	public int Count => count;

	public void Add(TKey key)
	{
		int bucket = GetBucket(key);
		int index = buckets[bucket];

		while (index != 0)
		{
			if (comparer.Equals(entries[index].key, key))
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

	public bool Contains(TKey key)
	{
		int bucket = GetBucket(key);
		int index = buckets[bucket];
		while (index != 0)
		{
			if (comparer.Equals(entries[index].key, key))
				return true;

			index = entries[index].next;
		}

		return false;
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
			TKey key = entries[i].key;
			int bucket = GetBucket(key);
			buckets[bucket] = 0;
			entries[i].key = default(TKey);
		}

		count = 0;
	}

	public void ForEach(Action<TKey> action)
	{
		for (int i = 1; i <= count; i++)
		{
			action(entries[i].key);
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void Resize()
	{
		FastHashSet<TKey> temp = new FastHashSet<TKey>(buckets.Length * 2);
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
	private int GetBucket(TKey key)
	{
		int hash = comparer.GetHashCode(key);
		return hash & capacityMask;
	}

	public struct Entry<K>
	{
		public K key;
		public int next;
	}
}
