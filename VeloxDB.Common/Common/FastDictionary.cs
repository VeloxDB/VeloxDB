using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace VeloxDB.Common;

internal sealed class FastDictionary<TKey, TValue>
{
	const float loadFactor = 0.7f;

	int capacityMask;
	int limitCapacity;
	int count;
	int[] buckets;
	Entry<TKey, TValue>[] entries;
	IEqualityComparer<TKey> comparer;

	public FastDictionary(int capacity, IEqualityComparer<TKey> comparer = null)
	{
		capacity = Math.Max(4, (int)Utils.GetNextPow2((ulong)capacity));
		this.capacityMask = capacity - 1;
		this.limitCapacity = (int)(capacity * loadFactor);
		buckets = new int[capacity];
		entries = new Entry<TKey, TValue>[limitCapacity + 1];
		this.comparer = comparer == null ? EqualityComparer<TKey>.Default : comparer;
	}

	public int Count => count;

	public TValue this[TKey key]
	{
		get
		{
			int bucket = GetBucket(key);
			int index = buckets[bucket];
			while (index != 0)
			{
				if (comparer.Equals(entries[index].key, key))
					return entries[index].value;

				index = entries[index].next;
			}

			throw new KeyNotFoundException();
		}

		set
		{
			int bucket = GetBucket(key);
			int index = buckets[bucket];
			while (index != 0)
			{
				if (comparer.Equals(entries[index].key, key))
				{
					entries[index].value = value;
					return;
				}

				index = entries[index].next;
			}

			Add(key, value);
		}
	}

	public void Add(TKey key, TValue value)
	{
		int bucket = GetBucket(key);

#if DEBUG
		int index = buckets[bucket];
		while (index != 0)
		{
			if (comparer.Equals(entries[index].key, key))
				throw new InvalidOperationException();

			index = entries[index].next;
		}
#endif

		count++;
		entries[count].key = key;
		entries[count].value = value;
		entries[count].next = buckets[bucket];
		buckets[bucket] = count;

		if (count == limitCapacity)
			Resize();
	}

	public bool TryGetValue(TKey key, out TValue value)
	{
		int bucket = GetBucket(key);
		int index = buckets[bucket];
		while (index != 0)
		{
			if (comparer.Equals(entries[index].key, key))
			{
				value = entries[index].value;
				return true;
			}

			index = entries[index].next;
		}

		value = default(TValue);
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
			entries[i].value = default(TValue);
		}

		count = 0;
	}

	public void ForEach(Action<KeyValuePair<TKey, TValue>> action)
	{
		for (int i = 1; i <= count; i++)
		{
			action(new KeyValuePair<TKey, TValue>(entries[i].key, entries[i].value));
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void Resize()
	{
		FastDictionary<TKey, TValue> temp = new FastDictionary<TKey, TValue>(buckets.Length * 2);
		for (int i = 1; i <= count; i++)
		{
			temp.Add(entries[i].key, entries[i].value);
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

	public struct Entry<K, V>
	{
		public K key;
		public V value;
		public int next;
	}
}
