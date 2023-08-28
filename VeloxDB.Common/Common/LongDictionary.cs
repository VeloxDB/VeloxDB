using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace VeloxDB.Common;

internal sealed class LongDictionary<T>
{
	const float loadFactor = 0.7f;

	int capacityMask;
	int limitCapacity;
	int count;
	int[] buckets;
	Entry<T>[] entries;

	public LongDictionary(int capacity)
	{
		capacity = Math.Max(4, (int)Utils.GetNextPow2((ulong)capacity));
		this.capacityMask = capacity - 1;
		this.limitCapacity = (int)(capacity * loadFactor);
		buckets = new int[capacity];
		entries = new Entry<T>[limitCapacity + 1];
	}

	public int Count => count;

	public T this[long key]
	{
		get
		{
			int bucket = GetBucket(key);
			int index = buckets[bucket];
			while (index != 0)
			{
				if (entries[index].key == key)
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
				if (entries[index].key == key)
				{
					entries[index].value = value;
					return;
				}

				index = entries[index].next;
			}

			Add(key, value);
		}
	}

	public void Add(long key, T value)
	{
		int bucket = GetBucket(key);

#if DEBUG
			int index = buckets[bucket];
			while (index != 0)
			{
				if (entries[index].key == key)
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

	public bool TryGetValue(long key, out T value)
	{
		int bucket = GetBucket(key);
		int index = buckets[bucket];
		while (index != 0)
		{
			if (entries[index].key == key)
			{
				value = entries[index].value;
				return true;
			}

			index = entries[index].next;
		}

		value = default(T);
		return false;
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
			entries[count].value = default(T);
			count--;
			return;
		}

		entries[index] = entries[count];
		entries[count].value = default(T);

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
		if (count >= (int)limitCapacity / 32 && count > 4)
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
			entries[i].value = default(T);
		}

		count = 0;
	}

	public void ForEach(Action<KeyValuePair<long, T>> action)
	{
		for (int i = 1; i <= count; i++)
		{
			action(new KeyValuePair<long, T>(entries[i].key, entries[i].value));
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void Resize()
	{
		LongDictionary<T> temp = new LongDictionary<T>(buckets.Length * 2);
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
	private int GetBucket(long key)
	{
		return ((int)key ^ (int)(key >> 32)) & capacityMask;
	}

	public struct Entry<K>
	{
		public long key;
		public K value;
		public int next;
	}
}
