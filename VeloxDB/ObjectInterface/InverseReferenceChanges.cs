using System;
using System.Collections.Generic;
using VeloxDB.Common;

namespace VeloxDB.ObjectInterface;

internal sealed class InverseReferenceChanges
{
	const float loadFactor = 0.6f;

	int count;
	int capacity, limitCapacity;
	uint capacityMask;
	BucketNode[] buckets;

	int takenCount;
	int[] takenBuckets;

	int itemCount;
	ItemNode[] items;

	public InverseReferenceChanges(int capacity, int itemCapacity)
	{
		capacity = Math.Max((int)(capacity / loadFactor) + 1, 8);
		capacity = HashUtils.CalculatePow2Capacity(capacity, loadFactor, out limitCapacity);
		this.capacity = capacity;
		this.capacityMask = (uint)this.capacity - 1;
		limitCapacity = (int)(this.capacity * loadFactor);
		buckets = new BucketNode[this.capacity];
		takenBuckets = new int[this.capacity];
		items = new ItemNode[itemCapacity];
	}

	private InverseReferenceChanges(int capacity)
	{
		capacity = Math.Max((int)(capacity / loadFactor) + 1, 8);
		capacity = HashUtils.CalculatePow2Capacity(capacity, loadFactor, out limitCapacity);
		this.capacity = capacity;
		this.capacityMask = (uint)this.capacity - 1;
		limitCapacity = (int)(this.capacity * loadFactor);
		buckets = new BucketNode[this.capacity];
		takenBuckets = new int[this.capacity];
	}

	public int Capacity => capacity;
	public int ItemCapacity => items.Length;

	public void Add(long directRefId, long inverseRefId, int propId, bool isInserted)
	{
		ulong hash = HashUtils.GetHash96((ulong)directRefId, (uint)propId, 1);
		int bucket = (int)(hash & capacityMask);
		while (true)
		{
			if (buckets[bucket].directRefId == 0)
				break;

			if (buckets[bucket].directRefId == directRefId && buckets[bucket].propId == propId)
				break;

			bucket++;
			if (bucket == capacity)
				bucket = 0;
		}

		if (items.Length == itemCount)
			ResizeItems();

		int newItemIndex = itemCount++;
		items[newItemIndex].inverseRefId = isInserted ? inverseRefId : -inverseRefId;

		if (buckets[bucket].directRefId == 0)
		{
			count++;
			buckets[bucket].directRefId = directRefId;
			buckets[bucket].propId = propId;
			items[newItemIndex].nextItem = -1;
			buckets[bucket].index = newItemIndex;
			takenBuckets[takenCount++] = bucket;

			if (count > limitCapacity)
				Resize();
		}
		else
		{
			items[newItemIndex].nextItem = buckets[bucket].index;
			buckets[bucket].index = newItemIndex;
		}
	}

	private void AddOnResize(BucketNode node)
	{
		ulong hash = HashUtils.GetHash96((ulong)node.directRefId, (uint)node.propId, 1);
		int bucket = (int)(hash & capacityMask);
		while (true)
		{
			if (buckets[bucket].directRefId == 0)
				break;

			bucket++;
			if (bucket == capacity)
				bucket = 0;
		}

		count++;
		buckets[bucket] = node; ;
		takenBuckets[takenCount++] = bucket;
	}

	public bool TryCollectChanges(long directRefId, int propId, LongDictionary<int> deletedRefs, ref long[] references, ref int refCount)
	{
		ulong hash = HashUtils.GetHash96((ulong)directRefId, (uint)propId, 1);
		int bucket = (int)(hash & capacityMask);
		while (true)
		{
			if (buckets[bucket].directRefId == 0)
				return false;

			if (buckets[bucket].directRefId == directRefId && buckets[bucket].propId == propId)
			{
				CollectChanges(buckets[bucket].index, deletedRefs, ref references, ref refCount);
				return true;
			}

			bucket++;
			if (bucket == capacity)
				bucket = 0;
		}

		throw new InvalidOperationException();  // Should never end up here
	}

	private void CollectChanges(int index, LongDictionary<int> deletedRefs, ref long[] references, ref int refCount)
	{
		while (index != -1)
		{
			long inverseRefId = items[index].inverseRefId;
			if (inverseRefId > 0)
			{
				if (references.Length == refCount)
					Array.Resize(ref references, references.Length * 2);

				references[refCount++] = inverseRefId;
			}
			else
			{
				deletedRefs.TryGetValue(-inverseRefId, out int c);
				c++;
				deletedRefs[-inverseRefId] = c;
			}

			index = items[index].nextItem;
		}
	}

	public void Clear()
	{
		for (int i = 0; i < takenCount; i++)
		{
			int bucket = takenBuckets[i];
			buckets[bucket].directRefId = 0;
		}

		itemCount = 0;
		count = 0;
		takenCount = 0;
	}

	private void Resize()
	{
		InverseReferenceChanges newMap = new InverseReferenceChanges(count * 2);
		for (int i = 0; i < takenCount; i++)
		{
			int bucket = takenBuckets[i];
			if (buckets[bucket].directRefId != 0)
				newMap.AddOnResize(buckets[bucket]);
		}

		Checker.AssertTrue(count == newMap.count);
		capacity = newMap.capacity;
		limitCapacity = newMap.limitCapacity;
		buckets = newMap.buckets;
		takenCount = newMap.takenCount;
		takenBuckets = newMap.takenBuckets;
	}

	private void ResizeItems()
	{
		Array.Resize(ref items, items.Length * 2);
	}

	private struct BucketNode
	{
		public long directRefId;
		public int propId;
		public int index;
	}

	private struct ItemNode
	{
		public long inverseRefId;
		public int nextItem;
	}
}
