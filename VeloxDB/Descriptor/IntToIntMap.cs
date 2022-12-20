using System;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal sealed class IntToIntMap
{
	const float loadFactor = 0.75f;

	int capacity, capacityMask;
	Node[] buckets;

	public IntToIntMap(int capacity)
	{
		this.capacity = capacity = HashUtils.CalculatePow2Capacity(capacity, loadFactor, out _);
		capacityMask = this.capacity - 1;

		buckets = new Node[this.capacity];
	}

	public void Add(int key, int index)
	{
		int bucket = (int)(key * HashUtils.PrimeMultiplier32) & capacityMask;
		while (true)
		{
			Node node = buckets[bucket];
			if (node.Key == 0)
				break;

			if (key == node.Key)
				throw new ArgumentException();

			bucket = (bucket + 1) & capacityMask;
		}

		buckets[bucket] = new Node() { Value = index, Key = key };
	}

	public bool TryGetValue(int key, out int index)
	{
		int bucket = (int)(key * HashUtils.PrimeMultiplier32) & capacityMask;
		while (true)
		{
			Node node = buckets[bucket];
			if (key == node.Key)
			{
				index = node.Value;
				return true;
			}
			else if (node.Key == 0)
			{
				index = 0;
				return false;
			}

			bucket = (bucket + 1) & capacityMask;
		}

		throw new InvalidOperationException();	// Should never end up here
	}

	private struct Node
	{
		public int Key { get; set; }
		public int Value { get; set; }
	}
}
