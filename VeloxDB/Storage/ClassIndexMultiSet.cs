using System;
using System.Drawing;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using VeloxDB.Common;

namespace VeloxDB.Storage;

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal unsafe struct ClassIndexMultiSet
{
	const uint dataOffset = 20;
	const float loadFactor = 0.7f;

	ulong bufferHandle;

	int count;
	int capacity;
	int limitCapacity;

	public int Capacity => capacity;
	public int Count => count;

	public static ClassIndexMultiSet* Create(int capacity, MemoryManager memoryManager)
	{
		ulong handle = memoryManager.Allocate((int)(dataOffset + capacity * (Bucket.Size * sizeof(ushort))));

		ClassIndexMultiSet* p = (ClassIndexMultiSet*)memoryManager.GetBuffer(handle);
		Utils.FillMemory((byte*)p + dataOffset, capacity * Bucket.Size, 0xff);

		p->bufferHandle = handle;
		p->count = 0;
		p->capacity = capacity;
		p->limitCapacity = (int)(capacity * loadFactor);

		return p;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static ushort GetClassIndex(ClassIndexMultiSet* set, int index, out ushort count)
	{
		Bucket* buckets = (Bucket*)((byte*)set + dataOffset);
		ushort* list = (ushort*)(buckets + set->capacity);
		buckets += list[index];
		count = buckets->count;
		return buckets->index;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool Contains(ClassIndexMultiSet* set, ushort classIndex)
	{
		Bucket* buckets = (Bucket*)((byte*)set + dataOffset);

		int bucketIndex = CalculateBucket(set, classIndex);
		while (true)
		{
			Bucket* bucket = buckets + bucketIndex;
			if (bucket->index == classIndex)
				return true;

			if (bucket->index == ushort.MaxValue)
				return false;

			bucketIndex = (bucketIndex + 1) & (set->capacity - 1);
		}
	}

	public static bool TryAdd(MemoryManager memoryManager, ref ClassIndexMultiSet* pset, ushort classIndex, bool addIfExists = false, ushort addCount = 1)
	{
		ClassIndexMultiSet* set = pset; // Avoid double dereferencing (because of ref param)

		Bucket* buckets = (Bucket*)((byte*)set + dataOffset);

		int bucketIndex = CalculateBucket(set, classIndex);

		while (true)
		{
			Bucket* bucket = buckets + bucketIndex;
			if (bucket->index == classIndex)
			{
				if (!addIfExists)
					return false;

				buckets[bucketIndex].count += addCount;
				return true;
			}

			if (bucket->index == ushort.MaxValue)
				break;

			bucketIndex = (bucketIndex + 1) & (set->capacity - 1);
		}

		buckets[bucketIndex].index = classIndex;
		buckets[bucketIndex].count = addCount;

		ushort* list = (ushort*)(buckets + set->capacity);
		list[set->count] = (ushort)bucketIndex;
		set->count++;

		if (set->count > set->limitCapacity)
			Resize(memoryManager, ref pset);

		return true;
	}

	public static void Clear(ClassIndexMultiSet* set)
	{
		Bucket* buckets = (Bucket*)((byte*)set + dataOffset);

		if (set->count <= (set->capacity >> 2))
		{
			ushort* list = (ushort*)(buckets + set->capacity);
			for (int i = 0; i < set->count; i++)
			{
				Bucket* bucket = buckets + list[i];
				bucket->index = ushort.MaxValue;
			}
		}
		else
		{
			Utils.FillMemory((byte*)buckets, set->capacity * Bucket.Size, 0xff);
		}

		set->count = 0;
	}

	public static void Destroy(MemoryManager memoryManager, ClassIndexMultiSet* set)
	{
		memoryManager.Free(set->bufferHandle);
	}

	public static void Merge(MemoryManager memoryManger, ref ClassIndexMultiSet* set1, ClassIndexMultiSet* set2)
	{
		for (int i = 0; i < set2->count; i++)
		{
			ushort index = GetClassIndex(set2, i, out ushort indexCount);
			TryAdd(memoryManger, ref set1, index, true, indexCount);
		}

		Clear(set2);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static int CalculateBucket(ClassIndexMultiSet* set, ushort classIndex)
	{
		return ((int)(classIndex * HashUtils.PrimeMultiplier32)) & (set->capacity - 1);
	}

	private static unsafe void Resize(MemoryManager memoryManager, ref ClassIndexMultiSet* set)
	{
		if (set->capacity == ushort.MaxValue)
			return;

		ClassIndexMultiSet* set1 = set;
		ClassIndexMultiSet* set2 = Create(Math.Min(ushort.MaxValue, set1->capacity * 2), memoryManager);

		int count = set1->count;
		Bucket* buckets = (Bucket*)((byte*)set1 + dataOffset);
		ushort* list = (ushort*)(buckets + set1->capacity);
		for (int i = 0; i < count; i++)
		{
			Bucket* bucket = buckets + list[i];
			TryAdd(null, ref set2, bucket->index, true, bucket->count);
		}

		Destroy(memoryManager, set1);
		set = set2;
	}

	private struct Bucket
	{
		public const int Size = 4;

		public ushort index;
		public ushort count;
	}
}
