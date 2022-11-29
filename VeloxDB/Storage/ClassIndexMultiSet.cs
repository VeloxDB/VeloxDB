using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Velox.Common;

namespace Velox.Storage;

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
		ulong handle = memoryManager.Allocate((int)(dataOffset + capacity * sizeof(ushort) * 2));

		ClassIndexMultiSet* p = (ClassIndexMultiSet*)memoryManager.GetBuffer(handle);
		Utils.FillMemory((byte*)p + dataOffset, capacity * sizeof(ushort), 0xff);

		p->bufferHandle = handle;
		p->count = 0;
		p->capacity = capacity;
		p->limitCapacity = (int)(capacity * loadFactor);

		return p;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static ushort GetClassIndex(ClassIndexMultiSet* set, int index)
	{
		ushort* buckets = (ushort*)((byte*)set + dataOffset);
		ushort* list = (ushort*)((byte*)set + dataOffset) + set->capacity;
		return buckets[list[index]];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool Contains(ClassIndexMultiSet* set, ushort classIndex)
	{
		ushort* buckets = (ushort*)((byte*)set + dataOffset);

		int bucket = CalculateBucket(set, classIndex);
		while (true)
		{
			if (buckets[bucket] == classIndex)
				return true;

			if (buckets[bucket] == ushort.MaxValue)
				return false;

			bucket = (bucket + 1) & (set->capacity - 1);
		}
	}

	public static bool TryAdd(MemoryManager memoryManager, ref ClassIndexMultiSet* pset, ushort classIndex, bool addIfExists = false)
	{
		ClassIndexMultiSet* set = pset;	// Avoid double dereferencing (because of ref param)

		ushort* buckets = (ushort*)((byte*)set + dataOffset);

		var bucket = CalculateBucket(set, classIndex);

		while (true)
		{
			if (buckets[bucket] == classIndex && !addIfExists)
				return false;

			if (buckets[bucket] == ushort.MaxValue)
				break;

			bucket = (bucket + 1) & (set->capacity - 1);
		}

		buckets[bucket] = classIndex;

		ushort* list = buckets + set->capacity;
		list[set->count] = (ushort)bucket;
		set->count++;

		if (set->count > set->limitCapacity)
			Resize(memoryManager, ref pset);

		return true;
	}

	public static void Clear(ClassIndexMultiSet* set)
	{
		ushort* buckets = (ushort*)((byte*)set + dataOffset);

		if (set->count <= (set->capacity >> 2))
		{
			ushort* list = buckets + set->capacity;
			for (int i = 0; i < set->count; i++)
			{
				buckets[list[i]] = ushort.MaxValue;
			}
		}
		else
		{
			Utils.FillMemory((byte*)buckets, set->capacity * sizeof(ushort), 0xff);
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
			TryAdd(memoryManger, ref set1, GetClassIndex(set2, i), true);
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
		ClassIndexMultiSet* set2 = Create(set1->capacity * 2, memoryManager);

		int count = set1->count;
		ushort* buckets = (ushort*)((byte*)set1 + dataOffset);
		ushort* list = buckets + set1->capacity;
		for (int i = 0; i < count; i++)
		{
			TryAdd(null, ref set2, (ushort)(buckets[list[i]]), true);
		}

		Destroy(memoryManager, set1);
		set = set2;
	}
}
