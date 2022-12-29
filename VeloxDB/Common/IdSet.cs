using System;
using System.Runtime.CompilerServices;
using System.Threading;
using VeloxDB.Common;
using VeloxDB.Networking;

namespace VeloxDB.Common;

internal unsafe sealed class IdSet
{
	const float loafFactor = 0.75f;

	long capacity;
	ulong capacityMask;
	long* buckets;

	public IdSet(long capacity)
	{
		this.capacity = capacity = HashUtils.CalculatePow2Capacity(capacity, loafFactor, out long countLimit);
		capacityMask = (ulong)capacity - 1;
		this.buckets = (long*)NativeAllocator.Allocate(capacity * sizeof(long), true);
	}

	public IdSet()
	{
	}

	~IdSet()
	{
#if DEBUG
		throw new CriticalDatabaseException();
#else
		Dispose();	
#endif
	}

	public void Add(long id)
	{
		long bucket = GetBucket(id);

		while (true)
		{
			if (buckets[bucket] == 0)
			{
				NativeInterlocked64* up = (NativeInterlocked64*)(buckets + bucket);
				if (up->CompareExchange(id, 0) == 0)
					return;
			}

			bucket++;
			if (bucket == capacity)
				bucket = 0;

			if (bucket == GetBucket(id))
				throw new CriticalDatabaseException();
		}
	}

	public bool Contains(long id)
	{
		Checker.AssertTrue(buckets != null);

		long bucket = GetBucket(id);
		while (true)
		{
			long v = buckets[bucket];
			if (v == id)
				return true;

			if (v == 0)
				return false;

			bucket++;
			if (bucket == capacity)
				bucket = 0;
		}
	}

	public void Serialize(MessageWriter writer)
	{
		writer.WriteLong(capacity);
		for (int i = 0; i < capacity; i++)
		{
			if (buckets[i] != 0)
				writer.WriteLong(buckets[i]);
		}

		writer.WriteLong(0);
	}

	public void Deserialize(MessageReader reader)
	{
		this.capacity = reader.ReadLong();
		this.capacityMask = (ulong)capacity - 1;
		this.buckets = (long*)NativeAllocator.Allocate(capacity * sizeof(long), true);
		while (true)
		{
			long id = reader.ReadLong();
			if (id == 0)
				return;

			long bucket = GetBucket(id);
			while (true)
			{
				if (buckets[bucket] == 0)
				{
					buckets[bucket] = id;
					break;
				}

				bucket++;
				if (bucket == capacity)
					bucket = 0;
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private long GetBucket(long id)
	{
		return (long)(HashUtils.GetHash64((ulong)id, 1) & capacityMask);
	}

	public void Dispose()
	{
		GC.SuppressFinalize(this);
		if (buckets != null)            // Can be null if deserialize failed due to closed connection
		{
			NativeAllocator.Free((IntPtr)buckets);
			buckets = null;
		}
	}
}
