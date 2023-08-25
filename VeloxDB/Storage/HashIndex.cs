using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using static System.Math;

namespace VeloxDB.Storage;

internal unsafe sealed class HashIndex : Index
{
	HashIndexDescriptor indexDesc;
	Database database;

	ParallelResizeCounter resizeCounter;

	long capacity;
	long bucketCountLimit;
	ulong capacityMask;
	float growthFactor;
	float loadFactor;
	long minCapacity;
	ulong seed;
	Bucket* buckets;

	MemoryManager memoryManager;
	MemoryManager.FixedAccessor fixedMemoryManager;
	StringStorage stringStorage;

	long traceId;
	bool pendingRefill;

	public HashIndex(Database database, HashIndexDescriptor indexDesc, long capacity)
	{
		TTTrace.Write(database.TraceId, indexDesc.Id, capacity);

		StorageEngine engine = database.Engine;

		this.traceId = database.TraceId;
		this.indexDesc = indexDesc;
		this.database = database;
		this.memoryManager = database.Engine.MemoryManager;
		this.fixedMemoryManager = memoryManager.RegisterFixedConsumer(HashIndexItem.Size);
#if !HUNT_CORRUPT
		Checker.AssertTrue(fixedMemoryManager.BufferSize == HashIndexItem.Size);
#endif
		this.stringStorage = engine.StringStorage;

		// Since we count used buckets, load factor needs to be smaller than that of a class for example.
		this.loadFactor = Min(0.5f, engine.Settings.HashLoadFactor * 0.7f);

		minCapacity = database.Id == DatabaseId.User ? ParallelResizeCounter.SingleThreadedLimit * 2 : 64;

		capacity = HashUtils.CalculatePow2Capacity(Math.Max(minCapacity, capacity), loadFactor, out bucketCountLimit);
		capacityMask = (ulong)capacity - 1;
		seed = engine.HashSeed;
		buckets = (Bucket*)NativeAllocator.Allocate(capacity * Bucket.Size, false);
		for (long i = 0; i < capacity; i++)
		{
			buckets[i].Init();
		}

		resizeCounter = new ParallelResizeCounter(bucketCountLimit);

		this.capacity = capacity;
		growthFactor = engine.Settings.CollectionGrowthFactor;

		TTTrace.Write(traceId, capacity, bucketCountLimit);
	}

	public HashIndexDescriptor HashIndexDesc => indexDesc;
	public override IndexDescriptor IndexDesc => indexDesc;
	public override bool PendingRefill => pendingRefill;

	public override void ModelUpdated(IndexDescriptor indexDesc)
	{
		this.indexDesc = (HashIndexDescriptor)indexDesc;
	}

	public override DatabaseErrorDetail Insert(Transaction tran, long id, ulong objectHandle, byte* key,
		KeyComparer comparer, Func<short, KeyComparer> comparerFinder = null)
	{
		comparer.TTTraceKeys(traceId, tran == null ? 0 : tran.Id, indexDesc.Id, key, null, stringStorage, 2);
		Checker.AssertFalse(tran != null && comparerFinder != null);

		bool emptyBucket = false;
		ulong hash = comparer.CalculateHashCode(key, seed, null, stringStorage);

		ulong itemHandle = fixedMemoryManager.Allocate();
		HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(itemHandle);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(hash);
		TTTrace.Write(hash, itemHandle, bucket->Handle);

		DatabaseErrorDetail error = null;
		Bucket.LockAccess(bucket);

		if (indexDesc.IsUnique)
			error = CheckKeyUniqueness(tran, bucket, key, comparer, comparerFinder, id);

		if (error == null && tran != null && database.GetKeyLocker(indexDesc.Index).IsLocked(tran, key, hash, comparer))
			error = DatabaseErrorDetail.Create(DatabaseErrorType.IndexConflict);

		if (error == null)
		{
			emptyBucket = bucket->Handle == 0;
			item->objectHandle = objectHandle;
			item->nextCollision = bucket->Handle;
			bucket->Handle = itemHandle;
		}

		Bucket.UnlockAccess(bucket);

		resizeCounter.ExitReadLock(lockHandle);

		if (error != null)
			return error;

		if (emptyBucket && resizeCounter.Add(lockHandle, 1) && resizeCounter.Count > bucketCountLimit)
			Resize();

		return null;
	}

	public override void ReplaceObjectHandle(ulong objectHandle, ulong newObjectHandle, byte* key, long id, KeyComparer comparer)
	{
		comparer.TTTraceKeys(traceId, 0, indexDesc.Id, key, null, stringStorage, 3);

		ulong hash = comparer.CalculateHashCode(key, seed, null, stringStorage);

		Bucket* bucket = buckets + CalculateBucket(hash);
		ulong* pBucketHandle = Bucket.LockAccess(bucket);

		TTTrace.Write(hash, indexDesc.Id, objectHandle, newObjectHandle, bucket->Handle);

		HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(*pBucketHandle);
		while (item->objectHandle != objectHandle)
		{
			item = (HashIndexItem*)fixedMemoryManager.GetBuffer(item->nextCollision);
		}

		item->objectHandle = newObjectHandle;

		Bucket.UnlockAccess(bucket);
	}

	public override void Delete(ulong objectHandle, byte* key, long id, KeyComparer comparer)
	{
		comparer.TTTraceKeys(traceId, 0, indexDesc.Id, key, null, stringStorage, 4);

		int lockHandle = resizeCounter.EnterReadLock();

		bool clearedBucket = false;
		ulong hash = comparer.CalculateHashCode(key, seed, null, stringStorage);

		Bucket* bucket = buckets + CalculateBucket(hash);
		ulong* pBucketHandle = Bucket.LockAccess(bucket);

		TTTrace.Write(hash, indexDesc.Id, objectHandle, bucket->Handle);

		if (bucket->Handle != 0)
		{
			ulong* pHandle = pBucketHandle;
			while (pHandle[0] != 0)
			{
				HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(*pHandle);
				if (item->objectHandle == objectHandle)
				{
					TTTrace.Write(item->nextCollision, item->objectHandle);
					ulong handle = *pHandle;
					*pHandle = item->nextCollision;
					memoryManager.Free(handle);
					break;
				}

				pHandle = &item->nextCollision;
			}

			clearedBucket = bucket->Handle == 0;
		}

		Bucket.UnlockAccess(bucket);

		resizeCounter.ExitReadLock(lockHandle);

		if (clearedBucket)
			resizeCounter.Dec(lockHandle);
	}

	[SkipLocalsInit]
	public DatabaseErrorDetail GetItems(Transaction tran, byte* key, KeyComparer comparer,
		string[] requestStrings, ref ObjectReader[] rs, out int count)
	{
		comparer.TTTraceKeys(traceId, tran == null ? 0 : tran.Id, indexDesc.Id, key, requestStrings, stringStorage, 13);

		HashedItemId* initItems = stackalloc HashedItemId[4];
		HashedIdCollection items = new HashedIdCollection(4, initItems);

		try
		{
			count = 0;

			DatabaseErrorDetail error = GetVersionsByKey(tran, key, comparer, requestStrings, ref items);
			if (error != null)
				return error;

			for (int i = 0; i < items.count; i++)
			{
				HashedItemId* item = &items.items[i];
				Class @class = database.GetClass(item->classIndex).MainClass;

				if (tran.Type == TransactionType.ReadWrite)
				{
					error = @class.ReadLockObjectFromIndex(tran, item->objectHandle, indexDesc.Id);
					if (error != null)
						return error;
				}

				ObjectReader r = new ObjectReader(ClassObject.ToDataPointer(Class.GetObjectByHandle(item->objectHandle)), @class);
				StoreObject(ref rs, ref count, r);
			}

			return null;
		}
		finally
		{
			items.Dispose(memoryManager);
		}
	}

	[SkipLocalsInit]
	private DatabaseErrorDetail CheckKeyUniqueness(Transaction tran, Bucket* bucket, byte* key,
		KeyComparer comparer, Func<short, KeyComparer> comparerFinder, long exceptId)
	{
		comparer.TTTraceKeys(traceId, tran == null ? 0 : tran.Id, indexDesc.Id, key, null, stringStorage, 5);

		ulong itemHandle = bucket->Handle;
		while (itemHandle != 0)
		{
			HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(itemHandle);
			byte* objectKey = Class.GetKey(item->objectHandle, out ClassObject* obj);
			Checker.AssertFalse(obj->IsDeleted);
			TTTrace.Write(database.TraceId, tran != null ? tran.Id : 0, exceptId, obj->id, obj->version);
			if (obj->id != exceptId)
			{
				Class @class = database.GetClassById(IdHelper.GetClassId(obj->id)).MainClass;
				KeyComparer itemComparer = comparerFinder != null ?
					comparerFinder(@class.ClassDesc.Id) : @class.GetKeyComparer(indexDesc.Id, true);

				if (itemComparer.Equals(objectKey, null, key, comparer, stringStorage))
				{
					bool isVisible = IsVisible(tran, obj, out bool isConflicting);
					if (isConflicting)
						return DatabaseErrorDetail.Create(DatabaseErrorType.IndexConflict);

					if (isVisible)
						return DatabaseErrorDetail.CreateUniquenessConstraint(indexDesc.Name);
				}
			}

			itemHandle = item->nextCollision;
		}

		return null;
	}

	public override IndexScanRange[] SplitScanRange()
	{
		Utils.Range[] ranges = Utils.SplitRange(capacity, database.Engine.Settings.ResizeSplitSize, ProcessorNumber.CoreCount);
		IndexScanRange[] scanRanges = new IndexScanRange[ranges.Length];
		for (int i = 0; i < ranges.Length; i++)
		{
			scanRanges[i] = new HashScanRange(ranges[i].Offset, ranges[i].Count);
		}

		return scanRanges;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail GetVersionsByKey(Transaction tran, byte* key,
		KeyComparer comparer, string[] requestStrings, ref HashedIdCollection items)
	{
		ulong hash = comparer.CalculateHashCode(key, seed, requestStrings, null);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(hash);
		Bucket.LockAccess(bucket);

		try
		{
			TTTrace.Write(database.TraceId, hash, tran.Id, bucket->Handle);

			if (tran.Type == TransactionType.ReadWrite)
			{
				KeyReadLocker locker = database.GetKeyLocker(indexDesc.Index);
				DatabaseErrorDetail error = locker.LockKey(tran, key, comparer, requestStrings, hash);
				if (error != null)
					return error;
			}

			ulong itemHandle = bucket->Handle;
			while (itemHandle != 0)
			{
				HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(itemHandle);
				byte* objectKey = Class.GetKey(item->objectHandle, out ClassObject* obj);
				Checker.AssertFalse(obj->IsDeleted);
				TTTrace.Write(database.TraceId, tran.Id, tran.ReadVersion, obj->id, obj->version, obj->NewerVersion);

				Class @class = database.GetClassById(IdHelper.GetClassId(obj->id)).MainClass;
				KeyComparer itemComparer = @class.GetKeyComparer(indexDesc.Id, true);

				if (comparer.Equals(key, requestStrings, objectKey, itemComparer, stringStorage))
				{
					bool isVisible = IsVisible(tran, obj, out bool isConflicting);
					if (tran.Type == TransactionType.ReadWrite && isConflicting)
						return DatabaseErrorDetail.Create(DatabaseErrorType.IndexConflict);

					if (isVisible)
					{
						if (items.count == items.capacity)
							items.Resize(memoryManager);

						items.Add(item->objectHandle, (ushort)@class.ClassDesc.Index);
					}
				}

				itemHandle = item->nextCollision;
			}

			return null;
		}
		finally
		{
			Bucket.UnlockAccess(bucket);
			resizeCounter.ExitReadLock(lockHandle);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool IsVisible(Transaction tran, ClassObject* obj, out bool isConflicting)
	{
		if (tran == null)
		{
			isConflicting = false;
			return true;
		}

		if (obj->version <= tran.ReadVersion)
		{
			isConflicting = false;
			ulong newerVersion = obj->NewerVersion;
			return newerVersion == 0 || (newerVersion > tran.ReadVersion && newerVersion != tran.Id);
		}

		if (obj->version == tran.Id)
		{
			isConflicting = false;
			return true;
		}

		isConflicting = true;
		return false;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void StoreObject(ref ObjectReader[] rs, ref int count, ObjectReader r)
	{
		if (rs.Length == count)
			Array.Resize(ref rs, rs.Length * 2);

		rs[count++] = r;
	}

	private KeyComparer ProvideComparerInModelUpdate(ref Dictionary<short, KeyComparer> localCache, ClassDescriptor classDesc)
	{
		if (localCache == null)
			localCache = new Dictionary<short, KeyComparer>(2);

		if (localCache.TryGetValue(classDesc.Id, out KeyComparer comparer))
			return comparer;

		KeyComparerDesc kad = classDesc.GetIndexAccessDescByPropertyName(indexDesc);
		comparer = new KeyComparer(kad);
		localCache.Add(classDesc.Id, comparer);

		return comparer;
	}

	public override DatabaseErrorDetail CheckUniqueness(IndexScanRange scanRange)
	{
		HashScanRange range = (HashScanRange)scanRange;

		Dictionary<short, KeyComparer> localCache = null;
		for (long i = range.Offset; i < range.Offset + range.Count; i++)
		{
			ulong handle1 = buckets[i].Handle;
			while (handle1 != 0)
			{
				HashIndexItem* item1 = (HashIndexItem*)fixedMemoryManager.GetBuffer(handle1);
				ulong nextCollision1 = item1->nextCollision;
				if (nextCollision1 == 0)
					break;

				ClassObject* obj1 = Class.GetObjectByHandle(item1->objectHandle);
				Class class1 = database.GetClassById(IdHelper.GetClassId(obj1->id)).MainClass;
				KeyComparer comparer1 = class1.GetKeyComparer(indexDesc.Id, false);
				if (comparer1 == null)
					comparer1 = ProvideComparerInModelUpdate(ref localCache, class1.ClassDesc);

				byte* key1 = (byte*)obj1 + ClassObject.DataOffset;

				ulong handle2 = nextCollision1;
				while (handle2 != 0)
				{
					HashIndexItem* item2 = (HashIndexItem*)fixedMemoryManager.GetBuffer(handle2);
					ulong nextCollision2 = item2->nextCollision;
					ClassObject* obj2 = Class.GetObjectByHandle(item2->objectHandle);
					Class class2 = database.GetClassById(IdHelper.GetClassId(obj2->id)).MainClass;
					KeyComparer comparer2 = class2.GetKeyComparer(indexDesc.Id, false);
					if (comparer2 == null)
						comparer2 = ProvideComparerInModelUpdate(ref localCache, class2.ClassDesc);

					byte* key2 = (byte*)obj2 + ClassObject.DataOffset;
					if (comparer1.Equals(key1, null, key2, comparer2, stringStorage))
					{
						comparer1.TTTraceKeys(traceId, 0, indexDesc.Id, key1, null, stringStorage, 7);
						comparer2.TTTraceKeys(traceId, 0, indexDesc.Id, key2, null, stringStorage, 8);
						return DatabaseErrorDetail.CreateUniquenessConstraint(indexDesc.FullName);
					}

					handle2 = nextCollision2;
				}

				handle1 = nextCollision1;
			}
		}

		return null;
	}

	private void Resize()
	{
		TTTrace.Write(traceId);

		resizeCounter.EnterWriteLock();

		if (resizeCounter.Count <= bucketCountLimit)
		{
			resizeCounter.ExitWriteLock();
			return;
		}

		try
		{
			long newCapacity = HashUtils.CalculatePow2Capacity((long)(capacity * growthFactor), loadFactor, out long newLimitCapacity);
			ulong newCapacityMask = (ulong)newCapacity - 1;

			Bucket* newBuckets = (Bucket*)NativeAllocator.Allocate((long)newCapacity * Bucket.Size, false);
			for (long i = 0; i < newCapacity; i++)
			{
				newBuckets[i].Init();
			}

			long usedBucketCount = 0;
			Utils.Range[] ranges = Utils.SplitRange(capacity, database.Engine.Settings.ResizeSplitSize, ProcessorNumber.CoreCount);
			int workerCount = Math.Min(ProcessorNumber.CoreCount, ranges.Length);
			string workerName = string.Format("{0}: vlx-HashIndexResizeWorker", database.Engine.Trace.Name);
			JobWorkers<Utils.Range>.Execute(workerName, workerCount, r => Interlocked.Add(ref usedBucketCount, RehashRange(newBuckets, newCapacityMask, r)), ranges);

			database.Trace.Debug("HashIndex {0} resized from {1} to {2}, hashIndexId={3}.",
				indexDesc.FullName, this.capacity, newCapacity, indexDesc.Id);

			resizeCounter.SetCount(usedBucketCount);

			this.capacity = newCapacity;
			this.bucketCountLimit = newLimitCapacity;
			this.capacityMask = newCapacityMask;

			NativeAllocator.Free((IntPtr)buckets);
			buckets = newBuckets;
		}
		finally
		{
			resizeCounter.Resized(bucketCountLimit);
			resizeCounter.ExitWriteLock();
		}
	}

	private long RehashRange(Bucket* newBuckets, ulong newCapacityMask, Utils.Range range)
	{
		long usedBucketCount = 0;
		Dictionary<short, KeyComparer> localCache = null;
		for (long i = range.Offset; i < range.Offset + range.Count; i++)
		{
			ulong handle = buckets[i].Handle;
			while (handle != 0)
			{
				HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(handle);
				ulong nextCollision = item->nextCollision;

				byte* key = Class.GetKey(item->objectHandle, out ClassObject* obj);
				Class @class = database.GetClassById(IdHelper.GetClassId(obj->id)).MainClass;
				KeyComparer comparer = @class.GetKeyComparer(indexDesc.Id, false);
				if (comparer == null)
					comparer = ProvideComparerInModelUpdate(ref localCache, @class.ClassDesc);

				ulong hash = comparer.CalculateHashCode(key, seed, null, stringStorage);
				long bucketIndex = CalculateBucket(hash, newCapacityMask);

				Bucket* bucket = newBuckets + bucketIndex;
				Bucket.LockAccess(bucket);

				item->nextCollision = newBuckets[bucketIndex].Handle;
				if (item->nextCollision == 0)
					usedBucketCount++;

				newBuckets[bucketIndex].Handle = handle;

				Bucket.UnlockAccess(bucket);

				handle = nextCollision;
			}
		}

		return usedBucketCount;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private long CalculateBucket(ulong hash)
	{
		return (long)(hash & capacityMask);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static long CalculateBucket(ulong hash, ulong capacityMask)
	{
		return (long)(hash & capacityMask);
	}

#if TEST_BUILD
	public override void Validate(ulong readVersion)
	{
		TTTrace.Write(traceId, capacity, readVersion);

		int bucketCount = 0;
		for (long i = 0; i < capacity; i++)
		{
			Bucket* bucket = buckets + i;

			if (bucket->Handle != 0)
				bucketCount++;

			ulong itemHandle = bucket->Handle;
			HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(itemHandle);

			while (item != null)
			{
				byte* key = Class.GetKey(item->objectHandle, out ClassObject* obj);

				Class @class = database.GetClassById(IdHelper.GetClassId(obj->id)).MainClass;
				KeyComparer comparer = @class.GetKeyComparer(indexDesc.Id, true);

				if (!@class.IsObjectPresent(item->objectHandle))
					throw new InvalidOperationException();

				ulong hash = comparer.CalculateHashCode(key, seed, null, stringStorage);
				long bucketIndex = CalculateBucket(hash);

				if (bucketIndex != i)
					throw new InvalidOperationException();

				TTTrace.Write(itemHandle, item->objectHandle, @class.ClassDesc.Id);
				comparer.TTTraceKeys(traceId, 0, indexDesc.Id, key, null, stringStorage, 9);

				itemHandle = item->nextCollision;
				item = (HashIndexItem*)fixedMemoryManager.GetBuffer(itemHandle);
			}
		}

		if (bucketCount != resizeCounter.Count)
			throw new InvalidOperationException();
	}

	public override bool HasObject(ClassObject* tobj, byte* key, ulong objectHandle, KeyComparer comparer)
	{
		ulong hash = comparer.CalculateHashCode(key, seed, null, stringStorage);
		long bucket = CalculateBucket(hash);

		Bucket* bn = buckets + bucket;

		ulong cr = bn->Handle;
		while (cr != 0)
		{
			HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(cr);
			ClassObject* fobj = Class.GetObjectByHandle(item->objectHandle);
			Class @class = database.GetClassById(IdHelper.GetClassId(fobj->id)).MainClass;

			if (fobj == tobj)
				return true;

			cr = item->nextCollision;
		}

		return false;
	}
#endif

	public override void Dispose(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(traceId, indexDesc.Id);

		FreeBuffers(workers);

		NativeAllocator.Free((IntPtr)buckets);
		buckets = null;

		resizeCounter.Dispose();
	}

	public override void PrepareForPendingRefill(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(traceId, indexDesc.Id);

		FreeBuffers(workers);
		NativeAllocator.Free((IntPtr)buckets);
		buckets = null;

		pendingRefill = true;
	}

	public override void PendingRefillStarted(long capacity)
	{
		capacity = HashUtils.CalculatePow2Capacity(Math.Max(64, capacity), loadFactor, out bucketCountLimit);
		capacityMask = (ulong)capacity - 1;
		buckets = (Bucket*)NativeAllocator.Allocate(capacity * Bucket.Size, false);
		for (long i = 0; i < capacity; i++)
		{
			buckets[i].Init();
		}

		resizeCounter.Resized(bucketCountLimit);

		this.capacity = capacity;
	}

	public override void PendingRefillFinished()
	{
		pendingRefill = false;
	}

	private void FreeBuffers(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(database.TraceId, indexDesc.Id);

		Utils.Range[] ranges = Utils.SplitRange(capacity, database.Engine.Settings.ResizeSplitSize, ProcessorNumber.CoreCount);
		workers.SetAction(o =>
		{
			Utils.Range r = (Utils.Range)o.ReferenceParam;
			for (long i = r.Offset; i < r.Offset + r.Count; i++)
			{
				Bucket* bn = buckets + i;

				ulong itemHandle = bn->Handle;
				HashIndexItem* item = (HashIndexItem*)memoryManager.GetBuffer(itemHandle);
				while (item != null)
				{
					ulong nextCollision = item->nextCollision;
					memoryManager.Free(itemHandle);

					itemHandle = nextCollision;
					item = (HashIndexItem*)memoryManager.GetBuffer(itemHandle);
				}
			}
		});

		Array.ForEach(ranges, x => workers.EnqueueWork(new CommonWorkerParam() { ReferenceParam = x }));
		workers.Drain();
	}

	private struct HashedIdCollection
	{
		public int count;
		public int capacity;
		public ulong handle;
		public HashedItemId* items;

		public HashedIdCollection(int capacity, HashedItemId* initItems)
		{
			this.count = 0;
			this.capacity = capacity;
			handle = 0;
			items = initItems;
		}

		[MethodImpl(MethodImplOptions.NoInlining)]
		public void Resize(MemoryManager memoryManager)
		{
			int newCapacity = capacity * 2;
			ulong newHandle = memoryManager.Allocate(HashedItemId.Size * newCapacity);
			HashedItemId* newItems = (HashedItemId*)memoryManager.GetBuffer(newHandle);

			Utils.CopyMemory((byte*)items, (byte*)newItems, HashedItemId.Size * capacity);
			if (handle != 0)
				memoryManager.Free(handle);

			handle = newHandle;
			capacity = newCapacity;
			items = newItems;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Dispose(MemoryManager memoryManager)
		{
			if (handle != 0)
				memoryManager.Free(handle);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Add(ulong objectHandle, ushort classIndex)
		{
			HashedItemId* p = items + count;
			p->objectHandle = objectHandle;
			p->classIndex = classIndex;
			count++;
		}
	}

	private sealed class HashScanRange : IndexScanRange
	{
		public long Offset { get; set; }
		public long Count { get; set; }

		public HashScanRange(long offset, long count)
		{
			this.Offset = offset;
			this.Count = count;
		}
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = HashedItemId.Size)]
internal unsafe struct HashedItemId
{
	public const int Size = 16;

	public ulong objectHandle;
	public ushort classIndex;
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal unsafe struct HashIndexItem
{
	public const int Size = 16;

	public ulong objectHandle;
	public ulong nextCollision;
}
