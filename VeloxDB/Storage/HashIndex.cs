using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Velox.Common;
using Velox.Descriptor;
using static System.Math;

namespace Velox.Storage;

internal unsafe sealed class HashIndex
{
	HashIndexDescriptor hashIndexDesc;
	Database database;

	ParallelResizeCounter resizeCounter;

	long capacity;
	long bucketCountLimit;
	ulong capacityMask;
	float growthFactor;
	float loadFactor;
	ulong seed;
	Bucket* buckets;

	MemoryManager memoryManager;
	MemoryManager.FixedAccessor fixedMemoryManager;
	StringStorage stringStorage;

	long traceId;

	bool pendingRefill;

	public HashIndex(Database database, HashIndexDescriptor hindDesc, long capacity)
	{
		TTTrace.Write(database.TraceId, hindDesc.Id, capacity);

		StorageEngine engine = database.Engine;

		this.traceId = database.TraceId;
		this.hashIndexDesc = hindDesc;
		this.database = database;
		this.memoryManager = database.Engine.MemoryManager;
		this.fixedMemoryManager = memoryManager.RegisterFixedConsumer(HashIndexItem.Size);
		Checker.AssertTrue(fixedMemoryManager.BufferSize == HashIndexItem.Size);
		this.stringStorage = engine.StringStorage;

		// Since we count used buckets, load factor needs to be smaller than that of a clasa for example.
		this.loadFactor = Min(0.5f, engine.Settings.HashLoadFactor * 0.7f);

		capacity = HashUtils.CalculatePow2Capacity(Math.Max(64, capacity), loadFactor, out bucketCountLimit);
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

	public HashIndexDescriptor HashIndexDesc => hashIndexDesc;
	public bool Disposed => buckets == null;

	public bool PendingRefill => pendingRefill;

	public void ModelUpdated(HashIndexDescriptor hindDesc)
	{
		this.hashIndexDesc = hindDesc;
	}

	public DatabaseErrorDetail Insert(Transaction tran, ulong classObjectHandle, byte* key, HashComparer comparer)
	{
		comparer.TTTraceKeys(traceId, tran == null ? 0 : tran.Id, HashIndexDesc.Id, key, stringStorage, 2);

		if (comparer.HasNullStrings(key, stringStorage))
			return null;

		bool emptyBucket = false;
		ulong hash = comparer.CalculateHashCode(key, seed, stringStorage);

		ulong itemHandle = fixedMemoryManager.Allocate();
		HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(itemHandle);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(hash);
		TTTrace.Write(hash, itemHandle, bucket->Handle);

		DatabaseErrorDetail error = null;
		Bucket.LockAccess(bucket);

		if (tran != null && database.GetHashIndexLocker(HashIndexDesc.Index).IsLocked(tran, key, hash, comparer))
		{
			error = DatabaseErrorDetail.Create(DatabaseErrorType.HashIndexConflict);
		}
		else
		{
			emptyBucket = bucket->Handle == 0;
			item->objectHandle = classObjectHandle;
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

	public void ReplaceObjectHandle(ulong classObjectHandle, ulong newClassObjectHandle, byte* key, HashComparer comparer)
	{
		comparer.TTTraceKeys(traceId, 0, HashIndexDesc.Id, key, stringStorage, 3);

		if (comparer.HasNullStrings(key, stringStorage))
			return;

		ulong hash = comparer.CalculateHashCode(key, seed, stringStorage);

		Bucket* bucket = buckets + CalculateBucket(hash);
		ulong* pBucketHandle = Bucket.LockAccess(bucket);

		TTTrace.Write(hash, hashIndexDesc.Id, classObjectHandle, newClassObjectHandle, bucket->Handle);

		HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(*pBucketHandle);
		while (item->objectHandle != classObjectHandle)
		{
			item = (HashIndexItem*)fixedMemoryManager.GetBuffer(item->nextCollision);
		}

		item->objectHandle = newClassObjectHandle;

		Bucket.UnlockAccess(bucket);
	}

	public void Delete(Transaction tran, ulong classObjectHandle, byte* key, HashComparer comparer)
	{
		comparer.TTTraceKeys(traceId, tran == null ? 0 : tran.Id, HashIndexDesc.Id, key, stringStorage, 4);

		if (comparer.HasNullStrings(key, stringStorage))
			return;

		int lockHandle = resizeCounter.EnterReadLock();

		bool clearedBucket = false;
		ulong hash = comparer.CalculateHashCode(key, seed, stringStorage);

		Bucket* bucket = buckets + CalculateBucket(hash);
		ulong* pBucketHandle = Bucket.LockAccess(bucket);

		TTTrace.Write(hash, hashIndexDesc.Id, classObjectHandle, bucket->Handle);

		if (bucket->Handle != 0)
		{
			ulong* pHandle = pBucketHandle;
			while (pHandle[0] != 0)
			{
				HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(*pHandle);
				if (item->objectHandle == classObjectHandle)
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
	public DatabaseErrorDetail GetItems(Transaction tran, byte* key, HashComparer comparer, ref ObjectReader[] rs, out int count)
	{
		comparer.TTTraceKeys(traceId, tran == null ? 0 : tran.Id, HashIndexDesc.Id, key, stringStorage, 13);

		if (comparer.HasNullStrings(key, stringStorage))
		{
			count = 0;
			return null;
		}

		HashedItemId* initItems = stackalloc HashedItemId[4];
		HashedIdCollection items = new HashedIdCollection(4, initItems);

		try
		{
			count = 0;

			DatabaseErrorDetail error = GetVersionsByKey(tran, key, comparer, ref items);
			if (error != null)
				return error;

			for (int i = 0; i < items.count; i++)
			{
				HashedItemId* item = &items.items[i];
				Class @class = database.GetClass(item->classIndex).MainClass;
				ObjectReader r = @class.GetHashedObject(tran, item->objectHandle, false, out error);
				if (error != null)
				{
					if (error.ErrorType == DatabaseErrorType.Conflict)
						return DatabaseErrorDetail.Create(DatabaseErrorType.HashIndexConflict);

					return error;
				}
				else if (!r.IsEmpty())
				{
					StoreObject(ref rs, ref count, r);
				}
			}

			return null;
		}
		finally
		{
			items.Dispose(memoryManager);
		}
	}

	[SkipLocalsInit]
	public bool ContainsKey(Transaction tran, byte* key, HashComparer comparer, long objectId, out DatabaseErrorDetail error)
	{
		comparer.TTTraceKeys(traceId, tran == null ? 0 : tran.Id, HashIndexDesc.Id, key, stringStorage, 5);

		error = null;
		if (comparer.HasNullStrings(key, stringStorage))
			return false;

		HashedItemId* initItems = stackalloc HashedItemId[4];
		HashedIdCollection items = new HashedIdCollection(4, initItems);

		try
		{
			error = GetVersionsByKey(tran, key, comparer, ref items, objectId);
			if (error == null)
			{
				for (int i = 0; i < items.count; i++)
				{
					HashedItemId* item = &items.items[i];
					Class @class = database.GetClass(item->classIndex).MainClass;
					ObjectReader r = @class.GetHashedObject(tran, item->objectHandle, true, out error);
					if (error != null)
						break;

					if (!r.IsEmpty())
						return true;
				}
			}

			return false;
		}
		finally
		{
			items.Dispose(memoryManager);
		}
	}

	public bool ContainsKey(byte* key, HashComparer comparer, Func<short, HashComparer> comparerFinder)
	{
		comparer.TTTraceKeys(traceId, 0, HashIndexDesc.Id, key, stringStorage, 6);

		ulong hash = comparer.CalculateHashCode(key, seed, stringStorage);

		Bucket* bucket = buckets + CalculateBucket(hash);
		Bucket.LockAccess(bucket);

		try
		{
			ulong itemHandle = bucket->Handle;
			while (itemHandle != 0)
			{
				HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(itemHandle);
				byte* objectKey = Class.GetHashKey(item->objectHandle, out ClassObject* classObject);
				Class @class = database.GetClassById(IdHelper.GetClassId(classObject->id)).MainClass;
				HashComparer itemComparer = comparerFinder(@class.ClassDesc.Id);

				if (itemComparer.AreKeysEqual(objectKey, key, comparer, stringStorage))
					return true;

				itemHandle = item->nextCollision;
			}
		}
		finally
		{
			Bucket.UnlockAccess(bucket);
		}

		return false;
	}

	public Utils.Range[] SplitScanRange()
	{
		return Utils.SplitRange(capacity, database.Engine.Settings.ResizeSplitSize, ProcessorNumber.CoreCount);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail GetVersionsByKey(Transaction tran, byte* key,
		HashComparer comparer, ref HashedIdCollection items, long exceptId = 0)
	{
		ulong hash = comparer.CalculateHashCode(key, seed, stringStorage);

		DatabaseErrorDetail error = null;
		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(hash);
		Bucket.LockAccess(bucket);

		TTTrace.Write(database.TraceId, hash, tran.Id, exceptId, bucket->Handle);

		if (tran != null && tran.Type == TransactionType.ReadWrite)
		{
			HashKeyReadLocker locker = database.GetHashIndexLocker(HashIndexDesc.Index);
			error = locker.Lock(tran, key, comparer, hash);
		}

		if (error == null)
		{
			ulong itemHandle = bucket->Handle;
			while (itemHandle != 0)
			{
				HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(itemHandle);
				byte* objectKey = Class.GetHashKey(item->objectHandle, out ClassObject* obj);
				TTTrace.Write(database.TraceId, tran.Id, exceptId, obj->id, obj->version);
				if (obj->id != exceptId)
				{
					Class @class = database.GetClassById(IdHelper.GetClassId(obj->id)).MainClass;
					HashComparer itemComparer = @class.GetHashedComparer(HashIndexDesc.Id, true);

					if (itemComparer.AreKeysEqual(objectKey, key, comparer, stringStorage))
					{
						bool readLockUnavailable = Database.IsUncommited(obj->version) ? obj->version != tran.Id : obj->version > tran.ReadVersion;
						if (tran != null && tran.Type == TransactionType.ReadWrite && readLockUnavailable)
						{
							error = DatabaseErrorDetail.Create(DatabaseErrorType.HashIndexConflict);
							break;
						}

						if (items.count == items.capacity)
							items.Resize(memoryManager);

						items.Add(obj->id, item->objectHandle, (ushort)@class.ClassDesc.Index);
					}
				}

				itemHandle = item->nextCollision;
			}
		}

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);

		return error;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void StoreObject(ref ObjectReader[] rs, ref int count, ObjectReader r)
	{
		if (rs.Length == count)
			Array.Resize(ref rs, rs.Length * 2);

		rs[count++] = r;
	}

	private HashComparer ProvideComparerInModelUpdate(ref Dictionary<short, HashComparer> localCache, ClassDescriptor classDesc)
	{
		if (localCache == null)
			localCache = new Dictionary<short, HashComparer>(2);

		if (localCache.TryGetValue(classDesc.Id, out HashComparer comparer))
			return comparer;

		KeyComparerDesc kad = classDesc.GetHashAccessDescByPropertyName(hashIndexDesc);
		comparer = new HashComparer(kad, null);
		localCache.Add(classDesc.Id, comparer);

		return comparer;
	}

	public DatabaseErrorDetail CheckUniqueness(Utils.Range range)
	{
		Dictionary<short, HashComparer> localCache = null;
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
				HashComparer comparer1 = class1.GetHashedComparer(hashIndexDesc.Id, false);
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
					HashComparer comparer2 = class2.GetHashedComparer(hashIndexDesc.Id, false);
					if (comparer2 == null)
						comparer2 = ProvideComparerInModelUpdate(ref localCache, class2.ClassDesc);

					byte* key2 = (byte*)obj2 + ClassObject.DataOffset;
					if (comparer1.AreKeysEqual(key1, key2, comparer2, stringStorage))
					{
						comparer1.TTTraceKeys(traceId, 0, hashIndexDesc.Id, key1, stringStorage, 7);
						comparer2.TTTraceKeys(traceId, 0, hashIndexDesc.Id, key2, stringStorage, 8);
						return DatabaseErrorDetail.CreateUniquenessConstraint(HashIndexDesc.FullName);
					}

					handle2 = nextCollision2;
				}

				handle1 = nextCollision1;
			}
		}

		return null;
	}

	private bool IdExists(long[] ids, int idCount, long id)
	{
		for (int i = 0; i < idCount; i++)
		{
			if (ids[i] == id)
				return true;
		}

		return false;
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
				hashIndexDesc.FullName, this.capacity, newCapacity, hashIndexDesc.Id);

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
		Dictionary<short, HashComparer> localCache = null;
		for (long i = range.Offset; i < range.Offset + range.Count; i++)
		{
			ulong handle = buckets[i].Handle;
			while (handle != 0)
			{
				HashIndexItem* item = (HashIndexItem*)fixedMemoryManager.GetBuffer(handle);
				ulong nextCollision = item->nextCollision;

				byte* key = Class.GetHashKey(item->objectHandle, out ClassObject* obj);
				Class @class = database.GetClassById(IdHelper.GetClassId(obj->id)).MainClass;
				HashComparer comparer = @class.GetHashedComparer(HashIndexDesc.Id, false);
				if (comparer == null)
					comparer = ProvideComparerInModelUpdate(ref localCache, @class.ClassDesc);

				ulong hash = comparer.CalculateHashCode(key, seed, stringStorage);
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
	public void Validate(ulong readVersion)
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
				byte* key = Class.GetHashKey(item->objectHandle, out ClassObject* obj);

				Class @class = database.GetClassById(IdHelper.GetClassId(obj->id)).MainClass;
				HashComparer comparer = @class.GetHashedComparer(HashIndexDesc.Id, true);

				if (!@class.IsObjectPresent(item->objectHandle))
					throw new InvalidOperationException();

				if (comparer.HasNullStrings(key, stringStorage))
					throw new InvalidOperationException();     

				ulong hash = comparer.CalculateHashCode(key, seed, stringStorage);
				long bucketIndex = CalculateBucket(hash);

				if (bucketIndex != i)
					throw new InvalidOperationException();

				TTTrace.Write(itemHandle, item->objectHandle, @class.ClassDesc.Id);
				comparer.TTTraceKeys(traceId, 0, HashIndexDesc.Id, key, stringStorage, 9);

				itemHandle = item->nextCollision;
				item = (HashIndexItem*)fixedMemoryManager.GetBuffer(itemHandle);
			}
		}

		if (bucketCount != resizeCounter.Count)
			throw new InvalidOperationException();
	}

	public bool HasObject(ClassObject* tobj, byte* key, HashComparer comparer)
	{
		ulong hash = comparer.CalculateHashCode(key, seed, stringStorage);
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

	public void Dispose(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(traceId, hashIndexDesc.Id);

		FreeBuffers(workers);

		NativeAllocator.Free((IntPtr)buckets);
		buckets = null;

		resizeCounter.Dispose();
	}

	public void PrepareForPendingRefill(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(traceId, hashIndexDesc.Id);

		FreeBuffers(workers);
		NativeAllocator.Free((IntPtr)buckets);
		buckets = null;

		pendingRefill = true;
	}

	public void PendingRefillStarted(long capacity)
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

	public void PendingRefillFinished()
	{
		pendingRefill = false;
	}

	private void FreeBuffers(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(database.TraceId, HashIndexDesc.Id);

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
		public void Add(long id, ulong objectHandle, ushort classIndex)
		{
			HashedItemId* p = items + count;
			p->id = id;
			p->objectHandle = objectHandle;
			p->classIndex = classIndex;
			count++;
		}
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = HashedItemId.Size)]
internal unsafe struct HashedItemId
{
	public const int Size = 24;

	public long id;
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
