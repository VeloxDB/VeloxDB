using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal unsafe sealed partial class HashKeyReadLocker : IDisposable
{
	StorageEngine engine;

	HashIndexDescriptor hashIndexDesc;
	Database database;

	ParallelResizeCounter resizeCounter;

	long capacity;
	long keyCountLimit;
	ulong capacityMask;
	float growthFactor;
	float loadFactor;
	ulong seed;
	Bucket* buckets;

	MemoryManager memoryManager;
	StringStorage stringStorage;

	HashComparer comparer;

	public HashKeyReadLocker(HashIndexDescriptor hindDesc, Database database)
	{
		TTTrace.Write(database.TraceId, hindDesc.Id);

		engine = database.Engine;

		this.stringStorage = engine.StringStorage;
		this.memoryManager = database.Engine.MemoryManager;
		this.hashIndexDesc = hindDesc;
		this.database = database;

		CreateComparer();

		loadFactor = engine.Settings.HashLoadFactor;
		capacity = HashUtils.CalculatePow2Capacity(1024, loadFactor, out keyCountLimit);
		capacityMask = (ulong)capacity - 1;
		seed = engine.HashSeed;
		buckets = (Bucket*)NativeAllocator.Allocate(capacity * Bucket.Size, false);
		for (long i = 0; i < capacity; i++)
		{
			buckets[i].Init();
		}

		growthFactor = engine.Settings.CollectionGrowthFactor;
		resizeCounter = new ParallelResizeCounter(keyCountLimit);
	}

	public HashIndexDescriptor HashIndexDesc => hashIndexDesc;

	public void ModelUpdated(HashIndexDescriptor hindDesc)
	{
		this.hashIndexDesc = hindDesc;
		CreateComparer();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public DatabaseErrorDetail Lock(Transaction tran, byte* key, HashComparer comparer, ulong hash)
	{
		comparer.TTTraceKeys(engine.TraceId, tran.Id, hashIndexDesc.Id, key, stringStorage, 10);

		DatabaseErrorDetail err = null;

		int lockHandle = resizeCounter.EnterReadLock();

		TTTrace.Write(engine.TraceId, tran.Id, hash);

		Bucket* bucket = buckets + CalculateBucket(hash);
		Bucket.LockAccess(bucket);

		ReaderInfo* r = TryInsert(bucket, key, comparer, out ulong itemHandle, out bool resize);
		if (!ReaderInfo.TryTakeHashKeyLock(tran, r, itemHandle, hashIndexDesc.Index, hash))
			err = DatabaseErrorDetail.CreateHashIndexLockContentionLimitExceeded(hashIndexDesc.FullName);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);

		if (resize && resizeCounter.Count > keyCountLimit)
			Resize();

		return err;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool IsLocked(Transaction tran, byte* key, ulong hash, HashComparer comparer)
	{
		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(hash);
		Bucket.LockAccess(bucket);

		HashLockerItem* item = FindItem(bucket, key, comparer, out var temp);
		bool isLocked = item != null && ReaderInfo.IsHashKeyInConflict(tran, &item->readerInfo);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);

		return isLocked;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Commit(ulong itemHandle, ulong hash, Transaction tran, ushort slot)
	{
		int lockHandle = resizeCounter.EnterReadLock();

		HashLockerItem* item = (HashLockerItem*)memoryManager.GetBuffer(itemHandle);

		TTTrace.Write(engine.TraceId, tran.Id, item->readerInfo.CommReadLockVer, item->readerInfo.LockCount);

		Bucket* bucket = buckets + CalculateBucket(hash);
		Bucket.LockAccess(bucket);

		ReaderInfo.FinalizeHashKeyLock(tran, &item->readerInfo, true, slot);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void RemapLockSlot(ulong itemHandle, ulong hash, ushort prevSlot, ushort newSlot)
	{
		int lockHandle = resizeCounter.EnterReadLock();

		HashLockerItem* item = (HashLockerItem*)memoryManager.GetBuffer(itemHandle);

		TTTrace.Write(engine.TraceId, prevSlot, newSlot, item->readerInfo.CommReadLockVer, item->readerInfo.LockCount);

		Bucket* bucket = buckets + CalculateBucket(hash);
		Bucket.LockAccess(bucket);

		ReaderInfo.RemapSlot(&item->readerInfo, prevSlot, newSlot);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void GarbageCollect(ulong itemHandle, ulong hash, ulong oldestReadVersion)
	{
		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(hash);
		ulong* pbnHandle = Bucket.LockAccess(bucket);
		FindItemAndGarbageCollect(bucket, pbnHandle, itemHandle, oldestReadVersion);
		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Rollback(ulong itemHandle, ulong hash, Transaction tran)
	{
		Checker.AssertTrue(tran.NextMerged == null);

		int lockHandle = resizeCounter.EnterReadLock();

		HashLockerItem* item = (HashLockerItem*)memoryManager.GetBuffer(itemHandle);

		Bucket* bucket = buckets + CalculateBucket(hash);
		Bucket.LockAccess(bucket);

		TTTrace.Write(engine.TraceId, tran.Id, item->readerInfo.CommReadLockVer, item->readerInfo.LockCount);

		ReaderInfo.FinalizeHashKeyLock(tran, &item->readerInfo, false, tran.Slot);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);
	}

	public void Rewind(ulong version)
	{
		TTTrace.Write(engine.TraceId, hashIndexDesc.Id, version);

		int lockHandle = resizeCounter.EnterReadLock();

		for (long i = 0; i < capacity; i++)
		{
			ulong handle = buckets[i].Handle;
			while (handle != 0)
			{
				HashLockerItem* item = (HashLockerItem*)memoryManager.GetBuffer(handle);
				item->readerInfo.CommReadLockVer = 0;
				handle = item->nextCollision;
			}
		}

		resizeCounter.ExitReadLock(lockHandle);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe HashLockerItem* FindItem(Bucket* bucket, byte* key, HashComparer comparer, out ulong handle)
	{
		TTTrace.Write(database.TraceId);

		handle = bucket->Handle;
		while (handle != 0)
		{
			HashLockerItem* item = (HashLockerItem*)memoryManager.GetBuffer(handle);
			if (this.comparer.AreKeysEqual(HashLockerItem.GetKey(item), key, comparer, stringStorage))
				return item;

			handle = item->nextCollision;
		}

		return null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ReaderInfo* TryInsert(Bucket* bucket, byte* key, HashComparer comparer, out ulong itemHandle, out bool resize)
	{
		HashLockerItem* item = FindItem(bucket, key, comparer, out itemHandle);
		if (item == null)
		{
			itemHandle = memoryManager.Allocate(HashLockerItem.Size + (int)hashIndexDesc.KeySize);
			item = (HashLockerItem*)memoryManager.GetBuffer(itemHandle);
			item->nextCollision = bucket->Handle;
			ReaderInfo.Init(&item->readerInfo);
			comparer.CopyKeyWithStringRetention(key, HashLockerItem.GetKey(item), stringStorage);
			bucket->Handle = itemHandle;
			resize = resizeCounter.Add(1);
			comparer.TTTraceKeys(engine.TraceId, 0, hashIndexDesc.Id, key, stringStorage, 11);
		}
		else
		{
			resize = false;
			comparer.TTTraceKeys(engine.TraceId, 0, hashIndexDesc.Id, key, stringStorage, 12);
		}

		return &item->readerInfo;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void FindItemAndGarbageCollect(Bucket* bucket, ulong* pbnHandle, ulong itemHandle, ulong oldestReadVersion)
	{
		ulong* pHandle = pbnHandle;
		while (*pHandle != 0)
		{
			HashLockerItem* item = (HashLockerItem*)memoryManager.GetBuffer(*pHandle);
			TTTrace.Write(engine.TraceId, pHandle[0], item->readerInfo.CommReadLockVer, item->readerInfo.LockCount);

			if (*pHandle == itemHandle && item->readerInfo.LockCount == 0 && item->readerInfo.CommReadLockVer <= oldestReadVersion)
			{
				ulong handle = *pHandle;
				*pHandle = item->nextCollision;
				comparer.ReleaseStrings(HashLockerItem.GetKey(item), stringStorage);
				memoryManager.Free(handle);
				resizeCounter.Dec();
				return;
			}
			else
			{
				pHandle = &item->nextCollision;
			}
		}
	}

	private void FreeBuffers()
	{
		TTTrace.Write(database.TraceId);

		for (long i = 0; i < capacity; i++)
		{
			Bucket* bucket = buckets + i;

			ulong itemHandle = bucket->Handle;
			HashLockerItem* item = (HashLockerItem*)memoryManager.GetBuffer(itemHandle);
			while (item != null)
			{
				ulong nextCollision = item->nextCollision;
				memoryManager.Free(itemHandle);

				itemHandle = nextCollision;
				item = (HashLockerItem*)memoryManager.GetBuffer(itemHandle);
			}
		}
	}

	private void FreeStrings()
	{
		if (resizeCounter.Count != 0)
		{
			for (long i = 0; i < capacity; i++)
			{
				Bucket* bucket = buckets + i;
				HashLockerItem* item = (HashLockerItem*)memoryManager.GetBuffer(bucket->Handle);
				while (item != null)
				{
					comparer.ReleaseStrings(HashLockerItem.GetKey(item), stringStorage);
					item = (HashLockerItem*)memoryManager.GetBuffer(item->nextCollision);
				}
			}
		}
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

	private void Resize()
	{
		TTTrace.Write(engine.TraceId, hashIndexDesc.Id);

		resizeCounter.EnterWriteLock();

		try
		{
			if (resizeCounter.Count <= keyCountLimit)
				return;

			long minCap = (long)(resizeCounter.Count * growthFactor);
			long newCapacity = HashUtils.CalculatePow2Capacity(minCap, loadFactor, out long newLimitCapacity);
			ulong newCapacityMask = (ulong)newCapacity - 1;

			Bucket* newBuckets = (Bucket*)NativeAllocator.Allocate(newCapacity * Bucket.Size, false);
			for (long i = 0; i < newCapacity; i++)
			{
				newBuckets[i].Init();
			}

			for (long i = 0; i < capacity; i++)
			{
				ulong handle = buckets[i].Handle;
				while (handle != 0)
				{
					HashLockerItem* item = (HashLockerItem*)memoryManager.GetBuffer(handle);
					ulong temp = item->nextCollision;
					RehashItem(item, handle, newCapacityMask, newBuckets);
					handle = temp;
				}
			}

			NativeAllocator.Free((IntPtr)buckets);

			this.keyCountLimit = newLimitCapacity;
			this.capacity = newCapacity;
			this.capacityMask = newCapacityMask;

			buckets = newBuckets;

			resizeCounter.Resized(keyCountLimit);
		}
		finally
		{
			resizeCounter.ExitWriteLock();
		}
	}

	private void RehashItem(HashLockerItem* item, ulong handle, ulong newCapacityMask, Bucket* newBuckets)
	{
		ulong hash = comparer.CalculateHashCode(HashLockerItem.GetKey(item), seed, stringStorage);
		long bucketIndex = CalculateBucket(hash, newCapacityMask);
		item->nextCollision = newBuckets[bucketIndex].Handle;
		newBuckets[bucketIndex].Handle = handle;
	}

	private void CreateComparer()
	{
		KeyProperty[] properties = new KeyProperty[hashIndexDesc.Properties.Length];
		int offset = 0;
		for (int i = 0; i < properties.Length; i++)
		{
			properties[i] = new KeyProperty(hashIndexDesc.Properties[i].PropertyType, offset);
			offset += PropertyTypesHelper.GetItemSize(hashIndexDesc.Properties[i].PropertyType);
		}

		comparer = comparer = new HashComparer(new KeyComparerDesc(properties), null);
	}

	public void Dispose()
	{
		TTTrace.Write(engine.TraceId, hashIndexDesc.Id);

		FreeBuffers();

		FreeStrings();
		NativeAllocator.Free((IntPtr)buckets);
		buckets = null;

		resizeCounter.Dispose();
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal unsafe struct HashLockerItem
{
	public const int Size = 24;

	public ReaderInfo readerInfo;
	public ulong nextCollision;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static byte* GetKey(HashLockerItem* item)
	{
		return (byte*)item + Size;
	}
}
