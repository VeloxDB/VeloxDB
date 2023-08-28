using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal unsafe sealed partial class KeyReadLocker : IDisposable
{
	StorageEngine engine;

	IndexDescriptor indexDesc;
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

	KeyComparer localComparer;

	public KeyReadLocker(IndexDescriptor indexDesc, Database database)
	{
		TTTrace.Write(database.TraceId, indexDesc.Id);

		engine = database.Engine;

		this.stringStorage = engine.StringStorage;
		this.memoryManager = database.Engine.MemoryManager;
		this.indexDesc = indexDesc;
		this.database = database;

		CreateComparer();

		loadFactor = engine.Settings.HashLoadFactor;
		capacity = HashUtils.CalculatePow2Capacity(1024 * 4, loadFactor, out keyCountLimit);
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

	public IndexDescriptor IndexDesc => indexDesc;

	public void ModelUpdated(IndexDescriptor indexDesc)
	{
		this.indexDesc = indexDesc;
		CreateComparer();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public DatabaseErrorDetail LockKey(Transaction tran, byte* key, KeyComparer comparer, string[] requestStrings, ulong hash)
	{
		comparer.TTTraceKeys(engine.TraceId, tran.Id, indexDesc.Id, key, requestStrings, stringStorage, 10);

		DatabaseErrorDetail err = null;

		int lockHandle = resizeCounter.EnterReadLock();

		TTTrace.Write(engine.TraceId, tran.Id, hash);

		Bucket* bucket = buckets + CalculateBucket(hash);
		Bucket.LockAccess(bucket);

		ReaderInfo* r = TryInsertKey(bucket, key, comparer, requestStrings, out ulong itemHandle, out bool resize);
		if (!ReaderInfo.TryTakeKeyLock(tran, r, itemHandle, indexDesc.Index, hash))
			err = DatabaseErrorDetail.CreateIndexLockContentionLimitExceeded(indexDesc.FullName);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);

		if (resize && resizeCounter.Count > keyCountLimit)
			ResizeKeyStorage();

		return err;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool IsLocked(Transaction tran, byte* key, ulong hash, KeyComparer comparer)
	{
		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(hash);
		Bucket.LockAccess(bucket);

		KeyLockerItem* item = FindKey(bucket, key, comparer, null, out var temp);
		bool isLocked = item != null && ReaderInfo.IsKeyInConflict(tran, &item->readerInfo);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);

		return isLocked;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void FinalizeKeyLock(ulong itemHandle, ulong hash, Transaction tran, ushort slot)
	{
		int lockHandle = resizeCounter.EnterReadLock();

		KeyLockerItem* item = (KeyLockerItem*)memoryManager.GetBuffer(itemHandle);

		TTTrace.Write(engine.TraceId, tran.Id, item->readerInfo.StandardLockCount, item->readerInfo.ExistanceLockCount);

		Bucket* bucket = buckets + CalculateBucket(hash);
		ulong* pbnHandle = Bucket.LockAccess(bucket);

		if (ReaderInfo.FinalizeKeyLock(tran, &item->readerInfo, true, slot))
			FindKeyAndRemove(bucket, pbnHandle, itemHandle);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void RemapKeyLockSlot(ulong itemHandle, ulong hash, ushort prevSlot, ushort newSlot)
	{
		int lockHandle = resizeCounter.EnterReadLock();

		KeyLockerItem* item = (KeyLockerItem*)memoryManager.GetBuffer(itemHandle);

		TTTrace.Write(engine.TraceId, prevSlot, newSlot, item->readerInfo.StandardLockCount, item->readerInfo.ExistanceLockCount);

		Bucket* bucket = buckets + CalculateBucket(hash);
		Bucket.LockAccess(bucket);

		ReaderInfo.RemapSlot(&item->readerInfo, prevSlot, newSlot);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe KeyLockerItem* FindKey(Bucket* bucket, byte* key, KeyComparer comparer, string[] requestStrings, out ulong handle)
	{
		TTTrace.Write(database.TraceId);

		handle = bucket->Handle;
		while (handle != 0)
		{
			KeyLockerItem* item = (KeyLockerItem*)memoryManager.GetBuffer(handle);
			if (comparer.Equals(key, requestStrings, KeyLockerItem.GetKey(item), localComparer, stringStorage))
				return item;

			handle = item->nextCollision;
		}

		return null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ReaderInfo* TryInsertKey(Bucket* bucket, byte* key, KeyComparer comparer, string[] requestStrings, out ulong itemHandle, out bool resize)
	{
		KeyLockerItem* item = FindKey(bucket, key, comparer, requestStrings, out itemHandle);
		if (item == null)
		{
			itemHandle = memoryManager.Allocate(KeyLockerItem.Size + (int)indexDesc.KeySize);
			item = (KeyLockerItem*)memoryManager.GetBuffer(itemHandle);
			item->nextCollision = bucket->Handle;
			ReaderInfo.Clear(&item->readerInfo);
			comparer.CopyWithStringStorage(key, requestStrings, KeyLockerItem.GetKey(item), stringStorage);
			bucket->Handle = itemHandle;
			resize = resizeCounter.Add(1);
			comparer.TTTraceKeys(engine.TraceId, 0, indexDesc.Id, key, requestStrings, stringStorage, 11);
		}
		else
		{
			resize = false;
			comparer.TTTraceKeys(engine.TraceId, 0, indexDesc.Id, key, requestStrings, stringStorage, 12);
		}

		return &item->readerInfo;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void FindKeyAndRemove(Bucket* bucket, ulong* pbnHandle, ulong itemHandle)
	{
		ulong* pHandle = pbnHandle;
		while (*pHandle != 0)
		{
			KeyLockerItem* item = (KeyLockerItem*)memoryManager.GetBuffer(*pHandle);
			TTTrace.Write(engine.TraceId, pHandle[0], item->readerInfo.StandardLockCount, item->readerInfo.ExistanceLockCount);

			if (*pHandle == itemHandle && item->readerInfo.TotalLockCount == 0)
			{
				ulong handle = *pHandle;
				*pHandle = item->nextCollision;
				localComparer.ReleaseStrings(KeyLockerItem.GetKey(item), stringStorage);
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

	private void FreeKeyBuffers()
	{
		TTTrace.Write(database.TraceId);

		for (long i = 0; i < capacity; i++)
		{
			Bucket* bucket = buckets + i;

			ulong itemHandle = bucket->Handle;
			KeyLockerItem* item = (KeyLockerItem*)memoryManager.GetBuffer(itemHandle);
			while (item != null)
			{
				ulong nextCollision = item->nextCollision;
				memoryManager.Free(itemHandle);

				itemHandle = nextCollision;
				item = (KeyLockerItem*)memoryManager.GetBuffer(itemHandle);
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
				KeyLockerItem* item = (KeyLockerItem*)memoryManager.GetBuffer(bucket->Handle);
				while (item != null)
				{
					localComparer.ReleaseStrings(KeyLockerItem.GetKey(item), stringStorage);
					item = (KeyLockerItem*)memoryManager.GetBuffer(item->nextCollision);
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

	private void ResizeKeyStorage()
	{
		TTTrace.Write(engine.TraceId, indexDesc.Id);

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
					KeyLockerItem* item = (KeyLockerItem*)memoryManager.GetBuffer(handle);
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

	private void RehashItem(KeyLockerItem* item, ulong handle, ulong newCapacityMask, Bucket* newBuckets)
	{
		ulong hash = localComparer.CalculateHashCode(KeyLockerItem.GetKey(item), seed, null, stringStorage);
		long bucketIndex = CalculateBucket(hash, newCapacityMask);
		item->nextCollision = newBuckets[bucketIndex].Handle;
		newBuckets[bucketIndex].Handle = handle;
	}

	private void CreateComparer()
	{
		KeyProperty[] properties = new KeyProperty[indexDesc.Properties.Length];
		int offset = 0;
		for (int i = 0; i < properties.Length; i++)
		{
			properties[i] = new KeyProperty(indexDesc.Properties[i].PropertyType, offset, SortOrder.Asc);
			offset += PropertyTypesHelper.GetItemSize(indexDesc.Properties[i].PropertyType);
		}

		localComparer = localComparer = new KeyComparer(new KeyComparerDesc(properties, indexDesc.CultureName, indexDesc.CaseSensitive));
	}

	public void Dispose()
	{
		TTTrace.Write(engine.TraceId, indexDesc.Id);

		FreeKeyBuffers();

		FreeStrings();
		NativeAllocator.Free((IntPtr)buckets);
		buckets = null;

		resizeCounter.Dispose();
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal unsafe struct KeyLockerItem
{
	public const int Size = 16;

	public ReaderInfo readerInfo;
	public ulong nextCollision;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static byte* GetKey(KeyLockerItem* item)
	{
		return (byte*)item + Size;
	}
}
