using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Diagnostics;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using System.Collections.Generic;
using System.Reflection.Metadata;

namespace VeloxDB.Storage;

internal unsafe sealed partial class InverseReferenceMap
{
	const int mergeOnStackLimit = 1024;

	Database database;
	ClassDescriptor classDesc;
	ushort classIndex;

	ParallelResizeCounter resizeCounter;
	float growthFactor;
	float loadFactor;
	long capacity;
	long minCapacity;
	long bucketCountLimit;
	ulong capacityMask;
	ulong seed;
	Bucket* buckets;

	MemoryManager memoryManager;

	public InverseReferenceMap(Database database, ClassDescriptor classDesc)
	{
		TTTrace.Write(database.TraceId, classDesc.Id);

		// Since we count used buckets, load factor needs to be smaller than that of a clasa for example.
		this.loadFactor = Math.Min(0.5f, database.Engine.Settings.HashLoadFactor * 0.7f);

		growthFactor = database.Engine.Settings.CollectionGrowthFactor;

		this.database = database;
		this.classDesc = classDesc;
		this.memoryManager = database.Engine.MemoryManager;

		classIndex = (ushort)classDesc.Index;

		minCapacity = database.Id == DatabaseId.User ? ParallelResizeCounter.SingleThreadedLimit * 2 : 64;
		capacity = Math.Max(minCapacity, 128);

		capacity = HashUtils.CalculatePow2Capacity(capacity, loadFactor, out bucketCountLimit);
		capacityMask = (ulong)capacity - 1;
		seed = database.Engine.HashSeed;

		buckets = (Bucket*)AlignedAllocator.Allocate((long)capacity * Bucket.Size, false);
		for (long i = 0; i < capacity; i++)
		{
			buckets[i].Init();
		}

		resizeCounter = new ParallelResizeCounter(bucketCountLimit);

		TTTrace.Write(capacity);
	}

	public ClassDescriptor ClassDesc => classDesc;
	public bool Disposed => buckets == null;
	public Database Database => database;

	public void ModelUpdated(ClassDescriptor classDesc)
	{
		this.classDesc = classDesc;
		this.classIndex = (ushort)classDesc.Index;
	}

	public void CommitModification(Transaction tran, AffectedInverseReferences* affected, ulong newVersion)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, affected->id, affected->isDelete, affected->propertyId, affected->classIndex, newVersion);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(affected->id, affected->propertyId);
		ulong* pBucketHandle = Bucket.LockAccess(bucket);

		if (affected->isDelete)
		{
			CommitDeleteInternal(tran, bucket, affected, newVersion);
		}
		else
		{
			CommitInternal(tran, bucket, pBucketHandle, affected, newVersion);
		}

		Bucket.UnlockAccess(bucket);

		resizeCounter.ExitReadLock(lockHandle);
	}

	public long CommitReadLock(Transaction tran, ulong item, ushort slot, out int propertyId)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, item, tran.CommitVersion);

		int lockHandle = resizeCounter.EnterReadLock();

		InvRefBaseItem* bitem = (InvRefBaseItem*)memoryManager.GetBuffer(item);
		propertyId = bitem->propertyId;

		Bucket* bucket = buckets + CalculateBucket(bitem->id, bitem->propertyId);
		Bucket.LockAccess(bucket);

		TTTrace.Write(bitem->id, bitem->propertyId);
		ReaderInfo.FinalizeInvRefLock(tran, bitem->id, bitem->propertyId, &bitem->readerInfo, true, slot);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);

		return bitem->id;
	}

	public long RemapReadLockSlot(ulong item, ushort prevSlot, ushort newSlot)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, prevSlot, newSlot);

		int lockHandle = resizeCounter.EnterReadLock();

		InvRefBaseItem* bitem = (InvRefBaseItem*)memoryManager.GetBuffer(item);

		Bucket* bucket = buckets + CalculateBucket(bitem->id, bitem->propertyId);
		Bucket.LockAccess(bucket);

		TTTrace.Write(bitem->id, bitem->propertyId);
		ReaderInfo.RemapSlot(&bitem->readerInfo, prevSlot, newSlot);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);

		return bitem->id;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void GetReferences(Transaction tran, long id, ReadOnlyArray<ReferencePropertyDescriptor> properties,
		ref long* refs, ref int refsSize, int* refCounts)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, id);

		InverseRefsStorage r = new InverseRefsStorage(refsSize, refs);
		GetReferencesForProps(tran, id, properties, ref r, refCounts);
		refs = r.prefs;
		refsSize = r.size;
	}

	public DatabaseErrorDetail GetReferences(Transaction tran, long id, int propertyId,
		ref long[] refs, out int refCount, out bool refsTracked)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, (byte)tran.Type, id, propertyId);

		InverseRefsStorage r = new InverseRefsStorage(refs);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(id, propertyId);
		ulong* pBucketHandle = Bucket.LockAccess(bucket);

		TTTrace.Write(bucket->Handle);

		DatabaseErrorDetail error;
		if (tran.Type == TransactionType.Read)
		{
			error = GetReferencesInternal(tran, bucket, pBucketHandle, id, propertyId, false, ref r, out refCount, out refsTracked, out _);
			refs = r.refs;
			Bucket.UnlockAccess(bucket);
			resizeCounter.ExitReadLock(lockHandle);
		}
		else
		{
			ulong prevItem = bucket->Handle;
			error = GetReferencesInternal(tran, bucket, pBucketHandle, id, propertyId, true, ref r, out refCount, out refsTracked, out _);
			ulong newItem = bucket->Handle;
			refs = r.refs;
			Bucket.UnlockAccess(bucket);

			resizeCounter.ExitReadLock(lockHandle);

			TTTrace.Write(newItem);
			if (prevItem == 0 && newItem != 0)
			{
				if (resizeCounter.Add(lockHandle, 1) && resizeCounter.Count > bucketCountLimit)
					Resize();
			}
		}

		return error;
	}

	public void ApplyAlignmentModification(TransactionContext tc, ulong commitVersion, long id, int propertyId, bool isTracked, int insertCount,
		int deleteCount, InverseReferenceOperation* inserts, InverseReferenceOperation* deletes)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, id, propertyId, isTracked, insertCount, deleteCount);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(id, propertyId);
		ulong* pBucketHandle = Bucket.LockAccess(bucket);

		TTTrace.Write(bucket->Handle);

		ulong prevItem = bucket->Handle;
		ApplyAlignmentModificationInternal(tc, bucket, commitVersion, pBucketHandle, id, propertyId, isTracked, insertCount, deleteCount, inserts, deletes);
		GarbageCollectInternal(bucket, pBucketHandle, id, propertyId, Database.MaxCommitedVersion);
		ulong newItem = bucket->Handle;

		Bucket.UnlockAccess(bucket);

		resizeCounter.ExitReadLock(lockHandle);

		TTTrace.Write(newItem);
		if (prevItem == 0 && newItem != 0)
		{
			if (resizeCounter.Add(lockHandle, 1) && resizeCounter.Count > bucketCountLimit)
				Resize();
		}
		else if (prevItem != 0 && newItem == 0)
		{
			resizeCounter.Dec(lockHandle);
		}
	}

	public void GarbageCollect(long id, int propertyId, ulong oldestReadVersion)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, id, propertyId, oldestReadVersion);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(id, propertyId);
		ulong* pBucketHandle = Bucket.LockAccess(bucket);

		TTTrace.Write(capacity, bucket->Handle);

		bool b = bucket->Handle != 0;
		GarbageCollectInternal(bucket, pBucketHandle, id, propertyId, oldestReadVersion);
		b &= bucket->Handle == 0;

		Bucket.UnlockAccess(bucket);

		resizeCounter.ExitReadLock(lockHandle);

		if (b)
		{
			TTTrace.Write(database.TraceId);
			resizeCounter.Dec(lockHandle);
		}
	}

	public void Insert(ulong version, long id, int propertyId, bool isTracked,
		bool isDeleted, int insertCount, InverseReferenceOperation* inserts)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, version, id, propertyId, isTracked, isDeleted, insertCount);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(id, propertyId);
		Bucket.LockAccess(bucket);

		TTTrace.Write(capacity, bucket->Handle);

		ulong prevItem = bucket->Handle;

		InvRefBaseItem* item = CreateBaseFromModification(id, propertyId, isTracked, insertCount, inserts, out ulong itemHandle);
		ReaderInfo.Init(&item->readerInfo);
		TTTrace.Write(database.TraceId, id, propertyId, itemHandle, version);
		item->Version = version;
		item->IsDeleted = isDeleted;
		item->NextCollision = bucket->Handle;
		item->NextBase = 0;
		item->nextDelta = 0;
		bucket->Handle = itemHandle;

		ulong newItem = bucket->Handle;

		Bucket.UnlockAccess(bucket);

		TTTrace.Write(newItem);
		if (prevItem == 0 && newItem != 0)
		{
			resizeCounter.ExitReadLock(lockHandle);

			if (resizeCounter.Add(lockHandle, 1) && resizeCounter.Count > bucketCountLimit)
				Resize();
		}
		else
		{
			resizeCounter.ExitReadLock(lockHandle);
		}
	}

	public DatabaseErrorDetail Delete(Transaction tran, long id)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, id);

		int lockHandle = resizeCounter.EnterReadLock();

		DatabaseErrorDetail res = null;
		int count = 0;
		for (int i = 0; i < classDesc.InverseReferences.Length; i++)
		{
			int propertyId = classDesc.InverseReferences[i].Id;

			Bucket* bucket = buckets + CalculateBucket(id, propertyId);
			ulong* pBucketHandle = Bucket.LockAccess(bucket);

			TTTrace.Write(capacity, bucket->Handle);

			ulong prevItem = bucket->Handle;
			res = DeleteInternal(tran, bucket, pBucketHandle, id, propertyId, out ulong itemHandle);

			ulong newItem = bucket->Handle;
			TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, id, newItem, itemHandle);
			Bucket.UnlockAccess(bucket);

			if (res != null)
			{
				Checker.AssertTrue(itemHandle == 0);
				break;
			}

			if (prevItem == 0 && newItem != 0)
				count++;

			if (itemHandle != 0)
				tran.Context.AddAffectedInvRef(classIndex, id, propertyId, itemHandle, true);
		}

		resizeCounter.ExitReadLock(lockHandle);
		if (count > 0 && resizeCounter.Add(lockHandle, count) && resizeCounter.Count > bucketCountLimit)
			Resize();

		return res;
	}

	public void DeleteProperties(Utils.Range range, HashSet<int> propIds)
	{
		TTTrace.Write(database.Id, range.Offset, range.Count, capacity, classDesc.Id, propIds.Count);

		long count = 0;
		for (long i = range.Offset; i < range.Offset + range.Count; i++)
		{
			Bucket* bucket = buckets + i;
			ulong* handlePointer = Bucket.LockAccess(bucket);

			ulong bitemHandle = bucket->Handle;
			ulong prevHandle = bucket->Handle;
			while (bitemHandle != 0)
			{
				InvRefBaseItem* bitem = (InvRefBaseItem*)memoryManager.GetBuffer(bitemHandle);
				TTTrace.Write(database.TraceId, classDesc.Id, bitem->id, bitem->propertyId, bitem->Version, bitem->IsDeleted, bitem->Count);

				if (propIds.Contains(bitem->propertyId))
				{
					Checker.AssertTrue(bitem->NextBase == 0); // Since we are in model updated an GC has been drained

					ulong ditemHandle = bitem->nextDelta;
					InvRefDeltaItem* ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(ditemHandle);
					while (ditem != null)
					{
						ulong nextDelta = ditem->nextDelta;
						memoryManager.Free(ditemHandle);
						ditemHandle = nextDelta;
						ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(ditemHandle);
					}

					ulong nextCollision = bitem->nextCollision;
					memoryManager.Free(bitemHandle);
					*handlePointer = bitemHandle = nextCollision;
				}
				else
				{
					handlePointer = &(bitem->nextCollision);
					bitemHandle = bitem->nextCollision;
				}
			}

			if (prevHandle != 0 && bucket->Handle == 0)
				count++;

			Bucket.UnlockAccess(bucket);
		}

		resizeCounter.Sub(count);
	}

	public DatabaseErrorDetail Modify(Transaction tran, bool ignoreDeleted, long id, int propertyId, bool isTracked,
		int insertCount, int deleteCount, InverseReferenceOperation* inserts, InverseReferenceOperation* deletes)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, ignoreDeleted, id, propertyId, isTracked, insertCount, deleteCount);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(id, propertyId);
		ulong* pBucketHandle = Bucket.LockAccess(bucket);

		TTTrace.Write(bucket->Handle);

		bool b = bucket->Handle == 0;
		DatabaseErrorDetail res = ModifyInternal(tran, ignoreDeleted, bucket, pBucketHandle, id, propertyId,
			isTracked, insertCount, deleteCount, inserts, deletes, out ulong itemHandle);
		b &= bucket->Handle != 0;

		Bucket.UnlockAccess(bucket);

		if (itemHandle != 0)
			tran.Context.AddAffectedInvRef(classIndex, id, propertyId, itemHandle, false);

		if (b)
		{
			TTTrace.Write(database.TraceId);
			resizeCounter.ExitReadLock(lockHandle);

			if (resizeCounter.Add(lockHandle, 1) && resizeCounter.Count > bucketCountLimit)
				Resize();
		}
		else
		{
			resizeCounter.ExitReadLock(lockHandle);
		}

		return res;
	}

	public void Merge(TransactionContext tc, long id, int propertyId, ulong commitVersion, bool forceMerge)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, id, propertyId, forceMerge);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(id, propertyId);
		ulong* pBucketHandle = Bucket.LockAccess(bucket);

		if (bucket->Handle != 0)
			TryMergeInternal(tc, bucket, pBucketHandle, id, propertyId, commitVersion, forceMerge);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);
	}

	public void Rewind(ulong version)
	{
		TTTrace.Write(database.TraceId, ClassDesc.Id, version);

		int lockHandle = resizeCounter.EnterReadLock();

		for (long i = 0; i < capacity; i++)
		{
			Bucket* bucket = buckets + i;

			ulong bitemHandle = bucket->Handle;
			while (bitemHandle != 0)
			{
				InvRefBaseItem* bitem = (InvRefBaseItem*)memoryManager.GetBuffer(bitemHandle);

				InvRefBaseItem* vbitem = bitem;
				while (vbitem != null)
				{
					vbitem->readerInfo.CommReadLockVer = 0;
					vbitem = (InvRefBaseItem*)memoryManager.GetBuffer(vbitem->NextBase);
				}

				bitemHandle = bitem->NextCollision;
			}
		}

		resizeCounter.ExitReadLock(lockHandle);
	}

	public void RollbackModification(Transaction tran, AffectedInverseReferences* affected)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, affected->id, affected->isDelete,
			affected->propertyId, affected->handle, affected->classIndex);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(affected->id, affected->propertyId);
		ulong* pBucketHandle = Bucket.LockAccess(bucket);

		Checker.AssertTrue(bucket->Handle != 0);

		if (affected->isDelete)
		{
			RollbackDeleteInternal(tran, bucket, pBucketHandle, affected);
		}
		else
		{
			RollbackInternal(tran, bucket, pBucketHandle, affected);
		}

		bool b = bucket->Handle == 0;

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);

		if (b)
			resizeCounter.Dec(lockHandle);
	}

	public long RollbackReadLock(Transaction tran, ulong item, out int propertyId)
	{
		int lockHandle = resizeCounter.EnterReadLock();

		InvRefBaseItem* bitem = (InvRefBaseItem*)memoryManager.GetBuffer(item);
		propertyId = bitem->propertyId;

		Bucket* bucket = buckets + CalculateBucket(bitem->id, bitem->propertyId);
		Bucket.LockAccess(bucket);

		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, bucket->Handle, bitem->IsDeleted, bitem->id,
			bitem->IsTracked, bitem->NextBase, bitem->nextDelta, bitem->Count, bitem->Version);

		ReaderInfo.FinalizeInvRefLock(tran, bitem->id, bitem->propertyId, &bitem->readerInfo, false, tran.Slot);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);

		return bitem->id;
	}

	public void CompactUntrackedProperties(Utils.Range range, HashSet<int> propIds)
	{
		TTTrace.Write(database.Id, range.Offset, range.Count, capacity, classDesc.Id, propIds.Count);

		int count = 0;
		for (long i = range.Offset; i < range.Offset + range.Count; i++)
		{
			Bucket* bucket = buckets + i;
			ulong* handlePointer = Bucket.LockAccess(bucket);

			ulong bitemHandle = bucket->Handle;
			ulong prevHandle = bitemHandle;

			while (bitemHandle != 0)
			{
				InvRefBaseItem* bitem = (InvRefBaseItem*)memoryManager.GetBuffer(bitemHandle);
				TTTrace.Write(database.TraceId, classDesc.Id, bitem->id, bitem->propertyId, bitem->Version, bitem->IsDeleted, bitem->Count);

				if (propIds.Contains(bitem->propertyId))
				{
					Checker.AssertTrue(bitem->NextBase == 0); // Since we are in model updated an GC has been drained

					int refCount = bitem->Count;
					ulong ditemHandle = bitem->nextDelta;
					ulong version = bitem->Version;
					InvRefDeltaItem* ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(ditemHandle);
					while (ditem != null)
					{
						refCount += (ditem->insertCount - ditem->deleteCount);
						ulong nextDelta = ditem->nextDelta;
						version = ditem->version;
						memoryManager.Free(ditemHandle);
						ditemHandle = nextDelta;
						ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(ditemHandle);
					}

					ulong* nextCollisionPointer = &(bitem->nextCollision);
					ulong nextCollision = bitem->nextCollision;
					memoryManager.Free(bitemHandle);

					if (refCount == 0)
					{
						*handlePointer = bitemHandle = nextCollision;
					}
					else
					{
						InvRefBaseItem* newItem = CreateBase(bitem->id, bitem->propertyId, false, refCount, null, out ulong newItemHandle);
						newItem->Version = version;
						newItem->NextBase = 0;
						newItem->nextDelta = 0;
						newItem->IsTracked = false;
						newItem->NextCollision = nextCollision;
						*handlePointer = newItemHandle;

						handlePointer = &(newItem->nextCollision);
						bitemHandle = nextCollision;
					}
				}
				else
				{
					handlePointer = &(bitem->nextCollision);
					bitemHandle = bitem->nextCollision;
				}
			}

			if (prevHandle != 0 && bucket->Handle == 0)
				count++;

			Bucket.UnlockAccess(bucket);
		}

		resizeCounter.Sub(count);
	}

	public Utils.Range[] GetScanRanges()
	{
		return Utils.SplitRange(capacity, database.Engine.Settings.ResizeSplitSize, ProcessorNumber.CoreCount);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe InvRefDeltaItem* CreateDelta(bool isTracked, int insertCount, int deleteCount, InverseReferenceOperation* inserts, InverseReferenceOperation* deletes, out ulong itemHandle)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, isTracked, insertCount, deleteCount);

		int size = InvRefDeltaItem.RefsOffset + (isTracked ? (insertCount + deleteCount) * 8 : 0);
		itemHandle = memoryManager.Allocate((int)size);
		InvRefDeltaItem* item = (InvRefDeltaItem*)memoryManager.GetBuffer(itemHandle);

		item->insertCount = insertCount;
		item->deleteCount = deleteCount;

		if (isTracked)
		{
			long* refs = (long*)((byte*)item + InvRefDeltaItem.RefsOffset);
			for (int i = 0; i < insertCount; i++)
			{
				TTTrace.Write(inserts[i].inverseReference);
				refs[i] = inserts[i].inverseReference;
			}

			refs += insertCount;
			for (int i = 0; i < deleteCount; i++)
			{
				TTTrace.Write(deletes[i].inverseReference);
				refs[i] = deletes[i].inverseReference;
			}
		}

		return item;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe InvRefBaseItem* CreateBase(long id, int propertyId, bool isTracked, int insertCount, long* inserts, out ulong itemHandle)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, id, propertyId, isTracked, insertCount);

		int size = InvRefBaseItem.RefsOffset + (isTracked ? insertCount * sizeof(long) : 0);
		itemHandle = memoryManager.Allocate(size);
		InvRefBaseItem* item = (InvRefBaseItem*)memoryManager.GetBuffer(itemHandle);

		item->id = id;
		item->propertyId = propertyId;
		ReaderInfo.InitWithUnusedBit(&item->readerInfo);
		item->Count = insertCount;
		item->IsTracked = isTracked;
		item->IsDeleted = false;

		if (isTracked)
		{
			long* refs = (long*)((byte*)item + InvRefBaseItem.RefsOffset);
			for (int i = 0; i < insertCount; i++)
			{
				refs[i] = inserts[i];
			}
		}

		return item;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private InvRefBaseItem* CreateEmptyBase(Bucket* bucket, InvRefBaseItem* existingItem, long id, int propertyId, out ulong itemHandle)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, id, propertyId);

		ReferencePropertyDescriptor invRef = classDesc.FindInverseReference(propertyId);
		if (invRef == null) // Invalid request for this map
		{
			itemHandle = 0;
			return null;
		}

		InvRefBaseItem* item = CreateBase(id, propertyId, invRef.TrackInverseReferences, 0, null, out itemHandle);
		ReaderInfo.Init(&item->readerInfo);
		TTTrace.Write(database.TraceId, id, propertyId, itemHandle);

		item->Version = 0;
		item->NextCollision = 0;
		item->NextBase = 0;
		item->nextDelta = 0;
		item->IsDeleted = false;
		item->IsTracked = invRef.TrackInverseReferences;

		if (existingItem == null)
		{
			TTTrace.Write();
			item->NextCollision = bucket->Handle;
			bucket->Handle = itemHandle;
			return item;
		}

		ulong* handlePointer = &existingItem->nextBase;
		while (*handlePointer != 0)
		{
			InvRefBaseItem* curr = (InvRefBaseItem*)memoryManager.GetBuffer(*handlePointer);
			TTTrace.Write(curr->id, curr->propertyId, curr->IsDeleted, curr->Version);
			handlePointer = &curr->nextBase;
		}

		Checker.AssertTrue((itemHandle & 0x8000000000000000) == 0);
		*handlePointer = itemHandle;

		return item;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe InvRefBaseItem* CreateBaseFromModification(long id, int propertyId, bool isTracked,
		int insertCount, InverseReferenceOperation* inserts, out ulong itemHandle)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, id, propertyId, isTracked, insertCount);

		int size = InvRefBaseItem.RefsOffset + (isTracked ? insertCount * 8 : 0);
		itemHandle = memoryManager.Allocate((int)size);
		InvRefBaseItem* item = (InvRefBaseItem*)memoryManager.GetBuffer(itemHandle);

		item->id = id;
		item->propertyId = propertyId;
		ReaderInfo.InitWithUnusedBit(&item->readerInfo);
		item->Count = insertCount;
		item->IsTracked = isTracked;
		item->IsDeleted = false;

		if (isTracked)
		{
			long* refs = (long*)((byte*)item + InvRefBaseItem.RefsOffset);
			for (int i = 0; i < insertCount; i++)
			{
				TTTrace.Write(inserts->inverseReference);
				refs[i] = inserts->inverseReference;
				inserts++;
			}
		}

		return item;
	}

	private void CountDeltaOperations(InvRefBaseItem* item, Transaction tran, out int deleteCount, out int insertCount)
	{
		ulong tranId = tran.Id;
		ulong readVersion = tran.ReadVersion;

		deleteCount = 0;
		insertCount = 0;
		InvRefDeltaItem* ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(item->nextDelta);
		while (ditem != null)
		{
			TTTrace.Write(ditem->nextDelta, ditem->deleteCount, ditem->insertCount, ditem->version);
			if (ditem->version <= readVersion || ditem->version == tranId)
			{
				insertCount += ditem->insertCount;
				deleteCount += ditem->deleteCount;
			}

			ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(ditem->nextDelta);
		}
	}

	private InvRefBaseItem* FindReadBaseItemInTransaction(Transaction tran, InvRefBaseItem* latestItem,
		ulong itemHandle, bool readLock, out DatabaseErrorDetail error)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, itemHandle, readLock);

		if (!readLock)
		{
			ulong readVersion = tran.ReadVersion;
			while (latestItem != null)
			{
				TTTrace.Write(latestItem->id, latestItem->propertyId, latestItem->IsDeleted, latestItem->Version);
				if (latestItem->Version <= readVersion)
					break;

				latestItem = (InvRefBaseItem*)memoryManager.GetBuffer(latestItem->NextBase);
			}

			error = null;
			return latestItem;
		}

		if (IsInReadConflict(tran, latestItem->Version))
		{
			error = DatabaseErrorDetail.CreateConflict(latestItem->id, classDesc.FullName);
			return null;
		}

		InvRefBaseItem* bitem = latestItem;
		while (bitem != null)
		{
			TTTrace.Write(database.TraceId, bitem->id, bitem->IsDeleted, bitem->IsTracked,
				bitem->propertyId, bitem->Count, bitem->Version, bitem->nextDelta);

			ulong ditemHandle = bitem->nextDelta;
			while (ditemHandle != 0)
			{
				InvRefDeltaItem* ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(ditemHandle);
				TTTrace.Write(database.TraceId, ditem->deleteCount, ditem->insertCount, ditem->version);

				if (IsInReadConflict(tran, ditem->version))
				{
					error = DatabaseErrorDetail.CreateConflict(bitem->id, classDesc.FullName);
					return null;
				}

				ditemHandle = ditem->nextDelta;
			}

			bitem = (InvRefBaseItem*)memoryManager.GetBuffer(bitem->NextBase);
		}

		ReaderInfo.TakeInvRefLock(tran, latestItem->id, latestItem->propertyId, classIndex,
			latestItem->IsEmpty(), &latestItem->readerInfo, itemHandle);

		error = null;
		return latestItem;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool IsInReadConflict(Transaction tran, ulong version)
	{
		return version > tran.ReadVersion && version != tran.Id;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void GetDeleteMapRequirements(int count, out int capacity, out int byteSize, out int stackByteSize)
	{
		CompactLongMap.GetBufferRequirements(count, out capacity, out byteSize);
		stackByteSize = byteSize < mergeOnStackLimit ? byteSize : 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private CompactLongMap* CreateDeleteMap(byte* buffer, int byteSize, int capacity, out ulong handle)
	{
		if (capacity == 0)
		{
			handle = 0;
			return null;
		}

		handle = 0;
		if (byteSize >= mergeOnStackLimit)
			buffer = memoryManager.Allocate(byteSize, out handle);

		return CompactLongMap.Create(buffer, capacity);
	}

	[SkipLocalsInit]
	private int ReadMergeRefs(Transaction tran, InvRefBaseItem* item, ref InverseRefsStorage refs)
	{
		CountDeltaOperations(item, tran, out int deleteCount, out int insertCount);

		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, deleteCount, insertCount, item->id,
			item->IsDeleted, item->IsTracked, item->nextDelta, item->Count, item->Version);

		int refCount = item->Count + insertCount - deleteCount;
		if (!item->IsTracked)
			return refCount;

		if (refs.EmptyCount < refCount)
			refs.Resize(Math.Max((int)refCount, refs.size * 2));

		GetDeleteMapRequirements(deleteCount, out int capacity, out int byteSize, out int stackByteSize);
		byte* buffer = stackalloc byte[stackByteSize];
		CompactLongMap* deleted = CreateDeleteMap(buffer, byteSize, capacity, out ulong handle);

		if (refs.refs != null)
		{
			fixed (long* lp = refs.refs)
			{
				MergeReferencesAndRemoveVersion(deleted, item, lp + refs.offset, refCount, tran.Id, tran.ReadVersion, 0);
			}
		}
		else
		{
			MergeReferencesAndRemoveVersion(deleted, item, refs.prefs + refs.offset, refCount, tran.Id, tran.ReadVersion, 0);
		}

		memoryManager.FreeOptional(handle);

		return refCount;
	}

	private unsafe DatabaseErrorDetail ModifyInternal(Transaction tran, bool ignoreDeleted, Bucket* bucket, ulong* pBucketHandle,
		long id, int propertyId, bool isTracked, int insertCount, int deleteCount,
		InverseReferenceOperation* inserts, InverseReferenceOperation* deletes, out ulong itemHandle)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, bucket->Handle,
			ignoreDeleted, id, propertyId, isTracked, insertCount, deleteCount);

		InvRefBaseItem* item = FindWriteItemByIdAndProp(tran, ignoreDeleted, pBucketHandle,
			id, propertyId, out DatabaseErrorDetail error, out bool isDeleted);

		if (error != null || isDeleted)
		{
			itemHandle = 0;
			return error;
		}

		if (item == null)
		{
			item = CreateEmptyBase(bucket, item, id, propertyId, out itemHandle);
			Checker.AssertTrue(isTracked == item->IsTracked);

			// Possible if single changeset first set an invalid reference, than followed that
			// with a valid one. Reference validator only check final state.
			if (item == null)
				return null;
		}

		TTTrace.Write(item->IsDeleted, item->id, item->Version, item->Count);

		if (item->IsDeleted)
		{
			if (item->Version != tran.Id)
			{
				itemHandle = 0;
				return DatabaseErrorDetail.CreateConflict(id, classDesc.FullName);
			}

			if (insertCount > 0)
			{
				ClassDescriptor invalidRefClassDesc = IdHelper.GetClass(classDesc.Model, inserts[0].directReference);
				itemHandle = 0;
				return DatabaseErrorDetail.CreateUnknownReference(inserts[0].inverseReference, invalidRefClassDesc.FullName,
					invalidRefClassDesc.GetProperty(inserts[0].PropertyId).Name, id);
			}

			// Reference integrity validation should prevent this
			throw new CriticalDatabaseException();
		}

		InvRefBaseItem* writeItem = FindWriteBaseVersionInTransaction(tran, item, out error);
		if (error != null)
		{
			itemHandle = 0;
			return error;
		}

		// Base item might not be found if a GC, triggered by another transaction (touching this same inverse reference), has collected it.
		if (writeItem == null)
		{
			TTTrace.Write();
			writeItem = CreateEmptyBase(bucket, item, id, propertyId, out itemHandle);
			Checker.AssertTrue(isTracked == writeItem->IsTracked);
		}

		InvRefDeltaItem* deltaItem = CreateDelta(isTracked, insertCount, deleteCount, inserts, deletes, out itemHandle);
		deltaItem->version = tran.Id;
		deltaItem->nextDelta = writeItem->nextDelta;
		writeItem->nextDelta = itemHandle;

		return null;
	}

	[SkipLocalsInit]
	private unsafe void ApplyAlignmentModificationInternal(TransactionContext tc, Bucket* bucket, ulong commitVersion, ulong* pBucketHandle, long id, int propertyId, bool isTracked,
		int insertCount, int deleteCount, InverseReferenceOperation* inserts, InverseReferenceOperation* deletes)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, bucket->Handle, id, propertyId, isTracked, insertCount, deleteCount);

		InvRefBaseItem* item = FindItemByIdAndProp(pBucketHandle, id, propertyId, out ulong* prevItemPointer);

		if (item == null)
		{
			Checker.AssertTrue(deleteCount == 0);
			item = CreateBaseFromModification(id, propertyId, isTracked, insertCount, inserts, out ulong itemHandle);
			ReaderInfo.Init(&item->readerInfo);
			item->Version = commitVersion;
			item->IsDeleted = false;
			item->NextBase = 0;
			item->nextDelta = 0;
			item->NextCollision = bucket->Handle;
			bucket->Handle = itemHandle;
			return;
		}

		// We merge all the references (base item, deltas, alignment changes)
		Checker.AssertTrue(item->nextBase == 0);

		ulong existingItemHandle = *prevItemPointer;

		int totalDeleteCount = deleteCount;
		int totalInsertCount = insertCount;
		InvRefDeltaItem* ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(item->nextDelta);
		while (ditem != null)
		{
			TTTrace.Write(ditem->deleteCount, ditem->insertCount, ditem->version);

			totalInsertCount += ditem->insertCount;
			totalDeleteCount += ditem->deleteCount;

			ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(ditem->nextDelta);
		}

		int newRefCount = item->Count + totalInsertCount - totalDeleteCount;
		int mergedSize = InvRefBaseItem.RefsOffset;
		if (item->IsTracked)
			mergedSize += newRefCount * 8;

		ulong mergedHandle = memoryManager.Allocate((int)mergedSize);
		InvRefBaseItem* mergedItem = (InvRefBaseItem*)memoryManager.GetBuffer(mergedHandle);

		mergedItem->Count = newRefCount;

		if (item->IsTracked)
		{
			long* mergedBuffer = (long*)((byte*)mergedItem + InvRefBaseItem.RefsOffset);

			GetDeleteMapRequirements(totalDeleteCount, out int capacity, out int byteSize, out int stackByteSize);
			byte* buffer = stackalloc byte[stackByteSize];
			CompactLongMap* deleted = CreateDeleteMap(buffer, byteSize, capacity, out ulong handle);
			MergeReferencesAndRemoveVersion(deleted, item, mergedBuffer, newRefCount, 0,
				Database.MaxCommitedVersion, 0, inserts, insertCount, deletes, deleteCount);
			memoryManager.FreeOptional(handle);
		}

		mergedItem->id = id;
		mergedItem->propertyId = propertyId;
		ReaderInfo.Init(&mergedItem->readerInfo);
		TTTrace.Write(database.TraceId, id, propertyId, mergedHandle);
		mergedItem->IsTracked = item->IsTracked;
		mergedItem->deleted_version = commitVersion;
		mergedItem->NextBase = existingItemHandle;
		mergedItem->NextCollision = item->NextCollision;
		mergedItem->nextDelta = 0;

		*prevItemPointer = mergedHandle;
	}

	private void GetReferencesForProps(Transaction tran, long id, ReadOnlyArray<ReferencePropertyDescriptor> properties,
		ref InverseRefsStorage refs, int* refCounts)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, id);

		Checker.AssertTrue(tran.Type == TransactionType.ReadWrite);

		int lockHandle = resizeCounter.EnterReadLock();

		for (int i = 0; i < properties.Length; i++)
		{
			int propertyId = properties[i].Id;

			Bucket* bucket = buckets + CalculateBucket(id, propertyId);
			ulong* pBucketHandle = Bucket.LockAccess(bucket);

			GetReferencesInternal(tran, bucket, pBucketHandle, id, propertyId, false, ref refs,
				out int currCount, out bool refsTracked, out bool itemFound);

			TTTrace.Write(itemFound, refsTracked, currCount);

			Bucket.UnlockAccess(bucket);

			if (refsTracked)
			{
				refs.offset += currCount;
				refCounts[i] = currCount;
			}
			else
			{
				refCounts[i] = -currCount;
			}
		}

		resizeCounter.ExitReadLock(lockHandle);
	}

	private static DatabaseErrorDetail GetReferencesNoMerge(InvRefBaseItem* item, ref InverseRefsStorage refs, out int refCount)
	{
		if (refs.EmptyCount < item->Count)
			refs.Resize(Math.Max((int)item->Count, refs.size * 2));

		refCount = item->Count;
		if (!item->IsTracked)
			return null;

		long* p = (long*)((byte*)item + InvRefBaseItem.RefsOffset);
		refs.AddRange(p, item->Count);

		return null;
	}

	private DatabaseErrorDetail GetReferencesInternal(Transaction tran, Bucket* bucket, ulong* pBucketHandle, long id,
		int propertyId, bool shouldLock, ref InverseRefsStorage refs, out int refCount, out bool refsTracked, out bool found)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, bucket->Handle, id, propertyId, shouldLock);

		refCount = 0;
		refsTracked = true;
		found = false;

		InvRefBaseItem* item = FindItemByIdAndPropReadLockMode(shouldLock ? tran : null,
			pBucketHandle, id, propertyId, out ulong* prevItemPointer, out DatabaseErrorDetail error);

		if (error != null)
			return error;

		if (item != null)
		{
			TTTrace.Write(item->Version, item->Count, item->IsDeleted, item->IsTracked);
			item = FindReadBaseItemInTransaction(tran, item, *prevItemPointer, shouldLock, out error);

			if (error != null)
				return error;
		}

		if (item == null)
		{
			if (!shouldLock)
				return null;

			item = CreateEmptyBase(bucket, null, id, propertyId, out ulong itemHandle);

			// Check whether unexisting property was requested
			TTTrace.Write(database.TraceId, item == null);
			if (item == null)
				return null;

			ReaderInfo.TakeInvRefLock(tran, id, propertyId, classIndex, true, &item->readerInfo, itemHandle);
		}

		refsTracked = item->IsTracked;

		if (item->IsDeleted)
			return null;

		TTTrace.Write(item->IsDeleted, item->nextDelta, item->Count, item->Version,
			item->readerInfo.CommReadLockVer, item->readerInfo.LockCount);

		if (item->nextDelta == 0)
			return GetReferencesNoMerge(item, ref refs, out refCount);

		refCount = ReadMergeRefs(tran, item, ref refs);
		return null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe InvRefBaseItem* FindWriteItemByIdAndProp(Transaction tran, bool ignoreDeleted,
		ulong* pcurr, long id, int propertyId, out DatabaseErrorDetail error, out bool isDeleted)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, ignoreDeleted, id, propertyId);

		InvRefBaseItem* item;
		InvRefBaseItem* res = null;

		isDeleted = false;

		while (*pcurr != 0)
		{
			item = (InvRefBaseItem*)memoryManager.GetBuffer(*pcurr);
			TTTrace.Write(item->id, item->IsDeleted, item->Version, item->Count, item->propertyId, item->IsTracked);

			if (item->id == id && item->propertyId == propertyId)
			{
				if (item->IsDeleted)
				{
					isDeleted = true;
					if (item->Version != tran.Id)
					{
						error = DatabaseErrorDetail.CreateConflict(id, classDesc.FullName);
					}
					else if (ignoreDeleted)
					{
						error = null;
					}
					else
					{
						error = DatabaseErrorDetail.CreateUpdateNonExistent(id, classDesc.FullName);
					}

					return null;
				}

				res = item;
				break;
			}

			pcurr = &item->nextCollision;
		}

		error = null;
		return res;
	}

	private InvRefBaseItem* FindWriteBaseVersionInTransaction(Transaction tran, InvRefBaseItem* item, out DatabaseErrorDetail error)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id);

		long id = item->id;
		ulong readVersion = tran.ReadVersion;

		InvRefBaseItem* resItem = null;
		while (item != null)
		{
			TTTrace.Write(item->id, item->IsDeleted, item->propertyId, item->Version, readVersion);

			if (ReaderInfo.IsInvRefInConflict(tran, id, item->propertyId, &item->readerInfo))
			{
				error = DatabaseErrorDetail.CreateConflict(id, classDesc.FullName);
				return null;
			}

			// Even when the base item has been found, we continue checking for conflicts in other base items
			// Since older versions of base items might be locked. This occures when a transaction locks a base item
			// and later, merge is performed, creating new base item (which does not hold the lock)
			if (item->Version <= readVersion && resItem == null)
				resItem = item;

			item = (InvRefBaseItem*)memoryManager.GetBuffer(item->NextBase);
		}

		error = null;
		return resItem;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail DeleteInternal(Transaction tran, Bucket* bucket, ulong* pbnHande, long id, int propertyId, out ulong handle)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, id);

		handle = 0;

		DatabaseErrorDetail error = ValidateDelete(tran, bucket, id, propertyId);
		if (error != null)
			return error;

		ulong* handlePointer = pbnHande;
		ulong currItemHandle = bucket->Handle;
		while (currItemHandle != 0)
		{
			InvRefBaseItem* currItem = (InvRefBaseItem*)memoryManager.GetBuffer(currItemHandle);
			TTTrace.Write(currItem->id, currItem->IsDeleted, currItem->propertyId, currItem->Version);

			if (currItem->id == id && currItem->propertyId == propertyId)
			{
				handle = memoryManager.Allocate(InvRefBaseItem.RefsOffset);
				TTTrace.Write(currItem->id, tran.Id, currItem->IsDeleted, currItem->propertyId, currItem->Version, handle);
				InvRefBaseItem* item = (InvRefBaseItem*)memoryManager.GetBuffer(handle);
				item->id = id;
				item->propertyId = currItem->propertyId;
				ReaderInfo.Init(&item->readerInfo);
				item->NextCollision = currItem->NextCollision;
				item->Version = tran.Id;
				item->Count = 0;
				item->IsTracked = currItem->IsTracked;
				item->NextBase = *handlePointer;
				item->nextDelta = 0;
				item->IsDeleted = true;
				*handlePointer = handle;
				currItem = item;
				break;
			}

			handlePointer = &currItem->nextCollision;
			currItemHandle = currItem->NextCollision;
		}

		if (handle == 0)
		{
			// We need to create an ampty item to mark it as deleted.
			handle = memoryManager.Allocate(InvRefBaseItem.RefsOffset);
			TTTrace.Write(handle, tran.Id, id, propertyId);
			InvRefBaseItem* item = (InvRefBaseItem*)memoryManager.GetBuffer(handle);
			item->id = id;
			item->propertyId = propertyId;
			ReaderInfo.Init(&item->readerInfo);
			item->IsTracked = false;
			item->Version = tran.Id;
			item->IsDeleted = true;
			item->NextCollision = bucket->Handle;
			item->NextBase = 0;
			item->nextDelta = 0;
			item->Count = 0;
			bucket->Handle = handle;
		}

		return null;
	}

	private unsafe DatabaseErrorDetail ValidateDelete(Transaction tran, Bucket* bn, long id, int propId)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, id);

		if (bn->Handle == 0)
			return null;

		InvRefBaseItem* item = (InvRefBaseItem*)memoryManager.GetBuffer(bn->Handle);
		while (item != null && (item->id != id || item->propertyId != propId))
		{
			item = (InvRefBaseItem*)memoryManager.GetBuffer(item->NextCollision);
		}

		if (item == null)
			return null;

		TTTrace.Write(item->id, item->IsDeleted, item->Version);

		if (item->IsDeleted)
		{
			if (item->Version != tran.Id)
			{
				return DatabaseErrorDetail.CreateConflict(id, classDesc.FullName);
			}
			else
			{
				return DatabaseErrorDetail.CreateNonExistentDelete(id, classDesc.FullName);
			}
		}

		ulong tranId = tran.Id;
		ulong readVersion = tran.ReadVersion;
		while (item != null)
		{
			TTTrace.Write(item->id, item->IsDeleted, item->Version);

			if (item->Version != tranId && item->Version > readVersion)
				return DatabaseErrorDetail.CreateConflict(id, classDesc.FullName);

			if (ReaderInfo.IsInvRefInConflict(tran, id, item->propertyId, &item->readerInfo))
				return DatabaseErrorDetail.CreateConflict(id, classDesc.FullName);

			InvRefDeltaItem* ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(item->nextDelta);
			while (ditem != null)
			{
				TTTrace.Write(ditem->version);

				if (ditem->version != tranId && ditem->version > readVersion)
					return DatabaseErrorDetail.CreateConflict(id, classDesc.FullName);

				ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(ditem->nextDelta);
			}

			item = (InvRefBaseItem*)memoryManager.GetBuffer(item->NextBase);
		}

		return null;
	}

	private void GarbageCollectInternal(Bucket* bucket, ulong* pBucketHandle, long id, int propertyId, ulong readVersion)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, id, propertyId, readVersion);

		ulong* handlePointer = pBucketHandle;
		while (*handlePointer != 0)
		{
			InvRefBaseItem* item = (InvRefBaseItem*)memoryManager.GetBuffer(*handlePointer);

			TTTrace.Write(item->id, item->propertyId, item->IsDeleted, item->Version, *handlePointer);

			if (id == item->id && propertyId == item->propertyId)
			{
				ulong nextCollisionHandle = item->NextCollision;
				GarbageCollectItem(handlePointer, item, readVersion);

				if (*handlePointer == 0)
				{
					*handlePointer = nextCollisionHandle;
					Checker.AssertTrue((nextCollisionHandle & 0x8000000000000000) == 0);
					continue;
				}

				return;
			}

			handlePointer = &item->nextCollision;
		}
	}

	private void GarbageCollectItem(ulong* handlePointer, InvRefBaseItem* item, ulong readVersion)
	{
		while (item->Version > readVersion)
		{
			TTTrace.Write(item->id, item->propertyId, item->IsDeleted, item->Version);
			if (item->NextBase == 0)
				return;

			handlePointer = &item->nextBase;
			item = (InvRefBaseItem*)memoryManager.GetBuffer(item->NextBase);
		}

		TTTrace.Write(database.TraceId, classDesc.Id, readVersion, *handlePointer, item->id, item->propertyId, item->Version, item->IsDeleted,
			item->Count, item->readerInfo.LockCount, item->nextDelta);

		// If the item was created just for the purpose of locking
		if (item->NextBase == 0 && item->nextDelta == 0 && item->Count == 0 && item->readerInfo.LockCount == 0 &&
			item->readerInfo.CommReadLockVer <= readVersion)
		{
			ReleaseItems(*handlePointer, readVersion, out ulong dummy);
			*handlePointer = 0;
			return;
		}

		InvRefBaseItem* prevItem = null;
		if (!item->IsDeleted)
		{
			if (item->NextBase == 0)
				return;

			prevItem = item;
			handlePointer = &item->nextBase;
			item = (InvRefBaseItem*)memoryManager.GetBuffer(item->NextBase);
		}

		Checker.AssertTrue(item->readerInfo.LockCount == 0);

		// Since transaction release their read snapshot before fully being committed, it is possible that
		// we are attempting garbage collection while there are still uncommited delta items down the chain.
		// We can just skip the collection since it will occurr again anyways.
		if (HasUncommited(*handlePointer))
			return;

		ReleaseItems(*handlePointer, readVersion, out ulong commReadLockVer);
		*handlePointer = 0;

		if (prevItem != null && prevItem->readerInfo.CommReadLockVer < commReadLockVer)
			prevItem->readerInfo.CommReadLockVer = commReadLockVer;
	}

	private bool HasUncommited(ulong bhandle)
	{
		while (bhandle != 0)
		{
			InvRefBaseItem* bitem = (InvRefBaseItem*)memoryManager.GetBuffer(bhandle);
			TTTrace.Write(bitem->id, bitem->propertyId, bitem->IsDeleted, bitem->Version);

			ulong dhandle = bitem->nextDelta;
			while (dhandle != 0)
			{
				InvRefDeltaItem* ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(dhandle);
				TTTrace.Write(ditem->version);
				if (Database.IsUncommited(ditem->version))
					return true;

				dhandle = ditem->nextDelta;
			}

			bhandle = bitem->NextBase;
		}

		return false;
	}

	private void ReleaseItems(ulong bhandle, ulong readVersion, out ulong maxCommReadLockVer)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, bhandle, readVersion);

		maxCommReadLockVer = 0;
		while (bhandle != 0)
		{
			InvRefBaseItem* bitem = (InvRefBaseItem*)memoryManager.GetBuffer(bhandle);
			TTTrace.Write(bitem->id, bitem->propertyId, bitem->IsDeleted, bitem->Version);

			ulong nextBaseHandle = bitem->NextBase;

			ReleaseDeltaItems(bitem);

			if (bitem->readerInfo.CommReadLockVer > maxCommReadLockVer)
				maxCommReadLockVer = bitem->readerInfo.CommReadLockVer;

			Checker.AssertTrue(bitem->readerInfo.LockCount == 0);

			memoryManager.Free(bhandle);
			bhandle = nextBaseHandle;
		}
	}

	private void ReleaseDeltaItems(InvRefBaseItem* bitem)
	{
		ulong dhandle = bitem->nextDelta;
		while (dhandle != 0)
		{
			InvRefDeltaItem* ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(dhandle);
			TTTrace.Write(ditem->version);

			ulong nextDeltaHandle = ditem->nextDelta;
			memoryManager.Free(dhandle);
			dhandle = nextDeltaHandle;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe void CommitDeleteInternal(Transaction tran, Bucket* bucket, AffectedInverseReferences* affected, ulong newVersion)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, affected->id, affected->isDelete,
			affected->propertyId, affected->handle, affected->classIndex, newVersion);

		ulong itemHandle = bucket->Handle;
		InvRefBaseItem* item = (InvRefBaseItem*)memoryManager.GetBuffer(itemHandle);
		while (item != null)
		{
			TTTrace.Write(item->id, item->propertyId, item->IsDeleted, item->Version, itemHandle, newVersion);

			if (item->id == affected->id && item->propertyId == affected->propertyId)
			{
				TTTrace.Write(database.TraceId, affected->id, affected->isDelete, affected->propertyId,
					itemHandle, newVersion, item->Version);
				item->Version = newVersion;
			}

			itemHandle = item->NextCollision;
			item = (InvRefBaseItem*)memoryManager.GetBuffer(itemHandle);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe void CommitInternal(Transaction tran, Bucket* bucket, ulong* pBucketHandle,
		AffectedInverseReferences* affected, ulong newVersion)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, affected->id, affected->isDelete,
			affected->propertyId, affected->handle, affected->classIndex, newVersion);

		InvRefBaseItem* latestItem = FindItemByIdAndProp(pBucketHandle, affected->id, affected->propertyId, out ulong* prevItemPointer);
		Checker.AssertTrue(latestItem != null);

		InvRefDeltaItem* ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(affected->handle);
		ditem->version = newVersion;

		TTTrace.Write(latestItem->IsDeleted, latestItem->Version, ditem->version);

		ulong* deltaPointer = FindDeltaItem(affected->handle, latestItem, out InvRefBaseItem* ownerBaseItem);
		Checker.AssertTrue(deltaPointer != null);

		// Delta item needs to be commited to the latest base item. If a new base item was introduced (by a merge process)
		// we need to transfer the delta item that base item.
		if (latestItem != ownerBaseItem)
		{
			TTTrace.Write();
			*deltaPointer = ditem->nextDelta;
			ditem->nextDelta = latestItem->nextDelta;
			latestItem->nextDelta = affected->handle;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe void RollbackDeleteInternal(Transaction tran, Bucket* bucket, ulong* pBucketHandle, AffectedInverseReferences* affected)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, affected->id, affected->isDelete,
			affected->propertyId, affected->handle, affected->classIndex);

		ulong* handlePointer = pBucketHandle;
		InvRefBaseItem* item = (InvRefBaseItem*)memoryManager.GetBuffer(*handlePointer);
		while (item != null)
		{
			TTTrace.Write(item->id, item->propertyId, item->IsDeleted, item->Version);

			if (item->id == affected->id && item->propertyId == affected->propertyId)
			{
				ulong bitemHandle = *handlePointer;
				Checker.AssertTrue(item->IsDeleted);

				if (item->NextBase != 0)
				{
					*handlePointer = item->NextBase;
					Checker.AssertTrue((item->NextBase & 0x8000000000000000) == 0);

					InvRefBaseItem* nextBase = (InvRefBaseItem*)memoryManager.GetBuffer(item->NextBase);
					TTTrace.Write(nextBase->IsDeleted, nextBase->Version);

					nextBase->NextCollision = item->NextCollision;
					handlePointer = &nextBase->nextCollision;
				}
				else
				{
					*handlePointer = item->NextCollision;
					Checker.AssertTrue((item->NextCollision & 0x8000000000000000) == 0);
				}

				memoryManager.Free(bitemHandle);
			}
			else
			{
				handlePointer = &item->nextCollision;
			}

			item = (InvRefBaseItem*)memoryManager.GetBuffer(*handlePointer);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe void RollbackInternal(Transaction tran, Bucket* bucket, ulong* pBucketHandle, AffectedInverseReferences* affected)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran.Id, affected->id, affected->isDelete,
			affected->propertyId, affected->handle, affected->classIndex);

		InvRefBaseItem* latestItem = FindItemByIdAndProp(pBucketHandle, affected->id, affected->propertyId, out ulong* prevItemPointer);
		Checker.AssertTrue(latestItem != null);

		InvRefDeltaItem* ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(affected->handle);

		ulong* deltaPointer = FindDeltaItem(affected->handle, latestItem, out InvRefBaseItem* baseItem);
		Checker.AssertTrue(deltaPointer != null);

		*deltaPointer = ditem->nextDelta;
		memoryManager.Free(affected->handle);
	}

	private void TryMergeInternal(TransactionContext tc, Bucket* bucket, ulong* pBucketHandle,
		long id, int propertyId, ulong commitVersion, bool forceMerge)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, id, propertyId, forceMerge);

		InvRefBaseItem* latestItem = FindItemByIdAndProp(pBucketHandle, id, propertyId, out ulong* prevItemPointer);
		ulong latestItemHandle = *prevItemPointer;

		if (latestItem == null || latestItem->IsDeleted || latestItem->nextDelta == 0)
			return;

		TTTrace.Write(latestItem->IsDeleted, latestItem->Count, latestItem->Version);

		if (!ShouldMerge(latestItem, commitVersion, forceMerge, out ulong maxDeltaVer, out int insertCount, out int deleteCount))
			return;

		int newCount = latestItem->Count + insertCount - deleteCount;
		int mergedSize = InvRefBaseItem.RefsOffset + (latestItem->IsTracked ? newCount * 8 : 0);

		ulong mergedHandle = memoryManager.Allocate(mergedSize);
		InvRefBaseItem* mergedItem = (InvRefBaseItem*)memoryManager.GetBuffer(mergedHandle);
		ulong commitedReadLockVer = latestItem->readerInfo.CommReadLockVer;

		if (latestItem->IsTracked)
		{
			long* mergedBuffer = (long*)((byte*)mergedItem + InvRefBaseItem.RefsOffset);

			GetDeleteMapRequirements(deleteCount, out int capacity, out int byteSize, out int stackByteSize);
			byte* buffer = stackalloc byte[stackByteSize];
			CompactLongMap* deleted = CreateDeleteMap(buffer, byteSize, capacity, out ulong handle);
			MergeReferencesAndRemoveVersion(deleted, latestItem, mergedBuffer, newCount, 0, Database.MaxCommitedVersion, maxDeltaVer);
			memoryManager.FreeOptional(handle);
		}

		mergedItem->id = id;
		mergedItem->propertyId = propertyId;
		ReaderInfo.Init(&mergedItem->readerInfo);
		mergedItem->readerInfo.CommReadLockVer = commitedReadLockVer;
		mergedItem->Count = newCount;
		mergedItem->IsTracked = latestItem->IsTracked;
		mergedItem->deleted_version = maxDeltaVer;
		mergedItem->NextBase = latestItemHandle;
		mergedItem->NextCollision = latestItem->NextCollision;
		mergedItem->nextDelta = 0;
		*prevItemPointer = mergedHandle;

		TTTrace.Write(database.TraceId, id, propertyId, mergedHandle, mergedItem->Count, maxDeltaVer);
	}

	private bool ShouldMerge(InvRefBaseItem* item, ulong commitVersion, bool forceMerge,
		out ulong maxDeltaVer, out int insertCount, out int deleteCount)
	{
		maxDeltaVer = 0;
		insertCount = 0;
		deleteCount = 0;
		int deltaCount = 0;

		if (item->readerInfo.LockCount > 0)
			return false;

		InvRefDeltaItem* ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(item->nextDelta);
		while (ditem != null)
		{
			TTTrace.Write(ditem->deleteCount, ditem->insertCount, ditem->version);

			// If this is not the latest transaction to modify this item, the latest transaction will merge
			if (ditem->version > commitVersion || Database.IsUncommited(ditem->version))
				return false;

			insertCount += ditem->insertCount;
			deleteCount += ditem->deleteCount;
			deltaCount++;

			if (ditem->version > maxDeltaVer)
				maxDeltaVer = ditem->version;

			ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(ditem->nextDelta);
		}

		if (forceMerge || !item->IsTracked)
			return true;

		return deltaCount * 32 + deleteCount * 4 + insertCount >= item->Count / 4;
	}

	private unsafe void MergeReferencesAndRemoveVersion(CompactLongMap* map, InvRefBaseItem* baseItem,
		long* mergedBuffer, int newRefCount, ulong tempVersion, ulong readVersion, ulong unneededVersion,
		InverseReferenceOperation* addInserts = null, int addInsertCount = 0, InverseReferenceOperation* addDeletes = null, int addDeletesCount = 0)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, baseItem->id, baseItem->propertyId, baseItem->IsDeleted, baseItem->Version, baseItem->Count);

		PopulateDeletesMap(map, baseItem, tempVersion, readVersion, addDeletes, addDeletesCount);

		long* buckets = (long*)((byte*)map + CompactLongMap.BucketsOffset);

		MergeBaseReferences(map, baseItem, mergedBuffer, addInserts, addInsertCount, buckets, out int mergedCount);
		MergeDeltaReferences(map, baseItem, mergedBuffer, mergedCount, tempVersion, readVersion, unneededVersion, buckets);

		Checker.AssertTrue(map == null || map->Count == 0);
	}

	private void MergeDeltaReferences(CompactLongMap* map, InvRefBaseItem* baseItem, long* mergedBuffer, int mergedCount,
		ulong tempVersion, ulong readVersion, ulong unneededVersion, long* buckets)
	{
		ulong* handlePointer = &baseItem->nextDelta;
		InvRefDeltaItem* deltaItem = (InvRefDeltaItem*)memoryManager.GetBuffer(*handlePointer);
		while (deltaItem != null)
		{
			TTTrace.Write(deltaItem->deleteCount, deltaItem->insertCount, deltaItem->version);

			if (deltaItem->version == tempVersion || deltaItem->version <= readVersion)
			{
				long* inserts = (long*)((byte*)deltaItem + InvRefDeltaItem.RefsOffset);
				for (int i = 0; i < deltaItem->insertCount; i++)
				{
					if (map == null || !map->TryRemove(inserts[i], buckets))
					{
						mergedBuffer[mergedCount++] = inserts[i];
					}
				}
			}

			if (deltaItem->version != unneededVersion)
			{
				handlePointer = &deltaItem->nextDelta;
			}
			else
			{
				ulong temp = *handlePointer;
				*handlePointer = deltaItem->nextDelta;
				memoryManager.Free(temp);
			}

			deltaItem = (InvRefDeltaItem*)memoryManager.GetBuffer(*handlePointer);
		}
	}

	private void MergeBaseReferences(CompactLongMap* map, InvRefBaseItem* baseItem, long* mergedBuffer,
		InverseReferenceOperation* addInserts, int addInsertCount, long* buckets, out int mergedCount)
	{
		mergedCount = 0;
		long* baseRefs = (long*)((byte*)baseItem + InvRefBaseItem.RefsOffset);
		int rcount = baseItem->Count;

		for (int i = 0; i < rcount; i++)
		{
			if (map == null || !map->TryRemove(baseRefs[i], buckets))
			{
				mergedBuffer[mergedCount++] = baseRefs[i];
			}
		}

		for (int i = 0; i < addInsertCount; i++)
		{
			if (map == null || !map->TryRemove(addInserts[i].inverseReference, buckets))
			{
				mergedBuffer[mergedCount++] = addInserts[i].inverseReference;
			}
		}
	}

	private void PopulateDeletesMap(CompactLongMap* map, InvRefBaseItem* baseItem, ulong tempVersion,
		ulong readVersion, InverseReferenceOperation* addDeletes, int addDeletesCount)
	{
		long* buckets = (long*)((byte*)map + CompactLongMap.BucketsOffset);

		InvRefDeltaItem* deltaItem = (InvRefDeltaItem*)memoryManager.GetBuffer(baseItem->nextDelta);
		while (deltaItem != null)
		{
			TTTrace.Write(deltaItem->deleteCount, deltaItem->insertCount, deltaItem->version);

			if (deltaItem->version <= readVersion || deltaItem->version == tempVersion)
			{
				long* deletes = (long*)((byte*)deltaItem + InvRefDeltaItem.RefsOffset) + deltaItem->insertCount;
				for (int i = 0; i < deltaItem->deleteCount; i++)
				{
					TTTrace.Write(deletes[i]);
					map->Add(deletes[i], buckets);
				}
			}

			deltaItem = (InvRefDeltaItem*)memoryManager.GetBuffer(deltaItem->nextDelta);
		}

		for (int i = 0; i < addDeletesCount; i++)
		{
			map->Add(addDeletes[i].inverseReference, buckets);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe InvRefBaseItem* FindItemByIdAndPropReadLockMode(Transaction tran, ulong* pcurr, long id, int propertyId,
		out ulong* prevItemPointer, out DatabaseErrorDetail error)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, tran != null ? tran.Id : 0, id, propertyId);

		error = null;
		prevItemPointer = null;

		while (*pcurr != 0)
		{
			InvRefBaseItem* currItem = (InvRefBaseItem*)memoryManager.GetBuffer(*pcurr);
			TTTrace.Write(currItem->id, currItem->IsDeleted, currItem->propertyId, currItem->Count, currItem->Version);

			if (currItem->id == id && currItem->propertyId == propertyId)
			{
				if (currItem->IsDeleted && tran != null && currItem->Version != tran.Id)
				{
					error = DatabaseErrorDetail.CreateConflict(id, classDesc.FullName);
					return null;
				}

				prevItemPointer = pcurr;
				return currItem;
			}

			pcurr = &currItem->nextCollision;
		}

		return null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe InvRefBaseItem* FindItemByIdAndProp(ulong* pcurr, long id, int propertyId, out ulong* prevItemPointer)
	{
		return FindItemByIdAndPropReadLockMode(null, pcurr, id, propertyId, out prevItemPointer, out DatabaseErrorDetail error);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe ulong* FindDeltaItem(ulong itemHandle, InvRefBaseItem* latestBaseItem, out InvRefBaseItem* baseItem)
	{
		baseItem = latestBaseItem;
		while (baseItem != null)
		{
			ulong* handlePointer = &baseItem->nextDelta;
			while (*handlePointer != 0)
			{
				if (*handlePointer == itemHandle)
					return handlePointer;

				InvRefDeltaItem* deltaItem = (InvRefDeltaItem*)memoryManager.GetBuffer(*handlePointer);
				TTTrace.Write(deltaItem->version);

				handlePointer = &deltaItem->nextDelta;
			}

			baseItem = (InvRefBaseItem*)memoryManager.GetBuffer(baseItem->NextBase);
		}

		return null;
	}

	public void Resize(long count)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, count);

		resizeCounter.EnterWriteLock();

		try
		{
			ResizeInternal((long)(count * 1.5));
		}
		finally
		{
			resizeCounter.ExitWriteLock();
		}
	}

	private void Resize()
	{
		TTTrace.Write(database.TraceId, classDesc.Id);

		resizeCounter.EnterWriteLock();

		if (resizeCounter.Count <= bucketCountLimit)
		{
			resizeCounter.ExitWriteLock();
			return;
		}

		try
		{
			ResizeInternal((long)(capacity * growthFactor));
		}
		finally
		{
			resizeCounter.Resized(bucketCountLimit);
			resizeCounter.ExitWriteLock();
		}
	}

	private void ResizeInternal(long size)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, size, capacity);

		long newCapacity = HashUtils.CalculatePow2Capacity(size, loadFactor, out long newLimitCount);

		ulong newCapacityMask = (ulong)newCapacity - 1;
		Bucket* newBuckets = (Bucket*)AlignedAllocator.Allocate(newCapacity * Bucket.Size, false);
		for (long i = 0; i < newCapacity; i++)
		{
			newBuckets[i].Init();
		}

		long usedBucketCount = 0;
		Utils.Range[] ranges = Utils.SplitRange(capacity, database.Engine.Settings.ResizeSplitSize, ProcessorNumber.CoreCount);
		int workerCount = Math.Min(ProcessorNumber.CoreCount, ranges.Length);
		string workerName = string.Format("{0}: vlx-InverseRefResizeWorker", database.Engine.Trace.Name);
		JobWorkers<Utils.Range>.Execute(workerName, workerCount, r => Interlocked.Add(ref usedBucketCount, RehashRange(newBuckets, newCapacityMask, r)), ranges);

		resizeCounter.SetCount(usedBucketCount);

		this.capacity = newCapacity;
		this.bucketCountLimit = newLimitCount;
		this.capacityMask = newCapacityMask;

		AlignedAllocator.Free((IntPtr)buckets);
		buckets = newBuckets;
	}

	private long RehashRange(Bucket* newBuckets, ulong newCapacityMask, Utils.Range range)
	{
		long usedBucketCount = 0;
		for (long i = range.Offset; i < range.Offset + range.Count; i++)
		{
			ulong handle = buckets[i].Handle;
			while (handle != 0)
			{
				InvRefBaseItem* item = (InvRefBaseItem*)memoryManager.GetBuffer(handle);
				ulong nextCollision = item->NextCollision;

				long bucketIndex = CalculateBucket(item->id, item->propertyId, seed, newCapacityMask);
				Bucket* bucket = newBuckets + bucketIndex;
				Bucket.LockAccess(bucket);

				item->NextCollision = newBuckets[bucketIndex].Handle;
				if (item->NextCollision == 0)
					usedBucketCount++;

				newBuckets[bucketIndex].Handle = handle;
				Bucket.UnlockAccess(bucket);

				handle = nextCollision;
			}
		}

		return usedBucketCount;
	}

	private void FreeBuffers(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(database.TraceId, classDesc.Id);

		Utils.Range[] ranges = Utils.SplitRange(capacity, database.Engine.Settings.ResizeSplitSize, ProcessorNumber.CoreCount);
		workers.SetAction(o =>
		{
			Utils.Range r = (Utils.Range)o.ReferenceParam;
			FreeBufferInRange(r);
		});

		Array.ForEach(ranges, x => workers.EnqueueWork(new CommonWorkerParam() { ReferenceParam = x }));
		workers.Drain();
	}

	private void FreeBufferInRange(Utils.Range r)
	{
		for (long i = r.Offset; i < r.Offset + r.Count; i++)
		{
			Bucket* bucket = buckets + i;

			ulong bitemHandle = bucket->Handle;
			InvRefBaseItem* bitem = (InvRefBaseItem*)memoryManager.GetBuffer(bitemHandle);
			while (bitem != null)
			{
				ulong nextCollision = bitem->NextCollision;
				while (bitem != null)
				{
					ulong nextBase = bitem->NextBase;
					FreeDeltaBuffers(bitem);
					memoryManager.Free(bitemHandle);

					bitemHandle = nextBase;
					bitem = (InvRefBaseItem*)memoryManager.GetBuffer(bitemHandle);
				}

				bitemHandle = nextCollision;
				bitem = (InvRefBaseItem*)memoryManager.GetBuffer(bitemHandle);
			}
		}
	}

	private void FreeDeltaBuffers(InvRefBaseItem* bitem)
	{
		ulong ditemHandle = bitem->nextDelta;
		InvRefDeltaItem* ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(ditemHandle);
		while (ditem != null)
		{
			ulong nextDelta = ditem->nextDelta;
			memoryManager.Free(ditemHandle);

			ditemHandle = nextDelta;
			ditem = (InvRefDeltaItem*)memoryManager.GetBuffer(ditemHandle);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private long CalculateBucket(long id, int propertyId)
	{
		return (long)(HashUtils.GetHash96((ulong)id, (uint)propertyId, seed) & capacityMask);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static long CalculateBucket(long id, int propertyId, ulong seed, ulong capacityMask)
	{
		return (long)(HashUtils.GetHash96((ulong)id, (uint)propertyId, seed) & capacityMask);
	}

	public void Dispose(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(database.TraceId, classDesc.Id);

		FreeBuffers(workers);

		AlignedAllocator.Free((IntPtr)buckets);
		buckets = null;

		resizeCounter.Dispose();
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal unsafe struct InverseRefsStorage
{
	public long* prefs;
	public long[] refs;
	public int offset;
	public int size;

	public InverseRefsStorage(int size, long* prefs)
	{
		this.size = size;
		this.refs = null;
		this.prefs = prefs;
		this.offset = 0;
	}

	public InverseRefsStorage(long[] refs)
	{
		this.size = refs.Length;
		this.refs = refs;
		this.prefs = null;
		this.offset = 0;
	}

	public int EmptyCount => size - offset;

	public void AddRange(long* p, int count)
	{
		if (refs != null)
		{
			for (int i = 0; i < count; i++)
			{
				refs[offset + i] = p[i];
			}
		}
		else
		{
			for (int i = 0; i < count; i++)
			{
				prefs[offset + i] = p[i];
			}
		}
	}

	public void Resize(int newSize)
	{
		if (refs != null)
		{
			long[] temp = new long[newSize];
			size = newSize;
			if (offset > 0)
				Array.Copy(refs, temp, offset);

			refs = temp;
		}
		else
		{
			long* tempp = (long*)AlignedAllocator.Allocate((long)newSize * 8, false);
			size = newSize;
			if (offset > 0)
				Utils.CopyMemory((byte*)prefs, (byte*)tempp, (long)offset * 8);

			AlignedAllocator.Free((IntPtr)prefs);
			prefs = tempp;
		}
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal unsafe struct InvRefDeltaItem
{
	public const int RefsOffset = 24;

	public ulong nextDelta;
	public ulong version;
	public int insertCount;
	public int deleteCount;
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal unsafe struct InvRefBaseItem
{
	public const int RefsOffset = 64;

	public ulong nextCollision;
	public ulong nextBase;
	public ulong deleted_version;
	public long id;
	public ulong nextDelta;
	public ReaderInfo readerInfo;
	public int propertyId;
	public uint tracked_refCount;

	public bool IsTracked { get => (byte)(tracked_refCount >> 31) != 0; set => tracked_refCount = (tracked_refCount & 0x7fffffff) | ((uint)(value ? 1 : 0) << 31); }

	public int Count { get => (int)(tracked_refCount & 0x7fffffff); set => tracked_refCount = (tracked_refCount & 0x80000000) | (uint)value; }

	public ulong Version { get => deleted_version & 0x7fffffffffffffff; set => deleted_version = (deleted_version & 0x8000000000000000) | value; }

	public bool IsDeleted { get => (byte)(deleted_version >> 63) != 0; set => deleted_version = (deleted_version & 0x7fffffffffffffff) | ((ulong)(value ? 1 : 0) << 63); }

	public ulong NextCollision
	{
		get => nextCollision;
		set
		{
			Checker.AssertTrue((value & 0x8000000000000000) == 0);
			nextCollision = value;
		}
	}

	public ulong NextBase
	{
		get => nextBase;
		set
		{
			Checker.AssertTrue((value & 0x8000000000000000) == 0);
			nextBase = value;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool IsEmpty()
	{
		return (nextDelta | (uint)Count) == 0;
	}
}
