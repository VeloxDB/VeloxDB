﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.SymbolStore;
using System.Linq;
using System.Reflection.Metadata;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Remoting;
using System.Threading;
using System.Threading.Tasks;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage.ModelUpdate;
using VeloxDB.Storage.Replication;
using static System.Runtime.InteropServices.JavaScript.JSType;
using static VeloxDB.Common.SegmentBinaryWriter;
using static VeloxDB.Storage.Persistence.LogBufferPool;

namespace VeloxDB.Storage;

internal unsafe sealed partial class Class : ClassBase
{
	public const int ExecuteRangeSize = 16;

	ObjectStorage storage;
	StringStorage stringStorage;
	BlobStorage blobStorage;

	ParallelResizeCounter resizeCounter;

	float hashLoadFactor;
	ulong seed;

	long capacity;
	long countLimit;
	ulong capacityMask;
	long minCapacity;
	Bucket* buckets;

	ushort classIndex;

	int objectSize;
	int* propertyOffsets;

	IntPtr defaultValues;

	bool hasStringsOrBlobs;

	IndexComparerPair[] indexes;
	FastDictionary<short, KeyComparer> keyComparers;
	bool uniqueIndexesExist;

	PendingRestoreDelegate pendingRestorer;
	bool isPersistanceActive;

	IndexDeleteDelegate indexDeleteDelegate;

	ModelUpdateData modelUpdateData;

	public Class(Database database, ClassDescriptor classDesc, long capacity) :
		base(database, classDesc)
	{
		TTTrace.Write(database.TraceId, classDesc.Id, capacity);

		this.stringStorage = database.Engine.StringStorage;
		this.blobStorage = database.Engine.BlobStorage;
		this.hashLoadFactor = database.Engine.Settings.HashLoadFactor;
		this.classIndex = (ushort)classDesc.Index;
		this.isPersistanceActive = database.PersistenceDesc != null;
		base.MainClass = this;
		hasStringsOrBlobs = classDesc.HasStringsOrBlobs;

		PreparePropertyData(ClassDesc, out defaultValues, out propertyOffsets, out objectSize);

		storage = new ObjectStorage(this, objectSize);
		pendingRestorer = (p, t, b) => RestorePendingChange((PendingRestoreObjectHeader*)p, (ulong*)t, b);
		indexDeleteDelegate = (o, h, i) => DeleteFromAffectedIndex(h, o, i);

		minCapacity = database.Id == DatabaseId.User ? ParallelResizeCounter.SingleThreadedLimit * 2 : 64;

		capacity = Math.Max(minCapacity, capacity);
		this.capacity = capacity = HashUtils.CalculatePow2Capacity(capacity, hashLoadFactor, out countLimit);
		capacityMask = (ulong)capacity - 1;
		seed = Engine.HashSeed;

		buckets = (Bucket*)AlignedAllocator.Allocate(capacity * Bucket.Size, false);
		for (long i = 0; i < capacity; i++)
		{
			buckets[i].Init();
		}

		resizeCounter = new ParallelResizeCounter(countLimit);

		TTTrace.Write(Database.TraceId, classDesc.Id, capacity, countLimit);
	}

	internal StringStorage StringStorage => stringStorage;
	internal BlobStorage BlobStorage => blobStorage;
	public long EstimatedObjectCount => resizeCounter.Count;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static ClassObject* GetObjectByHandle(ulong handle)
	{
		return (ClassObject*)ObjectStorage.GetBuffer(handle);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static byte* GetKey(ulong objectHandle, out ClassObject* @object)
	{
		@object = (ClassObject*)ObjectStorage.GetBuffer(objectHandle);
		return ClassObject.ToDataPointer(@object);
	}

	protected override void OnStartPropertyUpdate(ClassDescriptor newClassDesc, bool propertyListModified)
	{
		PreparePropertyData(newClassDesc, out IntPtr newDefaultValues, out int* newPropertyOffsets, out int newObjectSize);
		TTTrace.Write(Database.TraceId, ClassDesc.Id, newObjectSize);

		modelUpdateData = new ModelUpdateData()
		{
			DefaultValues = newDefaultValues,
			ObjectSize = newObjectSize,
			PropertyOffsets = newPropertyOffsets
		};

		if (propertyListModified)
		{
			Engine.Trace.Debug("Class {0} properties modified, classId={1}.", ClassDesc.FullName, ClassDesc.Id);
			modelUpdateData.Storage = new ObjectStorage(this, modelUpdateData.ObjectSize);
		}
	}

	protected override void OnFinishPropertyUpdate()
	{
		if (modelUpdateData.Storage != null)
		{
			storage.Dispose(false);
			storage = modelUpdateData.Storage;
		}

		defaultValues = modelUpdateData.DefaultValues;
		propertyOffsets = modelUpdateData.PropertyOffsets;
		objectSize = modelUpdateData.ObjectSize;
		modelUpdateData = null;

		hasStringsOrBlobs = ClassDesc.HasStringsOrBlobs;

		TTTrace.Write(Database.TraceId, ClassDesc.Id, hasStringsOrBlobs);
	}

	protected override void OnModelUpdated()
	{
		TTTrace.Write(Database.TraceId, ClassDesc.Id, ClassDesc.Index, ClassDesc.Properties.Length);
		classIndex = (ushort)ClassDesc.Index;
	}

	public void UpdateModelForObject(ulong objectHandle, ObjectCopyDelegate copyDelegate, IndexComparerPair[] indexes, ulong commitVersion)
	{
		ClassObject* obj = GetObjectByHandle(objectHandle);
		Checker.AssertTrue(obj->nextVersionHandle == 0);

		TTTrace.Write(Database.TraceId, ClassDesc.Id, ClassDesc.Index, obj->id, obj->version, obj->nextVersionHandle, commitVersion);

		ulong newObjHandle = modelUpdateData.Storage.Allocate();
		ClassObject* newObj = (ClassObject*)ObjectStorage.GetBuffer(newObjHandle);
		newObj->nextVersionHandle = 0;
		ReaderInfo.Clear(&newObj->readerInfo);
		copyDelegate((IntPtr)(ClassObject.ToDataPointer(obj)), (IntPtr)(ClassObject.ToDataPointer(newObj)), stringStorage, blobStorage);
		if (commitVersion != 0)
			newObj->version = commitVersion;

		ObjectStorage.MarkBufferAsUsed(newObjHandle);

		Bucket* bucket = buckets + CalculateBucket(obj->id);
		ulong* handlePointer = Bucket.LockAccess(bucket);

		RefreshIndexHandles(objectHandle, obj, newObjHandle, indexes);
		FindObject(handlePointer, obj->id, out ulong* pObjPointer);

		newObj->nextCollisionHandle = obj->nextCollisionHandle;
		*pObjPointer = newObjHandle;

		Bucket.UnlockAccess(bucket);
	}

	public void GarbageCollect(long id, ulong oldestVisibleVersion)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, id, oldestVisibleVersion);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(id);
		ulong* handlePointer = Bucket.LockAccess(bucket);

		bool isDeleted = FindObjectAndGarbageCollect(bucket, handlePointer, id, oldestVisibleVersion);

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);

		if (isDeleted)
			resizeCounter.Dec(lockHandle);
	}

	public void AssignIndexes(Func<int, Index> indexFinder, IndexReadersCollection indexReaders)
	{
		TTTrace.Write(TraceId, ClassDesc.Id);

		uniqueIndexesExist = false;

		this.indexes = new IndexComparerPair[ClassDesc.Indexes.Length];
		keyComparers = new FastDictionary<short, KeyComparer>(ClassDesc.Indexes.Length);
		for (int k = 0; k < ClassDesc.Indexes.Length; k++)
		{
			IndexDescriptor indexDesc = ClassDesc.Indexes[k];
			TTTrace.Write(TraceId, ClassDesc.Id, indexDesc.Id, indexDesc.IsUnique);
			Index index = indexFinder(indexDesc.Index);
			KeyComparer comparer = new KeyComparer(ClassDesc.GetIndexAccessDesc(indexDesc));
			this.indexes[k] = new IndexComparerPair(index, comparer);
			keyComparers.Add(indexDesc.Id, comparer);
			uniqueIndexesExist |= indexDesc.IsUnique;
		}
	}

	public bool HasPendingRefillIndexes()
	{
		for (int i = 0; i < indexes.Length; i++)
		{
			if (indexes[i].Index.PendingRefill)
				return true;
		}

		return false;
	}

	public void BuildIndexes(ulong objectHandle, bool pendingRefillOnly = false)
	{
		ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objectHandle);
		InsertIntoIndexes(null, objectHandle, obj, pendingRefillOnly);
	}

	public DatabaseErrorDetail BuildIndex(ulong objectHandle, Index index,
		KeyComparer comparer, Func<short, KeyComparer> comparerFinder)
	{
		ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objectHandle);
		TTTrace.Write(TraceId, ClassDesc.Id, obj->id, obj->version, objectHandle, index.IndexDesc.Id);

		byte* key = ClassObject.ToDataPointer(obj);
		return index.Insert(null, obj->id, objectHandle, key, comparer, comparerFinder);
	}

	public void DeleteFromIndex(ulong objectHandle, Index index, KeyComparer comparer)
	{
		ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objectHandle);
		TTTrace.Write(TraceId, ClassDesc.Id, obj->id, obj->version, objectHandle, index.IndexDesc.Id);
		byte* key = ClassObject.ToDataPointer(obj);
		index.Delete(objectHandle, key, obj->id, comparer);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public KeyComparer GetKeyComparer(short id, bool mandatoryFind)
	{
		if (!keyComparers.TryGetValue(id, out KeyComparer comparer))
		{
			if (mandatoryFind)
				throw new CriticalDatabaseException();
		}

		return comparer;
	}

	private void RefreshIndexHandles(ulong objectHandle, ClassObject* obj, ulong newObjectHandle, IndexComparerPair[] indexes)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, objectHandle, newObjectHandle, obj->id, obj->version);

		byte* key = ClassObject.ToDataPointer(obj);
		for (int i = 0; i < indexes.Length; i++)
		{
			if (!indexes[i].Index.PendingRefill)
				indexes[i].Index.ReplaceObjectHandle(objectHandle, newObjectHandle, key, obj->id, indexes[i].Comparer);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail InsertIntoIndexes(Transaction tran, ulong objectHandle, ClassObject* obj, bool pendingRefillOnly = false)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, objectHandle, obj->id, obj->version, pendingRefillOnly);
		Checker.AssertFalse(obj->IsDeleted);

		byte* key = ClassObject.ToDataPointer(obj);
		for (int i = 0; i < indexes.Length; i++)
		{
			if (pendingRefillOnly && !indexes[i].Index.PendingRefill)
				continue;

			DatabaseErrorDetail err = indexes[i].Index.Insert(tran, obj->id, objectHandle, key, indexes[i].Comparer);
			if (err != null)
			{
				for (int j = 0; j < i; j++)
				{
					indexes[j].Index.Delete(objectHandle, key, obj->id, indexes[j].Comparer);
				}

				return err;
			}
		}

		return null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void DeleteFromIndexes(ulong objectHandle, ClassObject* obj)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, objectHandle, obj->id, obj->version, obj->IsDeleted);

		if (obj->IsDeleted)
			return;

		Byte* key = ClassObject.ToDataPointer(obj);
		for (int j = 0; j < indexes.Length; j++)
		{
			if (!indexes[j].Index.PendingRefill)
				indexes[j].Index.Delete(objectHandle, key, obj->id, indexes[j].Comparer);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void InsertIntoAffectedIndexes(ulong objectHandle, ClassObject* obj, ulong affectedIndexesMask)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, objectHandle, obj->id, obj->version, affectedIndexesMask, indexes.Length);
		Checker.AssertFalse(obj->IsDeleted);

		if (affectedIndexesMask != 0)
		{
			byte* key = ClassObject.ToDataPointer(obj);
			for (int j = 0; j < indexes.Length; j++)
			{
				if ((affectedIndexesMask & ((ulong)1 << j)) != 0 && !indexes[j].Index.PendingRefill)
				{
					DatabaseErrorDetail err = indexes[j].Index.Insert(null, obj->id, objectHandle, key, indexes[j].Comparer);
					Checker.AssertNull(err);
				}
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void DeleteFromAffectedIndex(ulong objectHandle, ClassObject* obj, int indexIndex)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, objectHandle, obj->id, obj->version,
			obj->IsDeleted, indexes[indexIndex].Index.IndexDesc.Id);

		if (indexes[indexIndex].Index.PendingRefill)
			return;

		byte* bp = ClassObject.ToDataPointer(obj);
		indexes[indexIndex].Index.Delete(objectHandle, bp, obj->id, indexes[indexIndex].Comparer);
	}

	public ObjectReader GetScanObjectIfInTransaction(ulong handle, Transaction tran)
	{
		// Lock free scan algorithm:
		// Each object buffer has a version assigned to it. The version is incremented each time the buffer is placed inside the class
		// and incremented, again, each time the buffer is released. This means that when buffer is used in the class, its version number
		// is odd. Important thing to mention is that before the version is incremented during allocation,
		// all the buffer data is guaranteed to be written to the buffer (with guaranteed order on all memory models).
		// Also, during releasing, version is guaranteed to be incremented before the buffer is actually released.
		// We first start checking whether the buffer is actually used in the class (and remember the version with which we confirmed that).
		// Once we determine that the object is visible to the transaction, we recheck whether the version of the buffer is still the same.
		// Couple of memory barriers ensure that version checks are not reordered with the transaction check.

		if (!ObjectStorage.IsBufferUsed(handle, out ulong bufferVersion))
			return new ObjectReader();

		// IsBufferUsed performs memory barrier internally (on ARM) so we are
		// certain that version check will not be reordered with transaction check.

		ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(handle);

		bool visible;

		// Scanning without the transaction assumes that the latest version is visible
		if (tran == null)
		{
			visible = obj->NewerVersion == 0 && !obj->IsDeleted;
		}
		else
		{
			ulong newVersion = obj->NewerVersion;
			if (newVersion != 0 && (newVersion == tran.Id || newVersion <= tran.ReadVersion))
				return new ObjectReader();

			visible = !obj->IsDeleted && (obj->version <= tran.ReadVersion || obj->version == tran.Id);
		}

		// We need to make sure that the transaction check is not reordered with the next version check.
		// This is not needed on x64 since loads are never reordered with other loads.
#if !X86_64
		Thread.MemoryBarrier();
#endif

		if (!visible || !ObjectStorage.IsVersionEqual(handle, bufferVersion))
			return new ObjectReader();

		return new ObjectReader(ClassObject.ToDataPointer(obj), this);
	}

	public DatabaseErrorDetail ReadLockObjectFromIndex(Transaction tran, ulong handle, short indexId)
	{
		ClassObject* obj = GetObjectByHandle(handle);
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, obj->id);
		Checker.AssertFalse(obj->IsDeleted);

		int lockHandle = resizeCounter.EnterReadLock();
		Bucket* bucket = buckets + CalculateBucket(obj->id);
		ulong* handlePointer = Bucket.LockAccess(bucket);

		try
		{
			if (obj->NewerVersion != 0)
			{
				ClassObject* newerObj = FindObject(handlePointer, obj->id, out _);
				if (HasKeyBeenModified(newerObj, obj, indexId))
					return DatabaseErrorDetail.Create(DatabaseErrorType.IndexConflict);

				return DatabaseErrorDetail.CreateConflict(obj->id, ClassDesc.FullName);
			}

			if (Database.IsCommited(obj->version))
			{
				if (!ReaderInfo.TryTakeObjectStandardLock(tran, obj->id, classIndex, &obj->readerInfo))
					return DatabaseErrorDetail.CreateObjectLockContentionLimitExceeded(ClassDesc.FullName);
			}

			return null;
		}
		finally
		{
			Bucket.UnlockAccess(bucket);
			resizeCounter.ExitReadLock(lockHandle);
		}
	}

	private bool HasKeyBeenModified(ClassObject* newerObj, ClassObject* obj, short indexId)
	{
		byte* objKey = ClassObject.ToDataPointer(obj);

		KeyComparer comparer = GetKeyComparer(indexId, true);
		while (newerObj != obj)
		{
			if (newerObj->IsDeleted)
				return true;

			byte* newerKey = ClassObject.ToDataPointer(newerObj);
			if (!comparer.Equals(newerKey, null, objKey, comparer, stringStorage))
				return true;

			newerObj = (ClassObject*)ObjectStorage.GetBuffer(newerObj->nextVersionHandle);
		}

		return false;
	}

	public override ObjectReader GetObjectNoReadLock(Transaction tran, long id)
	{
		ClassObject* obj = GetObjectInternalRead(tran, id, out _);
		return new ObjectReader(ClassObject.ToDataPointer(obj), this);
	}

	public override ObjectReader GetObject(Transaction tran, long id, bool forceNoReadLock, out DatabaseErrorDetail err)
	{
		int lockHandle = resizeCounter.EnterReadLock();

		ClassObject* obj;
		if (tran.Type == TransactionType.Read || forceNoReadLock)
		{
			err = null;
			obj = GetObjectInternalRead(tran, id, out _);
		}
		else
		{
			obj = GetObjectInternalReadLock(tran, id, out err);
		}

		resizeCounter.ExitReadLock(lockHandle);

		return new ObjectReader(ClassObject.ToDataPointer(obj), this);
	}

	public void CreateObjectDiff(ClassObject* obj, ulong commonVersion, ChangesetWriter writer,
		IdSet partnerIds, List<long> otherIds, GenerateAlignDelegate alignDelegate)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, obj->id, obj->version, obj->IsDeleted, commonVersion);

		if (obj->IsDeleted)
			return;

		bool sbyMissing = partnerIds != null && !partnerIds.Contains(obj->id);
		if (obj->version > commonVersion || sbyMissing)
		{
			// If the standby deleted the object (due to split brain scenario) we need to pack an entire object which
			// will be achieved with the commonVersion being set to zero (this forces strings and blobs to be packed as well).
			TTTrace.Write(TraceId, ClassDesc.Id, obj->id, obj->version, commonVersion, sbyMissing);
			ulong version = sbyMissing ? 0 : commonVersion;
			alignDelegate(obj, stringStorage, blobStorage, writer, version);
			writer.LastValueWritten();
		}
		else
		{
			TTTrace.Write(TraceId, ClassDesc.Id, obj->id, obj->version, commonVersion);
			otherIds ??= new List<long>();
			otherIds.Add(obj->id);
		}
	}

	public bool IsObjectDifferent(ClassObject* obj, ulong commonVersion)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, obj->id, obj->version, commonVersion);

		if (obj->IsDeleted)
			return false;

		return obj->version > commonVersion;
	}

	public void RestoreSnapshot(int[] propertyIndexes, PropertyType[] propTypes, bool altered, long objectCount, SegmentBinaryReader reader)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, altered, objectCount, propTypes.Length);

		for (int j = 0; j < objectCount; j++)
		{
			ClassObject* obj = RestoreObjectSnapshot(reader, propertyIndexes, propTypes, altered, out ulong objHandle);
			AddRestoredObject(obj, objHandle);
		}

		resizeCounter.Add(objectCount);
	}

	public override ObjectStorage.ScanRange[] GetScanRanges(bool scanInhereted, out long totalCount)
	{
		return storage.SplitScanRange(Engine.Settings.ScanClassSplitSize, int.MaxValue, out totalCount);
	}

	public override ObjectStorage.ScanRange[] GetDisposingScanRanges(bool scanInhereted, out long totalCount)
	{
		return storage.SplitDisposableScanRange(Engine.Settings.ScanClassSplitSize, out totalCount);
	}

	public bool ObjectExists(Transaction tran, long id, out DatabaseErrorDetail error)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, tran == null ? 0 : tran.Id, id);

		error = null;
		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(id);
		ulong* phead = Bucket.LockAccess(bucket);

		try
		{
			ClassObject* latest = FindObject(phead, id, out _);
			if (latest == null)
				return false;

			if (tran == null)
				return latest != null;

			ClassObject* obj = FindVersion(latest, tran.ReadVersion, tran.Id);
			if (obj == null)
				return false;

			TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, obj->IsDeleted, obj->version, latest->IsDeleted, latest->version);

			if (obj->version == tran.Id)
				return true;

			if (obj->IsDeleted)
			{
				Checker.AssertTrue(obj == latest);
				return false;
			}

			if (latest->IsDeleted)
			{
				error = DatabaseErrorDetail.CreateConflict(id, ClassDesc.FullName);
				return false;
			}

			if (tran.Type == TransactionType.ReadWrite &&
				!ReaderInfo.TryTakeObjectExistanceLock(tran, latest->id, classIndex, &(latest->readerInfo)))
			{
				error = DatabaseErrorDetail.CreateObjectLockContentionLimitExceeded(ClassDesc.FullName);
				return false;
			}

			return true;
		}
		finally
		{
			Bucket.UnlockAccess(bucket);
			resizeCounter.ExitReadLock(lockHandle);
		}
	}

	public bool ObjectDeletedInTransaction(Transaction tran, long id)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, id);
		Checker.AssertFalse(tran != null && tran.IsAlignment);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(id);
		ulong* phead = Bucket.LockAccess(bucket);

		ClassObject* obj = FindObject(phead, id, out ulong* pObjPointer);
		if (obj != null)
			TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, obj->IsDeleted, obj->version);

		if (obj == null)
			return false;

		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, obj->IsDeleted, obj->version);
		bool res = obj->IsDeleted && obj->version == tran.Id;

		Bucket.UnlockAccess(bucket);
		resizeCounter.ExitReadLock(lockHandle);

		return res;
	}

	[SkipLocalsInit]
	public DatabaseErrorDetail Insert(Transaction tran, ChangesetReader reader)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, ClassDesc.LogIndex);
		Checker.AssertFalse(tran.IsAlignment);

		TransactionContext tc = tran.Context;
		int c = 0;
		DatabaseErrorDetail error = null;

		int lockHandle = resizeCounter.EnterReadLock();

		error = WriteClassLocker(tran, tc);

		int operationCount = tc.ChangesetBlock.OperationCount;
		ulong* objHandles = stackalloc ulong[Math.Min(operationCount, ExecuteRangeSize)];

		while (error == null && operationCount > 0)
		{
			int executeCount = Math.Min(operationCount, ExecuteRangeSize);
			storage.AllocateMultiple(objHandles, executeCount);

			for (int i = 0; i < executeCount; i++)
			{
				ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objHandles[i]);
				Utils.CopyMemory((byte*)defaultValues + ClassObject.DataOffset,
					ClassObject.ToDataPointer(obj), objectSize - ClassObject.DataOffset);
			}

			for (int i = 0; i < executeCount; i++)
			{
				OperationHeader opHead = reader.GetOperationHeader();
				opHead.WritePreviousVersion(0);

				ulong objHandle = objHandles[i];
				ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
				obj->nextVersionHandle = 0;
				obj->nextCollisionHandle = 0;
				obj->id = reader.ReadLong();
				obj->version = tran.Id;
				ReaderInfo.Clear(&obj->readerInfo);

				Checker.AssertFalse(ObjectStorage.IsBufferUsed(objHandle));

				error = PopulateObjectFromChangeset(tc, reader, obj, false, false, true);
				if (error == null)
					error = InsertObjectSynced(tran, obj, objHandle);

				if (error != null)
				{
					FreeStringsAndBlobs(obj);
					storage.FreeMultiple(objHandles + i, executeCount - i);
					break;
				}
				else
				{
					ObjectStorage.MarkBufferAsUsed(objHandle);
				}

				c++;
				tc.AddAffectedObject(classIndex, objHandle);
			}

			operationCount -= executeCount;
		}

		resizeCounter.ExitReadLock(lockHandle);

		if (resizeCounter.Add(lockHandle, c) && resizeCounter.Count > countLimit)
			Resize();

		return error;
	}

	[SkipLocalsInit]
	public void RestoreInsert(ChangesetBlock block, PendingRestoreOperations pendingOps, ulong commitVersion, ChangesetReader reader)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, commitVersion, pendingOps.Count, block.OperationCount);

		int lockHandle = resizeCounter.EnterReadLock();

		int operationCount = block.OperationCount;
		ulong* objHandles = stackalloc ulong[Math.Min(ExecuteRangeSize, operationCount)];

		while (operationCount > 0)
		{
			int executeCount = Math.Min(operationCount, ExecuteRangeSize);
			storage.AllocateMultiple(objHandles, executeCount);

			for (int i = 0; i < executeCount; i++)
			{
				OperationHeader opHead = reader.GetOperationHeader();

				ulong objHandle = objHandles[i];
				ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
				Utils.CopyMemory((byte*)defaultValues + ClassObject.DataOffset,
					ClassObject.ToDataPointer(obj), objectSize - ClassObject.DataOffset);
				obj->nextVersionHandle = 0;
				obj->nextCollisionHandle = 0;
				ReaderInfo.Clear(&obj->readerInfo);
				obj->id = reader.ReadLong();
				obj->version = commitVersion;

				TTTrace.Write(TraceId, ClassDesc.Id, commitVersion, obj->id, objHandle);

				// If this is not the last modification of this object in the transaction we have to mark it that way
				// so that concurrent modifications from newer transaction do not modify it until all the modifications
				// from this transaction have finished.
				if (!opHead.IsLastInTransaction)
				{
					TTTrace.Write(TraceId, ClassDesc.Id, commitVersion, obj->id, obj->version);
					obj->version |= OperationHeader.NotLastInTranFlag;
				}

				RestoreObjectFromChangeset(block, reader, obj);
				ObjectStorage.MarkBufferAsUsed(objHandle);

				RestoreInsertObject(pendingOps, opHead, obj, objHandle);
			}

			operationCount -= executeCount;
		}

		resizeCounter.ExitReadLock(lockHandle);

		if (resizeCounter.Add(lockHandle, block.OperationCount) && resizeCounter.Count > countLimit)
			Resize();
	}

	[SkipLocalsInit]
	public DatabaseErrorDetail Update(Transaction tran, ChangesetReader reader)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, ClassDesc.LogIndex);
		Checker.AssertFalse(tran.IsAlignment);

		TransactionContext tc = tran.Context;
		DatabaseErrorDetail error = null;

		int lockHandle = resizeCounter.EnterReadLock();

		int c = 0;
		error = WriteClassLocker(tran, tc);

		int operationCount = tc.ChangesetBlock.OperationCount;
		ulong* objHandles = stackalloc ulong[Math.Min(operationCount, ExecuteRangeSize)];

		while (error == null && operationCount > 0)
		{
			int executeCount = Math.Min(operationCount, ExecuteRangeSize);
			storage.AllocateMultiple(objHandles, executeCount);

			for (int i = 0; i < executeCount; i++)
			{
				OperationHeader opHead = reader.GetOperationHeader();
				long id = reader.ReadLong();

				ClassObject* obj = null;
				ulong objHandle = objHandles[i];
				obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
				obj->nextVersionHandle = 0;
				obj->nextCollisionHandle = 0;
				obj->id = id;
				obj->version = tran.Id;
				ReaderInfo.Clear(&obj->readerInfo);

				error = UpdateObjectSynced(tran, reader, opHead, obj, id, objHandle,
					out UpdateObjectResult updateResult, out ulong updatedVersionHandle);
				ClassObject* finalObject = (ClassObject*)ObjectStorage.GetBuffer(updatedVersionHandle);

				if (error != null)
				{
					storage.FreeMultiple(objHandles + i, executeCount - i);
					break;
				}

				TTTrace.Write(TraceId, ClassDesc.Id, finalObject->id, (byte)updateResult, finalObject != null);

				if (updateResult == UpdateObjectResult.Merged)
				{
					storage.Free(objHandle);
				}
				else
				{
					if (updateResult == UpdateObjectResult.InsertedObject)
						c++;

					tc.AddAffectedObject(classIndex, objHandle);
					ObjectStorage.MarkBufferAsUsed(objHandle);
				}
			}

			operationCount -= executeCount;
		}

		resizeCounter.ExitReadLock(lockHandle);

		if (resizeCounter.Add(lockHandle, c) && resizeCounter.Count > countLimit)
			Resize();

		return error;
	}

	public void RestoreUpdate(ChangesetBlock block, PendingRestoreOperations pendingOps, ulong commitVersion,
		ChangesetReader reader, bool isAlignment)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, commitVersion, block.OperationCount);

		int lockHandle = resizeCounter.EnterReadLock();

		int c = 0;
		for (int i = 0; i < block.OperationCount; i++)
		{
			OperationHeader opHead = reader.GetOperationHeader();
			long id = reader.ReadLong();
			if (RestoreUpdateObject(block, reader, pendingOps, id, opHead, commitVersion, isAlignment) == UpdateObjectResult.InsertedObject)
				c++;
		}

		resizeCounter.ExitReadLock(lockHandle);

		if (resizeCounter.Add(lockHandle, c) && resizeCounter.Count > countLimit)
			Resize();
	}

	public void Align(Transaction tran, ChangesetReader reader, ApplyAlignDelegate alignDelegate)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, ClassDesc.LogIndex);

		TransactionContext tc = tran.Context;

		int c = 0;
		ClassObject* obj = null;
		ulong objHandle = 0;

		int operationCount = tc.ChangesetBlock.OperationCount;
		while (operationCount > 0)
		{
			int executeCount = Math.Min(operationCount, ExecuteRangeSize);

			for (int i = 0; i < executeCount; i++)
			{
				OperationHeader opHead = reader.GetOperationHeader();
				long id = reader.ReadLong();
				Checker.AssertTrue(IdHelper.GetClassId(id) == this.ClassDesc.Id);

				if (objHandle == 0)
				{
					objHandle = storage.Allocate();
					obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
				}

				obj->nextVersionHandle = 0;
				obj->nextCollisionHandle = 0;
				obj->id = id;
				ReaderInfo.Clear(&obj->readerInfo);

				AlignObjectSynced(tran, reader, obj, objHandle, alignDelegate,
					out UpdateObjectResult updateResult, out ulong finalObjectHandle);

				ClassObject* finalObject = (ClassObject*)ObjectStorage.GetBuffer(finalObjectHandle);

				if (!tran.IsPropagated)
					finalObject->nextVersionHandle = ClassObject.AlignedFlag;

				TTTrace.Write(TraceId, ClassDesc.Id, obj->id, (byte)updateResult, finalObject != null);

				if (updateResult != UpdateObjectResult.Merged)
				{
					c++;
					ObjectStorage.MarkBufferAsUsed(objHandle);
					objHandle = 0;
				}
			}

			operationCount -= executeCount;
		}

		if (objHandle != 0)
			storage.Free(objHandle);

		resizeCounter.Add(c);
	}

	[SkipLocalsInit]
	public DatabaseErrorDetail Delete(Transaction tran, ChangesetReader reader)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, ClassDesc.LogIndex);

		TransactionContext tc = tran.Context;

		int lockHandle = resizeCounter.EnterReadLock();

		int c = 0;
		DatabaseErrorDetail error = WriteClassLocker(tran, tc);
		int operationCount = tc.ChangesetBlock.OperationCount;
		ulong* objHandles = stackalloc ulong[Math.Min(operationCount, ExecuteRangeSize)];

		while (error == null && operationCount > 0)
		{
			int executeCount = Math.Min(operationCount, ExecuteRangeSize);
			storage.AllocateMultiple(objHandles, executeCount);

			for (int i = 0; i < executeCount; i++)
			{
				OperationHeader opHead = reader.GetOperationHeader();
				ulong objHandle = objHandles[i];
				ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
				obj->nextVersionHandle = 0;
				obj->nextCollisionHandle = 0;
				obj->id = reader.ReadLong();
				obj->version = tran.Id;
				ReaderInfo.Clear(&obj->readerInfo);
				obj->IsDeleted = true;

				error = DeleteObjectSynced(tran, obj, opHead, objHandle, out ulong mergedWithHandle);
				if (error != null)
				{
					storage.FreeMultiple(objHandles + i, executeCount - i);
					break;
				}

				tc.AddDeleted(classIndex, obj->id);

				TTTrace.Write(mergedWithHandle);
				if (mergedWithHandle != 0)
				{
					if (tran.IsAlignment)
						c++;

					storage.Free(objHandle);
				}
				else
				{
					Checker.AssertFalse(tran.IsAlignment);
					ObjectStorage.MarkBufferAsUsed(objHandle);
					tc.AddAffectedObject(classIndex, objHandle);
				}
			}

			operationCount -= executeCount;
		}

		resizeCounter.ExitReadLock(lockHandle);

		if (c > 0)
			resizeCounter.Sub(lockHandle, c);

		return error;
	}

	public void RestoreDelete(ChangesetBlock block, PendingRestoreOperations pendingOps, ulong commitVersion,
		ChangesetReader reader, bool isAlignment)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, commitVersion, block.OperationCount);

		int lockHandle = resizeCounter.EnterReadLock();

		for (int i = 0; i < block.OperationCount; i++)
		{
			OperationHeader opHead = reader.GetOperationHeader();
			RestoreDeleteObject(pendingOps, reader.ReadLong(), commitVersion, opHead, isAlignment, false);
		}

		resizeCounter.ExitReadLock(lockHandle);
	}

	public void RestoreClassDrop(ChangesetBlock block, ChangesetReader reader)
	{
		TTTrace.Write(TraceId, ClassDesc.Id);

		resizeCounter.EnterWriteLock();

		reader.GetOperationHeader();
		long id = reader.ReadLong();
		Checker.AssertTrue(block.OperationCount == 1 && id == 0);

		using (ClassScan scan = this.GetClassScan(null, false, out _))
		{
			foreach (ObjectReader r in scan)
			{
				TTTrace.Write(TraceId, ClassDesc.Id, r.GetIdOptimized());
				RestoreDeleteObject(null, r.GetIdOptimized(), 0, new OperationHeader(), false, true);
			}
		}

		resizeCounter.ExitWriteLock();
	}

	public void RestoreDefaultValue(ChangesetBlock block, ulong commitVersion, ChangesetReader reader)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, commitVersion, block.OperationCount);

		int lockHandle = resizeCounter.EnterReadLock();

		bool firstDone = false;
		ReaderPosition position = new ReaderPosition();
		using (ClassScan scan = this.GetClassScan(null, false, out _))
		{
			foreach (ObjectReader r in scan)
			{
				if (!firstDone)
				{
					OperationHeader opHead = reader.GetOperationHeader();
					long id = reader.ReadLong();
					Checker.AssertTrue(block.OperationCount == 1 && id == 0);
					position = reader.GetPosition();
					firstDone = true;
				}

				reader.SetPosition(position);
				ClassObject* obj = r.ClassObject;
				TTTrace.Write(TraceId, ClassDesc.Id, obj->id, commitVersion);
				obj->version = commitVersion;
				RestoreObjectFromChangeset(block, reader, obj);
			}
		}

		if (!firstDone) // There were no objects
			ChangesetReader.SkipBlock(reader, block);

		resizeCounter.ExitReadLock(lockHandle);
	}

	public void MarkAsAligned(long id)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, id);

		Bucket* bucket = buckets + CalculateBucket(id);
		ulong* handlePointer = Bucket.LockAccess(bucket);

		ClassObject* obj = FindObject(handlePointer, id, out _);
		obj->nextVersionHandle = ClassObject.AlignedFlag;

		Bucket.UnlockAccess(bucket);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void GenerateAlignmentDeletes(ClassObject* obj, ref ChangesetWriter writer)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, obj->id, obj->nextVersionHandle);
		if (obj->nextVersionHandle != 0)
		{
			obj->nextVersionHandle = 0;
			return;
		}

		if (writer == null)
		{
			writer = Engine.ChangesetWriterPool.Get();
			writer.StartDeleteBlock(ClassDesc);
		}

		writer.AddDelete(obj->id);
	}

	public long RollbackObject(Transaction tran, AffectedObject* ap)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, ap->objectHandle, tran.Id);

		int lockHandle = resizeCounter.EnterReadLock();

		ulong handle = ap->objectHandle;

		ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(handle);
		TTTrace.Write(obj->id, obj->IsDeleted, obj->version);

		long id = obj->id;
		Bucket* bucket = buckets + CalculateBucket(id);
		ulong* handlePointer = Bucket.LockAccess(bucket);
		try
		{
			FindObject(handlePointer, id, out ulong* pObjPointer);
			Checker.AssertTrue(*pObjPointer == handle);
			TTTrace.Write(TraceId, ClassDesc.Id, obj->id, obj->IsDeleted, obj->nextVersionHandle);

			if (!obj->IsDeleted)
				DeleteFromIndexes(handle, obj);

			ClassObject* nextVerObj = (ClassObject*)ObjectStorage.GetBuffer(obj->nextVersionHandle);
			if (nextVerObj == null)
			{
				Checker.AssertTrue(obj->readerInfo.TotalLockCount == 0);
				*pObjPointer = obj->nextCollisionHandle;
				resizeCounter.Dec();
			}
			else
			{
				// Even if there was standard lock by this transaction, it has already been rolled back.
				Checker.AssertTrue(obj->readerInfo.StandardLockCount == 0);

				nextVerObj->readerInfo = obj->readerInfo.Locks;   // Take over Existance locks if any (without IsDeleted).
				*pObjPointer = obj->nextVersionHandle;
				nextVerObj->nextCollisionHandle = obj->nextCollisionHandle;
			}
		}
		finally
		{
			Bucket.UnlockAccess(bucket);
			if (!obj->IsDeleted)
				FreeStringsAndBlobs(obj);

			ObjectStorage.MarkBufferNotUsed(handle);
			storage.Free(handle);
		}

		resizeCounter.ExitReadLock(lockHandle);
		return id;
	}

	public long RollbackReadLock(Transaction tran, long id)
	{
		Checker.AssertTrue(tran.NextMerged == null);

		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(id);
		ulong* ph = Bucket.LockAccess(bucket);

		ClassObject* obj = FindObject(ph, id, out _);

		ReaderInfo* r = &obj->readerInfo;
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, tran.CommitVersion, tran.Slot,
			obj->id, obj->version, obj->readerInfo.ExistanceLockCount, obj->readerInfo.StandardLockCount);

		ReaderInfo.FinalizeObjectLock(tran, obj->id, r, false, tran.Slot);

		Bucket.UnlockAccess(bucket);

		resizeCounter.ExitReadLock(lockHandle);

		return id;
	}

	public long CommitObject(Transaction tran, ulong handle)
	{
		ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(handle);

		ReaderInfo* r = &obj->readerInfo;
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, tran.CommitVersion, obj->version, obj->nextVersionHandle,
			tran.Slot, obj->id, obj->IsDeleted);

		// Alignment does not commit the object because it has a preassigned commit version.
		Checker.AssertFalse(tran.IsAlignment);

		if (Database.IsUncommited(obj->version) || obj->version == tran.CommitVersion)
		{
			obj->version = tran.CommitVersion;

			if (obj->nextVersionHandle != 0)
			{
				ClassObject* prevObj = (ClassObject*)ObjectStorage.GetBuffer(obj->nextVersionHandle);
				prevObj->NewerVersion = tran.CommitVersion;
			}

			if (!obj->IsDeleted && hasStringsOrBlobs)
				CommitStringsAndBlobs(tran, obj);

			TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, tran.CommitVersion, obj->version, obj->nextVersionHandle,
				tran.Slot, obj->id, obj->IsDeleted);
		}

		return obj->id;
	}

	private void CommitStringsAndBlobs(Transaction tran, ClassObject* currObj)
	{
		ClassObject* prevObj = (ClassObject*)ObjectStorage.GetBuffer(currObj->nextVersionHandle);

		byte* currBuffer = ClassObject.ToDataPointer(currObj);
		byte* prevBuffer = ClassObject.ToDataPointer(prevObj);

		ReadOnlyArray<int> stringProps = ClassDesc.StringPropertyIndexes;
		int count = stringProps.Length;
		for (int i = 0; i < count; i++)
		{
			int index = stringProps[i];
			int offset = propertyOffsets[index];
			ulong* currp = (ulong*)(currBuffer + offset);
			if (prevObj == null || *currp != *(ulong*)(prevBuffer + offset))
			{
				TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, tran.CommitVersion, i, currObj->id,
					currObj->version, prevObj == null ? 0 : prevObj->version, *currp);
				stringStorage.SetStringVersion(*currp, tran.CommitVersion);
			}
		}

		ReadOnlyArray<int> blobProps = ClassDesc.BlobPropertyIndexes;
		count = blobProps.Length;
		for (int i = 0; i < count; i++)
		{
			int index = blobProps[i];
			int offset = propertyOffsets[index];
			ulong* currp = (ulong*)(currBuffer + offset);
			if (prevObj == null || *currp != *(ulong*)(prevBuffer + offset))
			{
				TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, tran.CommitVersion, i, currObj->id,
					currObj->version, prevObj == null ? 0 : prevObj->version, *currp);
				blobStorage.SetVersion(*currp, tran.CommitVersion);
			}
		}
	}

	public long CommitReadLock(Transaction tran, long id, ushort slot)
	{
		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(id);
		ulong* ph = Bucket.LockAccess(bucket);

		ClassObject* obj = FindObject(ph, id, out _);

		ReaderInfo* r = &obj->readerInfo;
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, tran.CommitVersion, tran.Slot,
			obj->id, obj->version, obj->readerInfo.ExistanceLockCount, obj->readerInfo.StandardLockCount);

		ReaderInfo.FinalizeObjectLock(tran, obj->id, r, true, slot);

		Bucket.UnlockAccess(bucket);

		resizeCounter.ExitReadLock(lockHandle);

		return obj->id;
	}

	public long RemapReadLockSlot(long id, ushort prevSlot, ushort newSlot)
	{
		int lockHandle = resizeCounter.EnterReadLock();

		Bucket* bucket = buckets + CalculateBucket(id);
		ulong* ph = Bucket.LockAccess(bucket);

		ClassObject* obj = FindObject(ph, id, out _);

		ReaderInfo* r = &obj->readerInfo;
		TTTrace.Write(TraceId, ClassDesc.Id, prevSlot, newSlot, obj->id);

		ReaderInfo.RemapSlot(r, prevSlot, newSlot);

		Bucket.UnlockAccess(bucket);

		resizeCounter.ExitReadLock(lockHandle);

		return obj->id;
	}

	public void CommitClassWriteLock(ClassLocker locker, ulong commitVersion)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, commitVersion);

		int lockHandle = resizeCounter.EnterReadLock();
		locker.CommitWrite(commitVersion);
		resizeCounter.ExitReadLock(lockHandle);
	}

	public void RollbackClassWriteLock(ClassLocker locker)
	{
		TTTrace.Write(TraceId, ClassDesc.Id);

		int lockHandle = resizeCounter.EnterReadLock();
		locker.RollbackWrite();
		resizeCounter.ExitReadLock(lockHandle);
	}

	public void CommitClassReadLock(ClassLocker locker, ulong commitVersion)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, commitVersion);

		resizeCounter.EnterWriteLock();
		locker.CommitReadLock(commitVersion);
		resizeCounter.ExitWriteLock();
	}

	public void RollbackClassReadLock(ClassLocker locker)
	{
		TTTrace.Write(TraceId, ClassDesc.Id);

		resizeCounter.EnterWriteLock();
		locker.RollbackReadLock();
		resizeCounter.ExitWriteLock();
	}

	public override DatabaseErrorDetail TakeReadLock(Transaction tran, ClassLocker locker)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id);

		DatabaseErrorDetail err = null;

		resizeCounter.EnterWriteLock();

		if (!locker.TryTakeReadLock(tran))
			err = DatabaseErrorDetail.CreateConflict(0, ClassDesc.FullName);

		resizeCounter.ExitWriteLock();

		return err;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail InsertObjectSynced(Transaction tran, ClassObject* obj, ulong objHandle)
	{
		Checker.AssertFalse(tran.IsAlignment);

		Bucket* bucket = buckets + CalculateBucket(obj->id);
		ulong* handlePointer = Bucket.LockAccess(bucket);
		DatabaseErrorDetail res = InsertObject(tran, bucket, handlePointer, obj, objHandle);
		Bucket.UnlockAccess(bucket);

		return res;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail UpdateObjectSynced(Transaction tran, ChangesetReader reader, OperationHeader opHead,
		ClassObject* obj, long id, ulong objHandle, out UpdateObjectResult updateResult, out ulong finalObjectHandle)
	{
		Bucket* bucket = buckets + CalculateBucket(id);
		ulong* handlePointer = Bucket.LockAccess(bucket);

		DatabaseErrorDetail res = UpdateObject(tran, bucket, handlePointer, reader,
			opHead, obj, id, objHandle, out updateResult, out finalObjectHandle);

		Bucket.UnlockAccess(bucket);

		return res;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void AlignObjectSynced(Transaction tran, ChangesetReader reader, ClassObject* obj, ulong objHandle,
		ApplyAlignDelegate alignDelegate, out UpdateObjectResult updateResult, out ulong finalObjectHandle)
	{
		Bucket* bucket = buckets + CalculateBucket(obj->id);
		ulong* handlePointer = Bucket.LockAccess(bucket);
		AlignObject(tran, bucket, handlePointer, reader, obj, objHandle, alignDelegate, out updateResult, out finalObjectHandle);
		Bucket.UnlockAccess(bucket);

		obj = (ClassObject*)ObjectStorage.GetBuffer(finalObjectHandle);
		if (updateResult == UpdateObjectResult.InsertedObject)
		{
			Utils.CopyMemory((byte*)defaultValues + ClassObject.UserPropertiesOffset,
				(byte*)obj + ClassObject.UserPropertiesOffset, objectSize - ClassObject.UserPropertiesOffset);
			alignDelegate(obj, stringStorage, blobStorage, tran.Context, reader, ClassDesc, null, 0);
			InsertIntoAffectedIndexes(objHandle, obj, 0xffffffffffffffff);
		}
		else
		{
			ulong affectedIndexes = alignDelegate(obj, stringStorage, blobStorage,
				tran.Context, reader, ClassDesc, indexDeleteDelegate, finalObjectHandle);
			InsertIntoAffectedIndexes(finalObjectHandle, obj, affectedIndexes);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail DeleteObjectSynced(Transaction tran, ClassObject* obj,
		OperationHeader opHead, ulong objHandle, out ulong mergedWithHandle)
	{
		Bucket* bucket = buckets + CalculateBucket(obj->id);
		ulong* handlePointer = Bucket.LockAccess(bucket);

		DatabaseErrorDetail res = DeleteObject(tran, bucket, handlePointer, opHead, obj, objHandle, out mergedWithHandle);

		if (tran.IsAlignment)
			FindObjectAndGarbageCollect(bucket, handlePointer, obj->id, ulong.MaxValue);

		Bucket.UnlockAccess(bucket);

		return res;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail InsertObject(Transaction tran, Bucket* bucket, ulong* handlePointer, ClassObject* obj, ulong objHandle)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, tran.IsCommitVersionPreAssigned, bucket->Handle, obj->id,
			obj->IsDeleted, obj->version);

		if (obj->id == 0)
			return DatabaseErrorDetail.CreateZeroIdProvided(obj->id);

		DatabaseErrorDetail err;
		if (bucket->Handle == 0)
		{
			err = InsertIntoIndexes(tran, objHandle, obj);
			if (err != null)
				return err;

			*handlePointer = objHandle;
			return null;
		}

		// It is valid to already have an object with the given id but it needs to be marked as deleted. This
		// is possible during alignment with rewind.
		ClassObject* existingObj = FindObject(handlePointer, obj->id, out ulong* pObjPointer);
		if (existingObj != null)
		{
			if (!tran.IsAlignment || !existingObj->IsDeleted)
				return DatabaseErrorDetail.CreateNonUniqueId(obj->id, ClassDesc.FullName);

			err = InsertIntoIndexes(tran, objHandle, obj);
			if (err != null)
				return err;

			obj->nextVersionHandle = *pObjPointer;
			existingObj->NewerVersion = obj->version;
			obj->nextCollisionHandle = existingObj->nextCollisionHandle;
			*pObjPointer = objHandle;
			return null;
		}

		err = InsertIntoIndexes(tran, objHandle, obj);
		if (err != null)
			return err;

		obj->nextCollisionHandle = bucket->Handle;
		*handlePointer = objHandle;
		return null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void RestoreInsertObject(PendingRestoreOperations pendingOps, OperationHeader opHead, ClassObject* obj, ulong objHandle)
	{
		Bucket* bucket = buckets + CalculateBucket(obj->id);
		ulong* handlePointer = Bucket.LockAccess(bucket);

		TTTrace.Write(TraceId, ClassDesc.Id, bucket->Handle, obj->id, obj->IsDeleted, obj->version,
			opHead.PreviousVersion, opHead.IsFirstInTransaction, opHead.IsLastInTransaction);

		bool hasPending = false;
		ClassObject* existingObj = FindObject(handlePointer, obj->id, out ulong* pObjPointer);
		if (existingObj != null)    // Marker (that we have pending operations)
		{
			Checker.AssertTrue(existingObj->version == ulong.MaxValue);
			TTTrace.Write(TraceId, ClassDesc.Id, bucket->Handle, obj->id, obj->IsDeleted, obj->version);

			if (opHead.IsLastInTransaction)
			{
				hasPending = true;
			}
			else
			{
				obj->nextVersionHandle = PendingRestoreObjectHeader.PendingRestore;
			}

			ulong handle = *pObjPointer;
			*pObjPointer = existingObj->nextCollisionHandle;
			ObjectStorage.MarkBufferNotUsed(handle);
			storage.Free(handle);
		}

		obj->nextCollisionHandle = bucket->Handle;
		*handlePointer = objHandle;

		if (hasPending)
		{
			if (!pendingOps.TryPrune(obj->id, obj->version, pendingRestorer, (IntPtr)handlePointer))
				obj->nextVersionHandle = PendingRestoreObjectHeader.PendingRestore;
		}

		Bucket.UnlockAccess(bucket);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail UpdateObject(Transaction tran, Bucket* bucket, ulong* handlePointer, ChangesetReader reader,
		OperationHeader opHead, ClassObject* obj, long id, ulong objHandle, out UpdateObjectResult result, out ulong finalObjectHandle)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, bucket->Handle, obj->id, tran.IsCommitVersionPreAssigned,
			obj->IsDeleted, obj->version, objHandle);

		result = UpdateObjectResult.InsertedVersion;
		finalObjectHandle = 0;

		if (id == 0)
			return DatabaseErrorDetail.CreateZeroIdProvided(0);

		ulong* existingObjPointer;
		ClassObject* existingObj = FindObject(handlePointer, id, out existingObjPointer);

		TransactionContext tc = tran.Context;

		if (existingObj == null)
			return DatabaseErrorDetail.CreateUpdateNonExistent(id, ClassDesc.FullName);

		TTTrace.Write(existingObj->IsDeleted, existingObj->version);

		if (Database.IsUncommited(existingObj->version))
		{
			if (existingObj->version != tran.Id)
			{
				if (FindVersion(existingObj, tran.ReadVersion, tran.Id) == null)
					return DatabaseErrorDetail.CreateUpdateNonExistent(id, ClassDesc.FullName);

				return DatabaseErrorDetail.CreateConflict(id, ClassDesc.FullName);
			}
		}
		else
		{
			if (existingObj->version > tran.ReadVersion)
				return DatabaseErrorDetail.CreateConflict(id, ClassDesc.FullName);

			if (ReaderInfo.IsObjectInUpdateConflict(tran, existingObj->id, &existingObj->readerInfo))
				return DatabaseErrorDetail.CreateConflict(id, ClassDesc.FullName);
		}

		if (existingObj->IsDeleted)
			return DatabaseErrorDetail.CreateUpdateNonExistent(id, ClassDesc.FullName);

		DatabaseErrorDetail err;
		if (existingObj != null)
		{
			if (existingObj->version == tran.Id)
			{
				TTTrace.Write();

				DeleteFromIndexes(*existingObjPointer, existingObj);
				err = PopulateObjectFromChangeset(tc, reader, existingObj, true, true, true);
				if (err == null)
					err = InsertIntoIndexes(tran, *existingObjPointer, existingObj);

				if (err != null)
					return err;

				opHead.WritePreviousVersion(0);
				if (isPersistanceActive)
					tc.AddMultiAffectedObject(existingObj->id);

				result = UpdateObjectResult.Merged;
				finalObjectHandle = *existingObjPointer;
				return null;
			}

			err = UpdateObjectFromChangeset(tc, reader, existingObj, obj);
			if (err == null)
				err = InsertIntoIndexes(tran, objHandle, obj);

			if (err != null)
			{
				FreeStringsAndBlobs(obj);
				return err;
			}

			opHead.WritePreviousVersion(existingObj->version);

			Checker.AssertTrue(existingObj->readerInfo.StandardLockCount <= 1);

			finalObjectHandle = objHandle;

			// Important to take over any existing Existance read locks. It is also possible that there is a single Standard lock
			// owned by this same transaction, and it is safe to copy that as well.
			obj->readerInfo = existingObj->readerInfo;

			obj->nextVersionHandle = *existingObjPointer;
			existingObj->NewerVersion = obj->version;
			obj->nextCollisionHandle = existingObj->nextCollisionHandle;
			*existingObjPointer = objHandle;
		}
		else
		{
			// We can end up here in alignment because Primary sends all records as Updates (does not detect inserts)
			Utils.CopyMemory((byte*)defaultValues + ClassObject.UserPropertiesOffset,
				(byte*)obj + ClassObject.UserPropertiesOffset, objectSize - ClassObject.UserPropertiesOffset);
			PopulateObjectFromChangeset(tc, reader, obj, false, false, false);
			InsertIntoIndexes(tran, objHandle, obj);

			obj->nextCollisionHandle = bucket->Handle;
			*handlePointer = objHandle;
			finalObjectHandle = objHandle;
			result = UpdateObjectResult.InsertedObject;
		}

		return null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void AlignObject(Transaction tran, Bucket* bucket, ulong* handlePointer, ChangesetReader reader, ClassObject* obj,
		ulong objHandle, ApplyAlignDelegate alignDelegate, out UpdateObjectResult updateResult, out ulong finalObjectHandle)
	{
		finalObjectHandle = 0;
		TransactionContext tc = tran.Context;
		ClassObject* existingObj = FindObject(handlePointer, obj->id, out ulong* existingObjPointer);

		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, bucket->Handle, obj->id, obj->version, objHandle,
			existingObj != null);

		if (existingObj != null)
		{
			TTTrace.Write(existingObj->version);
			finalObjectHandle = *existingObjPointer;
			updateResult = UpdateObjectResult.Merged;
		}
		else
		{
			obj->nextCollisionHandle = bucket->Handle;
			*handlePointer = objHandle;
			finalObjectHandle = objHandle;
			updateResult = UpdateObjectResult.InsertedObject;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool CanRestoreOperation(ClassObject* existingObject, OperationHeader opHead, ulong commitVersion)
	{
		return existingObject->version == opHead.PreviousVersion ||
			(!opHead.IsFirstInTransaction && existingObject->version == (commitVersion | OperationHeader.NotLastInTranFlag));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private UpdateObjectResult RestoreUpdateObject(ChangesetBlock block, ChangesetReader reader, PendingRestoreOperations pendingOps,
		long id, OperationHeader opHead, ulong commitVersion, bool isAlignment)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, id, commitVersion, opHead.PreviousVersion, opHead.IsFirstInTransaction,
			opHead.IsLastInTransaction, isAlignment);

		Bucket* bucket = buckets + CalculateBucket(id);
		ulong* handlePointer = Bucket.LockAccess(bucket);

		ClassObject* existingObj = FindObject(handlePointer, id, out ulong* pObjPointer);

		UpdateObjectResult result = UpdateObjectResult.Merged;
		if (existingObj != null && CanRestoreOperation(existingObj, opHead, commitVersion) || isAlignment)
		{
			if (existingObj == null)    // Valid in alignment since update is considered as insertOrUpdate
			{
				ulong objHandle = storage.Allocate();
				ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
				Utils.CopyMemory((byte*)defaultValues + ClassObject.DataOffset,
					ClassObject.ToDataPointer(obj), objectSize - ClassObject.DataOffset);

				obj->version = commitVersion;
				obj->id = id;
				obj->nextCollisionHandle = bucket->Handle;
				obj->nextVersionHandle = 0;
				ReaderInfo.Clear(&obj->readerInfo);
				bucket->Handle = objHandle;
				RestoreObjectFromChangeset(block, reader, obj);
				ObjectStorage.MarkBufferAsUsed(objHandle);
				result = UpdateObjectResult.InsertedObject;
			}
			else
			{
				TTTrace.Write(TraceId, ClassDesc.Id, id, commitVersion, opHead.PreviousVersion, isAlignment);
				existingObj->version = commitVersion;

				// If this is not the last modification of this object in the transaction we have to mark it that way
				// so that concurrent modifications from newer transaction do not modify it until all the modifications
				// from this transaction have finished.
				if (!opHead.IsLastInTransaction && !isAlignment)
					existingObj->version |= OperationHeader.NotLastInTranFlag;

				RestoreObjectFromChangeset(block, reader, existingObj);
				if (opHead.IsLastInTransaction && existingObj->nextVersionHandle == PendingRestoreObjectHeader.PendingRestore)
				{
					Checker.AssertFalse(isAlignment);
					if (!pendingOps.TryPrune(id, commitVersion, pendingRestorer, (IntPtr)pObjPointer))
						existingObj->nextVersionHandle = PendingRestoreObjectHeader.PendingRestore;
				}
			}
		}
		else
		{
			int size = block.GetStorageSize();
			ulong handle = Engine.MemoryManager.Allocate(PendingRestoreObjectHeader.Size + size);
			PendingRestoreObjectHeader* ph = (PendingRestoreObjectHeader*)Engine.MemoryManager.GetBuffer(handle);
			ph->id = id;
			ph->version = commitVersion;
			ph->isLastInTransaction = opHead.IsLastInTransaction;
			ph->prevVersion = opHead.PreviousVersion;
			ph->isDelete = false;
			FillPendingUpdate(block, reader, ph);
			pendingOps.Add(id, handle);

			if (existingObj != null)
			{
				TTTrace.Write(TraceId, ClassDesc.Id, id, commitVersion, opHead.PreviousVersion, isAlignment, existingObj->version);
				existingObj->nextVersionHandle = PendingRestoreObjectHeader.PendingRestore;
			}
			else
			{
				// Create empty object to signal that we have pending operations, this will be sorted out once the insert is encountered
				ulong objHandle = storage.Allocate();
				ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
				obj->version = ulong.MaxValue;
				obj->id = id;
				obj->nextCollisionHandle = bucket->Handle;
				obj->nextVersionHandle = PendingRestoreObjectHeader.PendingRestore;
				ReaderInfo.Clear(&obj->readerInfo);
				bucket->Handle = objHandle;
				ObjectStorage.MarkBufferAsUsed(objHandle);
			}
		}

		Bucket.UnlockAccess(bucket);

		return result;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail DeleteObject(Transaction tran, Bucket* bucket, ulong* handlePointer, OperationHeader opHead,
		ClassObject* obj, ulong objHandle, out ulong mergedWithHandle)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, tran.IsAlignment, bucket->Handle, objHandle, obj->id, obj->IsDeleted, obj->version);

		mergedWithHandle = 0;

		if (obj->id == 0)
			return DatabaseErrorDetail.CreateZeroIdProvided(0);

		ulong* pObjPointer;
		ClassObject* existingObj = FindObject(handlePointer, obj->id, out pObjPointer);

		if (existingObj == null)
			return DatabaseErrorDetail.CreateNonExistentDelete(obj->id, ClassDesc.FullName);

		TTTrace.Write(existingObj->IsDeleted, existingObj->version);

		if (Database.IsUncommited(existingObj->version))
		{
			Checker.AssertFalse(tran.IsAlignment);
			if (existingObj->version != tran.Id)
			{
				if (FindVersion(existingObj, tran.ReadVersion, tran.Id) != null)
				{
					return DatabaseErrorDetail.CreateConflict(obj->id, ClassDesc.FullName);
				}
				else
				{
					return DatabaseErrorDetail.CreateNonExistentDelete(obj->id, ClassDesc.FullName);
				}
			}
		}
		else
		{
			if (existingObj->version > tran.ReadVersion && !tran.IsAlignment)
				return DatabaseErrorDetail.CreateConflict(obj->id, ClassDesc.FullName);

			if (ReaderInfo.IsObjectInDeleteConflict(tran, existingObj->id, &existingObj->readerInfo))
				return DatabaseErrorDetail.CreateConflict(obj->id, ClassDesc.FullName);
		}

		if (existingObj->IsDeleted)
			return DatabaseErrorDetail.CreateNonExistentDelete(obj->id, ClassDesc.FullName);

		if (tran.IsAlignment)
		{
			CreateGroupingInvRefChanges(tran.Context, existingObj, InvRefChangeType.Delete);
		}
		else
		{
			RemoveExistingReferences(tran.Context, existingObj);
		}

		if (existingObj->version == tran.Id || tran.IsAlignment)
		{
			DeleteFromIndexes(*pObjPointer, existingObj);
			FreeStringsAndBlobs(existingObj);

			opHead.WritePreviousVersion(0);
			if (!tran.IsAlignment && isPersistanceActive)
				tran.Context.AddMultiAffectedObject(existingObj->id);

			existingObj->IsDeleted = true;
			mergedWithHandle = *pObjPointer;
			return null;
		}

		opHead.WritePreviousVersion(existingObj->version);

		Checker.AssertTrue(existingObj->readerInfo.StandardLockCount <= 1);

		// Important to take over any existing read locks. These locks are guaranteed to be from this transaction.
		// We need to transfer them so they can be commited/rolled back.
		obj->readerInfo = existingObj->readerInfo.Locks;
		obj->IsDeleted = true;      // Since the previous line has written over the IsDeleted bit

		obj->nextVersionHandle = *pObjPointer;
		existingObj->NewerVersion = obj->version;
		obj->nextCollisionHandle = existingObj->nextCollisionHandle;
		*pObjPointer = objHandle;
		return null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void RestoreDeleteObject(PendingRestoreOperations pendingOps, long id,
		ulong commitVersion, OperationHeader opHead, bool isAlignment, bool forceRestore)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, id, commitVersion, opHead.PreviousVersion,
			opHead.IsFirstInTransaction, opHead.IsLastInTransaction, isAlignment);

		Bucket* bucket = buckets + CalculateBucket(id);
		ulong* handlePointer = Bucket.LockAccess(bucket);

		ClassObject* existingObj = FindObject(handlePointer, id, out ulong* pObjPointer);
		Checker.AssertFalse(existingObj == null && isAlignment);

		if (forceRestore || existingObj != null && CanRestoreOperation(existingObj, opHead, commitVersion) || isAlignment)
		{
			Checker.AssertFalse(existingObj->nextVersionHandle == PendingRestoreObjectHeader.PendingRestore);
			TTTrace.Write(TraceId, ClassDesc.Id, id, commitVersion, opHead.PreviousVersion, isAlignment);

			FreeStringsAndBlobs(existingObj);

			ulong temp = existingObj->nextCollisionHandle;
			ObjectStorage.MarkBufferNotUsed(*pObjPointer);
			storage.Free(*pObjPointer);
			*pObjPointer = temp;
			resizeCounter.Dec();
		}
		else
		{
			ulong handle = Engine.MemoryManager.Allocate(PendingRestoreObjectHeader.Size);
			PendingRestoreObjectHeader* ph = (PendingRestoreObjectHeader*)Engine.MemoryManager.GetBuffer(handle);
			ph->id = id;
			ph->version = commitVersion;
			ph->isLastInTransaction = true; // Since this is delete
			Checker.AssertTrue(opHead.IsLastInTransaction);
			ph->prevVersion = opHead.PreviousVersion;
			ph->isDelete = true;
			pendingOps.Add(id, handle);

			if (existingObj != null)
			{
				TTTrace.Write(TraceId, ClassDesc.Id, id, commitVersion, opHead.PreviousVersion, isAlignment, existingObj->version);
				existingObj->nextVersionHandle = PendingRestoreObjectHeader.PendingRestore;
			}
			else
			{
				// Create empty object to signal that we have pending operations, this will be sorted out once the insert is encountered
				ulong objHandle = storage.Allocate();
				ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
				obj->version = ulong.MaxValue;
				obj->id = id;
				obj->nextCollisionHandle = bucket->Handle;
				obj->nextVersionHandle = PendingRestoreObjectHeader.PendingRestore;
				ReaderInfo.Clear(&obj->readerInfo);
				bucket->Handle = objHandle;
				ObjectStorage.MarkBufferAsUsed(objHandle);
			}
		}

		Bucket.UnlockAccess(bucket);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool FindObjectAndGarbageCollect(Bucket* bucket, ulong* handlePointer, long id, ulong oldestReadVersion)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, bucket->Handle, id, oldestReadVersion);

		if (bucket->Handle == 0)
			return false;

		ClassObject* obj = FindObject(handlePointer, id, out ulong* pObjPointer);
		if (obj == null)
			return false;

		TTTrace.Write(TraceId, ClassDesc.Id, obj->IsDeleted, obj->version);

		if (obj->IsDeleted && obj->version <= oldestReadVersion)
		{
			ulong nextCollision = obj->nextCollisionHandle;
			FreeVersionChain(*pObjPointer);
			*pObjPointer = nextCollision;
			return true;
		}

		while (obj != null)
		{
			if (obj->version <= oldestReadVersion)
			{
				// We found the last version that is visible to a transaction, so we can free all the older versions
				pObjPointer = &obj->nextVersionHandle;
				FreeVersionChain(*pObjPointer);
				*pObjPointer = 0;
			}

			pObjPointer = &obj->nextVersionHandle;
			obj = (ClassObject*)ObjectStorage.GetBuffer(obj->nextVersionHandle);
		}

		return false;
	}

	private void FreeVersionChain(ulong objHandle)
	{
		while (objHandle != 0)
		{
			ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
			TTTrace.Write(TraceId, ClassDesc.Id, obj->id, obj->IsDeleted, obj->version);

			ulong nextHandle = obj->nextVersionHandle;
			DeleteFromIndexes(objHandle, obj);

			if (!obj->IsDeleted)
				FreeStringsAndBlobs(obj);

			ObjectStorage.MarkBufferNotUsed(objHandle);
			storage.Free(objHandle);
			objHandle = nextHandle;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ClassObject* FindVersion(ClassObject* curr, ulong snapshotVersion, ulong tranId = ulong.MaxValue)
	{
		ulong objectHandle = 0;
		return FindVersion(curr, snapshotVersion, ref objectHandle, tranId);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ClassObject* FindVersion(ClassObject* curr, ulong snapshotVersion, ref ulong objectHandle, ulong tranId = ulong.MaxValue)
	{
		while (curr != null)
		{
			TTTrace.Write(TraceId, ClassDesc.Id, snapshotVersion, curr->IsDeleted, curr->version);
			if (curr->version <= snapshotVersion || curr->version == tranId)
				return curr;

			objectHandle = curr->nextVersionHandle;
			curr = (ClassObject*)ObjectStorage.GetBuffer(curr->nextVersionHandle);
		}

		return null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ClassObject* FindObject(ulong* pcurr, long id, out ulong* handlePointer)
	{
		while (*pcurr != 0)
		{
			ClassObject* currObj = (ClassObject*)ObjectStorage.GetBuffer(*pcurr);
			TTTrace.Write(TraceId, ClassDesc.Id, currObj->id, currObj->IsDeleted, currObj->version, currObj->nextVersionHandle);

			if (currObj->id == id)
			{
				handlePointer = pcurr;
				return currObj;
			}

			pcurr = &currObj->nextCollisionHandle;
		}

		handlePointer = null;
		return null;
	}

	private ulong ReadBlob(ChangesetReader reader, PropertyType propType, out bool isDefined)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, (int)propType);

		if (propType != PropertyType.StringArray)
		{
			int len = reader.ReadLength(out bool isNull, out isDefined);
			if (!isDefined)
				return 0;

			int size = len * PropertyTypesHelper.GetElementSize(propType);
			ulong handle = blobStorage.AllocBlob(isNull, size + 4, out byte* buffer);
			if (isNull)
				return handle;

			*((int*)buffer) = len;
			reader.ReadBytes(buffer + 4, size);
			return handle;
		}
		else
		{
			int size = reader.ReadStringArraySize(out bool isNull, out isDefined);
			if (!isDefined)
				return 0;

			ulong handle = blobStorage.AllocBlob(isNull, size, out byte* buffer);
			if (size == 0)
				return handle;

			reader.ReadBytes(buffer, size);
			return handle;
		}
	}

	private void FillPendingUpdate(ChangesetBlock block, ChangesetReader reader, PendingRestoreObjectHeader* ph)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, ph->id, ph->version, ph->prevVersion, ph->isDelete, ph->isLastInTransaction);
		byte* buffer = (byte*)ph + PendingRestoreObjectHeader.Size;

		int c = 0;
		int propertyCount = block.PropertyCount;
		for (int i = 1; i < propertyCount; i++) // First property is always id and has been written
		{
			ChangesetBlockProperty prop = block.GetProperty(i);
			if (prop.Index == -1)
			{
				reader.SkipValue(prop.Type);
				continue;
			}

			c++;
			*((int*)buffer) = prop.Index;
			buffer += sizeof(int);

			PropertyDescriptor pd = ClassDesc.Properties[prop.Index];
			PropertyType propType = pd.PropertyType;

			TTTrace.Write((byte)propType);

			if (propType < PropertyType.String)
			{
				int size = PropertyTypesHelper.GetItemSize(pd.PropertyType);
				reader.ReadSimpleValue(buffer, size);
				buffer += size;
			}
			else if (propType == PropertyType.String)
			{
				string s = reader.ReadString(out bool isDefined);
				Checker.AssertTrue(isDefined);    // Only alignment skips string/blob updates
				*((ulong*)buffer) = stringStorage.AddString(s);
				stringStorage.SetStringVersion(*((ulong*)buffer), ph->version);
				buffer += sizeof(ulong);
			}
			else
			{
				*((ulong*)buffer) = ReadBlob(reader, propType, out bool isDefined);
				Checker.AssertTrue(isDefined);    // Only alignment skips string/blob updates
				blobStorage.SetVersion(*((ulong*)buffer), ph->version);
				buffer += sizeof(ulong);
			}
		}

		ph->propCount = c;
	}

	private void RestorePendingChange(PendingRestoreObjectHeader* ph, ulong* prevObjPtr, bool hasMore)
	{
		ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(*prevObjPtr);
		TTTrace.Write(TraceId, ClassDesc.Id, obj->id, hasMore, ph->id, ph->version,
			ph->prevVersion, ph->isDelete, ph->isLastInTransaction);

		obj->nextVersionHandle = hasMore ? PendingRestoreObjectHeader.PendingRestore : 0;
		obj->version = ph->version;
		obj->IsDeleted = ph->isDelete;

		// If this is not the last modification of this object in the transaction we have to mark it that way
		// so that concurrent modifications from newer transaction do not modify it until all the modifications
		// from this transaction have finished.
		if (!ph->isLastInTransaction)
			obj->version |= OperationHeader.NotLastInTranFlag;

		if (ph->isDelete)
		{
			FreeStringsAndBlobs(obj);
			ulong handle = *prevObjPtr;
			*prevObjPtr = obj->nextCollisionHandle;
			ObjectStorage.MarkBufferNotUsed(handle);
			storage.Free(handle);
			resizeCounter.Dec();
			return;
		}

		byte* src = (byte*)ph + PendingRestoreObjectHeader.Size;
		byte* dst = ClassObject.ToDataPointer(obj);
		for (int i = 0; i < ph->propCount; i++)
		{
			int index = *(int*)src;
			src += sizeof(int);

			PropertyDescriptor pd = ClassDesc.Properties[index];
			int offset = ClassDesc.PropertyByteOffsets[index];
			PropertyType propType = pd.PropertyType;

			TTTrace.Write((byte)propType);

			if (propType < PropertyType.String)
			{
				int size = PropertyTypesHelper.GetItemSize(pd.PropertyType);
				CopySimpleValue(src, dst + offset, size);
				src += size;
			}
			else if (propType == PropertyType.String)
			{
				stringStorage.DecRefCount(*(ulong*)(dst + offset));
				CopySimpleValue(src, dst + offset, sizeof(ulong));
				src += sizeof(ulong);
			}
			else
			{
				blobStorage.DecRefCount(*(ulong*)(dst + offset));
				CopySimpleValue(src, dst + offset, sizeof(ulong));
				src += sizeof(ulong);
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail UpdateObjectFromChangeset(TransactionContext tc, ChangesetReader reader, ClassObject* prev, ClassObject* obj)
	{
		Utils.CopyMemory((byte*)prev + ClassObject.UserPropertiesOffset,
			(byte*)obj + ClassObject.UserPropertiesOffset, objectSize - ClassObject.UserPropertiesOffset);

		DatabaseErrorDetail err = PopulateObjectFromChangeset(tc, reader, obj, false, true, true);

		if (hasStringsOrBlobs)
			IncReusedRefCounts(tc, prev);

		return err;
	}

	internal DatabaseErrorDetail PopulateObjectFromChangeset(TransactionContext tc,
		ChangesetReader reader, ClassObject* obj, bool isMerge, bool isUpdate, bool collectUntrackedRefs)
	{
		TTTrace.Write(TraceId, obj->id, isMerge, isUpdate);

		ChangesetBlock block = tc.ChangesetBlock;
		int propCount = block.PropertyCount;
		byte* data = ClassObject.ToDataPointer(obj);

		DatabaseErrorDetail err = null;
		for (int i = 1; i < propCount; i++)
		{
			ChangesetBlockProperty prop = block.GetProperty(i);
			if (prop.Index == -1)
			{
				reader.SkipValue(prop.Type);
				continue;
			}

			PropertyDescriptor propDesc = ClassDesc.Properties[prop.Index];
			int offset = ClassDesc.PropertyByteOffsets[prop.Index];
			PropertyType type = propDesc.PropertyType;
			PropertyKind kind = propDesc.Kind;

			TTTrace.Write((byte)type, propDesc.Id);

			DatabaseErrorDetail tempErr = null;
			if (type < PropertyType.String)
			{
				if (kind != PropertyKind.Reference)
				{
					int size = PropertyTypesHelper.GetItemSize(propDesc.PropertyType);
					reader.ReadSimpleValue(data + offset, (int)size);
				}
				else
				{
					long prevRef = *((long*)(data + offset));
					reader.ReadSimpleValue(data + offset, sizeof(long));

					ReferencePropertyDescriptor rd = (ReferencePropertyDescriptor)propDesc;
					if (isUpdate && prevRef != 0 && rd.TrackInverseReferences)
						tc.AddInverseReferenceChange(classIndex, obj->id, prevRef, rd.Id, (byte)InvRefChangeType.Delete);

					tempErr = CreateSingleRefInvChanges(tc, obj->id, data, rd, offset, InvRefChangeType.Insert, collectUntrackedRefs);
				}
			}
			else if (type == PropertyType.String)
			{
				string s = reader.ReadString(out bool isDefined);
				if (isDefined)
				{
					if (isMerge)
						stringStorage.DecRefCount(*((ulong*)(data + offset)));

					*((ulong*)(data + offset)) = stringStorage.AddString(s);
					stringStorage.SetStringVersion(*((ulong*)(data + offset)), obj->version);
				}
			}
			else
			{
				ulong prevBlob = *((ulong*)(data + offset));
				ulong blob = ReadBlob(reader, type, out bool isDefined);
				TTTrace.Write(blob);

				if (isDefined)
				{
					blobStorage.SetVersion(blob, obj->version);

					if (isUpdate && kind == PropertyKind.Reference)
						CreateRefArrayInvDelete(tc, obj->id, prevBlob, (ReferencePropertyDescriptor)propDesc);

					if (isMerge)
						blobStorage.DecRefCount(*((ulong*)(data + offset)));

					*((ulong*)(data + offset)) = blob;

					if (kind == PropertyKind.Reference)
					{
						tempErr = CreateRefArrayInvChanges(tc, obj->id, data,
							(ReferencePropertyDescriptor)propDesc, offset, InvRefChangeType.Insert, collectUntrackedRefs);
					}
				}
			}

			if (tempErr != null)
				err = tempErr;
		}

		return err;
	}

	internal DatabaseErrorDetail RestoreObjectFromChangeset(ChangesetBlock block, ChangesetReader reader, ClassObject* obj)
	{
		TTTrace.Write(TraceId, obj->id);

		byte* buffer = ClassObject.ToDataPointer(obj);

		DatabaseErrorDetail err = null;
		int propertyCount = block.PropertyCount;

		for (int i = 1; i < propertyCount; i++) // First property is always id and has been written
		{
			ChangesetBlockProperty prop = block.GetProperty(i);
			if (prop.Index == -1)
			{
				reader.SkipValue(prop.Type);
				continue;
			}

			PropertyDescriptor pd = ClassDesc.Properties[prop.Index];
			int byteOffset = ClassDesc.PropertyByteOffsets[prop.Index];
			PropertyType propType = pd.PropertyType;

			TTTrace.Write((byte)propType, pd.Id);

			DatabaseErrorDetail tempErr = null;
			if (propType < PropertyType.String)
			{
				int size = PropertyTypesHelper.GetItemSize(pd.PropertyType);
				reader.ReadSimpleValue(buffer + byteOffset, (int)size);
			}
			else if (propType == PropertyType.String)
			{
				string s = reader.ReadString(out bool isDefined);
				if (isDefined)
				{
					stringStorage.DecRefCount(*((ulong*)(buffer + byteOffset)));
					ulong handle = stringStorage.AddString(s);
					stringStorage.SetStringVersion(handle, obj->version);
					*((ulong*)(buffer + byteOffset)) = handle;
				}
			}
			else
			{
				ulong handle = ReadBlob(reader, propType, out bool isDefined);
				if (isDefined)
				{
					blobStorage.SetVersion(handle, obj->version);
					blobStorage.DecRefCount(*((ulong*)(buffer + byteOffset)));
					*((ulong*)(buffer + byteOffset)) = handle;
				}
			}

			if (tempErr != null)
				err = tempErr;
		}

		return err;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void RemoveExistingReferences(TransactionContext tc, ClassObject* obj)
	{
		byte* data = ClassObject.ToDataPointer(obj);
		int propCount = ClassDesc.RefeferencePropertyIndexes.Length;
		ReadOnlyArray<int> offsets = ClassDesc.PropertyByteOffsets;
		for (int i = 0; i < propCount; i++)
		{
			int index = ClassDesc.RefeferencePropertyIndexes[i];
			ReferencePropertyDescriptor rd = (ReferencePropertyDescriptor)ClassDesc.Properties[index];
			if (!rd.TrackInverseReferences)
				continue;

			long value = *((long*)(data + offsets[index]));
			if (value == 0)
				continue;

			if (rd.PropertyType == PropertyType.Long)
			{
				tc.AddInverseReferenceChange(classIndex, obj->id, value, rd.Id, (byte)InvRefChangeType.Delete);
			}
			else
			{
				byte* blob = BlobStorage.RetrieveBlob((ulong)value);
				int refCount = *((int*)blob);
				long* refs = (long*)(blob + 4);
				for (int j = 0; j < refCount; j++)
				{
					if (refs[j] != 0)
						tc.AddInverseReferenceChange(classIndex, obj->id, refs[j], rd.Id, (byte)InvRefChangeType.Delete);
				}
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void CreateGroupingInvRefChanges(TransactionContext tc, ClassObject* obj, InvRefChangeType opType)
	{
		CreateGroupingInvRefChanges(tc, obj, opType, ClassDesc.RefeferencePropertyIndexes);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void CreateGroupingInvRefChanges(TransactionContext tc, ClassObject* obj,
		InvRefChangeType opType, ReadOnlyArray<int> refPropIndexes, bool[] trackingOverride = null)
	{
		byte* buffer = ClassObject.ToDataPointer(obj);

		int propCount = refPropIndexes.Length;
		for (int i = 0; i < propCount; i++)
		{
			int index = refPropIndexes[i];
			ReferencePropertyDescriptor rp = (ReferencePropertyDescriptor)ClassDesc.Properties[index];
			PropertyType propType = rp.PropertyType;

			bool isTracked = trackingOverride != null ? trackingOverride[i] : rp.TrackInverseReferences;

			if (propType == PropertyType.Long)
			{
				CreateGroupingSingleRefInvChanges(tc, obj->id, buffer, rp.Id, isTracked, ClassDesc.PropertyByteOffsets[index], opType);
			}
			else if (propType == PropertyType.LongArray)
			{
				CreateGroupingRefArrayInvChanges(tc, obj->id, buffer, rp.Id, isTracked, ClassDesc.PropertyByteOffsets[index], opType);
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail CreateSingleRefInvChanges(TransactionContext tc, long id, byte* buffer,
		ReferencePropertyDescriptor refDesc, int byteOffset, InvRefChangeType opType, bool collectUntrackedRefs)
	{
		long refVal = *((long*)(buffer + byteOffset));
		if (refVal != 0)
		{
			if (refDesc.TrackInverseReferences || collectUntrackedRefs)
				tc.AddInverseReferenceChange(classIndex, id, refVal, refDesc.Id, (byte)opType);
		}
		else if (refDesc.Multiplicity == Multiplicity.One)
		{
			return DatabaseErrorDetail.CreateNullReferenceNotAllowed(id, ClassDesc.FullName, refDesc.Name);
		}

		return null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void CreateGroupingSingleRefInvChanges(TransactionContext tc, long id, byte* buffer, int propId,
		bool trackInverse, int byteOffset, InvRefChangeType opType)
	{
		if (!trackInverse)
			return;

		long refVal = *((long*)(buffer + byteOffset));
		if (refVal != 0)
			tc.AddGroupingInvRefChange(classIndex, id, refVal, propId, (byte)opType);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void CreateRefArrayInvDelete(TransactionContext tc, long id, ulong handle, ReferencePropertyDescriptor refDesc)
	{
		if (handle == 0 || !refDesc.TrackInverseReferences)
			return;

		byte* blob = BlobStorage.RetrieveBlob(handle);
		int refCount = *((int*)blob);
		long* refs = (long*)(blob + 4);
		for (int j = 0; j < refCount; j++)
		{
			tc.AddInverseReferenceChange(classIndex, id, refs[j], refDesc.Id, (byte)InvRefChangeType.Delete);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail CreateRefArrayInvChanges(TransactionContext tc, long id, byte* buffer,
		ReferencePropertyDescriptor refDesc, int byteOffset, InvRefChangeType opType, bool collectUntrackedRefs)
	{
		ulong handle = *((ulong*)(buffer + byteOffset));
		if (handle == 0)
			return null;

		byte* blob = BlobStorage.RetrieveBlob(handle);
		int refCount = *((int*)blob);
		long* refs = (long*)(blob + 4);
		for (int j = 0; j < refCount; j++)
		{
			if (refs[j] == 0)
				return DatabaseErrorDetail.CreateNullReferenceNotAllowed(id, ClassDesc.FullName, refDesc.Name);

			if (refDesc.TrackInverseReferences || collectUntrackedRefs)
				tc.AddInverseReferenceChange(classIndex, id, refs[j], refDesc.Id, (byte)opType);
		}

		return null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void CreateGroupingRefArrayInvChanges(TransactionContext tc, long id, byte* buffer, int propId,
		bool trackInverse, int byteOffset, InvRefChangeType opType)
	{
		if (!trackInverse)
			return;

		ulong handle = *((ulong*)(buffer + byteOffset));
		byte* blob = BlobStorage.RetrieveBlob(handle);
		if (blob != null)
		{
			int refCount = *((int*)blob);
			long* refs = (long*)(blob + 4);
			for (int j = 0; j < refCount; j++)
			{
				tc.AddGroupingInvRefChange(classIndex, id, refs[j], propId, (byte)opType);
			}
		}
	}

	private void WriteStringsAndBlobs(SegmentBinaryWriter writer, ClassObject* obj)
	{
		if (ClassDesc.FirstStringBlobIndex == -1)
			return;

		TTTrace.Write(TraceId, ClassDesc.Id, obj->id, obj->version);

		byte* buffer = ClassObject.ToDataPointer(obj);

		int count = ClassDesc.Properties.Length;
		for (int i = ClassDesc.FirstStringBlobIndex; i < count; i++)
		{
			PropertyDescriptor propDesc = ClassDesc.Properties[i];
			if (propDesc.PropertyType == PropertyType.String)
			{
				int offset = propertyOffsets[i];
				ulong* up = (ulong*)(buffer + offset);
				string s = stringStorage.GetString(*up);
				writer.Write((long)stringStorage.GetStringVersion(*up));
				writer.Write(s);
			}
			else
			{
				int offset = propertyOffsets[i];
				ulong* up = (ulong*)(buffer + offset);
				WriteBlob(writer, propDesc.PropertyType, *up);
			}
		}
	}

	private void ReadStringsAndBlobs(SegmentBinaryReader reader, ClassObject* obj)
	{
		if (ClassDesc.FirstStringBlobIndex == -1)
			return;

		TTTrace.Write(TraceId, ClassDesc.Id, obj->id, obj->version);

		byte* buffer = ClassObject.ToDataPointer(obj);

		int count = ClassDesc.Properties.Length;
		for (int i = ClassDesc.FirstStringBlobIndex; i < count; i++)
		{
			PropertyDescriptor propDesc = ClassDesc.Properties[i];
			if (propDesc.PropertyType == PropertyType.String)
			{
				int offset = propertyOffsets[i];
				ulong* up = (ulong*)(buffer + offset);
				*up = ReadSnapshotString(reader);
			}
			else
			{
				int offset = propertyOffsets[i];
				ulong* up = (ulong*)(buffer + offset);
				*up = ReadSnapshotBlob(reader, propDesc.PropertyType);
			}
		}
	}

	private void WriteBlob(SegmentBinaryWriter writer, PropertyType propertyType, ulong handle)
	{
		writer.Write((long)blobStorage.GetVersion(handle));

		if (handle == 0)
		{
			writer.Write((byte)0);
		}
		else
		{
			writer.Write((byte)1);
			byte* blob = blobStorage.RetrieveBlob(handle);

			int size = propertyType == PropertyType.StringArray ? PropertyTypesHelper.DBUnpackStringArraySize(blob) :
				((int*)blob)[0] * PropertyTypesHelper.GetElementSize(propertyType) + sizeof(int);
			writer.Write(size);
			writer.Write(blob, size);
		}
	}

	private void SkipSnapshotBlob(SegmentBinaryReader reader)
	{
		reader.ReadLong();  // Version

		if (reader.ReadByte() == 0)
			return;

		int size = reader.ReadInt();
		reader.SkipBytes(size);
	}

	private void SkipSnapshotString(SegmentBinaryReader reader)
	{
		reader.ReadLong();  // Version
		reader.ReadString();
	}

	private ulong ReadSnapshotBlob(SegmentBinaryReader reader, PropertyType propertyType)
	{
		ulong version = (ulong)reader.ReadLong();

		if (reader.ReadByte() == 0)
			return 0;

		int size = reader.ReadInt();
		ulong handle = blobStorage.AllocBlob(false, size, out byte* buffer);
		blobStorage.SetVersion(handle, version);
		reader.ReadBytes(buffer, size);
		return handle;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ulong ReadSnapshotString(SegmentBinaryReader reader)
	{
		ulong version = (ulong)reader.ReadLong();
		string s = reader.ReadString();
		ulong handle = stringStorage.AddString(s);
		stringStorage.SetStringVersion(handle, version);
		return handle;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void IncReusedRefCounts(TransactionContext tc, ClassObject* obj)
	{
		byte* data = ClassObject.ToDataPointer(obj);

		ChangesetBlock block = tc.ChangesetBlock;
		ClassDescriptor classDesc = block.ClassDescriptor;

		TTTrace.Write(classDesc.StringPropertyIndexes.Length, classDesc.BlobPropertyIndexes.Length);

		ReadOnlyArray<int> props = classDesc.StringPropertyIndexes;
		int count = props.Length;
		for (int i = 0; i < count; i++)
		{
			int index = props[i];
			TTTrace.Write(index, block.StringBlobNotAffected(index));
			if (block.StringBlobNotAffected(index))
			{
				int offset = propertyOffsets[index];
				ulong* up = (ulong*)(data + offset);
				stringStorage.IncRefCount(*up);
			}
		}

		props = classDesc.BlobPropertyIndexes;
		count = props.Length;
		for (int i = 0; i < count; i++)
		{
			int index = props[i];
			TTTrace.Write(index, block.StringBlobNotAffected(index));
			if (block.StringBlobNotAffected(index))
			{
				int offset = propertyOffsets[index];
				ulong* up = (ulong*)(data + offset);
				blobStorage.IncRefCount(*up);
			}
		}
	}

	private void FreeStringsAndBlobs(ClassObject* obj)
	{
		TTTrace.Write(TraceId, ClassDesc.Id);

		if (!hasStringsOrBlobs)
			return;

		TTTrace.Write(ClassDesc.StringPropertyIndexes.Length, ClassDesc.BlobPropertyIndexes.Length);

		byte* data = ClassObject.ToDataPointer(obj);

		ReadOnlyArray<int> stringProps = ClassDesc.StringPropertyIndexes;
		int count = stringProps.Length;
		for (int i = 0; i < count; i++)
		{
			ulong* p = (ulong*)(data + propertyOffsets[stringProps[i]]);
			TTTrace.Write(*p, stringProps[i]);
			stringStorage.DecRefCount(*p);
		}

		ReadOnlyArray<int> blobProps = ClassDesc.BlobPropertyIndexes;
		count = blobProps.Length;
		for (int i = 0; i < count; i++)
		{
			ulong* p = (ulong*)(data + propertyOffsets[blobProps[i]]);
			TTTrace.Write(*p, blobProps[i]);
			blobStorage.DecRefCount(*p);
		}
	}

	public void CreateObjectSnapshot(SegmentBinaryWriter writer, ClassObject* obj)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, obj->id, obj->IsDeleted, obj->version);

		if (hasStringsOrBlobs)
		{
			writer.Write(ClassObject.ToDataPointer(obj), ClassDesc.FirstStringBlobOffset);
			WriteStringsAndBlobs(writer, obj);
		}
		else
		{
			writer.Write(ClassObject.ToDataPointer(obj), objectSize - ClassObject.DataOffset);
		}
	}

	private ClassObject* RestoreObjectSnapshot(SegmentBinaryReader reader, int[] propertyIndexes,
		PropertyType[] propertyTypes, bool alatered, out ulong objHandle)
	{
		objHandle = storage.Allocate();
		ClassObject* dst = (ClassObject*)ObjectStorage.GetBuffer(objHandle);

		// Since this is restoration of the database, no scans are running so we can
		// just mark the buffer as used here (without it being initialized with data)
		ObjectStorage.MarkBufferAsUsed(objHandle);

		ReaderInfo.Clear(&dst->readerInfo);

		if (!alatered)
		{
			if (hasStringsOrBlobs)
			{
				reader.ReadBytes(ClassObject.ToDataPointer(dst), ClassDesc.FirstStringBlobOffset);
				ReadStringsAndBlobs(reader, dst);
			}
			else
			{
				reader.ReadBytes(ClassObject.ToDataPointer(dst), objectSize - ClassObject.DataOffset);
			}

			return dst;
		}

		dst->version = (ulong)reader.ReadLong();
		dst->id = reader.ReadLong();

		Utils.CopyMemory((byte*)defaultValues + ClassObject.UserPropertiesOffset,
			(byte*)dst + ClassObject.UserPropertiesOffset, objectSize - ClassObject.UserPropertiesOffset);

		TTTrace.Write(TraceId, ClassDesc.Id, dst->id, dst->IsDeleted, dst->version);

		byte* dstBuff = ClassObject.ToDataPointer(dst);
		for (int i = 0; i < propertyIndexes.Length; i++)
		{
			if (propertyIndexes[i] != -1)
			{
				PropertyDescriptor pd = ClassDesc.Properties[propertyIndexes[i]];
				byte* dstProp = dstBuff + ClassDesc.PropertyByteOffsets[propertyIndexes[i]];

				PropertyType propType = pd.PropertyType;
				if (propType == PropertyType.String)
				{
					*((ulong*)dstProp) = ReadSnapshotString(reader);
				}
				else if (propType >= PropertyType.ByteArray)
				{
					*((ulong*)dstProp) = ReadSnapshotBlob(reader, pd.PropertyType);
				}
				else
				{
					int valSize = (int)PropertyTypesHelper.GetItemSize(propertyTypes[i]);
					ReadSimpleValue(reader, dstProp, valSize);
				}
			}
			else
			{
				PropertyType propType = propertyTypes[i];
				if (propType == PropertyType.String)
				{
					SkipSnapshotString(reader);
				}
				else if (propType >= PropertyType.ByteArray)
				{
					SkipSnapshotBlob(reader);
				}
				else
				{
					int valSize = (int)PropertyTypesHelper.GetItemSize(propertyTypes[i]);
					reader.SkipBytes(valSize);
				}
			}
		}

		return dst;
	}

	private void AddRestoredObject(ClassObject* obj, ulong handle)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, obj->id, obj->version);

		obj->nextCollisionHandle = 0;
		obj->nextVersionHandle = 0;

		Bucket* bucket = buckets + CalculateBucket(obj->id);
		ulong* handlePointer = Bucket.LockAccess(bucket);
		obj->nextCollisionHandle = bucket->Handle;
		*handlePointer = handle;
		Bucket.UnlockAccess(bucket);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void ReadSimpleValue(SegmentBinaryReader reader, byte* dst, int size)
	{
		if (size == 8)
		{
			*(long*)dst = reader.ReadLong();
		}
		else if (size == 4)
		{
			*(int*)dst = reader.ReadInt();
		}
		else if (size == 2)
		{
			*(short*)dst = reader.ReadShort();
		}
		else
		{
			*(byte*)dst = reader.ReadByte();
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void CopySimpleValue(byte* src, byte* dst, int size)
	{
		if (size == 8)
		{
			*(long*)dst = *(long*)src;
		}
		else if (size == 4)
		{
			*(int*)dst = *(int*)src;
		}
		else if (size == 2)
		{
			*(short*)dst = *(short*)src;
		}
		else
		{
			*(byte*)dst = *(byte*)src;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ClassObject* GetObjectInternalRead(Transaction tran, long id, out ulong objectHandle)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, id);

		Bucket* bucket = buckets + CalculateBucket(id);
		ulong* handlePointer = Bucket.LockAccess(bucket);

		objectHandle = 0;
		ClassObject* obj = FindObject(handlePointer, id, out ulong* pObjPointer);
		if (obj != null)
		{
			TTTrace.Write(obj->IsDeleted, obj->version);

			objectHandle = *pObjPointer;
			if (tran != null)
				obj = FindVersion(obj, tran.ReadVersion, ref objectHandle, tran.Id);

			if (obj != null && obj->IsDeleted)
				obj = null;
		}

		Bucket.UnlockAccess(bucket);

		return obj;
	}

	private ClassObject* GetObjectInternalReadLock(Transaction tran, long id, out DatabaseErrorDetail err)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, id);

		err = null;
		Bucket* bucket = buckets + CalculateBucket(id);
		ulong* handlePointer = Bucket.LockAccess(bucket);

		ClassObject* obj = FindObject(handlePointer, id, out ulong* pObjPointer);

		if (obj != null)
			obj = GetObjectWithReadLock(tran, obj, id, *pObjPointer, out err);

		if (obj != null)
		{
			TTTrace.Write(obj->id, obj->IsDeleted, obj->version);
			if (obj->IsDeleted)
				obj = null;
		}

		Bucket.UnlockAccess(bucket);

		return obj;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ClassObject* GetObjectWithReadLock(Transaction tran, ClassObject* obj, long id, ulong objHandle, out DatabaseErrorDetail err)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id, obj->id, obj->IsDeleted, obj->version, id, objHandle);

		err = null;

		TTTrace.Write(obj->IsDeleted, obj->version);
		if (Database.IsUncommited(obj->version))
		{
			if (obj->version != tran.Id)
			{
				err = DatabaseErrorDetail.CreateConflict(obj->id, ClassDesc.FullName);
				return null;
			}

			return obj;
		}

		if (obj->version > tran.ReadVersion)
		{
			err = DatabaseErrorDetail.CreateConflict(obj->id, ClassDesc.FullName);
			return null;
		}

		if (obj->IsDeleted)
			return obj;

		if (!ReaderInfo.TryTakeObjectStandardLock(tran, obj->id, classIndex, &obj->readerInfo))
		{
			err = DatabaseErrorDetail.CreateObjectLockContentionLimitExceeded(ClassDesc.FullName);
			return null;
		}

		return obj;
	}

	public void ResizeEmpty(long count)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, count, countLimit);

		if (count <= countLimit)
			return;

		AlignedAllocator.Free((IntPtr)buckets);

		capacity = Math.Max(minCapacity, capacity);
		capacity = capacity = HashUtils.CalculatePow2Capacity(count, hashLoadFactor, out countLimit);
		capacityMask = (ulong)capacity - 1;

		buckets = (Bucket*)AlignedAllocator.Allocate(capacity * Marshal.SizeOf(typeof(Bucket)), false);
		for (long i = 0; i < capacity; i++)
		{
			buckets[i].Init();
		}

		resizeCounter.Resized(countLimit);
	}

	public void Resize(long count)
	{
		resizeCounter.EnterWriteLock();

		if (count <= countLimit)
		{
			resizeCounter.ExitWriteLock();
			return;
		}

		ResizeInternal(count);
	}

	private void Resize()
	{
		TTTrace.Write(TraceId, ClassDesc.Id, resizeCounter.Count, countLimit);

		resizeCounter.EnterWriteLock();

		long idCount = resizeCounter.Count;
		if (idCount <= countLimit)
		{
			resizeCounter.ExitWriteLock();
			return;
		}

		idCount = (long)(idCount * Engine.Settings.CollectionGrowthFactor);
		ResizeInternal(idCount);
	}

	private void ResizeInternal(long count)
	{
		try
		{
			count = Math.Max(minCapacity, count);
			long newCapacity = HashUtils.CalculatePow2Capacity(count, hashLoadFactor, out long newCountLimit);
			ulong newCapacityMask = (ulong)newCapacity - 1;

			Bucket* newBuckets = (Bucket*)AlignedAllocator.Allocate(newCapacity * Marshal.SizeOf(typeof(Bucket)), false);
			for (long i = 0; i < newCapacity; i++)
			{
				newBuckets[i].Init();
			}

			Utils.Range[] ranges = Utils.SplitRange(capacity, Engine.Settings.ResizeSplitSize, ProcessorNumber.CoreCount);

			int workerCount = Math.Min(ProcessorNumber.CoreCount, ranges.Length);
			string workerName = string.Format("{0}: vlx-ClassResizeWorker", Engine.Trace.Name);
			JobWorkers<Utils.Range>.Execute(workerName, workerCount, range => RehashRange(newBuckets, newCapacityMask, range), ranges);

			Engine.Trace.Debug("Class {0} resized from {1} to {2}, classId={3}.",
				ClassDesc.FullName, this.capacity, newCapacity, ClassDesc.Id);

			this.capacity = newCapacity;
			this.countLimit = newCountLimit;
			this.capacityMask = newCapacityMask;

			TTTrace.Write(capacity, countLimit);

			AlignedAllocator.Free((IntPtr)buckets);
			buckets = newBuckets;
		}
		finally
		{
			resizeCounter.Resized(countLimit);
			resizeCounter.ExitWriteLock();
		}
	}

	private void RehashRange(Bucket* newBuckets, ulong newCapacityMask, Utils.Range range)
	{
		for (long i = range.Offset; i < range.Offset + range.Count; i++)
		{
			ulong handle = buckets[i].Handle;
			while (handle != 0)
			{
				ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(handle);
				ulong nextCollision = obj->nextCollisionHandle;

				long bucketIndex = CalculateBucket(obj->id, seed, newCapacityMask);
				Bucket* bucket = newBuckets + bucketIndex;
				ulong* handlePointer = Bucket.LockAccess(bucket);

				obj->nextCollisionHandle = newBuckets[bucketIndex].Handle;
				*handlePointer = handle;

				Bucket.UnlockAccess(bucket);

				handle = nextCollision;
			}
		}
	}

	private static void PreparePropertyData(ClassDescriptor classDesc, out IntPtr defaultValues, out int* propertyOffsets, out int objectSize)
	{
		propertyOffsets = (int*)AlignedAllocator.Allocate(classDesc.Properties.Length * 4, false);

		int offset = 0;
		for (int i = 0; i < classDesc.Properties.Length; i++)
		{
			PropertyDescriptor pd = classDesc.Properties[i];
			propertyOffsets[i] = classDesc.PropertyByteOffsets[i];
			offset += PropertyTypesHelper.GetItemSize(pd.PropertyType);
		}

		objectSize = offset + ClassObject.DataOffset;

		defaultValues = AlignedAllocator.Allocate(objectSize, true);
		offset = ClassObject.DataOffset;
		for (int i = 0; i < classDesc.Properties.Length; i++)
		{
			PropertyDescriptor pd = classDesc.Properties[i];
			if (pd.PropertyType >= PropertyType.String)
			{
				((ulong*)IntPtr.Add(defaultValues, (int)offset))[0] = 0;
			}
			else
			{
				PropertyTypesHelper.WriteDefaultPropertyValue(pd, IntPtr.Add(defaultValues, (int)offset));
			}

			offset += PropertyTypesHelper.GetItemSize(pd.PropertyType);
		}

		((long*)(defaultValues + (int)ClassObject.DataOffset))[0] = 0;  // Version
		((long*)(defaultValues + (int)ClassObject.DataOffset))[1] = 0;  // Id
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private long CalculateBucket(long id)
	{
		return (long)(HashUtils.GetHash64((ulong)id, seed) & capacityMask);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static long CalculateBucket(long id, ulong seed, ulong capacityMask)
	{
		return (long)(HashUtils.GetHash64((ulong)id, seed) & capacityMask);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private DatabaseErrorDetail WriteClassLocker(Transaction tran, TransactionContext tc)
	{
		TTTrace.Write(TraceId, ClassDesc.Id, tran.Id);

		if (tran.IsAlignment)
			return null;

		if (ClassIndexMultiSet.Contains(tc.WrittenClasses, classIndex))
			return null;

		ClassLocker locker = tran.Database.GetClassLocker(classIndex);
		if (!locker.TryAddWriter(tran))
			return DatabaseErrorDetail.CreateConflict(0, ClassDesc.FullName);

		return null;
	}

	private void FreeAllStringsAndBlobs(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(TraceId, ClassDesc.Id);

		if (!hasStringsOrBlobs)
			return;

		ClassScan[] scans = GetClassScans(null, false, out _);
		workers.SetAction(p =>
		{
			ClassScan scan = (ClassScan)p.ReferenceParam;
			using (scan)
			{
				foreach (ObjectReader r in scan)
				{
					ClassObject* obj = r.ClassObject;
					while (obj != null)
					{
						FreeStringsAndBlobs(obj);

						// It is possible that we are rolling back an alignment so some objects might still be marked as aligned
						if (obj->nextVersionHandle == ClassObject.AlignedFlag)
						{
							obj = null;
						}
						else
						{
							obj = (ClassObject*)ObjectStorage.GetBuffer(obj->nextVersionHandle);
						}
					}
				}
			}
		});

		workers.EnqueueWork(scans.Select(x => new CommonWorkerParam() { ReferenceParam = x }));
		workers.Drain();
	}

	public override string ToString()
	{
		return ClassDesc.FullName;
	}

	public override void Drop()
	{
		for (long i = 0; i < capacity; i++)
		{
			Bucket* bucket = buckets + i;
			if (bucket->Handle != 0)
			{
				ulong* pHandle = Bucket.LockAccess(bucket);
				ulong handle = bucket->Handle;

				while (handle != 0)
				{
					ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(handle);
					Checker.AssertTrue(obj->nextVersionHandle == 0);
					ulong temp = obj->nextCollisionHandle;
					FreeStringsAndBlobs(obj);
					ObjectStorage.MarkBufferNotUsed(handle);
					storage.Free(handle);
					handle = temp;
				}

				*pHandle = 0;
				Bucket.UnlockAccess(bucket);
			}
		}

		resizeCounter = new ParallelResizeCounter(countLimit);
	}

	public override void Dispose(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(TraceId, ClassDesc.Id);

		FreeAllStringsAndBlobs(workers);

		AlignedAllocator.Free((IntPtr)buckets);
		buckets = null;

		AlignedAllocator.Free((IntPtr)propertyOffsets);
		propertyOffsets = null;

		AlignedAllocator.Free(defaultValues);
		defaultValues = IntPtr.Zero;

		storage.Dispose();

		resizeCounter.Dispose();
	}

	private sealed class ModelUpdateData
	{
		public ObjectStorage Storage { get; set; }
		public int ObjectSize { get; set; }
		public int* PropertyOffsets { get; set; }
		public IntPtr DefaultValues { get; set; }
	}
}

internal struct IndexComparerPair
{
	public Index Index { get; set; }
	public KeyComparer Comparer { get; set; }

	public IndexComparerPair(Index index, KeyComparer comparer)
	{
		this.Index = index;
		this.Comparer = comparer;
	}
}

internal enum UpdateObjectResult
{
	Merged = 1,
	InsertedVersion = 2,
	InsertedObject = 3
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal unsafe struct PendingRestoreObjectHeader
{
	public const int Size = 46;
	public const ulong PendingRestore = 0xffffffffffffffff;

	public ulong version;
	public long id;

	public ulong prevVersion;
	public ulong nextPendingHandle;
	public ulong nextPendingInSameTranHandle;

	public int propCount;

	public bool isDelete;
	public bool isLastInTransaction;

	public bool IsFirstInTransaction => prevVersion != 0;
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = 1)]
internal unsafe struct ClassObject
{
	public const ulong AlignedFlag = 0xffffffffffffffff;

	public const int DataOffset = 24;
	public const int UserPropertiesOffset = DataOffset + sizeof(ulong) + sizeof(long);

	[FieldOffset(0)]
	public ulong nextCollisionHandle;

	[FieldOffset(8)]
	public ulong nextVersionHandle;

	// The following 2 fileds share the same space but are never used at the same time.
	// hasNextVersion_newerVersion is used once an object version is no longer the latest in which case it never gets read-locked.
	// It uses the highest bit to indicate whether the newer version is present. This is needed since this information is used by
	// class scan operation in a lock-free manner.
	//
	// It is important to mention that the highest bit (hasNextVersion) is only set when there is newer version set. This bit is never
	// set by the readerInfo field so can sfely be used as an indicator of newer version.
	// Also, IsDeleted (which uses the second bit) is never set if there is newer version.

	[FieldOffset(16)]
	public ulong hasNextVersion_deleted_newerVersion;

	[FieldOffset(16)]
	public ReaderInfo readerInfo;

	[FieldOffset(24)]
	public ulong version;

	[FieldOffset(32)]
	public long id;

	public bool IsDeleted
	{
		// newer version is not used and deleted is set
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		get => readerInfo.HighestTwoBits == 0x01;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		set
		{
			Checker.AssertFalse(this.IsDeleted && !value);
			Checker.AssertTrue((hasNextVersion_deleted_newerVersion & 0x8000000000000000) == 0);
			readerInfo.SecondHighestBit = (value ? 1 : 0);
		}
	}

	public ulong NewerVersion
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		get
		{
			if ((hasNextVersion_deleted_newerVersion & 0x8000000000000000) == 0)
				return 0;

			return hasNextVersion_deleted_newerVersion & 0x7fffffffffffffff;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		set
		{
			Checker.AssertTrue(readerInfo.HighestTwoBits != 0x01);
			Checker.AssertTrue(value != 0);
			hasNextVersion_deleted_newerVersion = value | 0x8000000000000000;
		}
	}

	public static byte* ToDataPointer(ClassObject* obj)
	{
		return obj == null ? null : (byte*)obj + DataOffset;
	}
}
