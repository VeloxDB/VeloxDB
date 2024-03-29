﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;
using System.Threading;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage.Persistence;
using VeloxDB.Storage.Replication;

namespace VeloxDB.Storage;

internal unsafe sealed class TransactionContext : IDisposable
{
	public const int TempInvRefSize = 1024 * 8;                 // 64KB

	const int classScanChunkSize = 32;

	const int inverseReferencesOperationCapacity = 1024 * 8;    // 192KB
	const int deletedObjectsCapacity = 1024 * 8;                // 128KB

	const int inverseReferenceReadLockCapacity = 1024 * 24;     // 384KB
	const int objectReadLockMapCapacity = 512;                  // Roughly 18KB

	const int defLockedClassesCapacity = 32;
	const int defWrittenClassesCapacity = 32;

	const int multiAffectedObjectsCapacity = 32;

	const int cascadeSetToNullAffectedCapacity = 32;

	StorageEngine engine;
	Database database;

	ulong tranId;

	ChangesetReader changesetReader;
	ChangesetBlock changesetBlock;

	// These fields need to be merged when merging transactions
	int totalMergedCount;
	long totalAffectedCount;
	ModifiedList affectedObjects;
	ModifiedList affectedInvRefs;
	ModifiedList objectReadLocks;
	NativeList invRefReadLocks;
	ModifiedList keyReadLocks;

	ClassIndexMultiSet* lockedClasses;
	ClassIndexMultiSet* writtenClasses;

	byte affectedLogGroups;
	Changeset changeset;
	////////////////////////////////////////////////////////////

	int* invRefGroupCounts;
	NativeList inverseRefChanges;

	ReferenceSorter invRefsSorter;
	ReferenceSorter propagatedInvRefsSorter;

	NativeList deleted;

	LongDictionary<LockType> objectReadLocksSet;
	FastHashSet<InverseReferenceKey> invRefReadLocksSet;

	long* origTempInvRefs;
	long* tempInvRefs;
	int* tempInvRefCounts;

	ObjectReader[] tempRecReaders;

	int poolIndex;
	ushort slot;

	WriteTransactionFlags writeFlags;

	AlignmentData alignment;
	bool userAssembliesModified;
	byte[] newModelDescBinary;
	byte[] newPersistenceDescBinary;

	uint localTerm;
	SimpleGuid globalTerm;
	ulong logSeqNum;

	IReplica originReplica;

	AutoResetEvent asyncCommitWaitEvent;
	DatabaseErrorDetail asyncError;

	AutoResetEvent commitWaitEvent;
	int asyncCommiterCount;
	bool asyncCommitResult;

	ulong standbyOrderNum;

	Transaction[] nextPersisted;

	bool isAlignmentMode;

	bool isAllocated;

	Action<long, OperationHeader> notLastInTransactionFlagger;
	LongDictionary<int> multiAffectedObjects;

	int cascadeSetToNullAffectedCount;
	long[] cascadeSetToNullAffected;

	public TransactionContext(StorageEngine engine, int physCorePool, ushort slot)
	{
		this.engine = engine;
		this.poolIndex = physCorePool;
		this.slot = slot;
	}

	public void Allocate()
	{
		if (isAllocated)
			return;

		isAllocated = true;

		affectedObjects = new ModifiedList(engine.MemoryManager);
		affectedInvRefs = new ModifiedList(engine.MemoryManager);

		invRefsSorter = new ReferenceSorter(InverseComparer.Instance);
		propagatedInvRefsSorter = new ReferenceSorter(PropagatedComparer.Instance);

		invRefGroupCounts = (int*)AlignedAllocator.Allocate(GroupingReferenceSorter.GroupCount * sizeof(int));
		inverseRefChanges = new NativeList(inverseReferencesOperationCapacity, InverseReferenceOperation.Size);

		deleted = new NativeList(deletedObjectsCapacity, DeletedObject.Size);

		objectReadLocksSet = new LongDictionary<LockType>(objectReadLockMapCapacity + 1);
		invRefReadLocksSet = new FastHashSet<InverseReferenceKey>(objectReadLockMapCapacity + 1);

		objectReadLocks = new ModifiedList(engine.MemoryManager);
		invRefReadLocks = new NativeList(inverseReferenceReadLockCapacity, InvRefReadLock.Size);
		keyReadLocks = new ModifiedList(engine.MemoryManager);

		lockedClasses = ClassIndexMultiSet.Create(defLockedClassesCapacity, engine.MemoryManager);
		writtenClasses = ClassIndexMultiSet.Create(defWrittenClassesCapacity, engine.MemoryManager);

		origTempInvRefs = tempInvRefs = (long*)AlignedAllocator.Allocate(TempInvRefSize * sizeof(long));
		tempInvRefCounts = (int*)AlignedAllocator.Allocate(ClassDescriptor.MaxInverseReferencesPerClass * sizeof(int), false);

		tempRecReaders = new ObjectReader[classScanChunkSize];

		multiAffectedObjects = new LongDictionary<int>(multiAffectedObjectsCapacity);
		notLastInTransactionFlagger = FlagNotLastInTransactionIfNeeded;

		commitWaitEvent = new AutoResetEvent(false);
		asyncCommitResult = true;

		cascadeSetToNullAffected = new long[cascadeSetToNullAffectedCapacity];
		cascadeSetToNullAffectedCount = 0;

		changesetReader = new ChangesetReader();
		changesetBlock = new ChangesetBlock();

		nextPersisted = new Transaction[PersistenceDescriptor.MaxLogGroups];

		ReplicationDescriptor replicationDesc = engine.ReplicationDesc;

		totalMergedCount = 1;
	}

	public Database Database => database;
	public ushort Slot => slot;
	public int PoolIndex => poolIndex;
	public NativeList Deleted => deleted;
	public ModifiedList AffectedObjects => affectedObjects;
	public ModifiedList AffectedInvRefs => affectedInvRefs;
	public NativeList InverseRefChanges => inverseRefChanges;
	public ModifiedList ObjectReadLocks => objectReadLocks;
	public NativeList InvRefReadLocks => invRefReadLocks;
	public ModifiedList KeyReadLocks => keyReadLocks;
	public LongDictionary<LockType> ObjectReadLocksSet => objectReadLocksSet;
	public FastHashSet<InverseReferenceKey> InvRefReadLocksSet => invRefReadLocksSet;
	public ClassIndexMultiSet* LockedClasses { get => lockedClasses; set => lockedClasses = value; }
	public ClassIndexMultiSet* WrittenClasses { get => writtenClasses; set => writtenClasses = value; }
	public long* TempInvRefs { get => tempInvRefs; set => tempInvRefs = value; }
	public int* TempInvRefCounts => tempInvRefCounts;
	public ObjectReader[] TempRecReaders => tempRecReaders;
	public Changeset Changeset => changeset;
	public bool IsTransactionEmpty => changeset == null;
	public byte AffectedLogGroups { get => affectedLogGroups; set => affectedLogGroups = value; }
	public WriteTransactionFlags WriteFlags { get => writeFlags; set => writeFlags = value; }
	public AlignmentData Alignment { get => alignment; set => alignment = value; }
	public uint LocalTerm { get => localTerm; set => localTerm = value; }
	public SimpleGuid GlobalTerm { get => globalTerm; set => globalTerm = value; }
	public ulong LogSeqNum { get => logSeqNum; set => logSeqNum = value; }
	public IReplica OriginReplica { get => originReplica; set => originReplica = value; }
	public AutoResetEvent CommitWaitEvent => commitWaitEvent;
	public AutoResetEvent AsyncCommitWaitEvent => asyncCommitWaitEvent;
	public bool AsyncCommitResult { get => asyncCommitResult; set => asyncCommitResult = value; }
	public ulong StandbyOrderNum { get => standbyOrderNum; set => standbyOrderNum = value; }
	public ChangesetReader ChangesetReader => changesetReader;
	public ChangesetBlock ChangesetBlock => changesetBlock;
	public DatabaseErrorDetail AsyncError { get => asyncError; set => asyncError = value; }
	public unsafe int* InvRefGroupCounts => invRefGroupCounts;
	public byte[] NewModelDescBinary { get => newModelDescBinary; set => newModelDescBinary = value; }
	public byte[] NewPersistenceDescBinary { get => newPersistenceDescBinary; set => newPersistenceDescBinary = value; }
	public bool UserAssembliesModified { get => userAssembliesModified; set => userAssembliesModified = value; }
	public long TotalAffectedCount => totalAffectedCount;
	public int TotalMergedCount => totalMergedCount;
	public Transaction[] NextPersisted => nextPersisted;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Init(Database database, ulong tranId)
	{
		TTTrace.Write(database.TraceId, tranId);
		Checker.AssertTrue(isAllocated);

		this.database = database;
		this.tranId = tranId;
		affectedObjects.Init();
		affectedInvRefs.Init();
		objectReadLocks.Init();
		keyReadLocks.Init();
	}

	public void AddSetToNullAffected(long id)
	{
		if (cascadeSetToNullAffectedCount == cascadeSetToNullAffected.Length)
			Array.Resize(ref cascadeSetToNullAffected, cascadeSetToNullAffected.Length * 2);

		cascadeSetToNullAffected[cascadeSetToNullAffectedCount++] = id;
	}

	public void ProcessSetToNullAffected<T>(Action<T, long> action, T arg)
	{
		for (int i = 0; i < cascadeSetToNullAffectedCount; i++)
		{
			action(arg, cascadeSetToNullAffected[i]);
		}

		cascadeSetToNullAffectedCount = 0;
	}

	public void SetAlignmentMode()
	{
		isAlignmentMode = true;
		inverseRefChanges.Resize(inverseReferencesOperationCapacity * 32);
	}

	public void ResetAlignmentMode()
	{
		isAlignmentMode = false;
		ClearInvRefChanges();
	}

	public void PrepareForAsyncCommit()
	{
		if (asyncCommitWaitEvent == null)
			asyncCommitWaitEvent = new AutoResetEvent(false);
	}

	public void AddChangeset(Changeset changeset)
	{
		TTTrace.Write(engine.TraceId, database.Id, this.changeset != null);
		changeset.TakeRef();

		if (this.changeset == null)
		{
			this.changeset = changeset;
		}
		else
		{
			Changeset last = this.changeset;
			while (last.Next != null)
			{
				last = last.Next;
			}

			last.Next = changeset;
		}
	}

	public void CollapseChangesets()
	{
		if (changeset == null)
			return;

		Changeset curr = changeset.Next;
		changeset.Next = null;
		while (curr != null)
		{
			changeset.Merge(curr);
			curr.ReleaseRef();
			curr = curr.Next;
		}

		for (int i = 0; i < changeset.LogChangesets.Length; i++)
		{
			var lch = changeset.LogChangesets[i];
			affectedLogGroups = (byte)(affectedLogGroups | (1 << lch.LogIndex));
		}
	}

	private void MergeChangeset(TransactionContext tc)
	{
		if (tc.changeset == null)
			return;

		for (int i = 0; i < tc.changeset.LogChangesets.Length; i++)
		{
			var lch = tc.changeset.LogChangesets[i];
			affectedLogGroups = (byte)(affectedLogGroups | (1 << lch.LogIndex));
		}

		if (changeset == null)
		{
			changeset = tc.changeset;
			tc.changeset = null;
		}
		else
		{
			changeset.Merge(tc.changeset);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void IncAsyncCommitterCount()
	{
		Interlocked.Increment(ref asyncCommiterCount);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int DecAsyncCommiterCount()
	{
		return Interlocked.Decrement(ref asyncCommiterCount);
	}

	public void ClearInvRefGroupCounts()
	{
		Utils.ZeroMemory((byte*)invRefGroupCounts, GroupingReferenceSorter.GroupCount * sizeof(int));
	}

	public void Merge(Transaction owner, Transaction tran)
	{
		TTTrace.Write(tran.Engine.TraceId, owner.Id, tran.Id);
		TransactionContext tc = tran.Context;

		Checker.AssertTrue(originReplica == null);
		Checker.AssertTrue(writeFlags == WriteTransactionFlags.None);

		Transaction lastInChain = tran;
		while (lastInChain.NextMerged != null)
			lastInChain = lastInChain.NextMerged;

		lastInChain.NextMerged = owner.NextMerged;
		owner.NextMerged = tran;
		totalMergedCount += tc.totalMergedCount;

		totalAffectedCount += tc.totalAffectedCount;
		affectedObjects.MergeChanges(tc.affectedObjects);
		affectedInvRefs.MergeChanges(tc.affectedInvRefs);
		objectReadLocks.MergeChanges(tc.objectReadLocks);
		keyReadLocks.MergeChanges(tc.keyReadLocks);
		invRefReadLocks.Append(tc.invRefReadLocks);
		tc.invRefReadLocks.Reset(inverseReferenceReadLockCapacity);

		ClassIndexMultiSet.Merge(engine.MemoryManager, ref lockedClasses, tc.lockedClasses);
		ClassIndexMultiSet.Clear(tc.lockedClasses);

		ClassIndexMultiSet.Merge(engine.MemoryManager, ref writtenClasses, tc.writtenClasses);
		ClassIndexMultiSet.Clear(tc.writtenClasses);

		MergeChangeset(tc);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void PrepareEmptyInvRefEntriesForGC()
	{
		NativeList l = InvRefReadLocks;
		long count = l.Count;
		InvRefReadLock* readLock = (InvRefReadLock*)l.Buffer;
		for (long i = 0; i < count; i++)
		{
			if (readLock->EligibleForGC)
				AddAffectedInvRef(readLock->classIndex, readLock->id, readLock->propertyId, 0, false);

			readLock++;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void RewindPerformed(ulong version)
	{
		TTTrace.Write(database.TraceId, tranId, version);
		affectedLogGroups = database.PersistenceDesc.CompleteLogMask;
		ClearInvRefChanges();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddDeleted(ushort classIndex, long id)
	{
		TTTrace.Write(database.TraceId, tranId, classIndex, id);

		DeletedObject* p = (DeletedObject*)deleted.Add();
		p->id = id;
		p->classIndex = classIndex;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddAffectedObject(ushort classIndex, ulong objectHandle)
	{
		TTTrace.Write(database.TraceId, tranId, classIndex, objectHandle);

		AffectedObject* p = (AffectedObject*)affectedObjects.AddItem(ModifiedType.Class, (int)AffectedObject.Size);
		totalAffectedCount++;
		p->objectHandle = objectHandle;
		p->classIndex = classIndex;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddAffectedInvRef(ushort classIndex, long id, int propId, ulong itemHandle, bool isDelete)
	{
		TTTrace.Write(database.TraceId, tranId, classIndex, id, propId, itemHandle, isDelete);

		AffectedInverseReferences* p = (AffectedInverseReferences*)affectedInvRefs.
			AddItem(ModifiedType.InverseReference, Storage.AffectedInverseReferences.Size);

		totalAffectedCount++;
		p->classIndex = classIndex;
		p->id = id;
		p->propertyId = propId;
		p->handle = itemHandle;
		p->isDelete = isDelete;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddMultiAffectedObject(long id)
	{
		TTTrace.Write(database.TraceId, id);

		multiAffectedObjects.TryGetValue(id, out int c);
		multiAffectedObjects[id] = c + 1;
	}

	public void ProcessMultiAffectedObjectsInChangesets()
	{
		if (multiAffectedObjects.Count == 0)
			return;

		int affectedCount = multiAffectedObjects.Count;

		Changeset currChangeset = changeset;

		while (currChangeset != null)
		{
			changesetReader.Init(database.ModelDesc, currChangeset);
			while (!changesetReader.EndOfStream())
			{
				if (!changesetReader.ReadBlock(false, changesetBlock))
					break;

				ClassDescriptor classDesc = changesetBlock.ClassDescriptor;
				OperationType opType = changesetBlock.OperationType;

				if (classDesc == null || opType > OperationType.Delete)
					ChangesetReader.SkipBlock(changesetReader, changesetBlock);

				ChangesetReader.ForEachOperation(changesetReader, changesetBlock, notLastInTransactionFlagger);
			}

			currChangeset = currChangeset.Next;
		}

		Checker.AssertTrue(multiAffectedObjects.Count == 0);
		if (affectedCount < multiAffectedObjectsCapacity * 2)
		{
			multiAffectedObjects.Clear();
		}
		else
		{
			multiAffectedObjects = new LongDictionary<int>(multiAffectedObjectsCapacity + 1);
		}
	}

	private void FlagNotLastInTransactionIfNeeded(long id, OperationHeader opHead)
	{
		TTTrace.Write(database.TraceId, id);

		if (!multiAffectedObjects.TryGetValue(id, out int c))
			return;

		if (c == 1)
			multiAffectedObjects.Remove(id);
		else
			multiAffectedObjects[id] = c - 1;

		opHead.SetNotLastInTransaction();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddObjectReadLock(ushort tranSlot, long id, ushort classIndex)
	{
		TTTrace.Write(database.TraceId, tranId, classIndex, id);

		ObjectReadLock* p = (ObjectReadLock*)objectReadLocks.AddItem(ModifiedType.ObjectReadLock, InvRefReadLock.Size);

		totalAffectedCount++;
		p->id = id;
		p->classIndex = classIndex;
		p->tranSlot = tranSlot;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddInvRefReadLock(ushort tranSlot, ulong handle, ushort classIndex, bool eligibleForGC)
	{
		TTTrace.Write(database.TraceId, tranId, classIndex, eligibleForGC, handle);

		InvRefReadLock* p = (InvRefReadLock*)invRefReadLocks.Add();

		totalAffectedCount++;
		p->handle = handle;
		p->classIndex = classIndex;
		p->SetEligibleForGC_TranSlot(eligibleForGC, tranSlot);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddInverseReferenceChange(ushort classIndex, long invRef, long directRef, int propId, byte opType)
	{
		TTTrace.Write(database.TraceId, tranId, classIndex, invRef, directRef, propId, opType);

		Checker.AssertFalse(opType > 3);

		InverseReferenceOperation* p = (InverseReferenceOperation*)inverseRefChanges.Add();
		p->inverseReference = invRef;
		p->directReference = directRef;
		p->SetValues(propId, opType, classIndex);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddKeyReadLock(ushort tranSlot, ulong itemHandle, int indexIndex, ulong hash)
	{
		TTTrace.Write(database.TraceId, tranId, itemHandle, indexIndex, hash);

		KeyReadLock* p = (KeyReadLock*)keyReadLocks.AddItem(ModifiedType.KeyReadLock, KeyReadLock.Size);
		totalAffectedCount++;
		p->itemHandle = itemHandle;
		p->SetIsRangeAndIndex(false, (ushort)indexIndex);
		p->hash = hash;
		p->tranSlot = tranSlot;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public KeyReadLockItem AddKeyRangeReadLock(ulong rangeHandle, int indexIndex)
	{
		TTTrace.Write(database.TraceId, tranId, indexIndex);

		KeyReadLock* p = (KeyReadLock*)keyReadLocks.AddItem(ModifiedType.KeyReadLock, KeyReadLock.Size);
		totalAffectedCount++;
		p->itemHandle = rangeHandle;
		p->SetIsRangeAndIndex(true, (ushort)indexIndex);
		return new KeyReadLockItem(p);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SortPropagatedInverseReferences()
	{
		propagatedInvRefsSorter.Sort((InverseReferenceOperation*)inverseRefChanges.Buffer, inverseRefChanges.Count);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SortInverseReferences()
	{
		invRefsSorter.Sort((InverseReferenceOperation*)inverseRefChanges.Buffer, inverseRefChanges.Count);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void GroupSortInvRefs(long[] groupCounts)
	{
		GroupingReferenceSorter sorter = new GroupingReferenceSorter();
		sorter.SetRefs((InverseReferenceOperation*)inverseRefChanges.Buffer, inverseRefChanges.Count, groupCounts);
		sorter.Sort();
	}

	public void CopyInvRefChanges(IntPtr p)
	{
		inverseRefChanges.CopyContent(p);
	}

	public IntPtr[] MergeInvRefChanges(TransactionContext[] tcs)
	{
		IntPtr[] ps = new IntPtr[tcs.Length];
		for (int i = 0; i < tcs.Length; i++)
		{
			ps[i] = (IntPtr)inverseRefChanges.AddRange(tcs[i].inverseRefChanges.Count);
			for (int j = 0; j < GroupingReferenceSorter.GroupCount; j++)
			{
				invRefGroupCounts[j] += tcs[i].InvRefGroupCounts[j];
			}
		}

		return ps;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddGroupingInvRefChange(ushort typeIndex, long invRef, long directRef, int propId, byte opType)
	{
		TTTrace.Write(database.TraceId, tranId, typeIndex, invRef, directRef, propId, opType);

		Checker.AssertFalse(opType > 3);

		InverseReferenceOperation* p = (InverseReferenceOperation*)inverseRefChanges.Add();
		p->inverseReference = invRef;
		p->directReference = directRef;
		p->SetValues(propId, opType, typeIndex);

		int group = GroupingReferenceSorter.GetGroup(p);
		invRefGroupCounts[group]++;
	}

	internal void ResizeInvRefChange(long capacity)
	{
		if (capacity < inverseReferencesOperationCapacity)
			return;

		inverseRefChanges.Resize(capacity);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ClearInvRefChanges()
	{
		if (!isAllocated)
			return;

		if (isAlignmentMode)
		{
			inverseRefChanges.Reset(inverseRefChanges.Capacity);
		}
		else
		{
			inverseRefChanges.Reset(inverseReferencesOperationCapacity);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ClearDeletes()
	{
		deleted.Reset();
	}

	public void Clear()
	{
		TTTrace.Write();

		Checker.AssertFalse(affectedObjects.NotEmpty);
		Checker.AssertFalse(objectReadLocks.NotEmpty);
		Checker.AssertFalse(keyReadLocks.NotEmpty);
		Checker.AssertFalse(affectedInvRefs.NotEmpty);

		invRefReadLocks.Reset(inverseReferenceReadLockCapacity);
		deleted.Reset(deletedObjectsCapacity);
		ClearInvRefChanges();

		if (lockedClasses->Capacity > defLockedClassesCapacity)
		{
			ClassIndexMultiSet.Destroy(database.Engine.MemoryManager, lockedClasses);
			lockedClasses = ClassIndexMultiSet.Create(defLockedClassesCapacity, database.Engine.MemoryManager);
		}
		else
		{
			ClassIndexMultiSet.Clear(lockedClasses);
		}

		if (writtenClasses->Capacity > defWrittenClassesCapacity)
		{
			ClassIndexMultiSet.Destroy(database.Engine.MemoryManager, writtenClasses);
			writtenClasses = ClassIndexMultiSet.Create(defWrittenClassesCapacity, database.Engine.MemoryManager);
		}
		else
		{
			ClassIndexMultiSet.Clear(writtenClasses);
		}

		if (origTempInvRefs != tempInvRefs)
		{
			AlignedAllocator.Free((IntPtr)tempInvRefs);
			tempInvRefs = (long*)AlignedAllocator.Allocate(TempInvRefSize * sizeof(long));
		}

		if (invRefReadLocksSet.Count > 0)
		{
			if (invRefReadLocksSet.Count < objectReadLockMapCapacity * 2)
			{
				invRefReadLocksSet.Clear();
			}
			else
			{
				invRefReadLocksSet = new FastHashSet<InverseReferenceKey>(objectReadLockMapCapacity + 1);
			}
		}

		if (objectReadLocksSet.Count > 0)
		{
			if (objectReadLocksSet.Count < objectReadLockMapCapacity * 2)
			{
				objectReadLocksSet.Clear();
			}
			else
			{
				objectReadLocksSet = new LongDictionary<LockType>(objectReadLockMapCapacity + 1);
			}
		}

		if (multiAffectedObjects.Count > 0)
		{
			if (multiAffectedObjects.Count < multiAffectedObjectsCapacity * 2)
			{
				multiAffectedObjects.Clear();
			}
			else
			{
				multiAffectedObjects = new LongDictionary<int>(multiAffectedObjectsCapacity + 1);
			}
		}

		cascadeSetToNullAffectedCount = 0;
		if (cascadeSetToNullAffected.Length > cascadeSetToNullAffectedCapacity)
			cascadeSetToNullAffected = new long[cascadeSetToNullAffectedCapacity];

		changesetReader.Clear();

		totalAffectedCount = 0;
		totalMergedCount = 1;
		asyncCommiterCount = 0;
		asyncCommitResult = true;
		writeFlags = WriteTransactionFlags.None;
		affectedLogGroups = 0;
		originReplica = null;
		alignment = null;
		standbyOrderNum = ulong.MaxValue;
		newModelDescBinary = null;
		newPersistenceDescBinary = null;
		userAssembliesModified = false;
		localTerm = 0;
		globalTerm.Low = 0;
		globalTerm.Hight = 0;

		while (changeset != null)
		{
			changeset.ReleaseRef();
			changeset = changeset.Next;
		}

		for (int i = 0; i < nextPersisted.Length; i++)
		{
			nextPersisted[i] = null;
		}

		database = null;
	}

	public void Dispose()
	{
		if (!isAllocated)
			return;

		Checker.AssertFalse(affectedObjects.NotEmpty);
		Checker.AssertFalse(affectedInvRefs.NotEmpty);
		Checker.AssertFalse(objectReadLocks.NotEmpty);
		Checker.AssertFalse(keyReadLocks.NotEmpty);

		invRefsSorter.Dispose();
		propagatedInvRefsSorter.Dispose();

		AlignedAllocator.Free((IntPtr)invRefGroupCounts);
		inverseRefChanges.Dispose();

		deleted.Dispose();

		invRefReadLocks.Dispose();

		commitWaitEvent.Dispose();

		ClassIndexMultiSet.Destroy(engine.MemoryManager, lockedClasses);
		ClassIndexMultiSet.Destroy(engine.MemoryManager, writtenClasses);

		AlignedAllocator.Free((IntPtr)tempInvRefs);

		AlignedAllocator.Free((IntPtr)tempInvRefCounts);
	}
}

internal enum InvRefChangeType : byte
{
	Insert = 0,
	Delete = 1
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = InverseReferenceOperation.Size)]
internal unsafe struct InverseReferenceOperation
{
	public const int Size = 24;

	public long inverseReference;
	public long directReference;
	public long propId_opType_classIndex;

	public ushort ClassIndex => (ushort)propId_opType_classIndex;
	public byte Type => (byte)((propId_opType_classIndex >> 16) & 0x03);
	public int PropertyId => (int)(propId_opType_classIndex >> 32);
	public long PropId_opType => propId_opType_classIndex >> 16;

	public void SetValues(int propId, byte opType, ushort classIndex)
	{
		propId_opType_classIndex = ((long)propId << 32) | ((uint)opType << 16) | (uint)classIndex;
	}

	public override string ToString()
	{
		return string.Format("{0}, {1} -> {2}", inverseReference, PropertyId, directReference);
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = Size)]
internal unsafe struct DeletedObject
{
	public const int Size = 16;

	public long id;
	public ushort classIndex;
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
internal unsafe struct ObjectReadLock
{
	public const int Size = 16;

	[FieldOffset(0)]
	public long id;

	[FieldOffset(8)]
	public ushort classIndex;

	[FieldOffset(10)]
	public ushort tranSlot;
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
internal unsafe struct InvRefReadLock
{
	public const int Size = 16;

	[FieldOffset(0)]
	public ulong handle;

	[FieldOffset(0)]
	public long id;

	[FieldOffset(8)]
	public int propertyId;

	[FieldOffset(12)]
	public ushort classIndex;

	[FieldOffset(14)]
	public ushort eligibleForGC_tranSlot;

	public bool EligibleForGC => (eligibleForGC_tranSlot >> 15) != 0;
	public ushort TranSlot => (ushort)(eligibleForGC_tranSlot & 0x7fff);

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetEligibleForGC_TranSlot(bool eligibleForGC, ushort slot)
	{
		eligibleForGC_tranSlot = (ushort)(((eligibleForGC ? 1 : 0) << 15) | slot);
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = Size)]
internal unsafe struct KeyReadLock
{
	public const int Size = 24;

	public ulong itemHandle;
	public ulong hash;
	public ushort isRange_indexIndex;
	public ushort tranSlot;

	public ushort IndexIndex => (ushort)(isRange_indexIndex & 0x7fff);
	public bool IsRange => (isRange_indexIndex & 0x8000) != 0;

	public void SetIsRangeAndIndex(bool isRange, ushort indexIndex)
	{
		this.isRange_indexIndex = (ushort)((isRange ? 0x8000 : 0x0000) | indexIndex);
	}
}

internal unsafe struct KeyReadLockItem
{
	KeyReadLock* item;

	public KeyReadLockItem(KeyReadLock* item)
	{
		this.item = item;
	}

	public void Modify(ulong rangeHandle)
	{
		item->itemHandle = rangeHandle;
	}
}

internal struct InverseReferenceKey : IEquatable<InverseReferenceKey>
{
	long id;
	int propertyId;

	public InverseReferenceKey(long id, int propertyId)
	{
		this.id = id;
		this.propertyId = propertyId;
	}

	public bool Equals(InverseReferenceKey other)
	{
		return id == other.id && propertyId == other.propertyId;
	}

	public override int GetHashCode()
	{
		return (int)HashUtils.GetHash64((ulong)id, (ulong)propertyId);
	}
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
internal unsafe struct AffectedObject
{
	public const int Size = 16;

	[FieldOffset(0)]
	public ulong objectHandle;

	[FieldOffset(0)]
	public long id;

	[FieldOffset(8)]
	public ushort classIndex;
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = Size)]
internal unsafe struct AffectedInverseReferences
{
	public const int Size = 24;

	public long id;
	public ulong handle;
	public int propertyId;
	public ushort classIndex;
	public bool isDelete;
}
