using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Velox.Common;
using Velox.Descriptor;
using Velox.Storage.Persistence;
using Velox.Storage.Replication;

namespace Velox.Storage;

internal enum TransactionCommitType
{
	Normal = 0,
	Merged = 1,
	MergedAsync = 2
}

internal unsafe sealed class TransactionContext : IDisposable
{
	public const int TempInvRefSize = 1024 * 8;					// 64KB

	const int classScanChunkSize = 32;

	const int inverseReferencesOperationCapacity = 1024 * 8;	// 192KB
	const int deletedObjectsCapacity = 1024 * 8;				// 128KB

	const int inverseReferenceReadLockCapacity = 1024 * 24;		// 384KB
	const int objectReadLockMapCapacity = 512;					// Roughly 18KB

	const int defLockedClassesCap = 32;
	const int defWrittenClassesCap = 32;

	StorageEngine engine;
	Database database;

	ulong tranId;

	ChangesetReader changesetReader;
	ChangesetBlock changesetBlock;

	// These fields need to be merged when merging transactions
	ModifiedList affectedObjects;
	ModifiedList affectedInvRefs;
	ModifiedList objectReadLocks;
	NativeList invRefReadLocks;
	ModifiedList hashReadLocks;

	ClassIndexMultiSet* lockedClasses;
	ClassIndexMultiSet* writtenClasses;

	byte affectedLogGroups;
	List<Changeset> persistedChangesets;
	List<LogChangeset>[] logChangesets;

	List<Transaction> mergedWith;
	ReplicaData[] replicaData;
	////////////////////////////////////////////////////////////

	int* invRefGroupCounts;
	NativeList inverseRefChanges;

	ReferenceSorter invRefsSorter;
	ReferenceSorter propagatedInvRefsSorter;

	NativeList deleted;

	LongHashSet objectReadLocksSet;
	FastHashSet<InverseReferenceKey> invRefReadLocksSet;

	long* origTempInvRefs;
	long* tempInvRefs;
	int* tempInvRefCounts;

	ObjectReader[] tempRecReaders;

	int poolIndex;
	ushort slot;

	WriteTransactionFlags writeFlags;

	int sourceReplicaIndex;

	ulong rewindVersion;

	AlignmentData alignment;
	bool userAssembliesModified;
	byte[] newModelDescBinary;
	byte[] newPersistenceDescBinary;

	uint localTerm;
	SimpleGuid globalTerm;
	ulong logSeqNum;

	IReplica originReplica;

	AutoResetEvent mergedCommitWaitEvent;
	DatabaseErrorDetail asyncError;
	TransactionCommitType commitType;
	Action<DatabaseException> asyncCallback;

	AutoResetEvent commitWaitEvent;
	int asyncCommiterCount;
	int regAsyncCommiterCount;
	bool asyncCommitResult;
	RWSpinLock asyncCommitLock;

	ulong standbyOrderNum;

	List<ClassScan> classScans;

	bool isAlignmentMode;

	public TransactionContext(StorageEngine engine, int physCorePool, ushort slot)
	{
		if (slot == 0)
			throw new CriticalDatabaseException();

		this.engine = engine;
		this.poolIndex = physCorePool;
		this.slot = slot;

		affectedObjects = new ModifiedList(engine.MemoryManager);
		affectedInvRefs = new ModifiedList(engine.MemoryManager);

		invRefsSorter = new ReferenceSorter(InverseComparer.Instance);
		propagatedInvRefsSorter = new ReferenceSorter(PropagatedComparer.Instance);

		invRefGroupCounts = (int*)AlignedAllocator.Allocate(GroupingReferenceSorter.GroupCount * sizeof(int));
		inverseRefChanges = new NativeList(inverseReferencesOperationCapacity, InverseReferenceOperation.Size);

		deleted = new NativeList(deletedObjectsCapacity, DeletedObject.Size);

		objectReadLocksSet = new LongHashSet(objectReadLockMapCapacity + 1);
		invRefReadLocksSet = new FastHashSet<InverseReferenceKey>(objectReadLockMapCapacity + 1);

		objectReadLocks = new ModifiedList(engine.MemoryManager);
		invRefReadLocks = new NativeList(inverseReferenceReadLockCapacity, ReadLock.Size);
		hashReadLocks = new ModifiedList(engine.MemoryManager);

		lockedClasses = ClassIndexMultiSet.Create(defLockedClassesCap, engine.MemoryManager);
		writtenClasses = ClassIndexMultiSet.Create(defWrittenClassesCap, engine.MemoryManager);

		origTempInvRefs = tempInvRefs = (long*)AlignedAllocator.Allocate(TempInvRefSize * sizeof(long));
		tempInvRefCounts = (int*)AlignedAllocator.Allocate(ClassDescriptor.MaxInverseReferencesPerClass * sizeof(int), false);

		tempRecReaders = new ObjectReader[classScanChunkSize];

		commitWaitEvent = new AutoResetEvent(false);
		mergedCommitWaitEvent = new AutoResetEvent(false);
		asyncCommitResult = true;

		changesetReader = new ChangesetReader();
		changesetBlock = new ChangesetBlock();

		mergedWith = new List<Transaction>(16);

		persistedChangesets = new List<Changeset>(2);
		logChangesets = new List<LogChangeset>[PersistenceDescriptor.MaxLogGroups];
		for (int i = 0; i < PersistenceDescriptor.MaxLogGroups; i++)
		{
			logChangesets[i] = new List<LogChangeset>(4);
		}

		classScans = new List<ClassScan>(2);

		ReplicationDescriptor replicationDesc = engine.ReplicationDesc;
		sourceReplicaIndex = replicationDesc.GetSourceReplicaIndex();

		int c = 0;
		replicaData = new ReplicaData[replicationDesc.AllReplicas.Count()];
		foreach (ReplicaDescriptor replicaDesc in replicationDesc.AllReplicas)
		{
			replicaData[c] = new ReplicaData(this, replicaDesc, c);
			c++;
		}

		rewindVersion = IReplicator.NoRewindVersion;
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
	public ModifiedList HashReadLocks => hashReadLocks;
	public LongHashSet ObjectReadLocksSet => objectReadLocksSet;
	public FastHashSet<InverseReferenceKey> InvRefReadLocksSet => invRefReadLocksSet;
	public ClassIndexMultiSet* LockedClasses { get => lockedClasses; set => lockedClasses = value; }
	public ClassIndexMultiSet* WrittenClasses { get => writtenClasses; set => writtenClasses = value; }
	public long* TempInvRefs { get => tempInvRefs; set => tempInvRefs = value; }
	public int* TempInvRefCounts => tempInvRefCounts;
	public ObjectReader[] TempRecReaders => tempRecReaders;
	public List<LogChangeset>[] LogChangesets => logChangesets;
	public List<Changeset> Changesets => persistedChangesets;
	public bool IsTransactionEmpty => persistedChangesets.Count == 0;
	public byte AffectedLogGroups { get => affectedLogGroups; set => affectedLogGroups = value; }
	public WriteTransactionFlags WriteFlags { get => writeFlags; set => writeFlags = value; }
	public ReplicaData[] ReplicaData => replicaData;
	public ulong RewindVersion => rewindVersion;
	public AlignmentData Alignment { get => alignment; set => alignment = value; }
	public uint LocalTerm { get => localTerm; set => localTerm = value; }
	public SimpleGuid GlobalTerm { get => globalTerm; set => globalTerm = value; }
	public ulong LogSeqNum { get => logSeqNum; set => logSeqNum = value; }
	public IReplica OriginReplica { get => originReplica; set => originReplica = value; }
	public AutoResetEvent CommitWaitEvent => commitWaitEvent;
	public AutoResetEvent MergedCommitWaitEvent => mergedCommitWaitEvent;
	public int AsyncCommitCount { get => asyncCommiterCount; set => asyncCommiterCount = value; }
	public int RegAsyncCommitCount { get => regAsyncCommiterCount; set => regAsyncCommiterCount = value; }
	public bool AsyncCommitResult { get => asyncCommitResult; set => asyncCommitResult = value; }
	public ulong StandbyOrderNum { get => standbyOrderNum; set => standbyOrderNum = value; }
	public ChangesetReader ChangesetReader => changesetReader;
	public ChangesetBlock ChangesetBlock => changesetBlock;
	public List<Transaction> MergedWith => mergedWith;
	public DatabaseErrorDetail AsyncError { get => asyncError; set => asyncError = value; }
	public TransactionCommitType CommitType { get => commitType; set => commitType = value; }
	public List<ClassScan> ClassScans => classScans;
	public Action<DatabaseException> AsyncCallback { get => asyncCallback; set => asyncCallback = value; }
	public unsafe int* InvRefGroupCounts => invRefGroupCounts;
	public byte[] NewModelDescBinary { get => newModelDescBinary; set => newModelDescBinary = value; }
	public byte[] NewPersistenceDescBinary { get => newPersistenceDescBinary; set => newPersistenceDescBinary = value; }
	public bool UserAssembliesModified { get => userAssembliesModified; set => userAssembliesModified = value; }

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Init(Database database, ulong tranId)
	{
		TTTrace.Write(database.TraceId, tranId);

		this.database = database;
		this.tranId = tranId;
		affectedObjects.Init();
		affectedInvRefs.Init();
		objectReadLocks.Init();
		hashReadLocks.Init();
	}

	public void SetAlignmentMode()
	{
		isAlignmentMode = true;
		inverseRefChanges.Resize(inverseReferencesOperationCapacity * 4);
	}

	public void ResetAlignmentMode()
	{
		isAlignmentMode = false;
		ClearInvRefChanges();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddChangeset(Changeset ch, IReplica source)
	{
		int sourceIndex = source == null ? -1 : source.Index;
		for (int i = 0; i < replicaData.Length; i++)
		{
			if (i == sourceReplicaIndex)
				continue;

			replicaData[i].AddChangeset(sourceIndex, ch);
		}
	}

	public void ClearInvRefGroupCounts()
	{
		Utils.ZeroMemory((byte*)invRefGroupCounts, GroupingReferenceSorter.GroupCount * sizeof(int));
	}

	public void Merge(Transaction tran)
	{
		TransactionContext tc = tran.Context;

		Checker.AssertTrue(originReplica == null);
		Checker.AssertTrue(writeFlags == WriteTransactionFlags.None);

		mergedWith.Add(tran);

		affectedObjects.MergeChanges(tc.affectedObjects);
		affectedInvRefs.MergeChanges(tc.affectedInvRefs);
		objectReadLocks.MergeChanges(tc.objectReadLocks);
		hashReadLocks.MergeChanges(tc.hashReadLocks);
		invRefReadLocks.Append(tc.invRefReadLocks);
		tc.invRefReadLocks.Reset(inverseReferenceReadLockCapacity);

		ClassIndexMultiSet.Merge(engine.MemoryManager, ref lockedClasses, tc.lockedClasses);
		ClassIndexMultiSet.Clear(tc.lockedClasses);

		ClassIndexMultiSet.Merge(engine.MemoryManager, ref writtenClasses, tc.writtenClasses);
		ClassIndexMultiSet.Clear(tc.writtenClasses);

		persistedChangesets.AddRange(tc.persistedChangesets);
		tc.persistedChangesets.Clear();

		for (int i = 0; i < logChangesets.Length; i++)
		{
			logChangesets[i].AddRange(tc.logChangesets[i]);
			tc.logChangesets[i].Clear();
		}

		affectedLogGroups |= tc.affectedLogGroups;

		for (int i = 0; i < replicaData.Length; i++)
		{
			replicaData[i].Merge(tc.replicaData[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void PrepareEmptyInvRefEntriesForGC()
	{
		NativeList l = InvRefReadLocks;
		long count = l.Count;
		ReadLock* readLock = (ReadLock*)l.Buffer;
		for (long i = 0; i < count; i++)
		{
			if (readLock->EligibleForGC)
				AddAffectedInvRef(readLock->classIndex, readLock->id, readLock->propertyId, 0, false);

			readLock++;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void EnterAsyncCommitStateLock()
	{
		asyncCommitLock.EnterWriteLock();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitAsyncCommitStateLock()
	{
		asyncCommitLock.ExitWriteLock();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void RewindPerformed(ulong version)
	{
		TTTrace.Write(database.TraceId, tranId, version);
		rewindVersion = version;
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
		p->objectHandle = objectHandle;
		p->classIndex = classIndex;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddAffectedInvRef(ushort classIndex, long id, int propId, ulong itemHandle, bool isDelete)
	{
		TTTrace.Write(database.TraceId, tranId, classIndex, id, propId, itemHandle, isDelete);

		AffectedInverseReferences* p = (AffectedInverseReferences*)affectedInvRefs.
			AddItem(ModifiedType.InverseReference, Storage.AffectedInverseReferences.Size);

		p->classIndex = classIndex;
		p->id = id;
		p->propertyId = propId;
		p->handle = itemHandle;
		p->isDelete = isDelete;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddReadLock(ushort tranSlot, ulong handle, ushort classIndex, bool isObject, bool eligibleForGC)
	{
		TTTrace.Write(database.TraceId, tranId, classIndex, isObject, eligibleForGC, handle);

		ReadLock* p = isObject ? (ReadLock*)objectReadLocks.AddItem(ModifiedType.ObjectReadLock, ReadLock.Size) :
			(ReadLock*)invRefReadLocks.Add();

		p->handle = handle;
		p->classIndex = classIndex;
		p->SetEligibleForGC_TranSlot(eligibleForGC, tranSlot);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddInverseReferenceChange(ushort typeIndex, long invRef, long directRef, int propId, bool isTracked, byte opType)
	{
		TTTrace.Write(database.TraceId, tranId, typeIndex, invRef, directRef, propId, isTracked, opType);

		Checker.AssertFalse(opType > 3);

		InverseReferenceOperation* p = (InverseReferenceOperation*)inverseRefChanges.Add();
		p->inverseReference = invRef;
		p->directReference = directRef;
		p->SetValues(propId, isTracked, opType, typeIndex);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddHashKeyReadLock(ushort tranSlot, ulong itemHandle, int index, ulong hash)
	{
		TTTrace.Write(database.TraceId, tranId, itemHandle, index, hash);

		HashReadLock* p = (HashReadLock*)hashReadLocks.AddItem(ModifiedType.HashReadLock, HashReadLock.Size);
		p->itemHandle = itemHandle;
		p->hashIndex = (ushort)index;
		p->hash = hash;
		p->tranSlot = tranSlot;
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
	public void AddGroupingInvRefChange(ushort typeIndex, long invRef, long directRef, int propId, bool isTracked, byte opType)
	{
		TTTrace.Write(database.TraceId, tranId, typeIndex, invRef, directRef, propId, isTracked, opType);

		Checker.AssertFalse(opType > 3);

		InverseReferenceOperation* p = (InverseReferenceOperation*)inverseRefChanges.Add();
		p->inverseReference = invRef;
		p->directReference = directRef;
		p->SetValues(propId, isTracked, opType, typeIndex);

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
		Checker.AssertFalse(hashReadLocks.NotEmpty);
		Checker.AssertFalse(affectedInvRefs.NotEmpty);

		invRefReadLocks.Reset(inverseReferenceReadLockCapacity);
		deleted.Reset(deletedObjectsCapacity);
		ClearInvRefChanges();

		if (lockedClasses->Capacity > defLockedClassesCap)
		{
			ClassIndexMultiSet.Destroy(database.Engine.MemoryManager, lockedClasses);
			lockedClasses = ClassIndexMultiSet.Create(defLockedClassesCap, database.Engine.MemoryManager);
		}
		else
		{
			ClassIndexMultiSet.Clear(lockedClasses);
		}

		if (writtenClasses->Capacity > defWrittenClassesCap)
		{
			ClassIndexMultiSet.Destroy(database.Engine.MemoryManager, writtenClasses);
			writtenClasses = ClassIndexMultiSet.Create(defWrittenClassesCap, database.Engine.MemoryManager);
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
				objectReadLocksSet = new LongHashSet(objectReadLockMapCapacity + 1);
			}
		}

		changesetReader.Clear();

		asyncCommiterCount = 0;
		asyncCommitResult = true;
		writeFlags = WriteTransactionFlags.None;
		affectedLogGroups = 0;
		rewindVersion = IReplicator.NoRewindVersion;
		originReplica = null;
		alignment = null;
		standbyOrderNum = ulong.MaxValue;
		commitType = TransactionCommitType.Normal;
		asyncCallback = null;
		newModelDescBinary = null;
		newPersistenceDescBinary = null;
		userAssembliesModified = false;
		localTerm = 0;
		globalTerm.Low = 0;
		globalTerm.Hight = 0;

		for (int i = 0; i < replicaData.Length; i++)
		{
			replicaData[i].Clear();
		}

		for (int i = 0; i < logChangesets.Length; i++)
		{
			logChangesets[i].Clear();
		}

		for (int i = 0; i < persistedChangesets.Count; i++)
		{
			persistedChangesets[i].ReleaseRef();
		}

		persistedChangesets.Clear();
		mergedWith.Clear();

		database = null;
	}

	public void Dispose()
	{
		Checker.AssertFalse(affectedObjects.NotEmpty);
		Checker.AssertFalse(affectedInvRefs.NotEmpty);
		Checker.AssertFalse(objectReadLocks.NotEmpty);
		Checker.AssertFalse(hashReadLocks.NotEmpty);

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
	public long propId_tracked_opType_classIndex;

	public ushort ClassIndex => (ushort)propId_tracked_opType_classIndex;
	public byte Type => (byte)((propId_tracked_opType_classIndex >> 16) & 0x03);
	public int PropertyId => (int)(propId_tracked_opType_classIndex >> 32);
	public bool IsTracked => (byte)((propId_tracked_opType_classIndex >> 18) & 0x01) != 0;
	public long PropId_tracked_opType => propId_tracked_opType_classIndex >> 16;

	public void SetValues(int propId, bool isTracked, byte opType, ushort typeIndex)
	{
		uint btr = isTracked ? (uint)1 : 0;
		propId_tracked_opType_classIndex = ((long)propId << 32) | (btr << 18) | ((uint)opType << 16) | (uint)typeIndex;
	}

	public override string ToString()
	{
		return string.Format("{0}, {1} -> {2}", inverseReference, PropertyId, directReference);
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = DeletedObject.Size)]
internal unsafe struct DeletedObject
{
	public const int Size = 16;

	public long id;
	public ushort classIndex;
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = ReadLock.Size)]
internal unsafe struct ReadLock
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

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = HashReadLock.Size)]
internal unsafe struct HashReadLock
{
	public const int Size = 24;

	public ulong itemHandle;
	public ulong hash;
	public ushort hashIndex;
	public ushort tranSlot;
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

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = AffectedObject.Size)]
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

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = AffectedInverseReferences.Size)]
internal unsafe struct AffectedInverseReferences
{
	public const int Size = 24;

	public long id;
	public ulong handle;
	public int propertyId;
	public ushort classIndex;
	public bool isDelete;
}
