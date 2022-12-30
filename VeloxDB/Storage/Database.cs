using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using static System.Math;
using VeloxDB.Common;
using VeloxDB.Storage.Persistence;
using VeloxDB.Storage.Replication;
using VeloxDB.Descriptor;
using VeloxDB.Storage.ModelUpdate;
using static VeloxDB.Storage.Persistence.DatabaseRestorer;
using System.IO;
using System.Threading;

namespace VeloxDB.Storage;

internal static class DatabaseId
{
	public const long None = -1;
	public const long SystemLocal = 0;
	public const long SystemGlobal = 1;
	public const long User = 2;

	public const long DatabaseCount = 3;
}

internal unsafe sealed partial class Database
{
	// Last bit is not used because different structures expect commitVersion/tranId to be 63-bits
	public const ulong MaxCommitedVersion = ulong.MaxValue / 4;
	public const ulong MinTranId = MaxCommitedVersion + 1;
	public const ulong MaxTranId = ((ulong)1 << 63);

	long id;

	StorageEngine engine;
	PersistenceDescriptor persistenceDesc;
	DataModelDescriptor modelDesc;
	Tracing.Source trace;

	DatabaseVersions versions;

	ClassEntry[] classes;
	Dictionary<short, int> hashIndexesMap;
	HashIndexEntry[] hashIndexes;

	GarbageCollector gc;
	DatabasePersister persister;
	TransactionCommitOrderer tranOrderer;
	HashReadersCollection hashReaders;
	ReferenceIntegrityValidator refValidator;
	IdGenerator idGenerator;

	PrimaryAlignCodeCache primaryAlignCodeCache;
	StandbyAlignCodeCache standbyAlignCodeCache;

	object currTransIdsHandle;
	ulong* currTransIds;

	private Database(StorageEngine engine, DataModelDescriptor model, PersistenceDescriptor persistenceDesc, long id)
	{
		TTTrace.Write(engine.TraceId);

		this.trace = engine.Trace;
		this.engine = engine;
		this.modelDesc = model;
		this.persistenceDesc = persistenceDesc;
		this.id = id;

		primaryAlignCodeCache = new PrimaryAlignCodeCache();
		standbyAlignCodeCache = new StandbyAlignCodeCache();

		versions = new DatabaseVersions(this);
		tranOrderer = new TransactionCommitOrderer(engine.Settings);
		hashReaders = new HashReadersCollection(model);
		refValidator = new ReferenceIntegrityValidator(engine, this);

		InitCurrTranIds();

		CreateClasses();
		gc = new GarbageCollector(this);
		idGenerator = new IdGenerator(this);

		primaryAlignCodeCache.InitCache(model);

		trace.Debug("Database {0} created.", id);
	}

	public StorageEngine Engine => engine;
	public long Id => id;
	public ulong ReadVersion => versions.ReadVersion;
	public DatabasePersister Persister => persister;
	public uint LocalTerm => versions.LocalTerm;
	public long TraceId => Engine.TraceId;
	public Tracing.Source Trace => Engine.Trace;
	public DatabaseVersions Versions => versions;
	public HashReadersCollection HashReaders => hashReaders;
	public DataModelDescriptor ModelDesc => modelDesc;
	public PersistenceDescriptor PersistenceDesc => persistenceDesc;
	public ReferenceIntegrityValidator RefValidator => refValidator;
	public IdGenerator IdGenerator => idGenerator;
	public PrimaryAlignCodeCache PrimaryAlignCodeCache => primaryAlignCodeCache;
	public StandbyAlignCodeCache StandbyAlignCodeCache => standbyAlignCodeCache;

	public static Database CreateEmpty(StorageEngine engine, DataModelDescriptor model, PersistenceDescriptor persistenceDesc, long id)
	{
		Database database = new Database(engine, model, persistenceDesc, id);
		database.CreatePersister();
		database.CreateHashIndexes(null);
		database.CreateInvRefs(null);
		return database;
	}

	public static void Restore(StorageEngine engine, DataModelDescriptor model, PersistenceDescriptor persistenceDesc,
	long id, Action<Database> databaseCreatedHandler, ulong invalidLogSeqNum = DatabaseRestorer.UnusedLogSeqNum)
	{
		TTTrace.Write(engine.TraceId, id, invalidLogSeqNum);
		engine.Trace.Debug("Restoring database {0}.", id);

		bool hasInvalidLog = invalidLogSeqNum != DatabaseRestorer.UnusedLogSeqNum;

		int workerCount = ProcessorNumber.CoreCount;
		if (!engine.Settings.AllowInternalParallelization)
			workerCount = 1;

		string name = string.Format("{0}: vlx-DatabaseRestorer", engine.Trace.Name);
		JobWorkers<CommonWorkerParam> workers = JobWorkers<CommonWorkerParam>.Create(name, workerCount);

		bool restoreConfig = model == null;
		PersistenceDescriptor origPersistenceDescriptor = persistenceDesc;

		Database database;
		DatabaseRestorer restorer;
		LogState[] logStates;
		DatabaseVersions versions;

		while (true)
		{
			if (restoreConfig)
			{
				TTTrace.Write(engine.TraceId, id, invalidLogSeqNum);
				model = DataModelDescriptor.CreateEmpty(origPersistenceDescriptor);
				database = new Database(engine, model, origPersistenceDescriptor, id);
				databaseCreatedHandler(database);
				restorer = new DatabaseRestorer(database, workers);
				restorer.TryRestore(out logStates, ref invalidLogSeqNum, out versions);
				database.versions = versions;
				engine.ReadUserConfiguration(out model, out persistenceDesc);
				if (persistenceDesc == null)
					persistenceDesc = origPersistenceDescriptor;

				database.Dispose();
			}

			TryPrepareRestoreDirectories(persistenceDesc, engine);

			database = new Database(engine, model, persistenceDesc, id);
			databaseCreatedHandler(database);
			restorer = new DatabaseRestorer(database, workers);
			if (restorer.TryRestore(out logStates, ref invalidLogSeqNum, out versions))
			{
				TTTrace.Write(engine.TraceId, id, invalidLogSeqNum);
				versions.TTTraceState();
				database.versions = versions;
				database.persister = new DatabasePersister(database, persistenceDesc, engine.SnapshotController,
					versions.CommitVersion, versions.LogSeqNum, logStates);

				break;
			}

			database.Dispose();
			hasInvalidLog = true;
			TTTrace.Write(engine.TraceId, id, invalidLogSeqNum);
		}

		if (hasInvalidLog && database.persister != null)
		{
			TTTrace.Write(engine.TraceId, id);
			database.persister.CreateSnapshots();
		}

		database.CreateHashIndexes(workers);
		database.CreateInvRefs(workers);

		workers.WaitAndClose();

		if (id == DatabaseId.User)
			engine.Trace.Info("Database restored.");
		else
			engine.Trace.Debug("Database {0} restored.", id);
	}

	private static bool LogExists(string[] logNames, string[] snapshotNames)
	{
		return File.Exists(logNames[0]) && File.Exists(logNames[1]) &&
			(File.Exists(snapshotNames[0]) || File.Exists(snapshotNames[1]));
	}

	public static bool TryPrepareRestoreDirectories(PersistenceDescriptor persistenceDesc, StorageEngine engine)
	{
		Checker.AssertTrue(persistenceDesc.LogDescriptors[0].Name.Equals(LogDescriptor.MasterLogName, StringComparison.Ordinal));

		LogPersister.GetLogNames(engine, persistenceDesc.LogDescriptors[0], out string[] logNames);
		LogPersister.GetSnapshotNames(engine, persistenceDesc.LogDescriptors[0], out string[] snapshotNames);

		if (LogExists(logNames, snapshotNames))
			return true;

		LogPersister.GetTempLogNames(engine, persistenceDesc.LogDescriptors[0], out string[] tempLogNames);
		LogPersister.GetTempSnapshotNames(engine, persistenceDesc.LogDescriptors[0], out string[] tempSnapshotNames);

		if (LogExists(tempLogNames, tempSnapshotNames))
		{
			for (int i = 0; i < persistenceDesc.LogDescriptors.Length; i++)
			{
				LogPersister.GetLogNames(engine, persistenceDesc.LogDescriptors[i], out logNames);
				LogPersister.GetTempLogNames(engine, persistenceDesc.LogDescriptors[i], out tempLogNames);

				LogPersister.GetSnapshotNames(engine, persistenceDesc.LogDescriptors[i], out snapshotNames);
				LogPersister.GetTempSnapshotNames(engine, persistenceDesc.LogDescriptors[i], out tempSnapshotNames);

				for (int j = 0; j < logNames.Length; j++)
				{
					if (File.Exists(logNames[i]))
						File.Delete(logNames[i]);

					if (File.Exists(snapshotNames[i]))
						File.Delete(snapshotNames[i]);

					File.Move(tempLogNames[i], logNames[i]);
					if (File.Exists(tempSnapshotNames[i]))
						File.Move(tempSnapshotNames[i], tempSnapshotNames[i]);
				}
			}

			return true;
		}

		return false;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AssignCommitAndLogSeqNum(Transaction tran)
	{
		versions.AssignCommitAndLogSeqNum(tran);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void PersistCommit(Transaction t)
	{
		if (persister != null)
			persister.BeginCommitTransaction(t);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void PublishTransactionCommit(Transaction tran)
	{
		versions.PublishTransactionCommit(tran, tranOrderer);
	}

	public GlobalVersion[] GetGlobalVersions(out uint localTerm)
	{
		TTTrace.Write(engine.TraceId, id, versions.ReadVersion, versions.LocalTerm, versions.LogSeqNum);
		return versions.UnpackClusterVersions(out localTerm);
	}

	internal void GetLocalTermAndReadVersion(out uint localTerm, out ulong readVersion)
	{
		TTTrace.Write(engine.TraceId, id, versions.ReadVersion, versions.LocalTerm, versions.LogSeqNum);
		versions.UnpackLocalTermAndReadVersion(out localTerm, out readVersion);
	}

	public void Rewind(ulong version)
	{
		TTTrace.Write(engine.TraceId, id, version);
		trace.Debug("Rewinding database {0} to version {1}.", id, version);
		persister?.Rewind(version);
		idGenerator = new IdGenerator(this);
		gc.Rewind(version);
		if (!versions.TryRewind(version))
			throw new CriticalDatabaseException();
	}

	public void EnsureClassCapacities(AlignmentData.ClassCapacity[] classCapacities)
	{
		Task[] tasks = new Task[classCapacities.Length];
		for (int i = 0; i < classCapacities.Length; i++)
		{
			tasks[i] = new Task(t =>
			{
				AlignmentData.ClassCapacity cc = (AlignmentData.ClassCapacity)t;
				ClassDescriptor classDesc = modelDesc.GetClass(cc.Id);
				if (classDesc != null)
				{
					TTTrace.Write(engine.TraceId, id, classDesc.Id, cc.Capacity);
					Class @class = GetClass(classDesc.Index).MainClass;
					if (@class != null)
						@class.Resize(cc.Capacity);
				}

			}, classCapacities[i]);
			tasks[i].Start();
		}

		Task.WaitAll(tasks);
	}

	public void MarkAsAligned(List<long> ids, short classId)
	{
		TTTrace.Write(engine.TraceId, id, classId);
		trace.Verbose("Marking class {0} in database {1} as aligned.", classId, id);

		ClassDescriptor classDesc = modelDesc.GetClass(classId);
		if (classDesc == null)
			return;

		Class @class = GetClass(classDesc.Index).MainClass;
		for (int i = 0; i < ids.Count; i++)
		{
			@class.MarkAsAligned(ids[i]);
		}
	}

	public Changeset[] GenerateAlignmentDeletes(IEnumerable<short> affectedClasses, JobWorkers<CommonWorkerParam> workers)
	{
		List<Changeset> changesets = new List<Changeset>();

		workers.SetAction(p =>
		{
			ChangesetWriter writer = null;
			try
			{
				ObjectReader[] objects = new ObjectReader[128];
				ClassScan scan = (ClassScan)p.ReferenceParam;
				Class @class = scan.Class;
				int count = objects.Length;
				while (scan.Next(objects, 0, ref count))
				{
					for (int i = 0; i < count; i++)
					{
						@class.GenerateAlignmentDeletes(objects[i].ClassObject, ref writer);
					}

					count = objects.Length;
				}

				if (writer != null)
				{
					TTTrace.Write(engine.TraceId, id, @class.ClassDesc.Id);
					Changeset ch = writer.FinishWriting();
					lock (changesets)
					{
						changesets.Add(ch);
					}
				}
			}
			finally
			{
				if (writer != null)
					Engine.ChangesetWriterPool.Put(writer);
			}
		});

		foreach (short classId in affectedClasses)
		{
			trace.Verbose("Generating alignment deletes for class {0} in database {1}.", classId, id);

			ClassDescriptor classDesc = modelDesc.GetClass(classId);
			if (classDesc == null)
				continue;

			TTTrace.Write(engine.TraceId, id, classId);

			Class @class = GetClass(classDesc.Index).MainClass;
			ClassScan[] scans = @class.GetClassScans(null, false, out long totalCount);
			for (int i = 0; i < scans.Length; i++)
			{
				workers.EnqueueWork(new CommonWorkerParam() { ReferenceParam = scans[i] });
			}
		}

		workers.Drain();

		return changesets.ToArray();
	}

	public void DropAndDispose()
	{
		TTTrace.Write(engine.TraceId, id);
		trace.Debug("Dropping database {0}.", id);
		Dispose(true);
	}

	public void CreatePersister()
	{
		TTTrace.Write(engine.TraceId, id);

		persister?.Dispose();
		persister = null;

		if (persistenceDesc == null)
			return;

		LogState[] logStates = new LogState[persistenceDesc.LogDescriptors.Length];
		for (int i = 0; i < logStates.Length; i++)
		{
			logStates[i] = DatabaseRestorer.CreateEmptyLog(this, persistenceDesc.LogDescriptors[i], i);
		}

		persister = new DatabasePersister(this, persistenceDesc, engine.SnapshotController, versions.CommitVersion, versions.LogSeqNum, logStates);
	}

	public LogState[] ReplacePersister(PersistenceUpdate update)
	{
		TTTrace.Write(engine.TraceId, id);

		LogState[] logStates = new LogState[update.PersistenceDescriptor.LogDescriptors.Length];
		for (int i = 0; i < logStates.Length; i++)
		{
			logStates[i] = DatabaseRestorer.CreateEmptyLog(this, update.PersistenceDescriptor.LogDescriptors[i], i);
		}

		persister?.Dispose();
		persister = new DatabasePersister(this, update.PersistenceDescriptor, engine.SnapshotController, versions.CommitVersion, versions.LogSeqNum, logStates);
		persister.CreateSnapshots();

		persistenceDesc = update.PersistenceDescriptor;
		return logStates;
	}

	public void MovePersistenceFromTempLocation()
	{
		persister.MovePersistenceFromTempLocation();
	}

	public void CreateSnapshots(IEnumerable<int> logIndexes)
	{
		persister.CreateSnapshots(logIndexes);
	}

	public void DeletePersistenceFiles(PersistenceDescriptor persistenceDesc)
	{
		TTTrace.Write(engine.TraceId, id);

		for (int i = 0; i < persistenceDesc.LogDescriptors.Length; i++)
		{
			LogDescriptor logDesc = persistenceDesc.LogDescriptors[i];
			LogPersister.GetLogNames(engine, persistenceDesc.LogDescriptors[i], out string[] logFiles);
			LogPersister.GetSnapshotNames(engine, persistenceDesc.LogDescriptors[i], out string[] snapshotFiles);

			Utils.ForEach(logFiles, x => File.Delete(x));
			Utils.ForEach(snapshotFiles, x => File.Delete(x));
		}
	}

	public void UpdatePersistenceConfiguration(PersistenceUpdate update)
	{
		TTTrace.Write(engine.TraceId, id);
		trace.Debug("Updating persistence configuration in database {0}.", id);
		persister.UpdateConfiguration(update);
	}

	public ModelUpdateContext ApplyModelUpdate(DataModelUpdate modelUpdate, ulong commitVersion)
	{
		TTTrace.Write(engine.TraceId, id);

		ModelUpdateContext context = new ModelUpdateContext(this, modelUpdate);

		context.Validate();
		context.ExecuteUpdate(commitVersion);

		DataModelUpdate update = context.ModelUpdate;
		modelDesc = update.ModelDesc;

		ClassEntry[] newClasses = new ClassEntry[modelDesc.ClassCount];
		for (int i = 0; i < modelDesc.ClassCount; i++)
		{
			ClassDescriptor classDesc = modelDesc.GetClassByIndex(i);
			ClassDescriptor prevClassDesc = update.PrevModelDesc.GetClass(classDesc.Id);

			if (!context.TryGetNewClass(classDesc.Id, out ClassBase @class, out ClassLocker locker))
			{
				@class = classes[prevClassDesc.Index].Class;
				locker = classes[prevClassDesc.Index].Locker;

				if (modelUpdate.UpdatedClasses.TryGetValue(classDesc.Id, out ClassUpdate cu) && cu.IsHierarchyTypeModified)
				{
					@class = ClassBase.ChangeHierarchyType(@class);
				}
			}

			if (!context.TryGetNewInverseReferenceMap(classDesc.Id, out InverseReferenceMap invRefMap) && prevClassDesc != null)
			{
				invRefMap = classes[prevClassDesc.Index].InvRefMap;
				if (invRefMap != null && invRefMap.Disposed)
					invRefMap = null;
			}

			@class.ModelUpdated(classDesc);
			locker?.ModelUpdated((ushort)classDesc.Index);
			invRefMap?.ModelUpdated(classDesc);
			newClasses[i] = new ClassEntry(@class, locker, invRefMap);
		}

		for (int i = 0; i < modelDesc.ClassCount; i++)
		{
			InheritedClass inhClass = newClasses[i].Class as InheritedClass;
			if (inhClass != null)
				inhClass.SetInheritedClasses(newClasses);
		}

		HashIndexEntry[] newHashIndexes = new HashIndexEntry[modelDesc.HashIndexCount];
		Dictionary<short, int> newHashIndexesMap = new Dictionary<short, int>(modelDesc.HashIndexCount);
		HashReadersCollection newHashReaders = context.NewHashReaders ?? hashReaders;
		for (int i = 0; i < modelDesc.HashIndexCount; i++)
		{
			HashIndexDescriptor hindDesc = modelDesc.GetHashIndexByIndex(i);
			HashIndexDescriptor prevHindDesc = update.PrevModelDesc.GetHashIndex(hindDesc.Id);

			if (!context.TryGetNewHashIndex(hindDesc.Id, out HashIndex hashIndex, out HashKeyReadLocker locker))
			{
				hashIndex = hashIndexes[prevHindDesc.Index].Index;
				locker = hashIndexes[prevHindDesc.Index].Locker;
			}

			if (locker == null)
			{
				locker = hashIndexes[prevHindDesc.Index].Locker;
			}

			Func<HashIndexReaderBase> creator = newHashReaders.GetReaderFactory(hindDesc.Id);
			HashIndexReaderBase reader = creator();
			reader.SetIndex(hashIndex);

			hashIndex.ModelUpdated(hindDesc);
			locker.ModelUpdated(hindDesc);
			newHashIndexes[i] = new HashIndexEntry(hashIndex, locker, reader);
			newHashIndexesMap.Add(hindDesc.Id, hindDesc.Index);
		}

		Func<int, HashIndex> finder = x => newHashIndexes[x].Index;
		for (int i = 0; i < newClasses.Length; i++)
		{
			Class @class = newClasses[i].Class.MainClass;
			@class?.AssignHashIndexes(finder, newHashReaders);
		}

		refValidator.ModelUpdated(modelDesc);
		classes = newClasses;
		hashReaders = newHashReaders;
		hashIndexesMap = newHashIndexesMap;
		hashIndexes = newHashIndexes;

		primaryAlignCodeCache.UpdateCache(context.ModelUpdate);
		standbyAlignCodeCache.UpdateCache(context.ModelUpdate);

		ValidateStructure();

		return context;
	}

	public Transaction CreateTransaction(TransactionType type, out DatabaseVersions versions)
	{
		TTTrace.Write(engine.TraceId, id, (byte)type, this.versions.ReadVersion, this.versions.CommitVersion);

		Transaction tran = new Transaction(this, type);

		tran.Init(ulong.MaxValue, TransactionSource.Internal, null, true);

		while (true)
		{
			versions = this.versions.Clone();
			tran.InitReadVersion(versions.ReadVersion);
			if (gc.TryInsertTransaction(tran))
				break;
		}

		return tran;
	}

	public Transaction CreateTransaction(TransactionType type, TransactionSource source,
		IReplica originReplica, bool allowOtherWriteTransactions)
	{
		TTTrace.Write(engine.TraceId, id, (byte)type, (byte)source, this.versions.ReadVersion, this.versions.CommitVersion);

		Transaction tran = new Transaction(this, type);

		ulong tranId = ulong.MaxValue;
		if (type == TransactionType.ReadWrite)
			tranId = GetNextTranId();

		while (true)
		{
			tran.InitReadVersion(versions.ReadVersion);
			if (gc.TryInsertTransaction(tran))
				break;
		}

		TTTrace.Write(engine.TraceId, id, tranId, tran.ReadVersion);
		tran.Init(tranId, source, originReplica, allowOtherWriteTransactions);

		return tran;
	}

	public void TransactionCompleted(Transaction tran)
	{
		TTTrace.Write(engine.TraceId, id, tran.Id);
		gc.TransactionCompleted(tran);
	}

	public void ProcessGarbage(Transaction tran)
	{
		Checker.AssertTrue(tran.Type == TransactionType.ReadWrite);
		gc.ProcessGarbage(tran);
	}

	public void BeginDatabaseAlignment()
	{
		DrainGC();
		Persister?.InitSequence(versions.LogSeqNum);
	}

	public void EndDatabaseAlignment(ModelUpdateContext modelUpdateContext)
	{
		RefillPendingIndexes(modelUpdateContext?.Workers);
		versions.TTTraceState();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ClassBase GetClassById(short id)
	{
		ClassDescriptor classDesc = modelDesc.GetClass(id);
		return classes[classDesc.Index].Class;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ClassBase GetClass(int index)
	{
		return classes[index].Class;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ClassBase GetClass(int index, out ClassLocker locker)
	{
		locker = classes[index].Locker;
		return classes[index].Class;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ClassLocker GetClassLocker(int index)
	{
		return classes[index].Locker;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public InverseReferenceMap GetInvRefs(int index)
	{
		return classes[index].InvRefMap;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public HashIndex GetHashIndex(short hashIndexId, out HashKeyReadLocker locker)
	{
		if (!hashIndexesMap.TryGetValue(hashIndexId, out int index))
		{
			locker = null;
			return null;
		}

		locker = hashIndexes[index].Locker;
		return hashIndexes[index].Index;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public HashKeyReadLocker GetHashIndexLocker(int index)
	{
		return hashIndexes[index].Locker;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool IsCommited(ulong version)
	{
		return version <= MaxCommitedVersion;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool IsUncommited(ulong version)
	{
		return version > MaxCommitedVersion;
	}

	internal bool HasTransactions()
	{
		return gc.HasActiveTransactions();
	}

	public void CancelAllTransactions()
	{
		TTTrace.Write(engine.TraceId);
		trace.Debug("Cancelling all transactions in database {0}.", id);
		gc.CancelTransactions();
	}

	private void InitCurrTranIds()
	{
		currTransIds = (ulong*)CacheLineMemoryManager.Allocate(sizeof(ulong), out currTransIdsHandle, false);
		ulong delta = (MaxTranId - MinTranId) / (ulong)ProcessorNumber.CoreCount;
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			ulong* curr = (ulong*)CacheLineMemoryManager.GetBuffer(currTransIds, i);
			*curr = MinTranId + (ulong)i * delta;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ulong GetNextTranId()
	{
		int procNum = ProcessorNumber.GetCore();
		NativeInterlocked64* curr = (NativeInterlocked64*)CacheLineMemoryManager.GetBuffer(currTransIds, procNum);
		return (ulong)curr->Increment();
	}

	public HashIndexReaderBase<TKey1> GetHashIndexReader<TKey1>(short hashIndexId)
	{
		TTTrace.Write(hashIndexId);

		if (hashIndexesMap.TryGetValue(hashIndexId, out int index))
		{
			HashIndexEntry entry = hashIndexes[index];
			HashIndexReaderBase<TKey1> reader = entry.Reader as HashIndexReaderBase<TKey1>;
			if (reader == null)
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.IndexPropertyWrongType));

			return reader;
		}

		return null;
	}

	public HashIndexReaderBase<TKey1, TKey2> GetHashIndexReader<TKey1, TKey2>(short hashIndexId)
	{
		TTTrace.Write(hashIndexId);

		if (hashIndexesMap.TryGetValue(hashIndexId, out int index))
		{
			HashIndexEntry entry = hashIndexes[index];
			HashIndexReaderBase<TKey1, TKey2> reader = entry.Reader as HashIndexReaderBase<TKey1, TKey2>;
			if (reader == null)
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.IndexPropertyWrongType));

			return reader;
		}

		return null;
	}

	public HashIndexReaderBase<TKey1, TKey2, TKey3> GetHashIndexReader<TKey1, TKey2, TKey3>(short hashIndexId)
	{
		TTTrace.Write(hashIndexId);

		if (hashIndexesMap.TryGetValue(hashIndexId, out int index))
		{
			HashIndexEntry entry = hashIndexes[index];
			HashIndexReaderBase<TKey1, TKey2, TKey3> reader = entry.Reader as HashIndexReaderBase<TKey1, TKey2, TKey3>;
			if (reader == null)
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.IndexPropertyWrongType));

			return reader;
		}

		return null;
	}

	public HashIndexReaderBase<TKey1, TKey2, TKey3, TKey4> GetHashIndexReader<TKey1, TKey2, TKey3, TKey4>(short hashIndexId)
	{
		TTTrace.Write(hashIndexId);

		if (hashIndexesMap.TryGetValue(hashIndexId, out int index))
		{
			HashIndexEntry entry = hashIndexes[index];
			HashIndexReaderBase<TKey1, TKey2, TKey3, TKey4> reader = entry.Reader as HashIndexReaderBase<TKey1, TKey2, TKey3, TKey4>;
			if (reader == null)
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.IndexPropertyWrongType));

			return reader;
		}

		return null;
	}

	public void CreateHashIndexes(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(engine.TraceId);
		trace.Debug("Creating hash indexes for database {0}.", id);

		Checker.AssertTrue(hashIndexesMap == null);

		Dictionary<short, long> expectedCapacities = DetermineExpectedHashIndexCapacities();

		hashIndexesMap = new Dictionary<short, int>(ModelDesc.GetHashIndexCount());
		hashIndexes = new HashIndexEntry[ModelDesc.GetHashIndexCount()];

		foreach (HashIndexDescriptor hind in ModelDesc.GetAllHashIndexes())
		{
			Func<HashIndexReaderBase> creator = hashReaders.GetReaderFactory(hind.Id);

			HashKeyReadLocker locker = new HashKeyReadLocker(hind, this);
			HashIndexReaderBase reader = creator();
			expectedCapacities.TryGetValue(hind.Id, out long cap);
			cap = (long)(cap * 1.2);
			HashIndex hashIndex = new HashIndex(this, hind, cap);
			reader.SetIndex(hashIndex);
			hashIndexes[hind.Index] = new HashIndexEntry(hashIndex, locker, reader);
			hashIndexesMap.Add(hind.Id, hind.Index);
		}

		trace.Verbose("Assigning hash indexes to classes in database {0}.", id);
		AssignClassHashIndexes(workers);
		trace.Verbose("Assigning hash indexes to classes finished.", id);
	}

	private void AssignClassHashIndexes(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(engine.TraceId);

		Func<int, HashIndex> finder = x => hashIndexes[x].Index;

		for (int i = 0; i < classes.Length; i++)
		{
			Class @class = classes[i].Class.MainClass;
			@class?.AssignHashIndexes(finder, hashReaders);
		}

		if (workers == null)
			return;

		Action<CommonWorkerParam>[] actions = new Action<CommonWorkerParam>[workers.WorkerCount];
		for (int i = 0; i < actions.Length; i++)
		{
			ulong[] handles = new ulong[128];
			actions[i] = x =>
			{
				ClassScan scan = (ClassScan)x.ReferenceParam;
				TTTrace.Write(engine.TraceId, id, scan.Class.ClassDesc.Id);
				int count = handles.Length;
				while (scan.NextHandles(handles, 0, ref count))
				{
					for (int j = 0; j < count; j++)
					{
						scan.Class.BuildHashIndexes(handles[j]);
					}

					count = handles.Length;
				}
			};
		}

		workers.SetActions(actions);

		for (int i = 0; i < classes.Length; i++)
		{
			Class @class = classes[i].Class.MainClass;
			if (@class != null)
			{
				ClassScan[] scans = @class.GetClassScans(null, false, out long totalCount);
				for (int j = 0; j < scans.Length; j++)
				{
					workers.EnqueueWork(new CommonWorkerParam() { ReferenceParam = scans[j] });
				}
			}
		}

		workers.Drain();
	}

	public void RefillPendingIndexes(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(engine.TraceId);

		Dictionary<short, long> expectedCapacities = DetermineExpectedHashIndexCapacities(true);
		if (expectedCapacities.Count == 0)
			return;

		for (int i = 0; i < hashIndexes.Length; i++)
		{
			if (hashIndexes[i].Index.PendingRefill)
				hashIndexes[i].Index.PendingRefillStarted(expectedCapacities[hashIndexes[i].Index.HashIndexDesc.Id]);
		}

		Action<CommonWorkerParam>[] actions = new Action<CommonWorkerParam>[workers.WorkerCount];
		for (int i = 0; i < actions.Length; i++)
		{
			ulong[] handles = new ulong[128];
			actions[i] = x =>
			{
				ClassScan scan = (ClassScan)x.ReferenceParam;
				TTTrace.Write(engine.TraceId, id, scan.Class.ClassDesc.Id);
				int count = handles.Length;
				while (scan.NextHandles(handles, 0, ref count))
				{
					for (int j = 0; j < count; j++)
					{
						scan.Class.BuildHashIndexes(handles[j], true);
					}

					count = handles.Length;
				}
			};
		}

		workers.SetActions(actions);

		for (int i = 0; i < classes.Length; i++)
		{
			Class @class = classes[i].Class.MainClass;
			if (@class != null && @class.HasPendingRefillIndexes())
			{
				ClassScan[] scans = @class.GetClassScans(null, false, out long totalCount);
				for (int j = 0; j < scans.Length; j++)
				{
					workers.EnqueueWork(new CommonWorkerParam() { ReferenceParam = scans[j] });
				}
			}
		}

		workers.Drain();

		for (int i = 0; i < hashIndexes.Length; i++)
		{
			if (hashIndexes[i].Index.PendingRefill)
				hashIndexes[i].Index.PendingRefillFinished();
		}
	}

	private Dictionary<short, long> DetermineExpectedHashIndexCapacities(bool pendingRefillOnly = false)
	{
		TTTrace.Write(engine.TraceId);

		Dictionary<short, long> r = new Dictionary<short, long>(32);
		for (int i = 0; i < classes.Length; i++)
		{
			ClassDescriptor classDesc = classes[i].Class.ClassDesc;
			for (int j = 0; j < classDesc.HashIndexes.Length; j++)
			{
				HashIndexDescriptor hindDesc = classDesc.HashIndexes[j];
				if (pendingRefillOnly && !hashIndexes[hindDesc.Index].Index.PendingRefill)
					continue;

				r.TryGetValue(hindDesc.Id, out long c);
				c += classes[i].Class.MainClass.EstimatedObjectCount;
				r[hindDesc.Id] = c;
			}
		}

		return r;
	}

	public void CreateInvRefs(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(engine.TraceId, id);
		trace.Debug("Creating inverse references in database {0}.", id);

		for (int i = 0; i < ModelDesc.ClassCount; i++)
		{
			ClassDescriptor cd = ModelDesc.GetClassByIndex(i);
			if (!cd.IsAbstract && cd.InverseReferences.Length > 0)
			{
				InverseReferenceMap map = new InverseReferenceMap(this, cd);
				classes[i].SetInverseRefs(map);
			}
		}

		if (workers != null)
		{
			InverseReferenceBuilder builder = new InverseReferenceBuilder(this);
			builder.Build(workers);
		}
	}

	private void CreateClasses()
	{
		TTTrace.Write(engine.TraceId, id);
		trace.Debug("Creating classes in database {0}.", id);

		classes = new ClassEntry[ModelDesc.ClassCount];
		for (int i = 0; i < ModelDesc.ClassCount; i++)
		{
			ClassDescriptor classDesc = ModelDesc.GetClassByIndex(i);

			long capacity = this.id == DatabaseId.User ? engine.Settings.InitClassSize : 128;
			ClassBase @class = ClassBase.CreateEmptyClass(this, classDesc, capacity);
			ClassLocker locker = classDesc.IsAbstract ? null : new ClassLocker(engine, (ushort)classDesc.Index);
			classes[i] = new ClassEntry(@class, locker);
		}

		for (int i = 0; i < ModelDesc.ClassCount; i++)
		{
			InheritedClass inhClass = classes[i].Class as InheritedClass;
			if (inhClass != null)
				inhClass.SetInheritedClasses(classes);
		}
	}

	public void DrainGC()
	{
		TTTrace.Write(engine.TraceId, id);
		gc.Flush();
		gc.Drain();
	}

	public IdSet CollectAlignmentIds(ulong commonVersion)
	{
		TTTrace.Write(engine.TraceId, id, commonVersion);
		trace.Debug("Collecting alignment ids in database {0}.", id);

		long idCount = 0;
		for (int i = 0; i < ModelDesc.ClassCount; i++)
		{
			Class @class = GetClass(i).MainClass;
			if (@class != null)
				idCount += @class.EstimatedObjectCount;
		}

		idCount = (long)(idCount * 1.05);
		IdSet set = new IdSet(idCount);

		ObjectReader[] objects = new ObjectReader[128];
		for (int i = 0; i < ModelDesc.ClassCount; i++)
		{
			Class @class = GetClass(i).MainClass;
			if (@class == null)
				continue;

			ClassScan[] scans = @class.GetClassScans(null, false, out long totalCount);
			for (int j = 0; j < scans.Length; j++)
			{
				ClassScan scan = scans[j];

				int count = objects.Length;
				while (scan.Next(objects, 0, ref count))
				{
					for (int k = 0; k < count; k++)
					{
						TTTrace.Write(TraceId, commonVersion, objects[k].GetVersionOptimized(), objects[k].GetIdOptimized());
						if (objects[k].GetVersionOptimized() <= commonVersion)
						{
							set.Add(objects[k].GetIdOptimized());
						}
					}

					count = objects.Length;
				}
			}
		}

		trace.Debug("Alignment ids collected in database {0}.", id);

		return set;
	}

	public void AutosnapshotIfNeeded()
	{
		if (persister != null && engine.Settings.AutoSnapshotOnShutdown && id == DatabaseId.User)
		{
			TTTrace.Write(engine.TraceId, id);
			trace.Info("Creating automatic snapshot...");
			persister.CreateSnapshots();
		}
	}

	public void Dispose()
	{
		Dispose(false);
	}

	private void Dispose(bool dropData)
	{
		TTTrace.Write(engine.TraceId);
		trace.Debug("Disposing database {0}.", id);

		gc.Dispose();

		CacheLineMemoryManager.Free(currTransIdsHandle);

		int workerCount = Engine.Settings.AllowInternalParallelization ? ProcessorNumber.CoreCount : 1;
		string workerName = string.Format("{0}: vlx-DatabaseDisposeWorker", Engine.Trace.Name);
		JobWorkers<CommonWorkerParam> workers = JobWorkers<CommonWorkerParam>.Create(workerName, workerCount);

		for (int i = 0; i < classes.Length; i++)
		{
			classes[i].Dispose(workers);
		}

		if (hashIndexes != null)
		{
			foreach (HashIndexEntry item in hashIndexes)
			{
				item.Dispose(workers);
			}
		}

		workers.WaitAndClose();

		if (persister != null)
		{
			if (dropData)
			{
				persister.DropAndDispose();
			}
			else
			{
				persister.Dispose();
			}
		}
	}

	public struct ClassEntry
	{
		ClassBase @class;
		InverseReferenceMap invRefMap;
		ClassLocker locker;

		public ClassEntry(ClassBase @class, ClassLocker locker)
		{
			this.@class = @class;
			this.invRefMap = null;
			this.locker = locker;
		}

		public ClassEntry(ClassBase @class, ClassLocker locker, InverseReferenceMap invRefMap)
		{
			this.@class = @class;
			this.invRefMap = invRefMap;
			this.locker = locker;
		}

		public ClassBase Class => @class;

		public InverseReferenceMap InvRefMap => invRefMap;

		public ClassLocker Locker => locker;

		public void SetInverseRefs(InverseReferenceMap invRefMap)
		{
			this.invRefMap = invRefMap;
		}

		public void DestroyInverseRefs(JobWorkers<CommonWorkerParam> workers)
		{
			if (invRefMap != null)
			{
				invRefMap.Dispose(workers);
				invRefMap = null;
			}
		}

		public void Dispose(JobWorkers<CommonWorkerParam> workers)
		{
			@class.Dispose(workers);

			if (locker != null)
				locker.Dispose();

			if (invRefMap != null)
				invRefMap.Dispose(workers);
		}
	}

	internal struct HashIndexEntry
	{
		HashIndex index;
		HashKeyReadLocker locker;
		HashIndexReaderBase reader;

		public HashIndexEntry(HashIndex index, HashKeyReadLocker locker, HashIndexReaderBase reader)
		{
			this.index = index;
			this.locker = locker;
			this.reader = reader;
		}

		public HashIndex Index => index;
		public HashIndexReaderBase Reader => reader;
		public HashKeyReadLocker Locker => locker;

		public void Dispose(JobWorkers<CommonWorkerParam> workers)
		{
			index.Dispose(workers);
			locker.Dispose();
		}
	}
}
