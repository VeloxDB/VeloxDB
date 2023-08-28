using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage.ModelUpdate;
using VeloxDB.Storage.Persistence;
using VeloxDB.Storage.Replication;
using VeloxDB.Storage.Replication.HighAvailability;

namespace VeloxDB.Storage;

internal unsafe sealed partial class StorageEngine : IDisposable
{
	EngineLock engineLock;

	StorageEngineSettings settings;
	Tracing.Source trace;
	long traceId;
	ulong hashSeed;

	ReplicationInfoPublisher statePublisher;

	MemoryManager memoryManager;
	StringStorage stringStorage;
	BlobStorage blobstorage;
	TransactionContextPool contextPool;
	ChangesetWriterPool changesetWriterPool;

	string systemDbPath;

	Database[] databases;
	IReplicator replicator;

	readonly object commitSync = new object();
	CommitWorkers commitWorkers;

	SortedIndex.GarbageCollectorBase sortedIndexGC;

	SnapshotSemaphore snapshotController;

	ConfigArtifactVersions configVersions;

	bool disposed;

	public StorageEngine(ModelSettings modelSettings, StorageEngineSettings settings = null,
		PersistenceSettings persistanceSettings = null, ReplicationSettings replicationSettings = null,
		ILeaderElector localElector = null, ILeaderElector globalElector = null, Tracing.Source trace = null)
	{
		if (replicationSettings != null && persistanceSettings == null)
			throw new ArgumentException("Database replication without persistence is not allowed.");

		this.settings = settings = (settings == null ? new StorageEngineSettings() : settings.Clone());
		settings.Validate();

		Initialize(replicationSettings, localElector, globalElector, trace);

		CreateOrRestoreDatabases(modelSettings, null, persistanceSettings);
		PublishAssemblies();

		replicator.Start();

		this.trace.Info("Storage engine started.");
	}

	public StorageEngine(string sysDbPath, ReplicationSettings replicationSettings = null,
		ILeaderElector localElector = null, ILeaderElector globalElector = null, Tracing.Source trace = null) :
		this(sysDbPath, new StorageEngineSettings(), replicationSettings, localElector, globalElector, trace)
	{
	}

	public StorageEngine(string sysDbPath, StorageEngineSettings settings, ReplicationSettings replicationSettings = null,
		ILeaderElector localElector = null, ILeaderElector globalElector = null, Tracing.Source trace = null)
	{
		this.settings = settings ?? new StorageEngineSettings();
		this.systemDbPath = sysDbPath;

		Initialize(replicationSettings, localElector, globalElector, trace);

		CreateOrRestoreDatabases(sysDbPath);
		PublishAssemblies();

		replicator.Start();

		this.trace.Info("Storage engine started.");
	}

	public MemoryManager MemoryManager => memoryManager;
	public StringStorage StringStorage => stringStorage;
	public BlobStorage BlobStorage => blobstorage;
	public TransactionContextPool ContextPool => contextPool;
	public ChangesetWriterPool ChangesetWriterPool => changesetWriterPool;
	public StorageEngineSettings Settings => settings;
	public Tracing.Source Trace => trace;
	public long TraceId => traceId;
	public ReplicationDescriptor ReplicationDesc => replicator.ReplicationDesc;
	public Database LocalSystemDatabase => databases[(int)DatabaseId.SystemLocal];
	public Database GlobalSystemDatabase => databases[(int)DatabaseId.SystemGlobal];
	public Database UserDatabase => databases[(int)DatabaseId.User];
	public Database GetDatabase(long id) => databases[id];
	public SimpleGuid AssemblyVersionGuid => configVersions.AssembliesVersionGuid;
	public SimpleGuid ModelVersionGuid => configVersions.ModelVersionGuid;
	public SimpleGuid PersistenceVersionGuid => configVersions.PersistenceVersionGuid;
	public ulong HashSeed => hashSeed;
	public EngineLock EngineLock => engineLock;
	public bool Disposed => disposed;
	public string SystemDbPath => systemDbPath;
	public SnapshotSemaphore SnapshotController => snapshotController;
	public SortedIndex.GarbageCollectorBase SortedIndexGC => sortedIndexGC;

	private void Initialize(ReplicationSettings replicationSettings, ILeaderElector localElector,
		ILeaderElector globalElector, Tracing.Source trace)
	{
		this.trace = trace = trace ?? Tracing.GlobalSource;

		engineLock = new EngineLock(this);

		if (trace.Name != null && replicationSettings != null)
			this.traceId = Math.Abs((long)Utils.GetStableStringHashCode(trace.Name) << 32);

		trace.Debug("Storage engine TimeTravelTrace identifier={0}.", traceId);
		TTTrace.Write(traceId, trace.Name);

		hashSeed = (ulong)RandomNumberGenerator.GetInt32(int.MaxValue) * (ulong)RandomNumberGenerator.GetInt32(int.MaxValue);

		memoryManager = new MemoryManager();
		stringStorage = new StringStorage();
		blobstorage = new BlobStorage(memoryManager);
		sortedIndexGC = SortedIndex.CreateGarbageCollector(memoryManager, traceId);
		statePublisher = new ReplicationInfoPublisher(replicationSettings);
		replicator = CreateReplicator(replicationSettings, localElector, globalElector);
		contextPool = new TransactionContextPool(this);
		changesetWriterPool = new ChangesetWriterPool(memoryManager);

		snapshotController = new SnapshotSemaphore();
	}

	public void SubscribeToStateChanges(Action<DatabaseInfo> handler)
	{
		statePublisher.Subscribe(handler);
	}

	public void PulishLocalWriteElectorChange(bool isWitnessConnected, bool isElectorConnected)
	{
		statePublisher.PublishLocalWriteElectorChange(isWitnessConnected, isElectorConnected);
	}

	public void PublishReplicaStateChange(ReplicaInfo replicaInfo, int index)
	{
		statePublisher.Publish(replicaInfo, index);
	}

	public PersistenceDescriptor GetPersistenceConfiguration()
	{
		ReadUserConfiguration(out _, out PersistenceDescriptor persistenceDesc);
		return persistenceDesc;
	}

	public UserAssembly[] GetUserAssemblies(out SimpleGuid modelVersionGuid,
		out SimpleGuid assemblyVersionGuid, out DataModelDescriptor modelDescriptor)
	{
		UserAssembly[] result = ReadUserAssemblies(out modelVersionGuid, out assemblyVersionGuid, out modelDescriptor);
		return result;
	}

	public ulong UpdateUserAssemblies(AssemblyUpdate assemblyUpdate, DataModelDescriptor newModelDesc,
		object customObj, out SimpleGuid modelVersionGuid, out SimpleGuid assemblyVersionGuid)
	{
		trace.Debug("Updating user assemblies.");

		// We are accessing UserDatabase ModelDesc and PersistenceDesc outside of the lock. This means that the engine might
		// be disposed at this point. However, it is still safe to read model descriptor from the database. This operation
		// will be aborted anyways once we try taking the lock.

		PersistenceDescriptor persistDesc = UserDatabase.PersistenceDesc;
		newModelDesc.AssignLogIndexes(persistDesc);

		modelVersionGuid = ModelVersionGuid;
		DataModelUpdate modelUpdate = new DataModelUpdate(UserDatabase, UserDatabase.ModelDesc, newModelDesc, false);
		bool hasModelChanges = !modelUpdate.IsEmpty;

		PersistenceUpdate persistenceUpdate = null;
		SimpleGuid persistenceVersionGuid = PersistenceVersionGuid;
		if (UserDatabase.PersistenceDesc != null)
		{
			persistenceUpdate = new PersistenceUpdate(UserDatabase.PersistenceDesc, null, modelUpdate);
			if (!persistenceUpdate.IsRecreationRequired)
				persistenceUpdate = null;
		}

		TTTrace.Write(traceId, persistenceUpdate != null, modelUpdate.ModelDesc.LastUsedClassId);

		engineLock.EnterWriteLock(true);

		ulong commitVersion;

		try
		{
			if (disposed)
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.DatabaseDisposed));

			if (!replicator.IsTransactionAllowed(DatabaseId.User, TransactionSource.Client, null, TransactionType.ReadWrite, out var err))
				throw new DatabaseException(err);

			UserDatabase.DrainGC();

			if (UserDatabase.PersistenceDesc != null && !PersistenceVersionGuid.Equals(persistenceVersionGuid) ||
				!ModelVersionGuid.Equals(modelVersionGuid))
			{
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.ConcurrentConfigUpdate));
			}

			using (Transaction tran = CreateTransaction(DatabaseId.User, TransactionType.Read, TransactionSource.Client, null, true))
			{
				ValidateAssemblyUpdate(tran, assemblyUpdate);
			}

			commitVersion = UserDatabase.Versions.CommitVersion + 1;
			if (hasModelChanges)
				UserDatabase.ApplyModelUpdate(modelUpdate, commitVersion, false);

			using (Transaction tran = CreateTransaction(DatabaseId.User, TransactionType.ReadWrite, TransactionSource.Client, null, true))
			{
				if (modelUpdate.RequiresDefaultValueWrites)
					ApplyDefaultValues(tran, modelUpdate);

				if (modelUpdate.HasClassesBecomingAbstract)
					ApplyClassDrops(tran, modelUpdate);

				ApplyAssemblyChanges(tran, assemblyUpdate, newModelDesc, hasModelChanges,
					out SimpleGuid asmVersionGuid, out modelVersionGuid);

#if TEST_BUILD
				UpdateUserModelAction?.Invoke(this, tran, modelVersionGuid);
#endif


				if (persistenceUpdate != null)
				{
					persistenceVersionGuid = SimpleGuid.NewValue();
					ApplyPersistenceChanges(tran, persistenceUpdate.PersistenceDescriptor, persistenceVersionGuid);
				}

				UserAssembly[] assemblies = ReadUserAssemblies(tran).ToArray();

				CommitTransactionDirect(tran);

				configVersions.AssembliesVersionGuid = asmVersionGuid;
				assemblyVersionGuid = asmVersionGuid;
				if (hasModelChanges)
					configVersions.ModelVersionGuid = modelVersionGuid;

				statePublisher.Publish(assemblies, modelVersionGuid, asmVersionGuid, newModelDesc, customObj);
			}

			if (persistenceUpdate != null)
			{
				persistenceUpdate.MarkDirectoriesAsTemp();
				UserDatabase.ReplacePersister(persistenceUpdate);
				persistenceUpdate.UnmarkDirectoriesAsTemp();

				if (persistenceUpdate.PrevPersistenceDescriptor != null)
					Database.DeletePersistenceFiles(this, persistenceUpdate.PrevPersistenceDescriptor);

				UserDatabase.MovePersistenceFromTempLocation();
				configVersions.PersistenceVersionGuid = persistenceVersionGuid;
			}
		}
		finally
		{
			engineLock.ExitWriteLock(true);
		}

		TTTrace.Write(TraceId);
		Trace.Info("User assemblies updated.");
		return commitVersion;
	}

	public void ReplicatedUpdateUserModel(DataModelUpdate modelUpdate, ulong commitVersion, bool retainUpdateContext)
	{
		TTTrace.Write(traceId, modelUpdate.ModelDesc.LastUsedClassId);

		UserDatabase.DrainGC();

		try
		{
			UserDatabase.ApplyModelUpdate(modelUpdate, commitVersion, retainUpdateContext);
		}
		catch (DatabaseException e)
		{
			// Replicated model update should never be invalid
			throw new CriticalDatabaseException("Invalid replicated model update.", e);
		}
	}

	public void DropUserDatabase()
	{
		TTTrace.Write(traceId);
		UserDatabase.Drop();
		configVersions = new ConfigArtifactVersions();
	}

	public void UpdatePersistenceConfiguration(LogDescriptor[] logDescriptors)
	{
		TTTrace.Write(traceId);

		// Because we are retaining the descriptor internally we clone it
		PersistenceDescriptor persistenceDesc = new PersistenceDescriptor(logDescriptors, DatabaseId.User).Clone();

		// We are accessing UserDatabase.PersistenceDesc outside of lock. This means that the engine might be disposed at this point.
		// However, it is still safe to read persistance descriptor from the database. This operation will be aborted anyways
		// once we try taking the lock.
		PersistenceUpdate update = new PersistenceUpdate(UserDatabase.PersistenceDesc, persistenceDesc, null);

		SimpleGuid persistenceVersionGuid = new SimpleGuid();

		engineLock.EnterWriteLock(true);

		try
		{
			if (disposed)
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.DatabaseDisposed));

			if (!replicator.IsTransactionAllowed(DatabaseId.User, TransactionSource.Client, null, TransactionType.ReadWrite, out var err))
				throw new DatabaseException(err);

			ValidatePersistenceUpdate(update);

			if (update.IsRecreationRequired)
			{
				persistenceVersionGuid = SimpleGuid.NewValue();
				update.MarkDirectoriesAsTemp();
				UserDatabase.ReplacePersister(update);
				update.UnmarkDirectoriesAsTemp();
			}
			else
			{
				persistenceVersionGuid = PersistenceVersionGuid;
				UserDatabase.UpdatePersistenceConfiguration(update);
			}

			using (Transaction tran = CreateTransaction(DatabaseId.User, TransactionType.ReadWrite, TransactionSource.Client, null, true))
			{
				ApplyPersistenceChanges(tran, update.PersistenceDescriptor, persistenceVersionGuid);
				CommitTransactionDirect(tran);
				configVersions.PersistenceVersionGuid = persistenceVersionGuid;
			}

			if (update.IsRecreationRequired)
			{
				if (update.PrevPersistenceDescriptor != null)
					Database.DeletePersistenceFiles(this, update.PrevPersistenceDescriptor);

				UserDatabase.MovePersistenceFromTempLocation();
			}
		}
		finally
		{
			engineLock.ExitWriteLock(true);
		}

		Trace.Info("Persistence configuration updated.");
	}

	public PersistenceUpdate ReplicatedUpdatePersistenceConfiguration(PersistenceDescriptor persistenceDesc, DataModelUpdate modelUpdate)
	{
		TTTrace.Write(traceId, modelUpdate != null);

		PersistenceUpdate persistenceUpdate = new PersistenceUpdate(UserDatabase.PersistenceDesc, persistenceDesc, modelUpdate);
		if (persistenceDesc == null && !persistenceUpdate.IsRecreationRequired)
			return null;

		try
		{
			ValidatePersistenceUpdate(persistenceUpdate);
		}
		catch (DatabaseException e)
		{
			throw new CriticalDatabaseException("Persistence descriptor is invalid on a replicated node.", e);
		}

		return persistenceUpdate;
	}

	public void ReplicatedPostUpdateConfiguration(bool userAssembliesModified, PersistenceUpdate persistenceUpdate)
	{
		TTTrace.Write(traceId, userAssembliesModified, persistenceUpdate != null);

		if (persistenceUpdate != null)
		{
			if (persistenceUpdate.IsRecreationRequired)
			{
				persistenceUpdate.MarkDirectoriesAsTemp();
				UserDatabase.ReplacePersister(persistenceUpdate);
				persistenceUpdate.UnmarkDirectoriesAsTemp();
			}
			else
			{
				UserDatabase.UpdatePersistenceConfiguration(persistenceUpdate);
			}
		}

		configVersions = ReadConfigArtifactVersions();

		if (persistenceUpdate != null && persistenceUpdate.IsRecreationRequired)
		{
			if (persistenceUpdate.PrevPersistenceDescriptor != null)
				Database.DeletePersistenceFiles(this, persistenceUpdate.PrevPersistenceDescriptor);

			UserDatabase.MovePersistenceFromTempLocation();
		}

		if (userAssembliesModified)
		{
			PublishAssemblies();
			Trace.Info("User assemblies updated through replication.");
		}
	}

	public IHashIndexReader<TKey1> GetHashIndex<TKey1>(short indexId)
	{
		return databases[(int)DatabaseId.User].GetHashIndexReader<TKey1>(indexId);
	}

	public IHashIndexReader<TKey1, TKey2> GetHashIndex<TKey1, TKey2>(short indexId)
	{
		return databases[(int)DatabaseId.User].GetHashIndexReader<TKey1, TKey2>(indexId);
	}

	public IHashIndexReader<TKey1, TKey2, TKey3> GetHashIndex<TKey1, TKey2, TKey3>(short indexId)
	{
		return databases[(int)DatabaseId.User].GetHashIndexReader<TKey1, TKey2, TKey3>(indexId);
	}

	public IHashIndexReader<TKey1, TKey2, TKey3, TKey4> GetHashIndex<TKey1, TKey2, TKey3, TKey4>(short indexId)
	{
		return databases[(int)DatabaseId.User].GetHashIndexReader<TKey1, TKey2, TKey3, TKey4>(indexId);
	}

	public ISortedIndexReader<TKey1> GetSortedIndex<TKey1>(short indexId)
	{
		return databases[(int)DatabaseId.User].GetSortedIndexReader<TKey1>(indexId);
	}

	public ISortedIndexReader<TKey1, TKey2> GetSortedIndex<TKey1, TKey2>(short indexId)
	{
		return databases[(int)DatabaseId.User].GetSortedIndexReader<TKey1, TKey2>(indexId);
	}

	public ISortedIndexReader<TKey1, TKey2, TKey3> GetSortedIndex<TKey1, TKey2, TKey3>(short indexId)
	{
		return databases[(int)DatabaseId.User].GetSortedIndexReader<TKey1, TKey2, TKey3>(indexId);
	}

	public ISortedIndexReader<TKey1, TKey2, TKey3, TKey4> GetSortedIndex<TKey1, TKey2, TKey3, TKey4>(short indexId)
	{
		return databases[(int)DatabaseId.User].GetSortedIndexReader<TKey1, TKey2, TKey3, TKey4>(indexId);
	}

	public ClassScan BeginClassScan(Transaction tran, ClassDescriptor classDesc)
	{
		TTTrace.Write(traceId, tran.Id, classDesc.Id);

		tran.ValidateUsage();
		if (tran.CancelRequested)
			CheckErrorAndRollback(DatabaseErrorDetail.Create(DatabaseErrorType.TransactionCanceled), tran);

		ClassScan scan = BeginClassScanInternal(tran, classDesc, true, false, out DatabaseErrorDetail error);
		CheckErrorAndRollback(error, tran);

		return scan;
	}

	public ClassScan BeginClassScanInternal(Transaction tran, ClassDescriptor classDesc,
		bool scanInherited, bool forceNoReadLock, out DatabaseErrorDetail error)
	{
		TTTrace.Write(traceId, tran.Id, classDesc.Id, scanInherited, forceNoReadLock);

		trace.Verbose("Creating class scan for class {0}.", classDesc.FullName);

		ClassBase @class = tran.Database.GetClass(classDesc.Index, out ClassLocker locker);

		if (!forceNoReadLock && tran.Type == TransactionType.ReadWrite &&
			!ClassIndexMultiSet.Contains(tran.Context.LockedClasses, (ushort)classDesc.Index))
		{
			error = @class.TakeReadLock(tran, locker);
			if (error != null)
				return null;
		}

		error = null;
		ClassScan cs = @class.GetClassScan(tran, scanInherited, out long totalCost);
		return cs;
	}

	public ObjectReader GetObject(Transaction tran, long id)
	{
		TTTrace.Write(traceId, tran.Id, id);

		if (!IdHelper.TryGetClassIndex(tran.Model, id, out int classIndex))
			return new ObjectReader();

		tran.ValidateUsage();
		if (tran.CancelRequested)
			CheckErrorAndRollback(DatabaseErrorDetail.Create(DatabaseErrorType.TransactionCanceled), tran);

		ClassBase @class = tran.Database.GetClass(classIndex);

		ObjectReader reader = @class.GetObject(tran, id, out DatabaseErrorDetail error);
		CheckErrorAndRollback(error, tran);

		return reader;
	}

	public void GetInverseReferences(Transaction tran, long id, int propId, ref long[] refs, out int count)
	{
		TTTrace.Write(traceId, tran.Id, id, propId);

		tran.ValidateUsage();
		if (tran.CancelRequested)
			CheckErrorAndRollback(DatabaseErrorDetail.Create(DatabaseErrorType.TransactionCanceled), tran);

		ClassDescriptor classDesc = IdHelper.GetClass(tran.Model, id);
		if (classDesc == null)
		{
			count = 0;
			return;
		}

		InverseReferenceMap invRefMap = tran.Database.GetInvRefs(classDesc.Index);
		if (invRefMap == null)
			CheckErrorAndRollback(DatabaseErrorDetail.CreateInverseReferenceNotTracked(propId, classDesc.FullName), tran);

		DatabaseErrorDetail error = invRefMap.GetReferences(tran, id, propId, ref refs, out count, out bool isTracked);

		if (!isTracked)
			error = DatabaseErrorDetail.CreateInverseReferenceNotTracked(propId, classDesc.FullName);

		CheckErrorAndRollback(error, tran);
	}

	public void ReadHashIndex(Transaction tran, HashIndex hashIndex, byte* key,
		KeyComparer comparer, string[] requestStrings, ref ObjectReader[] readers, out int count)
	{
		tran.ValidateUsage();
		if (tran.CancelRequested)
			CheckErrorAndRollback(DatabaseErrorDetail.Create(DatabaseErrorType.TransactionCanceled), tran);

		DatabaseErrorDetail error = hashIndex.GetItems(tran, key, comparer, requestStrings, ref readers, out count);
		CheckErrorAndRollback(error, tran);
	}

	public long ReserveIdRange(long count)
	{
		return UserDatabase.IdGenerator.TakeRange(count);
	}

	public Transaction CreateTransaction(TransactionType type)
	{
		return CreateTransaction(type, TransactionSource.Client, null, true);
	}

	public Transaction CreateTransaction(TransactionType type, bool allowOtherTrans)
	{
		return CreateTransaction(type, TransactionSource.Client, null, allowOtherTrans);
	}

	public Transaction CreateTransaction(TransactionType type, TransactionSource source,
		IReplica originReplica, bool allowOtherWriteTransactions = true)
	{
		return CreateTransaction(DatabaseId.User, type, source, originReplica, allowOtherWriteTransactions);
	}

	public Transaction CreateTransaction(long databaseId, TransactionType type, TransactionSource source,
		IReplica originReplica, bool allowOtherWriteTransactions = true, SimpleGuid? requiredUserModelVersion = null)
	{
		if (databaseId == DatabaseId.User && requiredUserModelVersion.HasValue && !UserDatabase.PersistenceDesc.HasNonMasterLogs)
			throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.MissingPersistanceDescriptor));

		// We are preparing to do some work in this thread (in the transaction) so this is a good
		// opportunity to refresh current executing physical CPU
		ProcessorNumber.Refresh();

		if (!engineLock.TryCreateTransactionAndEnterReadLock(out int handle))
			throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.DatabaseBusy));

		try
		{
			if (disposed)
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.DatabaseDisposed));

			if (!replicator.IsTransactionAllowed(databaseId, source, originReplica, type, out DatabaseErrorDetail error))
				throw new DatabaseException(error);

			if (requiredUserModelVersion.HasValue && !requiredUserModelVersion.Value.Equals(configVersions.ModelVersionGuid))
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.InvalidModelDescVersion));

			return databases[databaseId].CreateTransaction(type, source, originReplica, allowOtherWriteTransactions);
		}
		finally
		{
			engineLock.ExitReadLock(handle);
		}
	}

	public void RollbackTransaction(Transaction tran)
	{
		TTTrace.Write(traceId, tran.Id, (byte)tran.Type);

		if (tran.Closed)
			return;

		tran.ValidateUsage();

		tran.Database.TransactionCompleted(tran);

		if (tran.Type != TransactionType.Read)
		{
			Checker.AssertFalse(tran.IsAlignment);
			RollbackModifications(tran);
		}

		tran.Complete(false);
		tran.ClearContext();

		trace.Verbose("Transaction rolled back, tranId={0}.", tran.Id);
	}

	public ulong CommitTransaction(Transaction tran)
	{
		return CommitTransaction(tran, out ulong logSeqNum);
	}

	public ulong CommitTransaction(Transaction tran, out ulong logSeqNum)
	{
		TTTrace.Write(traceId, tran.Id, tran.CommitVersion, (byte)tran.Type);

		tran.ValidateUsage();
		if (tran.Closed)
			throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.CommitClosedTransaction));

		TransactionContext tc = tran.Context;

		if (tran.Type == TransactionType.Read || tran.Source != TransactionSource.Client || tran.Database.Id != DatabaseId.User)
		{
			try
			{
				tc?.CollapseChangesets();
				tran.Database.TransactionCompleted(tran);
				return CommitTransactionInternal(tran, out logSeqNum);
			}
			finally
			{
				tran.ClearContext();
			}
		}

		tc.CollapseChangesets();

		tc.PrepareForAsyncCommit();
		commitWorkers.Commit(tran);
		tc.AsyncCommitWaitEvent.WaitOne();

		DatabaseErrorDetail err = tc.AsyncError;
		logSeqNum = tc.LogSeqNum;
		tran.ClearContext();

		if (err != null)
			throw new DatabaseException(err);

		return tran.CommitVersion;
	}

	private void CommitTransactionDirect(Transaction tran)
	{
		Checker.AssertTrue(tran.Source == TransactionSource.Client);
		try
		{
			tran.Database.TransactionCompleted(tran);
			tran.Context.CollapseChangesets();
			CommitTransactionInternal(tran, out _);
		}
		finally
		{
			tran.ClearContext();
		}
	}

	public void CommitTransactionAsync(Transaction tran, Action<object, DatabaseException> callback, object state)
	{
		Checker.AssertTrue(tran.Database.Id == DatabaseId.User &&
			tran.Type == TransactionType.ReadWrite && tran.Source == TransactionSource.Client);

		tran.ValidateUsage();
		if (tran.Closed)
			throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.CommitClosedTransaction));

		TransactionContext tc = tran.Context;
		tc.CollapseChangesets();
		tran.AsyncCallback = callback;
		tran.AsyncCallbackState = state;

		commitWorkers.Commit(tran);
	}

	public ulong CommitTransactionInternal(Transaction tran, out ulong logSeqNum)
	{
		TTTrace.Write(traceId, tran.Id, tran.CommitVersion, (byte)tran.Type, disposed);

		if (tran.Type == TransactionType.Read)
		{
			tran.Complete(true);
			logSeqNum = ulong.MaxValue;
			return 0;
		}

		Database database = tran.Database;
		TransactionContext tc = tran.Context;
		bool isSuccess = false;

		replicator.PreTransactionCommit(tran);
		int commitHandle = engineLock.EnterReadLock();

		try
		{
#if TEST_BUILD
			tran.DelayCommit();
#endif

			lock (commitSync)
			{
				database.AssignCommitAndLogSeqNum(tran);
				replicator.CommitTransaction(tran);
				database.PersistCommit(tran);
			}

			CommitModifications(tran);

			tran.WaitAsyncCommitters();

#if TEST_BUILD
			tran.DelayInvRefMerge();
#endif

			logSeqNum = tran.LogSeqNum;
			MergeInverseReferences(tran);

			tran.Complete(true);

			isSuccess = tc.AsyncCommitResult;
			if (!isSuccess)
			{
				trace.Verbose("Transaction commit result is unavailable.");
				replicator.TransactionFailed();
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.UnavailableCommitResult));
			}

			return tran.CommitVersion;
		}
		finally
		{
			replicator.PostTransactionCommit(tran, isSuccess);
			engineLock.ExitReadLock(commitHandle);
		}
	}

	public void NodeWriteStateUpdated(bool isMainWriteNode)
	{
		if (isMainWriteNode)
		{
			if (commitWorkers == null)
				commitWorkers = new CommitWorkers(this);
		}
		else
		{
			commitWorkers?.Stop();
			commitWorkers = null;
		}
	}

	public List<Transaction> CreateAlignmentTransactions(TransactionType type,
		ReplicatedDatabases alignedDatabases, out List<DatabaseVersions> versions)
	{
		if (!engineLock.TryCreateTransactionAndEnterReadLock(out int handle))
			throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.DatabaseBusy));

		try
		{
			if (disposed)
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.DatabaseDisposed));

			if (!replicator.IsTransactionAllowed(DatabaseId.User, TransactionSource.Internal, null, TransactionType.Read, out DatabaseErrorDetail error))
				throw new DatabaseException(error);

			List<Transaction> trans = new List<Transaction>(databases.Length);
			versions = new List<DatabaseVersions>(databases.Length);

			IEnumerable<Database> dbs = alignedDatabases == ReplicatedDatabases.All ?
				databases : new Database[] { databases[(int)DatabaseId.SystemGlobal], databases[(int)DatabaseId.User] };

			foreach (Database db in dbs)
			{
				Transaction tran = db.CreateTransaction(type, out DatabaseVersions dbVersions);
				trans.Add(tran);
				versions.Add(dbVersions);
			}

			return trans;
		}
		finally
		{
			engineLock.ExitReadLock(handle);
		}
	}

	public Transaction TryCreateTransactionWithVersions(long databaseId, TransactionType type,
		bool forceThroughLock, int timeout, out DatabaseVersions versions)
	{
		if (forceThroughLock)
			engineLock.BorrowOwnerhsip();

		try
		{
			if (disposed)
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.DatabaseDisposed));

			if (!engineLock.TryCreateTransactionAndEnterReadLock(out int handle, timeout))
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.DatabaseBusy));

			try
			{
				if (!replicator.IsTransactionAllowed(databaseId, TransactionSource.Internal, null, TransactionType.Read, out DatabaseErrorDetail error))
					throw new DatabaseException(error);

				return databases[databaseId].CreateTransaction(type, out versions);
			}
			finally
			{
				engineLock.ExitReadLock(handle);
			}
		}
		finally
		{
			if (forceThroughLock)
				engineLock.RelinquishOwnership();
		}
	}

	public void BeginDatabaseAlignment(long databaseId)
	{
		TTTrace.Write(traceId, databaseId);
		databases[databaseId].BeginDatabaseAlignment();
	}

	public void EndDatabaseAlignment(Database database)
	{
		TTTrace.Write(traceId, database.Id);
		database.EndDatabaseAlignment();

		if (database.Id == DatabaseId.User)
			contextPool.ResetAlignmentMode();

		trace.Debug("Database {0} alignment finished.", database.Id);
	}

	public void ReRestoreDatabase(long databaseId, PersistenceUpdate persistenceUpdate, ulong alignmentLogSeqNum, bool retainConfig)
	{
		TTTrace.Write(TraceId);

		if (!retainConfig)
		{
			databases[databaseId].Dispose();

			if (persistenceUpdate != null && persistenceUpdate.PersistenceDescriptor != null && persistenceUpdate.IsRecreationRequired)
			{
				TTTrace.Write(TraceId);
				Database.DeletePersistenceFiles(this, persistenceUpdate.PersistenceDescriptor);
			}

			PersistenceDescriptor sysLogPersistDesc = new PersistenceDescriptor(new LogDescriptor[0], DatabaseId.User);
			if (!Database.TryPrepareRestoreDirectories(sysLogPersistDesc, this))
			{
				databases[databaseId] =
					Database.CreateEmpty(this, DataModelDescriptor.CreateEmpty(sysLogPersistDesc), sysLogPersistDesc, databaseId);
			}
			else
			{
				Database.Restore(this, null, sysLogPersistDesc, databaseId, db => databases[databaseId] = db, alignmentLogSeqNum);
			}
		}
		else
		{
			DataModelDescriptor modelDesc = databases[databaseId].ModelDesc;
			PersistenceDescriptor persistenceDescriptor = databases[databaseId].PersistenceDesc;
			databases[databaseId].Dispose();
			Database.Restore(this, modelDesc, persistenceDescriptor, databaseId, db => databases[databaseId] = db, alignmentLogSeqNum);
		}
	}

	public IdSet CollectAlignmentIds(ulong commonVersion, long databaseId)
	{
		return databases[databaseId].CollectAlignmentIds(commonVersion);
	}

	public GlobalVersion[][] GetGlobalVersions(ReplicatedDatabases replicatedDatabases)
	{
		if (replicatedDatabases == ReplicatedDatabases.Global)
		{
			return new GlobalVersion[][] {
				databases[(int)DatabaseId.SystemGlobal].GetGlobalVersions(out uint localTerm),
				databases[(int)DatabaseId.User].GetGlobalVersions(out localTerm)
			};
		}
		else
		{
			GlobalVersion[][] versions = new GlobalVersion[databases.Length][];
			for (int i = 0; i < databases.Length; i++)
			{
				versions[i] = databases[i].GetGlobalVersions(out uint localTerm);
			}

			return versions;
		}
	}

	public DatabaseElectionState[] GetElectionState()
	{
		DatabaseElectionState[] dbStates = new DatabaseElectionState[databases.Length];
		for (int i = 0; i < databases.Length; i++)
		{
			databases[i].GetLocalTermAndReadVersion(out uint localTerm, out ulong readVersion);
			dbStates[i] = new DatabaseElectionState(databases[i].Id, localTerm, readVersion);
		}

		return dbStates;
	}

	public bool HasTransactions()
	{
		for (int i = 0; i < databases.Length; i++)
		{
			if (databases[i].HasTransactions())
				return true;
		}

		CommitWorkers w = commitWorkers;
		if (w != null && w.HasTransactions)
			return true;

		return false;
	}

	public void CancelAllTransactions()
	{
		for (int i = 0; i < databases.Length; i++)
		{
			databases[i].CancelAllTransactions();
		}
	}

	public void AllowPersistenceSnapshots()
	{
		snapshotController.Unblock();
	}

	public void PreventPersistenceSnapshots()
	{
		snapshotController.Block();
	}

	public void ApplyChangeset(Transaction tran, Changeset changeset)
	{
		ApplyChangeset(tran, changeset, false);
	}

	public void ApplyChangeset(Transaction tran, Changeset changeset, bool onlyPropagateSetToNull)
	{
		TTTrace.Write(traceId, tran.Id, tran.InternalFlags, tran.Context != null ? (int)tran.Context.WriteFlags : 0);

		tran.ValidateUsage();
		if (tran.CancelRequested)
			CheckErrorAndRollback(DatabaseErrorDetail.Create(DatabaseErrorType.TransactionCanceled), tran);

		if (tran.Type == TransactionType.Read)
			CheckErrorAndRollback(DatabaseErrorDetail.Create(DatabaseErrorType.ReadTranWriteAttempt), tran);

		bool cascadeGenerated = false;
		while (changeset != null)
		{
			changeset = ApplyChangesetIter(tran, changeset, cascadeGenerated, onlyPropagateSetToNull);
			cascadeGenerated = true;
		}
	}

	public void ApplyAlignmentChangeset(Transaction tran, Changeset changeset)
	{
		TTTrace.Write(traceId, tran.Id);

		Database database = tran.Database;

		TransactionContext tc = tran.Context;
		ChangesetReader reader = tc.ChangesetReader;
		reader.Init(database.ModelDesc, changeset);

		while (!reader.EndOfStream())
		{
			ApplyAlignmentChangesetBlock(tran, reader);
		}

		tc.AddChangeset(changeset);
	}

	private Changeset ApplyChangesetIter(Transaction tran, Changeset changeset, bool cascadeGenerated, bool onlyPropagateSetToNull)
	{
		TTTrace.Write(traceId, tran.Id, cascadeGenerated);

		Database database = tran.Database;

		TransactionContext tc = tran.Context;
		ChangesetReader reader = tc.ChangesetReader;
		reader.Init(database.ModelDesc, changeset);

		while (!reader.EndOfStream())
		{
			DatabaseErrorDetail err = ApplyChangesetBlock(tran, reader);
			if (err != null && cascadeGenerated)
				changeset.Dispose();

			CheckErrorAndRollback(err, tran);
		}

		tc.AddChangeset(changeset);

		if (cascadeGenerated)
			changeset.Dispose();

		Changeset cascCh = null;
		if (!tran.IsAlignment)
		{
			DatabaseErrorDetail error = ApplyInverseReferenceChanges(tran,
				cascadeGenerated || tran.Source == TransactionSource.Replication, onlyPropagateSetToNull);
			CheckErrorAndRollback(error, tran);

			if (tran.Source != TransactionSource.Replication)
				cascCh = tran.Database.RefValidator.PropagateDeletes(tran, onlyPropagateSetToNull, out error);

			tc.ClearInvRefChanges();
			if (error == null)
				error = DeleteInverseReferences(tran);

			if (error != null && cascCh != null)
				cascCh.Dispose();

			CheckErrorAndRollback(error, tran);
		}

		return cascCh;
	}

	public void RestoreChangeset(Database database, ChangesetBlock block, PendingRestoreOperations pendingOps,
		ChangesetReader reader, ulong commitVersion, bool isAlignment, int logIndex)
	{
		TTTrace.Write(traceId, commitVersion);

		while (!reader.EndOfStream())
		{
			RestoreBlock(database, block, pendingOps, commitVersion, reader, isAlignment, logIndex);
		}
	}

	private void RestoreBlock(Database database, ChangesetBlock block, PendingRestoreOperations pendingOps,
		ulong commitVersion, ChangesetReader reader, bool isAlignment, int logIndex)
	{
		reader.ReadBlock(false, block, true);

		ClassDescriptor classDesc = block.ClassDescriptor;
		OperationType opType = block.OperationType;

		TTTrace.Write(traceId, commitVersion, classDesc == null ? 0 : classDesc.Id, (byte)block.OperationType, block.PropertyCount, block.OperationCount);

		if (opType == OperationType.DropDatabase)
		{
			database.DropClasses((byte)(1 << logIndex));
			return;
		}

		if (classDesc == null || classDesc.IsAbstract)
		{
			ChangesetReader.SkipBlock(reader, block);
			return;
		}

		Class @class = database.GetClass(classDesc.Index).MainClass;

		if (opType == OperationType.Update)
		{
			@class.RestoreUpdate(block, pendingOps, commitVersion, reader, isAlignment);
		}
		else if (opType == OperationType.Insert)
		{
			@class.RestoreInsert(block, pendingOps, commitVersion, reader);
		}
		else if (opType == OperationType.Delete)
		{
			@class.RestoreDelete(block, pendingOps, commitVersion, reader, isAlignment);
		}
		else if (opType == OperationType.DefaultValue)
		{
			@class.RestoreDefaultValue(block, commitVersion, reader);
		}
		else
		{
			@class.RestoreClassDrop(block, reader);
		}
	}

	private void ApplyAlignmentChangesetBlock(Transaction tran, ChangesetReader reader)
	{
		TransactionContext tc = tran.Context;

		ChangesetBlock block = tc.ChangesetBlock;
		if (!reader.ReadBlock(false, block))
			return;

		ClassDescriptor classDesc = block.ClassDescriptor;
		OperationType opType = block.OperationType;

		TTTrace.Write(traceId, tran.Id, classDesc == null ? 0 : classDesc.Id,
			(byte)block.OperationType, block.PropertyCount, block.OperationCount);

		if (opType == OperationType.Rewind)
		{
			RewindToVersion(tran, reader, block.RewindVersion);
			return;
		}

		// Source model is different from ours model (can happen in non user databases).
		if (classDesc == null)
		{
			ChangesetReader.SkipBlock(reader, block);
			return;
		}

		ApplyAlignDelegate alignDelegate = tran.Database.StandbyAlignCodeCache.GetAlignDelegate(block);
		Class @class = tran.Database.GetClass(classDesc.Index).MainClass;

		if (opType == OperationType.Update)
		{
			@class.Align(tran, reader, alignDelegate);
		}
		else
		{
			@class.Delete(tran, reader);
		}
	}

	private DatabaseErrorDetail ApplyChangesetBlock(Transaction tran, ChangesetReader reader)
	{
		TransactionContext tc = tran.Context;

		ChangesetBlock block = tc.ChangesetBlock;
		if (!reader.ReadBlock(tran.Source == TransactionSource.Client, block))
			return null;

		ClassDescriptor classDesc = block.ClassDescriptor;
		OperationType opType = block.OperationType;

		TTTrace.Write(traceId, tran.Id, classDesc == null ? 0 : classDesc.Id,
			(byte)block.OperationType, block.PropertyCount, block.OperationCount);

		if (opType == OperationType.Rewind)
			return DatabaseErrorDetail.Create(DatabaseErrorType.InvalidChangesetFormat);

		// Source model is different from ours model (can happen in non user databases).
		if (classDesc == null || opType == OperationType.DefaultValue || opType == OperationType.DropClass)
		{
			ChangesetReader.SkipBlock(reader, block);
			return null;
		}

		if (classDesc.IsAbstract)
			return DatabaseErrorDetail.CreateAbstractClassWriteAttempt(classDesc.FullName);

		Class @class = tran.Database.GetClass(classDesc.Index).MainClass;
		DatabaseErrorDetail error = null;
		if (opType == OperationType.Update)
		{
			error = @class.Update(tran, reader);
		}
		else if (opType == OperationType.Insert)
		{
			error = @class.Insert(tran, reader);
		}
		else
		{
			error = @class.Delete(tran, reader);
		}

		if (error != null)
			return error;

		return null;
	}

	private DatabaseErrorDetail RewindToVersion(Transaction tran, ChangesetReader reader, ulong version)
	{
		Checker.AssertTrue(tran.IsAlignment);
		Checker.AssertTrue(Database.IsCommited(version));

		TTTrace.Write(traceId, tran.Id, version);

		Database database = tran.Database;

		tran.Context.RewindPerformed(version);
		database.Rewind(version);
		trace.Debug("Rewinding, version={0}.", version);

		for (int i = 0; i < database.ModelDesc.ClassCount; i++)
		{
			ClassLocker locker = database.GetClassLocker(i);
			locker?.Rewind(version);
		}

		return null;
	}

	private DatabaseErrorDetail DeleteInverseReferences(Transaction tran)
	{
		TTTrace.Write(traceId, tran.Id);

		NativeList l = tran.Context.Deleted;
		long count = l.Count;

		if (count == 0)
			return null;

		DeletedObject* deleted = (DeletedObject*)l.Buffer;
		for (long i = 0; i < count; i++)
		{
			TTTrace.Write(traceId, tran.Id, deleted->classIndex, deleted->id);

			InverseReferenceMap invRefMap = tran.Database.GetInvRefs(deleted->classIndex);
			if (invRefMap != null)
			{
				DatabaseErrorDetail error = invRefMap.Delete(tran, deleted->id);
				if (error != null)
					return error;
			}

			deleted++;
		}

		tran.Context.ClearDeletes();
		return null;
	}

	private DatabaseErrorDetail ApplyInverseReferenceChanges(Transaction tran, bool ignoreDeleted, bool shouldValidate)
	{
		TTTrace.Write(traceId, tran.Id, ignoreDeleted, tran.Context.InverseRefChanges.Count);

		TransactionContext tc = tran.Context;
		if (tc.InverseRefChanges.Count == 0)
			return null;

		tc.SortInverseReferences();

		DatabaseErrorDetail error;

		long startIndex = 0;
		InverseReferenceOperation* startOp = (InverseReferenceOperation*)tc.InverseRefChanges.Buffer;
		long rangeDirectRef = startOp->directReference;
		int rangePropId = startOp->PropertyId;
		int rangeOpType = startOp->Type;
		long splitIndex = 0;

		InverseReferenceOperation* currOp = startOp + 1;
		long count = tc.InverseRefChanges.Count;
		for (long i = 1; i < count; i++)
		{
			if (currOp->PropertyId != rangePropId || currOp->directReference != rangeDirectRef)
			{
				error = ApplyRangeInverseReferenceChange(tran, ignoreDeleted, startOp,
					(int)(i - startIndex), (int)splitIndex, shouldValidate);

				if (error != null)
					return error;

				startIndex = i;
				startOp = currOp;
				splitIndex = 0;
				rangeDirectRef = startOp->directReference;
				rangePropId = startOp->PropertyId;
				rangeOpType = startOp->Type;
			}
			else if (currOp->Type != rangeOpType)
			{
				splitIndex = i - startIndex;
				rangeOpType = currOp->Type;
			}

			currOp++;
		}

		error = ApplyRangeInverseReferenceChange(tran, ignoreDeleted, startOp,
			(int)(count - startIndex), (int)splitIndex, shouldValidate);

		if (error != null)
			return error;

		tc.ClearInvRefChanges();
		return null;
	}

	private unsafe DatabaseErrorDetail ApplyRangeInverseReferenceChange(Transaction tran, bool ignoreDeleted,
		InverseReferenceOperation* ops, int count, int splitIndex, bool onlyCascadeSetToNull)
	{
		TTTrace.Write(traceId, tran.Id, ignoreDeleted, ops->Type, ops->PropertyId, ops->ClassIndex, ops->directReference, ops->inverseReference, count, splitIndex);

		Database database = tran.Database;

		int insertCount, deleteCount;
		if (splitIndex != 0)
		{
			insertCount = splitIndex;
			deleteCount = count - splitIndex;
		}
		else
		{
			if ((InvRefChangeType)ops->Type == InvRefChangeType.Insert)
			{
				insertCount = count;
				deleteCount = 0;
			}
			else
			{
				insertCount = 0;
				deleteCount = count;
			}
		}

		if (insertCount > 0 && !tran.IsAlignment && !onlyCascadeSetToNull)
		{
			DatabaseErrorDetail err = tran.Database.RefValidator.ValidateReference(tran, ops, insertCount);
			if (err != null)
				return err;
		}

		ClassDescriptor classDesc = IdHelper.GetClass(tran.Model, ops->directReference);
		if (classDesc == null)
		{
			// If an invalid reference (unexisting class) is set and than replaced in the same changeset,
			// we can end up in this situation.
			TTTrace.Write();
			return null;
		}

		InverseReferenceMap invRefMap = database.GetInvRefs(classDesc.Index);
		return invRefMap.Modify(tran, ignoreDeleted, ops->directReference, ops->PropertyId,
			ops->IsTracked, insertCount, deleteCount, ops, ops + insertCount);
	}

	private void MergeInverseReferences(Transaction tran)
	{
		TransactionContext tc = tran.Context;

		ModifiedList l = tc.AffectedInvRefs;

		if (l.Count == 0 || !settings.AutoMergeInvRefs)
			return;

		AffectedInverseReferences* invRefs = (AffectedInverseReferences*)l.StartIteration(out ModifiedBufferHeader* phead);
		for (int i = 0; phead != null; i++)
		{
			if (!invRefs->isDelete)
			{
				InverseReferenceMap invRefMap = tc.Database.GetInvRefs(invRefs->classIndex);
				invRefMap.Merge(tc, invRefs->id, invRefs->propertyId, tran.CommitVersion, tran.IsAlignment);
			}

			invRefs = (AffectedInverseReferences*)l.MoveToNext(ref phead, (byte*)invRefs, AffectedInverseReferences.Size);
		}
	}

	private void CommitModifications(Transaction tran)
	{
		TTTrace.Write(traceId, tran.Id, tran.CommitVersion);

		if (tran.IsAlignment)
		{
			tran.AsyncCommitterFinished();
			return;
		}

		Database database = tran.Database;
		TransactionContext tc = tran.Context;

		long count = tc.LockedClasses->Count;
		for (int i = 0; i < count; i++)
		{
			ushort classIndex = ClassIndexMultiSet.GetClassIndex(tc.LockedClasses, i, out ushort indexCount);
			for (int j = 0; j < indexCount; j++)
			{
				Class @class = database.GetClass(classIndex, out ClassLocker locker).MainClass;
				@class.CommitClassReadLock(locker, tran.CommitVersion);
			}
		}

		count = tc.WrittenClasses->Count;
		for (int i = 0; i < count; i++)
		{
			ushort classIndex = ClassIndexMultiSet.GetClassIndex(tc.WrittenClasses, i, out ushort indexCount);
			for (int j = 0; j < indexCount; j++)
			{
				Class @class = database.GetClass(classIndex, out ClassLocker locker).MainClass;
				@class.CommitClassWriteLock(locker, tran.CommitVersion);
			}
		}

		ModifiedList l1 = tc.AffectedObjects;
		AffectedObject* affObj = (AffectedObject*)l1.StartIteration(out ModifiedBufferHeader* phead);
		for (uint i = 0; phead != null; i++)
		{
			Class @class = database.GetClass(affObj->classIndex).MainClass;
			affObj->id = @class.CommitObject(tran, affObj->objectHandle);
			affObj = (AffectedObject*)l1.MoveToNext(ref phead, (byte*)affObj, AffectedObject.Size);
		}

		l1 = tc.AffectedInvRefs;
		AffectedInverseReferences* p = (AffectedInverseReferences*)l1.StartIteration(out phead);
		for (uint i = 0; phead != null; i++)
		{
			InverseReferenceMap invRefMap = database.GetInvRefs(p->classIndex);
			invRefMap.CommitModification(tran, p, tran.CommitVersion);
			p = (AffectedInverseReferences*)l1.MoveToNext(ref phead, (byte*)p, AffectedInverseReferences.Size);
		}

		l1 = tc.ObjectReadLocks;
		ReadLock* readLock = (ReadLock*)l1.StartIteration(out phead);
		while (phead != null)
		{
			Class @class = database.GetClass(readLock->classIndex).MainClass;
			readLock->id = @class.CommitReadLock(tran, readLock->handle, readLock->TranSlot);
			readLock = (ReadLock*)l1.MoveToNext(ref phead, (byte*)readLock, ReadLock.Size);
		}

		l1 = tc.KeyReadLocks;
		KeyReadLock* keyReadLock = (KeyReadLock*)l1.StartIteration(out phead);
		while (phead != null)
		{
			if (keyReadLock->IsRange)
			{
				SortedIndex sortedIndex = (SortedIndex)database.GetIndex(keyReadLock->IndexIndex, out _);
				sortedIndex.FinalizeRangeLock(keyReadLock->itemHandle);
			}
			else
			{
				KeyReadLocker locker = database.GetKeyLocker(keyReadLock->IndexIndex);
				locker.FinalizeKeyLock(keyReadLock->itemHandle, keyReadLock->hash, tran, keyReadLock->tranSlot);
			}

			keyReadLock = (KeyReadLock*)l1.MoveToNext(ref phead, (byte*)keyReadLock, KeyReadLock.Size);
		}

		NativeList l2 = tc.InvRefReadLocks;
		count = l2.Count;
		readLock = (ReadLock*)l2.Buffer;
		for (long i = 0; i < count; i++)
		{
			InverseReferenceMap invRefMap = database.GetInvRefs(readLock->classIndex);
			readLock->id = invRefMap.CommitReadLock(tran, readLock->handle, readLock->TranSlot, out int propId);
			readLock->propertyId = propId;
			readLock++;
		}

		tran.AsyncCommitterFinished();
	}

	public void RemapTransactionSlot(Transaction tran, ushort slot)
	{
		TTTrace.Write(traceId, tran.Id, slot);

		Database database = tran.Database;
		TransactionContext tc = tran.Context;

		ModifiedList l1 = tc.ObjectReadLocks;
		ReadLock* readLock = (ReadLock*)l1.StartIteration(out ModifiedBufferHeader* phead);
		while (phead != null)
		{
			Class @class = database.GetClass(readLock->classIndex).MainClass;
			readLock->id = @class.RemapReadLockSlot(readLock->handle, readLock->TranSlot, slot);
			readLock->SetEligibleForGC_TranSlot(readLock->EligibleForGC, slot);
			readLock = (ReadLock*)l1.MoveToNext(ref phead, (byte*)readLock, ReadLock.Size);
		}

		l1 = tc.KeyReadLocks;
		KeyReadLock* keyReadLock = (KeyReadLock*)l1.StartIteration(out phead);
		while (phead != null)
		{
			if (!keyReadLock->IsRange)
			{
				KeyReadLocker locker = database.GetKeyLocker(keyReadLock->IndexIndex);
				locker.RemapKeyLockSlot(keyReadLock->itemHandle, keyReadLock->hash, keyReadLock->tranSlot, slot);
				keyReadLock->tranSlot = slot;
			}

			keyReadLock = (KeyReadLock*)l1.MoveToNext(ref phead, (byte*)keyReadLock, KeyReadLock.Size);
		}

		NativeList l2 = tc.InvRefReadLocks;
		long count = l2.Count;
		readLock = (ReadLock*)l2.Buffer;
		for (long i = 0; i < count; i++)
		{
			InverseReferenceMap invRefMap = database.GetInvRefs(readLock->classIndex);
			readLock->id = invRefMap.RemapReadLockSlot(readLock->handle, readLock->TranSlot, slot);
			readLock->SetEligibleForGC_TranSlot(readLock->EligibleForGC, slot);
			readLock++;
		}
	}

	private void RollbackModifications(Transaction tran)
	{
		TTTrace.Write(traceId, tran.Id);

		Database database = tran.Database;
		TransactionContext tc = tran.Context;

		ModifiedList l1 = tc.ObjectReadLocks;
		ReadLock* readLock = (ReadLock*)l1.StartIteration(out ModifiedBufferHeader* phead);
		for (uint i = 0; phead != null; i++)
		{
			Class @class = database.GetClass(readLock->classIndex).MainClass;
			readLock->id = @class.RollbackReadLock(tran, readLock->handle);
			readLock = (ReadLock*)l1.MoveToNext(ref phead, (byte*)readLock, (int)ReadLock.Size);
		}

		NativeList l2 = tc.InvRefReadLocks;
		long count = l2.Count;
		readLock = (ReadLock*)l2.Buffer;
		for (uint i = 0; i < count; i++)
		{
			int propId;
			InverseReferenceMap invRefMap = database.GetInvRefs(readLock->classIndex);
			readLock->id = invRefMap.RollbackReadLock(tran, readLock->handle, out propId);
			readLock->propertyId = propId;
			readLock++;
		}

		l1 = tc.KeyReadLocks;
		KeyReadLock* keyReadLock = (KeyReadLock*)l1.StartIteration(out phead);
		for (uint i = 0; phead != null; i++)
		{
			if (keyReadLock->IsRange)
			{
				SortedIndex sortedIndex = (SortedIndex)database.GetIndex(keyReadLock->IndexIndex, out _);
				sortedIndex.FinalizeRangeLock(keyReadLock->itemHandle);
			}
			else
			{
				KeyReadLocker locker = database.GetKeyLocker(keyReadLock->IndexIndex);
				locker.FinalizeKeyLock(keyReadLock->itemHandle, keyReadLock->hash, tran, tran.Slot);
			}

			keyReadLock = (KeyReadLock*)l1.MoveToNext(ref phead, (byte*)keyReadLock, (int)KeyReadLock.Size);
		}

		count = tc.WrittenClasses->Count;
		for (int i = 0; i < count; i++)
		{
			ushort classIndex = ClassIndexMultiSet.GetClassIndex(tc.WrittenClasses, i, out ushort indexCount);
			for (int j = 0; j < indexCount; j++)
			{
				Class @class = database.GetClass(classIndex, out ClassLocker locker).MainClass;
				@class.RollbackClassWriteLock(locker);
			}
		}

		count = tc.LockedClasses->Count;
		for (int i = 0; i < count; i++)
		{
			ushort classIndex = ClassIndexMultiSet.GetClassIndex(tc.LockedClasses, i, out ushort indexCount);
			for (int j = 0; j < indexCount; j++)
			{
				Class @class = database.GetClass(classIndex, out ClassLocker locker).MainClass;
				@class.RollbackClassReadLock(locker);
			}
		}

		l1 = tran.Context.AffectedObjects;
		AffectedObject* affObj = (AffectedObject*)l1.StartIteration(out phead);
		for (uint i = 0; phead != null; i++)
		{
			Class @class = database.GetClass(affObj->classIndex).MainClass;
			affObj->id = @class.RollbackObject(tran, affObj);
			affObj = (AffectedObject*)l1.MoveToNext(ref phead, (byte*)affObj, (int)AffectedObject.Size);
		}

		l1 = tran.Context.AffectedInvRefs;
		if (l1.NotEmpty)
		{
			AffectedInverseReferences* p = (AffectedInverseReferences*)l1.StartIteration(out phead);
			for (uint i = 0; phead != null; i++)
			{
				InverseReferenceMap invRefMap = database.GetInvRefs(p->classIndex);
				invRefMap.RollbackModification(tran, p);
				p = (AffectedInverseReferences*)l1.MoveToNext(ref phead, (byte*)p, (int)AffectedInverseReferences.Size);
			}
		}
	}

	private void ValidatePersistenceUpdate(PersistenceUpdate update)
	{
		PersistenceDescriptor persistDesc = update.PersistenceDescriptor;
		if (persistDesc == null)
			return;

		if (persistDesc.LogDescriptors.Length > PersistenceDescriptor.MaxLogGroups)
			throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.LogCountLimitExceeeded));

		for (int i = 0; i < persistDesc.LogDescriptors.Length; i++)
		{
			string logDir = persistDesc.LogDescriptors[i].Directory;
			string snapshotDir = persistDesc.LogDescriptors[i].SnapshotDirectory;

			if (!AreDirectoriesValidAndAccessible(persistDesc.LogDescriptors[i]))
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.InvalidLogDirectory));

			if (persistDesc.LogDescriptors[i].Name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0 ||
				(i > 0 && persistDesc.LogDescriptors[i].Name.Equals(LogDescriptor.MasterLogName)))
			{
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.InvalidLogName));
			}

			LogDescriptor logDesc = persistDesc.LogDescriptors.
				First(x => persistDesc.LogDescriptors[i].Name.Equals(x.Name, StringComparison.OrdinalIgnoreCase));

			if (logDesc != null && !object.ReferenceEquals(logDesc, persistDesc.LogDescriptors[i]))
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.NonUniqueLogName));
		}
	}

	private void ApplyPersistenceChanges(Transaction tran, PersistenceDescriptor persistenceDesc, SimpleGuid versionGuid)
	{
		byte[] persistenceDescData = PersistenceDescriptor.Serialize(persistenceDesc);

		Database database = databases[DatabaseId.User];
		ChangesetWriter writer = ChangesetWriterPool.Get();
		try
		{
			Func<ClassDescriptor, BlockProperties> op = ReadConfigArtifactVersion(tran,
				IdGenerator.PersistenceVersionId).IsZero ? writer.StartInsertBlock : writer.StartUpdateBlock;

			op(database.ModelDesc.GetClass(SystemCode.ConfigArtifactVersion.Id)).
				Add(SystemCode.ConfigArtifactVersion.GuidV1).Add(SystemCode.ConfigArtifactVersion.GuidV2);
			writer.AddLong(IdGenerator.PersistenceVersionId).
				AddLong(versionGuid.Low).AddLong(versionGuid.Hight);

			op(database.ModelDesc.GetClass(SystemCode.ConfigArtifact.Id)).Add(SystemCode.ConfigArtifact.Binary);
			writer.AddLong(IdGenerator.PersistenceDescId).AddByteArray(persistenceDescData);

			tran.SetConfigurationUpdateArtifact(persistenceDescData);
			using (Changeset changeset = writer.FinishWriting())
			{
				ApplyChangeset(tran, changeset);
			}
		}
		finally
		{
			ChangesetWriterPool.Put(writer);
		}
	}

	private bool AreDirectoriesValidAndAccessible(LogDescriptor logDesc)
	{

		try
		{
			string[] dirs = new string[] { logDesc.FinalDirectory(this), logDesc.FinalSnapshotDirectory(this) };

			for (int i = 0; i < dirs.Length; i++)
			{
				string dir = dirs[i];
				if (!settings.AllowUnexistingDirectoryForLog && !Directory.Exists(dir))
					return false;

				DirectoryInfo di = new DirectoryInfo(dir);
				string temp = Path.Combine(dir, Guid.NewGuid().ToString("N"));
				Directory.CreateDirectory(temp);
				Directory.Delete(temp);
			}

			return true;
		}
		catch (Exception)
		{
			return false;
		}
	}

	private void ValidateAssemblyUpdate(Transaction tran, AssemblyUpdate assemblyUpdate)
	{
		IHashIndexReader<string> hr = databases[(int)DatabaseId.User].GetHashIndexReader<string>(SystemCode.Assembly.NameIndexId);
		ObjectReader[] rs = new ObjectReader[1];

		if (!assemblyUpdate.PreviousAssemblyVersionGuid.Equals(configVersions.AssembliesVersionGuid))
		{
			throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.InvalidAssemblyVersionGuid));
		}

		if (assemblyUpdate.Inserted != null)
		{
			foreach (UserAssembly ua in assemblyUpdate.Inserted)
			{
				hr.GetObjects(tran, ua.Name.ToLower(CultureInfo.InvariantCulture), ref rs, out int count);
				if (count > 0)
					Throw.AssemblyNameAlreadyExists(ua.Name);
			}
		}

		if (assemblyUpdate.Updated != null)
		{
			foreach (UserAssembly ua in assemblyUpdate.Updated)
			{
				hr.GetObjects(tran, ua.Name.ToLower(CultureInfo.InvariantCulture), ref rs, out int count);
				if (count == 0)
					Throw.UnknownUserAssembly(ua.Id, ua.Name);

				if (rs[0].GetId(tran) != ua.Id)
					Throw.UnknownUserAssembly(ua.Id, ua.Name);
			}
		}

		if (assemblyUpdate.Deleted != null)
		{
			foreach (long assemblyId in assemblyUpdate.Deleted)
			{
				ObjectReader r = GetObject(tran, assemblyId);
				if (r.IsEmpty())
					Throw.UnknownUserAssembly(assemblyId, "");
			}
		}
	}

	private void ApplyClassDrops(Transaction tran, DataModelUpdate modelUpdate)
	{
		TTTrace.Write(traceId, trace.Name);

		ChangesetWriter writer = ChangesetWriterPool.Get();
		try
		{
			foreach (ClassUpdate cu in modelUpdate.UpdatedClasses.Values)
			{
				if (!cu.IsAbstractModified || !cu.ClassDesc.IsAbstract)
					continue;

				writer.CreateDropClassBlock(cu.ClassDesc);
			}

			using (Changeset changeset = writer.FinishWriting())
			{
				ApplyChangeset(tran, changeset);
			}
		}
		finally
		{
			ChangesetWriterPool.Put(writer);
		}
	}

	private void ApplyDefaultValues(Transaction tran, DataModelUpdate modelUpdate)
	{
		TTTrace.Write(traceId, trace.Name);

		ChangesetWriter writer = ChangesetWriterPool.Get();
		try
		{
			foreach (ClassUpdate cu in modelUpdate.UpdatedClasses.Values)
			{
				if (!cu.RequiresDefaultValueWrite)
					continue;

				BlockProperties bp = writer.StartDefaultValueBlock(cu.ClassDesc);
				foreach (PropertyDescriptor propDesc in cu.InsertedProperties.Select(x => x.PropDesc))
				{
					bp.Add(propDesc.Id);
				}

				writer.AddLong(0);
				foreach (PropertyDescriptor propDesc in cu.InsertedProperties.Select(x => x.PropDesc))
				{
					writer.AddDefaultValue(propDesc);
				}
			}

			using (Changeset changeset = writer.FinishWriting())
			{
				ApplyChangeset(tran, changeset);
			}
		}
		finally
		{
			ChangesetWriterPool.Put(writer);
		}
	}

	private void ApplyAssemblyChanges(Transaction tran, AssemblyUpdate assemblyUpdate,
		DataModelDescriptor newModelDesc, bool hasModelChanges, out SimpleGuid assembliesVersionGuid, out SimpleGuid modelVersionGuid)
	{
		byte[] modelDescData = DataModelDescriptor.Serialize(newModelDesc);

		assembliesVersionGuid = SimpleGuid.NewValue();
		modelVersionGuid = ReadConfigArtifactVersion(tran, IdGenerator.ModelVersionId);
		if (hasModelChanges)
			modelVersionGuid = SimpleGuid.NewValue();

		Database sysDb = GlobalSystemDatabase;
		ChangesetWriter writer = ChangesetWriterPool.Get();
		try
		{
			Func<ClassDescriptor, BlockProperties> op = ReadConfigArtifactVersion(tran, IdGenerator.AssembliesVersionId).IsZero ?
				writer.StartInsertBlock : writer.StartUpdateBlock;

			op(sysDb.ModelDesc.GetClass(SystemCode.ConfigArtifactVersion.Id)).
				Add(SystemCode.ConfigArtifactVersion.GuidV1).Add(SystemCode.ConfigArtifactVersion.GuidV2);
			writer.AddLong(IdGenerator.AssembliesVersionId).AddLong(assembliesVersionGuid.Low).AddLong(assembliesVersionGuid.Hight);

			op = ReadConfigArtifactVersion(tran, IdGenerator.ModelVersionId).IsZero ? writer.StartInsertBlock : writer.StartUpdateBlock;

			if (hasModelChanges)
			{
				op(sysDb.ModelDesc.GetClass(SystemCode.ConfigArtifactVersion.Id)).
					Add(SystemCode.ConfigArtifactVersion.GuidV1).Add(SystemCode.ConfigArtifactVersion.GuidV2);
				writer.AddLong(IdGenerator.ModelVersionId).AddLong(modelVersionGuid.Low).AddLong(modelVersionGuid.Hight);

				op(sysDb.ModelDesc.GetClass(SystemCode.ConfigArtifact.Id)).Add(SystemCode.ConfigArtifact.Binary);
				writer.AddLong(IdGenerator.ModelDescId).AddByteArray(modelDescData);
			}

			if (assemblyUpdate.Inserted != null)
			{
				long currId = sysDb.IdGenerator.TakeRange(assemblyUpdate.Inserted.Count);
				ClassDescriptor assemblyClassDesc = sysDb.ModelDesc.GetClass(SystemCode.Assembly.Id);
				foreach (UserAssembly ua in assemblyUpdate.Inserted)
				{
					writer.StartInsertBlock(assemblyClassDesc).
						Add(SystemCode.Assembly.Name).Add(SystemCode.Assembly.FileName).Add(SystemCode.Assembly.Binary);
					writer.AddLong(assemblyClassDesc.MakeId(currId++)).
						AddString(ua.Name).AddString(ua.Name.ToLower(CultureInfo.InvariantCulture)).AddByteArray(ua.Binary);
				}
			}

			if (assemblyUpdate.Updated != null)
			{
				foreach (UserAssembly ua in assemblyUpdate.Updated)
				{
					writer.StartUpdateBlock(sysDb.ModelDesc.GetClass(SystemCode.Assembly.Id)).
						Add(SystemCode.Assembly.Name).Add(SystemCode.Assembly.FileName).Add(SystemCode.Assembly.Binary);
					writer.AddLong(ua.Id).AddString(ua.Name).
						AddString(ua.Name.ToLower(CultureInfo.InvariantCulture)).AddByteArray(ua.Binary);
				}
			}

			if (assemblyUpdate.Deleted != null)
			{
				foreach (long aid in assemblyUpdate.Deleted)
				{
					writer.StartDeleteBlock(sysDb.ModelDesc.GetClass(SystemCode.Assembly.Id));
					writer.AddDelete(aid);
				}
			}

			tran.SetConfigurationUpdateArtifact(true, hasModelChanges ? modelDescData : null);

			using (Changeset changeset = writer.FinishWriting())
			{
				ApplyChangeset(tran, changeset);
			}
		}
		finally
		{
			ChangesetWriterPool.Put(writer);
		}
	}

	private PersistenceDescriptor LoadSystemPersistenceDescAndModel(long databaseId,
		PersistenceSettings persistanceSettings, out DataModelDescriptor model)
	{
		PersistenceDescriptor persDesc = null;
		if (persistanceSettings != null)
			persDesc = new PersistenceDescriptor(new LogDescriptor[0], databaseId);

		using (XMLModelSettings ms = new XMLModelSettings(true))
		{
			string modelName = databaseId == DatabaseId.SystemLocal ?
				"VeloxDB.Storage.LocalSystemModel.xml" : "VeloxDB.Storage.GlobalSystemModel.xml";

			Stream s = Utils.GetResourceStream(Assembly.GetExecutingAssembly(), modelName);
			ms.AddStream(s);
			model = ms.CreateModel(null, null);
			if (persDesc != null)
				model.AssignLogIndexes(persDesc);
		}

		return persDesc;
	}

	private PersistenceDescriptor GetSystemConfiguration(long databaseId, out DataModelDescriptor model)
	{
		PersistenceDescriptor persistenceDesc = new PersistenceDescriptor(new LogDescriptor[0], databaseId);

		using (XMLModelSettings ms = new XMLModelSettings(true))
		{
			string modelName = databaseId == DatabaseId.SystemLocal ?
				"VeloxDB.Storage.LocalSystemModel.xml" : "VeloxDB.Storage.GlobalSystemModel.xml";

			Stream s = Utils.GetResourceStream(Assembly.GetExecutingAssembly(), modelName);
			ms.AddStream(s);
			model = ms.CreateModel(null, null);
		}

		return persistenceDesc;
	}

	private SimpleGuid ReadConfigArtifactVersion(Transaction tran, long id)
	{
		ObjectReader r = GetObject(tran, id);
		return r.IsEmpty() ? SimpleGuid.Zero :
			new SimpleGuid(r.GetLong(SystemCode.ConfigArtifactVersion.GuidV1, tran),
							r.GetLong(SystemCode.ConfigArtifactVersion.GuidV2, tran));
	}

	private ConfigArtifactVersions ReadConfigArtifactVersions()
	{
		using (Transaction tran = CreateTransaction(DatabaseId.User, TransactionType.Read, TransactionSource.Internal, null))
		{
			return new ConfigArtifactVersions()
			{
				AssembliesVersionGuid = ReadConfigArtifactVersion(tran, IdGenerator.AssembliesVersionId),
				ModelVersionGuid = ReadConfigArtifactVersion(tran, IdGenerator.ModelVersionId),
				PersistenceVersionGuid = ReadConfigArtifactVersion(tran, IdGenerator.PersistenceVersionId)
			};
		}
	}

	public void ReadUserConfiguration(out DataModelDescriptor modelDesc, out PersistenceDescriptor persistenceDesc)
	{
		using (Transaction tran = CreateTransaction(DatabaseId.User, TransactionType.Read, TransactionSource.Internal, null))
		{
			ObjectReader r = GetObject(tran, IdGenerator.ModelDescId);
			if (r.IsEmpty())
			{
				modelDesc = DataModelDescriptor.CreateEmpty(null);
			}
			else
			{
				using (BinaryReader br = new BinaryReader(new MemoryStream(r.GetByteArray(SystemCode.ConfigArtifact.Binary, tran))))
				{
					modelDesc = DataModelDescriptor.Deserialize(br);
				}
			}

			r = GetObject(tran, IdGenerator.PersistenceDescId);
			if (r.IsEmpty())
			{
				persistenceDesc = null;
			}
			else
			{
				using (BinaryReader br = new BinaryReader(new MemoryStream(r.GetByteArray(SystemCode.ConfigArtifact.Binary, tran))))
				{
					persistenceDesc = PersistenceDescriptor.Deserialize(br);
					modelDesc.AssignLogIndexes(persistenceDesc);
				}
			}
		}
	}

	private UserAssembly[] ReadUserAssemblies(out SimpleGuid modelVersionGuid,
		out SimpleGuid assemblyVersionGuid, out DataModelDescriptor modelDescriptor)
	{
		using (Transaction tran = CreateTransaction(DatabaseId.User, TransactionType.Read, TransactionSource.Internal, null))
		{
			var assemblies = ReadUserAssemblies(tran);

			modelVersionGuid = this.ModelVersionGuid;
			assemblyVersionGuid = configVersions.AssembliesVersionGuid;
			modelDescriptor = UserDatabase.ModelDesc;

			return assemblies.ToArray();
		}
	}

	private List<UserAssembly> ReadUserAssemblies(Transaction tran)
	{
		List<UserAssembly> assemblies = new List<UserAssembly>();
		using (ClassScan scan = BeginClassScan(tran, tran.Database.ModelDesc.GetClass(SystemCode.Assembly.Id)))
		{
			foreach (ObjectReader r in scan)
			{
				UserAssembly assembly = new UserAssembly(r.GetId(tran), r.GetString(SystemCode.Assembly.Name, tran),
					r.GetByteArray(SystemCode.Assembly.Binary, tran));
				assemblies.Add(assembly);
			}
		}

		return assemblies;
	}

	private void CreateOrRestoreDatabases(ModelSettings userModelSettings, DataModelDescriptor prevModelDesc, PersistenceSettings persistanceSettings)
	{
		this.systemDbPath = persistanceSettings?.SystemDirectory;

		databases = new Database[DatabaseId.DatabaseCount];

		PersistenceDescriptor userPersistDesc = null;
		if (persistanceSettings != null)
			userPersistDesc = new PersistenceDescriptor(persistanceSettings, DatabaseId.User);

		DataModelDescriptor userModelDesc = userModelSettings.CreateModel(persistanceSettings, prevModelDesc);

		PersistenceDescriptor locSysPersistDesc = LoadSystemPersistenceDescAndModel(
			DatabaseId.SystemLocal, persistanceSettings, out DataModelDescriptor sysLocModel);

		PersistenceDescriptor globSysPersistDesc = LoadSystemPersistenceDescAndModel(
			DatabaseId.SystemGlobal, persistanceSettings, out DataModelDescriptor sysGlobModel);

		userModelDesc.AssignLogIndexes(userPersistDesc);

		if (persistanceSettings != null)
		{
			Database.Restore(this, sysLocModel, locSysPersistDesc, DatabaseId.SystemLocal, db => databases[DatabaseId.SystemLocal] = db);
			Database.Restore(this, sysGlobModel, globSysPersistDesc, DatabaseId.SystemGlobal, db => databases[DatabaseId.SystemGlobal] = db);
			Database.Restore(this, userModelDesc, userPersistDesc, DatabaseId.User, db => databases[DatabaseId.User] = db);
		}
		else
		{
			databases[DatabaseId.SystemLocal] = Database.CreateEmpty(this, sysLocModel, locSysPersistDesc, DatabaseId.SystemLocal);
			databases[DatabaseId.SystemGlobal] = Database.CreateEmpty(this, sysGlobModel, globSysPersistDesc, DatabaseId.SystemGlobal);
			databases[DatabaseId.User] = Database.CreateEmpty(this, userModelDesc, null, DatabaseId.User);
		}

		configVersions = ReadConfigArtifactVersions();
	}

	private void CreateOrRestoreDatabases(string sysDbPath)
	{
		databases = new Database[DatabaseId.DatabaseCount];

		PersistenceDescriptor locSysPersistDesc = GetSystemConfiguration(DatabaseId.SystemLocal, out DataModelDescriptor sysLocModel);

		PersistenceDescriptor globSysPersistDesc = GetSystemConfiguration(DatabaseId.SystemGlobal, out DataModelDescriptor sysGlobModel);

		Database.Restore(this, sysLocModel, locSysPersistDesc, DatabaseId.SystemLocal, db => databases[DatabaseId.SystemLocal] = db);
		Database.Restore(this, sysGlobModel, globSysPersistDesc, DatabaseId.SystemGlobal, db => databases[DatabaseId.SystemGlobal] = db);

		PersistenceDescriptor sysLogPersistDesc = new PersistenceDescriptor(new LogDescriptor[0], DatabaseId.User);
		if (!Database.TryPrepareRestoreDirectories(sysLogPersistDesc, this))
		{
			databases[DatabaseId.User] = Database.CreateEmpty(this, DataModelDescriptor.CreateEmpty(sysLogPersistDesc), sysLogPersistDesc, DatabaseId.User);
		}
		else
		{
			Database.Restore(this, null, sysLogPersistDesc, DatabaseId.User, db => databases[DatabaseId.User] = db);
		}

		configVersions = ReadConfigArtifactVersions();
	}

	private void PublishAssemblies()
	{
		SimpleGuid modelVersionGuid;
		SimpleGuid assemblyVersionGuid;
		DataModelDescriptor modelDescriptor;

		UserAssembly[] assemblies = GetUserAssemblies(out modelVersionGuid, out assemblyVersionGuid, out modelDescriptor);
		statePublisher.Publish(assemblies, modelVersionGuid, assemblyVersionGuid, modelDescriptor, null);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void RollbackWithError(DatabaseErrorDetail detail, Transaction tran, string message)
	{
		if (tran.Type == TransactionType.ReadWrite && tran.IsAlignment)
			throw new CriticalDatabaseException("Alignment transaction failed.", new DatabaseException(detail));

		RollbackTransaction(tran);
		throw (message != null ? new DatabaseException(detail, message) : new DatabaseException(detail));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void CheckErrorAndRollback(DatabaseErrorDetail detail, Transaction tran, string message = null)
	{
		if (detail == null)
			return;

		RollbackWithError(detail, tran, message);
	}

	private IReplicator CreateReplicator(ReplicationSettings replicationSettings,
		ILeaderElector localElector, ILeaderElector globalElector)
	{
		Assembly repAssembly = IReplicatorFactory.FindReplicatorAssembly();

		if (repAssembly != null)
		{
			Type repFactType = repAssembly.GetTypes().FirstOrDefault(x => typeof(IReplicatorFactory).IsAssignableFrom(x));
			if (repFactType == null)
				throw new CriticalDatabaseException($"Replicator could not be found in {@repAssembly.FullName} file.");

			IReplicatorFactory repFact = (IReplicatorFactory)Activator.CreateInstance(repFactType);
			replicator = repFact.CreateReplicator(this, replicationSettings, localElector, globalElector);
			return replicator;
		}

		return new UnreplicatedReplicator(this, replicationSettings);
	}

	public void Dispose()
	{
		TTTrace.Write(traceId);

		engineLock.EnterWriteLock(true);

		IAsyncCleanup replicatorCleanup;

		try
		{
			for (int i = 0; i < databases.Length; i++)
			{
				databases[i].AutosnapshotIfNeeded();
			}

			if (disposed)
				return;

			disposed = true;

			commitWorkers?.Stop();
			replicatorCleanup = replicator.Dispose();

			for (int i = 0; i < databases.Length; i++)
			{
				databases[i].Dispose();
			}
		}
		finally
		{
			engineLock.ExitWriteLock(true);
		}

		sortedIndexGC.Dispose();

		replicatorCleanup.WaitCleanup();

		contextPool.Dispose();
		stringStorage.Dispose();
		memoryManager.Dispose();

		trace.Info("Storage engine stopped.");
	}

	public override string ToString()
	{
		return trace != null && trace.Name != null ? trace.Name : base.ToString();
	}

	private sealed class ConfigArtifactVersions
	{
		public SimpleGuid AssembliesVersionGuid { get; set; }
		public SimpleGuid ModelVersionGuid { get; set; }
		public SimpleGuid PersistenceVersionGuid { get; set; }
	}
}
