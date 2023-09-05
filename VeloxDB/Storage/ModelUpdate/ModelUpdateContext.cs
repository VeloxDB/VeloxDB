using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class ModelUpdateContext : IDisposable
{
	Database database;
	DataModelUpdate modelUpdate;
	JobWorkers<CommonWorkerParam> workers;
	Dictionary<short, IndexEntry> newIndexes;
	Dictionary<short, ClassEntry> newClasses;
	Dictionary<short, InverseReferenceMap> newInvRefMaps;
	IndexReadersCollection newIndexReaders;
	DatabaseErrorDetail error;

	public ModelUpdateContext(Database database, DataModelUpdate modelUpdate)
	{
		this.database = database;
		this.modelUpdate = modelUpdate;

		int workerCount = database.Engine.Settings.AllowInternalParallelization ? ProcessorNumber.CoreCount : 1;
		string workerName = string.Format("{0}: vlx-ModelUpdateWorker", database.Engine.Trace.Name);
		workers = JobWorkers<CommonWorkerParam>.Create(workerName, workerCount);
		ResetWorkersAction();

		newIndexReaders = new IndexReadersCollection(database.IndexReaders, modelUpdate);

		newIndexes = new Dictionary<short, IndexEntry>(2);
		newClasses = new Dictionary<short, ClassEntry>(2);
		newInvRefMaps = new Dictionary<short, InverseReferenceMap>(2);
	}

	~ModelUpdateContext()
	{
		throw new CriticalDatabaseException();
	}

	public JobWorkers<CommonWorkerParam> Workers => workers;
	public IndexReadersCollection NewIndexReaders => newIndexReaders;
	public DataModelUpdate ModelUpdate => modelUpdate;

	public void Validate()
	{
		if (modelUpdate.IsAlignment)
			return;

		database.Trace.Debug("Validating model update.");


		workers.EnqueueWork(ValidateReferenceTargetJob.Create(database, this).
			Select(x => new CommonWorkerParam() { ReferenceParam = x }));
		workers.EnqueueWork(ValidateNotNullReferenceJob.Create(database, this).
			Select(x => new CommonWorkerParam() { ReferenceParam = x }));

		workers.EnqueueWork(ValidateModifiedUniqueIndexJob.Create(database, this).
			Select(x => new CommonWorkerParam() { ReferenceParam = x }));
		workers.EnqueueWork(ValidateUniqueIndexJob.Create(database, this).
			Select(x => new CommonWorkerParam() { ReferenceParam = x }));

		workers.Drain();

		if (error != null)
		{
			TTTrace.Write(database.Engine.TraceId);
			CancelUpdate();
			throw new DatabaseException(error);
		}

		database.Trace.Debug("Validation of model update finished.");
	}

	public void ExecuteUpdate(ulong commitVersion)
	{
		database.Trace.Debug("Executing model update.");

		DeleteIndexJob.Execute(database, this);
		ResetWorkersAction();

		workers.EnqueueWork(DeleteClassFromIndexJob.Create(database, this).Select(x => new CommonWorkerParam() { ReferenceParam = x }));
		workers.Drain();

		DeleteInverseReferenceMapJob.Execute(database, this);

		InsertClassJob.Execute(database, this);
		InsertInverseReferenceMapJob.Execute(database, this);

		InsertIndexJob.Execute(database, this);
		if (modelUpdate.IsAlignment)
			PrepareIndexForPendingRefillJob.Execute(database, this);

		ResetWorkersAction();

		workers.EnqueueWork(InsertClassIntoIndexJob.Create(database, this).Select(x => new CommonWorkerParam() { ReferenceParam = x }));
		workers.EnqueueWork(DeleteInverseReferencesJob.Create(database, this).Select(x => new CommonWorkerParam() { ReferenceParam = x }));
		workers.Drain();

		TTTrace.Write();

		BuildInverseReferencesJob.Execute(database, this);
		ResetWorkersAction();

		workers.EnqueueWork(UpdateClassPropertiesJob.Start(database, this, commitVersion).Select(x => new CommonWorkerParam() { ReferenceParam = x }));
		workers.Drain();

		DeleteClassJob.Execute(database, this);
		UpdateClassPropertiesJob.Finish(database, this);

		database.Trace.Debug("Model update execution finished.");
	}

	public void SetError(DatabaseErrorDetail error)
	{
		Checker.AssertNotNull(error);
		this.error = error;
	}

	private void ResetWorkersAction()
	{
		workers.SetAction(job => ((ModelUpdateJob)job.ReferenceParam).Execute());
	}

	public void AddNewIndex(Index index, KeyReadLocker locker)
	{
		newIndexes.Add(index.IndexDesc.Id, new IndexEntry() { Index = index, IndexLocker = locker });
	}

	public void AddNewClass(ClassBase @class, ClassLocker locker)
	{
		newClasses.Add(@class.ClassDesc.Id, new ClassEntry() { Class = @class, ClassLocker = locker });
	}

	public void AddNewInverseReferenceMap(InverseReferenceMap invRefMap)
	{
		newInvRefMaps.Add(invRefMap.ClassDesc.Id, invRefMap);
	}

	public bool TryGetNewIndex(short id, out Index index, out KeyReadLocker locker)
	{
		if (!newIndexes.TryGetValue(id, out IndexEntry entry))
		{
			index = null;
			locker = null;
			return false;
		}

		index = entry.Index;
		locker = entry.IndexLocker;
		return true;
	}

	public bool TryGetNewClass(short id, out ClassBase @class, out ClassLocker locker)
	{
		if (!newClasses.TryGetValue(id, out ClassEntry entry))
		{
			@class = null;
			locker = null;
			return false;
		}

		@class = entry.Class;
		locker = entry.ClassLocker;
		return true;
	}

	public bool TryGetNewInverseReferenceMap(short id, out InverseReferenceMap invRefMap)
	{
		return newInvRefMaps.TryGetValue(id, out invRefMap);
	}

	private sealed class IndexEntry
	{
		public Index Index { get; set; }
		public KeyReadLocker IndexLocker { get; set; }
	}

	private sealed class ClassEntry
	{
		public ClassBase Class { get; set; }
		public ClassLocker ClassLocker { get; set; }
	}

	public void CancelUpdate()
	{
		foreach (IndexEntry he in newIndexes.Values)
		{
			he.Index.Dispose(workers);
			he.IndexLocker?.Dispose();
		}
	}

	public void Dispose()
	{
		GC.SuppressFinalize(this);
		workers.WaitAndClose();
	}
}

internal abstract class ModelUpdateJob
{
	public abstract void Execute();
}

internal abstract class ModelUpdateValidationJob : ModelUpdateJob
{
	protected ModelUpdateContext context;

	public ModelUpdateValidationJob(ModelUpdateContext context)
	{
		this.context = context;
	}
}
