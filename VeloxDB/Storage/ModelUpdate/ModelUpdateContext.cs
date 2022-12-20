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
	Dictionary<short, HashIndexEntry> newHashIndexes;
	Dictionary<short, ClassEntry> newClasses;
	Dictionary<short, InverseReferenceMap> newInvRefMaps;
	HashReadersCollection newHashReaders;
	DatabaseErrorDetail error;

	public ModelUpdateContext(Database database, DataModelUpdate modelUpdate)
	{
		this.database = database;
		this.modelUpdate = modelUpdate;

		int workerCount = database.Engine.Settings.AllowInternalParallelization ? ProcessorNumber.CoreCount : 1;
		string workerName = string.Format("{0}: vlx-ModelUpdateWorker", database.Engine.Trace.Name);
		workers = JobWorkers<CommonWorkerParam>.Create(workerName, workerCount);
		ResetWorkersAction();

		newHashReaders = new HashReadersCollection(database.HashReaders, modelUpdate);

		newHashIndexes = new Dictionary<short, HashIndexEntry>(2);
		newClasses = new Dictionary<short, ClassEntry>(2);
		newInvRefMaps = new Dictionary<short, InverseReferenceMap>(2);
	}

	public JobWorkers<CommonWorkerParam> Workers => workers;
	public HashReadersCollection NewHashReaders => newHashReaders;
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

		workers.EnqueueWork(ValidateModifiedUniqueHashIndexJob.Create(database, this).
			Select(x => new CommonWorkerParam() { ReferenceParam = x }));
		workers.EnqueueWork(ValidateUniqueHashIndexJob.Create(database, this).
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

		DeleteHashIndexJob.Execute(database, this);
		ResetWorkersAction();

		workers.EnqueueWork(DeleteClassFromHashIndexJob.Create(database, this).Select(x => new CommonWorkerParam() { ReferenceParam = x }));
		workers.Drain();

		DeleteInverseReferenceMapJob.Execute(database, this);

		InsertClassJob.Execute(database, this);
		InsertInverseReferenceMapJob.Execute(database, this);

		InsertHashIndexJob.Execute(database, this);
		if (modelUpdate.IsAlignment)
			PrepareHashIndexForPendingRefillJob.Execute(database, this);

		ResetWorkersAction();

		workers.EnqueueWork(InsertClassIntoHashIndexJob.Create(database, this).Select(x => new CommonWorkerParam() { ReferenceParam = x }));
		workers.EnqueueWork(DeleteInverseReferencesJob.Create(database, this).Select(x => new CommonWorkerParam() { ReferenceParam = x }));
		workers.Drain();

		TTTrace.Write();

		workers.EnqueueWork(UntrackInverseReferencesJob.Create(database, this).Select(x => new CommonWorkerParam() { ReferenceParam = x }));
		workers.Drain();

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

	public void AddNewHashIndex(HashIndex hashIndex, HashKeyReadLocker locker)
	{
		newHashIndexes.Add(hashIndex.HashIndexDesc.Id, new HashIndexEntry() { HashIndex = hashIndex, HashIndexLocker = locker });
	}

	public void AddNewClass(ClassBase @class, ClassLocker locker)
	{
		newClasses.Add(@class.ClassDesc.Id, new ClassEntry() { Class = @class, ClassLocker = locker });
	}

	public void AddNewInverseReferenceMap(InverseReferenceMap invRefMap)
	{
		newInvRefMaps.Add(invRefMap.ClassDesc.Id, invRefMap);
	}

	public bool TryGetNewHashIndex(short id, out HashIndex hashIndex, out HashKeyReadLocker locker)
	{
		if (!newHashIndexes.TryGetValue(id, out HashIndexEntry entry))
		{
			hashIndex = null;
			locker = null;
			return false;
		}

		hashIndex = entry.HashIndex;
		locker = entry.HashIndexLocker;
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

	private sealed class HashIndexEntry
	{
		public HashIndex HashIndex { get; set; }
		public HashKeyReadLocker HashIndexLocker { get; set; }
	}

	private sealed class ClassEntry
	{
		public ClassBase Class { get; set; }
		public ClassLocker ClassLocker { get; set; }
	}

	public void CancelUpdate()
	{
		foreach (HashIndexEntry he in newHashIndexes.Values)
		{
			he.HashIndex.Dispose(workers);
			he.HashIndexLocker?.Dispose();
		}
	}

	public void Dispose()
	{
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
