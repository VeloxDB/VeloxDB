using System;
using System.Collections.Generic;
using System.Threading;
using System.Diagnostics;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using System.Linq;

namespace VeloxDB.Storage.Persistence;

internal unsafe sealed class DatabasePersister : IDisposable
{
	public const short FormatVersion = 1;
	public const int MaxLogCount = 8;

	StorageEngine engine;
	Database database;
	PersistenceDescriptor persistenceDesc;
	LogPersister[] logPersisters;

	Orderer<Transaction> orderer;
	List<Transaction> orderedList = new List<Transaction>(8);

	public DatabasePersister(Database database, PersistenceDescriptor persistenceDesc, SnapshotSemaphore snapshotController,
		ulong commitVersion, ulong logSeqNum, DatabaseRestorer.LogState[] logStates)
	{
		TTTrace.Write(database.TraceId, database.ReadVersion, commitVersion, logSeqNum);
		DatabaseRestorer.LogState.TTTraceState(logStates);

		this.engine = database.Engine;
		this.database = database;
		this.persistenceDesc = persistenceDesc;

		orderer = new Orderer<Transaction>(database.TraceId, logSeqNum + 1, x => x.LogSeqNum, x => x == null);

		CreatePersisters(commitVersion, logSeqNum, logStates, snapshotController);
		HandleSectorSizeMismatches();

		engine.Trace.Debug("DatabasePersister created, commitVersion={0}, logSeqNum={1}.", commitVersion, logSeqNum);
	}

	public void BeginCommitTransaction(Transaction tran)
	{
		TTTrace.Write(engine.TraceId, database.Id, tran.Id, tran.CommitVersion,
			tran.LogSeqNum, tran.AffectedLogGroups, database.ReadVersion);

		Checker.AssertTrue(tran.LogSeqNum != 0);

		// We need to persist empty transactions because those are reported to the witnesss (potentially)
		if (tran.AffectedLogGroups == 0)
			tran.AffectedLogGroups = 1;

		if (tran.Source != TransactionSource.Replication)
		{
			byte affectedLogGroups = tran.AffectedLogGroups;
			for (int i = 0; i < logPersisters.Length; i++)
			{
				if ((affectedLogGroups & (byte)(1 << i)) != 0)
				{
					tran.RegisterAsyncCommitter();
					logPersisters[i].CommitTransaction(tran);
				}
			}
		}
		else
		{
			byte affectedLogGroups = tran.AffectedLogGroups;
			for (int i = 0; i < logPersisters.Length; i++)
			{
				if ((affectedLogGroups & (byte)(1 << i)) != 0)
					tran.RegisterAsyncCommitter();
			}

			orderer.Process(tran, orderedList);
			for (int i = 0; i < orderedList.Count; i++)
			{
				Transaction currTran = orderedList[i];
				TTTrace.Write(engine.TraceId, database.Id, tran.Id, tran.CommitVersion,
					tran.LogSeqNum, tran.AffectedLogGroups, database.ReadVersion);

				affectedLogGroups = currTran.AffectedLogGroups;
				for (int j = 0; j < logPersisters.Length; j++)
				{
					if ((affectedLogGroups & (byte)(1 << j)) != 0)
						logPersisters[j].CommitTransaction(currTran);
				}
			}

			orderedList.Clear();
		}
	}

	public void Rewind(ulong version)
	{
		TTTrace.Write(engine.TraceId, database.Id, database.ReadVersion, version);

		for (int i = 0; i < logPersisters.Length; i++)
		{
			logPersisters[i].RewindVersions(version);
		}
	}

	private void CreatePersisters(ulong commitVersion, ulong logSeqNum,
		DatabaseRestorer.LogState[] logStates, SnapshotSemaphore snapshotSemaphore)
	{
		if (persistenceDesc == null)
			return;

		logPersisters = new LogPersister[persistenceDesc.LogDescriptors.Length];
		for (int i = 0; i < persistenceDesc.LogDescriptors.Length; i++)
		{
			Checker.AssertTrue(logStates[i].LogIndex == i);

			LogDescriptor ld = persistenceDesc.LogDescriptors[i];
			logPersisters[i] = new LogPersister(i, database, snapshotSemaphore, commitVersion,
				logSeqNum, ld, logStates[i].ActiveFile, logStates[i].Header, logStates[i].Position);
		}
	}

	private void HandleSectorSizeMismatches()
	{
		List<int> logIndexes = new List<int>(1);
		for (int i = 0; i < logPersisters.Length; i++)
		{
			if (logPersisters[i].SectorSizeMismatch)
				logIndexes.Add(i);
		}

		if (logIndexes.Count > 0)
			CreateSnapshots(logIndexes);
	}

	public void UpdateConfiguration(PersistenceUpdate update)
	{
		database.Trace.Debug("Updating persistence configuration, databaseId={0}, IsRecreationRequired={1}.",
			database.Id, update.IsRecreationRequired);

		if (database.Trace.ShouldTrace(Common.TraceLevel.Debug))
		{
			for (int i = 0; i < update.UpdatedLogs.Length; i++)
			{
				TTTrace.Write(engine.TraceId, database.Id, update.IsRecreationRequired, update.UpdatedLogs[i]);
				database.Trace.Debug("Updated log {0}.", update.UpdatedLogs[i]);
			}
		}

		foreach (string logName in update.UpdatedLogs)
		{
			LogPersister persister = logPersisters.First(x => x.Name.Equals(logName, StringComparison.OrdinalIgnoreCase));
			LogDescriptor logDesc = persistenceDesc.LogDescriptors.First(x => x.Name.Equals(logName, StringComparison.OrdinalIgnoreCase));
			persister.UpdateConfiguration(logDesc);
		}

		persistenceDesc = update.PersistenceDescriptor;
	}

	public void CreateSnapshots(IEnumerable<int> logIndexes = null)
	{
		TTTrace.Write(engine.TraceId, database.Id, logIndexes != null);
		database.Trace.Debug("Creating persistence snapshots for databaseId={0}.", database.Id);

		logIndexes ??= Enumerable.Range(0, logPersisters.Length);

		int[] snapshotCounts = new int[logPersisters.Length];

		foreach (int index in logIndexes)
		{
			TTTrace.Write(engine.TraceId, database.Id, index);
			snapshotCounts[index] = logPersisters[index].SnapshotCount;
			logPersisters[index].RequestSnapshotCreation();
		}

		foreach (int index in logIndexes)
		{
			SpinWait.SpinUntil(() => snapshotCounts[index] < logPersisters[index].SnapshotCount);
		}
	}

	public void MovePersistenceFromTempLocation()
	{
		for (int i = 0; i < logPersisters.Length; i++)
		{
			logPersisters[i].MovePersistenceFromTempLocation();
		}
	}

	public void DropAndDispose()
	{
		for (int i = 0; i < logPersisters.Length; i++)
		{
			logPersisters[i].DropAndDispose();
		}
	}

	public void InitSequence(ulong logSeqNum)
	{
		orderer.ResetNextId(logSeqNum + 1);
	}

	public void Dispose()
	{
		for (int i = 0; i < logPersisters.Length; i++)
		{
			logPersisters[i].Dispose();
		}
	}

#if TEST_BUILD
	internal void CreateLogSnapshotAfterRestore(int logIndex)
	{
		TTTrace.Write(engine.TraceId, logIndex);
		logPersisters[logIndex].CreateSnapshotAfterRestore();
	}

	internal void PreventLogging(int logIndex)
	{
		logPersisters[logIndex].PreventLogging();
	}

	internal void WaitSnapshotFinished()
	{
		for (int i = 0; i < logPersisters.Length; i++)
		{
			logPersisters[i].WaitSnapshotFinished();
		}
	}
#endif
}
