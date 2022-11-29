using System;
using System.IO;
using System.Reflection.PortableExecutable;
using System.Threading;
using System.Threading.Tasks;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.Storage.Persistence;

internal unsafe sealed class LogPersister : IDisposable
{
	const string snapshotExtension = ".vxs";
	const string logExtension = ".vxl";
	const string logNameFormat = "{0}_{1}" + logExtension;
	const string snapshotNameFormat = "{0}_{1}" + snapshotExtension;

	const int initCapacity = 1024 * 16;

	readonly object sync = new object();

	int logIndex;
	Database database;

	LogDescriptor logDesc;

	long pendingSize;
	int itemCount;
	LogItem[] pendingItems;
	LogItem[] pendingItems2;

	bool shouldClose;
	Thread workerThread;
	SemaphoreSlim workSignal;

	volatile Task snapshotWriteTask;

	LogFileWriter fileWriter;

	int activeFile;
	string[] logFileNames;
	string[] snapshotFileNames;

	bool snapshotRequested;
	bool forceSnapshot;
	int snapshotCount;

	ulong maxCommitVersion;
	ulong maxLogSeqNum;

	SnapshotSemaphore snapshotController;

	public LogPersister(int logIndex, Database database, SnapshotSemaphore snapshotSemaphore, ulong commitVersion, ulong logSeqNum,
		LogDescriptor logDesc, int activeFile, LogFileHeader header, long initPosition)
	{
		TTTrace.Write(database.TraceId, database.Id, logIndex, commitVersion, logIndex, activeFile, initPosition);

		this.logIndex = logIndex;
		this.database = database;
		this.logDesc = logDesc;
		this.maxCommitVersion = commitVersion;
		this.maxLogSeqNum = logSeqNum;
		this.snapshotController = snapshotSemaphore;

		GetLogNames(database.Engine, logDesc, out logFileNames);
		GetSnapshotNames(database.Engine, logDesc, out snapshotFileNames);

		this.activeFile = activeFile;

		fileWriter = new LogFileWriter(logFileNames[activeFile], header.timestamp, header.marker,
			header.sectorSize, header.isPackedFormat, initPosition, logDesc.MaxSize);

		pendingItems = new LogItem[initCapacity];
		pendingItems2 = new LogItem[initCapacity];

		workSignal = new SemaphoreSlim(0);
		workerThread = Utils.RunThreadWithSupressedFlow(Worker, string.Format("{0}: vlx-Persister {1}",
			database.Engine.Trace.Name, logIndex.ToString()));

		database.Trace.Debug("Log persister created logIndex={0}, commitVersion={1}, logSeqNum={2}.", logIndex, commitVersion, logSeqNum);
	}

	public int SnapshotCount => snapshotCount;
	public string Name => logDesc.Name;
	public int LogIndex => logIndex;

	public static void GetLogNames(StorageEngine engine, LogDescriptor ld, out string[] logs)
	{
		logs = new string[2];
		for (int i = 0; i < logs.Length; i++)
		{
			logs[i] = string.Format(logNameFormat, ld.Name, i);
			logs[i] = Path.Combine(ld.FinalDirectory(engine), logs[i]);
		}
	}

	public static void GetTempLogNames(StorageEngine engine, LogDescriptor ld, out string[] logs)
	{
		logs = new string[2];
		for (int i = 0; i < logs.Length; i++)
		{
			logs[i] = string.Format(logNameFormat, ld.Name, i);
			logs[i] = Path.Combine(ld.FinalDirectory(engine), LogDescriptor.TempSufix, logs[i]);
		}
	}

	public static void GetSnapshotNames(StorageEngine engine, LogDescriptor ld, out string[] snapshots)
	{
		snapshots = new string[2];
		for (int i = 0; i < snapshots.Length; i++)
		{
			snapshots[i] = string.Format(snapshotNameFormat, ld.Name, i);
			snapshots[i] = Path.Combine(ld.FinalSnapshotDirectory(engine), snapshots[i]);
		}
	}

	public static void GetTempSnapshotNames(StorageEngine engine, LogDescriptor ld, out string[] snapshots)
	{
		snapshots = new string[2];
		for (int i = 0; i < snapshots.Length; i++)
		{
			snapshots[i] = string.Format(snapshotNameFormat, ld.Name, i);
			snapshots[i] = Path.Combine(ld.FinalSnapshotDirectory(engine), LogDescriptor.TempSufix, snapshots[i]);
		}
	}

	public void RequestSnapshotCreation(ulong snapshotVersion = 0)
	{
		lock (sync)
		{
			snapshotRequested = true;
			this.forceSnapshot = true;
			workSignal.Release();
		}
	}

	public void UpdateConfiguration(LogDescriptor logDesc)
	{
		this.logDesc = logDesc;
		fileWriter.SetLimitSize(logDesc.MaxSize);
	}

	public void CommitTransaction(Transaction tran)
	{
		TransactionContext tc = tran.Context;

		LogItem l = new LogItem(tran.CommitVersion, tran.LocalTerm, tran.GlobalTerm, tc.AffectedLogGroups,
			tran, tc.Alignment, tran.LogSeqNum, tc.LogChangesets[logIndex]);

		lock (sync)
		{
			TTTrace.Write(database.TraceId, database.Id, logIndex, tran.Id, tran.CommitVersion, tran.GlobalTerm.Low,
				tran.GlobalTerm.Hight, tran.LocalTerm, l.SerializedSize);

			if (itemCount == pendingItems.Length)
				Array.Resize(ref pendingItems, pendingItems.Length * 2);

			pendingItems[itemCount++] = l;
			pendingSize += l.SerializedSize;

			if (itemCount == 1)
				workSignal.Release();

			TTTrace.Write(database.TraceId, database.Id, logIndex, tran.Id, itemCount, activeFile);
		}
	}

	public void RewindVersions(ulong version)
	{
		TTTrace.Write(database.TraceId, database.Id, logIndex, version, maxCommitVersion);

		if (version < maxCommitVersion)
			maxCommitVersion = version;
	}

	public void DropAndDispose()
	{
		Dispose();
		for (int i = 0; i < 2; i++)
		{
			if (File.Exists(logFileNames[i]))
				File.Delete(logFileNames[i]);

			if (File.Exists(snapshotFileNames[i]))
				File.Delete(snapshotFileNames[i]);
		}
	}

	private void Worker()
	{
		TTTrace.Write(database.TraceId, database.Id, logIndex);

		while (true)
		{
			workSignal.Wait();

			bool b = shouldClose;
			if (b && snapshotWriteTask != null)
				snapshotWriteTask.Wait();

			if (snapshotWriteTask != null && snapshotWriteTask.IsCompleted)
				FinalizeSnapshotWriting();

			WritePending();

			if (b)
				return;

			if (fileWriter.IsFull || snapshotRequested)
				CreateSnapshot();

			snapshotRequested = false;
		}
	}

	private Transaction CreateTransactionRobust(out DatabaseVersions versions)
	{
		try
		{
			return database.Engine.TryCreateTransactionWithVersions(database.Id, TransactionType.Read, forceSnapshot, 1, out versions);
		}
		catch (DatabaseException e)
		{
			TTTrace.Write(database.TraceId, database.Id, forceSnapshot, logIndex, (int)e.Detail.ErrorType);
			if (e.Detail.ErrorType == DatabaseErrorType.DatabaseBusy || e.Detail.ErrorType == DatabaseErrorType.DatabaseDisposed)
			{
				versions = null;
				return null;
			}

			throw;
		}
		finally
		{
			forceSnapshot = false;
		}
	}

	private void CreateSnapshot()
	{
		TTTrace.Write(database.TraceId, database.Id, logIndex, activeFile, forceSnapshot);

		IDisposable snapshotLock = null;
		if (!forceSnapshot && !snapshotController.Enter(out snapshotLock))
			return;

		try
		{
			if (snapshotWriteTask != null)
				return;

			// This should be a very short wait (if any).
			SpinWait.SpinUntil(() => database.ReadVersion >= maxCommitVersion);

			Transaction snapshot = CreateTransactionRobust(out DatabaseVersions versions);
			if (snapshot == null)
				return;

			uint timestamp = fileWriter.Timestamp + 1;

			fileWriter.Dispose();
			activeFile = (activeFile + 1) % logFileNames.Length;

			TTTrace.Write(database.TraceId, database.Id, logIndex, activeFile, snapshot.Id);

			fileWriter = new LogFileWriter(logFileNames[activeFile], timestamp,
				fileWriter.SectorSize, fileWriter.IsPackedFormat, logDesc.MaxSize);
			fileWriter.Activate();

			WriteSnapshot(maxCommitVersion, maxLogSeqNum, snapshotFileNames[activeFile], snapshot, versions);
		}
		finally
		{
			snapshotLock?.Dispose();
		}
	}

	private void WriteSnapshot(ulong commitVersion, ulong logSeqNum, string fileName, Transaction transaction, DatabaseVersions versions)
	{
		TTTrace.Write(database.TraceId, database.Id, logIndex, commitVersion, logSeqNum);
		database.Engine.Trace.Debug(
			"Creating persistence snapshot, commitVersion={0}, logSeqNum={1}, logIndex={2}, readVersion={3}.",
			commitVersion, logSeqNum, logIndex, transaction.ReadVersion);

		snapshotWriteTask = new Task(() =>
		{
			transaction.TakeThreadOwnership();
			versions.SetCommitAndLogSeqNum(commitVersion, logSeqNum);

#if TEST_BUILD
			if (SnapshotDelay > 0) Thread.Sleep(SnapshotDelay);
#endif

			SnapshotFileWriter snapshotWriter = new SnapshotFileWriter(database, transaction, versions, logIndex);
			snapshotWriter.CreateSnapshot(fileName);

		}, TaskCreationOptions.LongRunning);

		snapshotWriteTask.ContinueWith(t =>
		{
			workSignal.Release();
		});

		snapshotWriteTask.Start();
	}

	private void FinalizeSnapshotWriting()
	{
#if TEST_BUILD
		try
		{
#endif
			fileWriter.MarkHasSnapshot();
			File.Delete(snapshotFileNames[(activeFile + 1) % snapshotFileNames.Length]);

			snapshotWriteTask = null;
			Interlocked.Increment(ref snapshotCount);

			database.Engine.Trace.Debug("Persisted snapshot created.");
#if TEST_BUILD
		}
		finally
		{
			if (syncSnapshotEvent != null)
				syncSnapshotEvent.Set();
		}
#endif
	}

	private void WritePending()
	{
		TakePendingItems(out int count, out long byteSize);

		if (count == 0)
			return;

		TTTrace.Write(database.TraceId, database.Id, logIndex, count, byteSize);

#if TEST_BUILD
		if (!preventLogging)
#endif
			fileWriter.WriteItems(pendingItems2, count, byteSize);

		for (int i = 0; i < count; i++)
		{
			LogItem li = pendingItems2[i];

			TTTrace.Write(database.TraceId, database.Id, logIndex, li.CommitVersion);

			if (li.LogSeqNum != 0)
			{
				if (li.LogSeqNum > maxLogSeqNum)
				{
					maxCommitVersion = li.CommitVersion;
					maxLogSeqNum = li.LogSeqNum;
				}
			}
			else if (li.CommitVersion > maxCommitVersion)
			{
				maxCommitVersion = li.CommitVersion;
			}

			if (li.Transaction != null)
				li.Transaction.AsyncCommiterFinished();
		}
	}

	private void TakePendingItems(out int count, out long byteSize)
	{
		lock (sync)
		{
			count = itemCount;
			byteSize = pendingSize;
			Utils.Exchange(ref pendingItems2, ref pendingItems);
			itemCount = 0;
			pendingSize = 0;
		}
	}

	public void MovePersistenceFromTempLocation()
	{
		GetLogNames(database.Engine, logDesc, out logFileNames);
		GetTempLogNames(database.Engine, logDesc, out var tempLogFileNames);
		GetSnapshotNames(database.Engine, logDesc, out snapshotFileNames);
		GetTempSnapshotNames(database.Engine, logDesc, out var tempSnapshotFileNames);

		fileWriter.MoveFile(() =>
		{
			for (int i = 0; i < logFileNames.Length; i++)
			{
				File.Move(tempLogFileNames[i], logFileNames[i]);
			}

			File.Move(tempSnapshotFileNames[activeFile], snapshotFileNames[activeFile]);
			return logFileNames[activeFile];
		});

		string tempLogDir = Path.GetDirectoryName(tempLogFileNames[0]);
		if (Directory.Exists(tempLogDir) && Directory.GetFileSystemEntries(tempLogDir).Length == 0)
			Directory.Delete(tempLogDir);

		string tempSnapshotDir = Path.GetDirectoryName(tempSnapshotFileNames[0]);
		if (Directory.Exists(tempSnapshotDir) && Directory.GetFileSystemEntries(tempSnapshotDir).Length == 0)
			Directory.Delete(tempSnapshotDir);
	}

#if TEST_BUILD
	public static int SnapshotDelay { get; set; }
	volatile AutoResetEvent syncSnapshotEvent;
	bool preventLogging = false;
	internal void PreventLogging()
	{
		preventLogging = true;
	}

	internal void WaitSnapshotFinished()
	{
		while (snapshotWriteTask != null)
		{
			Thread.Sleep(10);
		}

		while ((workerThread.ThreadState & System.Threading.ThreadState.WaitSleepJoin) == 0)
			Thread.Sleep(10);
	}

	public void CreateSnapshotAfterRestore()
	{
		syncSnapshotEvent = new AutoResetEvent(false);
		forceSnapshot = true;
		CreateSnapshot();
		syncSnapshotEvent.WaitOne();
		syncSnapshotEvent.Dispose();
		syncSnapshotEvent = null;
	}
#endif

	public void Dispose()
	{
		shouldClose = true;
		workSignal.Release();
		workerThread.Join();
		fileWriter.Dispose();
	}
}
