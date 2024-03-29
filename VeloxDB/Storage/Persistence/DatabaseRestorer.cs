﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using static System.Math;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using System.Threading;
using System.Reflection.PortableExecutable;

namespace VeloxDB.Storage.Persistence;

internal unsafe sealed class DatabaseRestorer
{
	public const ulong UnusedLogSeqNum = ulong.MaxValue;

	Database database;
	Tracing.Source trace;
	ulong[] snapshotLogSeqNums;
	JobWorkers<CommonWorkerParam> workers;

	byte restoredLogMask;

	// Transactions that span multiple log files are tracked here. When a multi-log transaction has been read from all
	// log files it is removed from this map. Any remaining transactions in this map after restoration have not been
	// fully logged and need to be discarded in the next restoration attempt.
	Dictionary<ulong, byte> splitTrans;

	public DatabaseRestorer(Database database, JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(database.TraceId);

		this.database = database;
		this.trace = database.Engine.Trace;
		this.workers = workers;

		splitTrans = new Dictionary<ulong, byte>(1024);
	}

	public bool TryRestore(out LogState[] logStates, ref ulong stoppingLogSeqNum, out DatabaseVersions databaseVersions)
	{
		TTTrace.Write(database.TraceId, database.Id, stoppingLogSeqNum, database.PersistenceDesc.CompleteLogMask);
		trace.Debug("Restoring database, databaseId={0}, stoppingLogSeqNum={1}.", database.Id, stoppingLogSeqNum);

		restoredLogMask = database.PersistenceDesc.CompleteLogMask;

		ReadOnlyArray<LogDescriptor> logDescs = database.PersistenceDesc.LogDescriptors;

		List<RestoreLogData> restoreLogs = DetermineRestoreLogs(logDescs);
		ReadSnapshotLogSeqNumbers(restoreLogs);

		RestoreData[] restoreData = new RestoreData[restoreLogs.Count];
		for (int i = 0; i < restoreLogs.Count; i++)
		{
			restoreData[i] = new RestoreData(database) { StoppingLogSeqNum = stoppingLogSeqNum };
			RestoreLog(restoreData[i], restoreLogs[i]);
		}

		databaseVersions = MergeDatabaseVersions(restoreData);

		TraceDatabaseVersions(databaseVersions);
		databaseVersions.TTTraceState();

		logStates = ExtractLogStates(restoreData);

		if (stoppingLogSeqNum == UnusedLogSeqNum)
		{
			stoppingLogSeqNum = DetermineStoppingLogSeqNum(restoreData);
			if (stoppingLogSeqNum != UnusedLogSeqNum)
			{
				trace.Debug("Incomplete log entries found. Stopping logSeqNum " +
					"is {0}. Database restoration will be restarted.", stoppingLogSeqNum);
			}

			TTTrace.Write(database.TraceId, database.Id, stoppingLogSeqNum);
			return stoppingLogSeqNum == UnusedLogSeqNum;
		}

		return true;
	}

	private void ReadSnapshotLogSeqNumbers(List<RestoreLogData> restoreLogs)
	{
		TTTrace.Write(database.TraceId);

		snapshotLogSeqNums = new ulong[restoreLogs.Count];
		for (int i = 0; i < restoreLogs.Count; i++)
		{
			DatabaseVersions versions = SnapshotFileReader.ReadVersions(database,
				restoreLogs[i].SnapshotFileName, restoreLogs[i].LogIndex);

			versions.TTTraceState();
			TTTrace.Write(database.TraceId, database.Id, i, versions.LogSeqNum);

			snapshotLogSeqNums[restoreLogs[i].LogIndex] = versions.LogSeqNum;
		}
	}

	private static DatabaseVersions MergeDatabaseVersions(RestoreData[] restoreData)
	{
		DatabaseVersions versions = restoreData[0].Versions;
		for (int i = 1; i < restoreData.Length; i++)
		{
			versions.MergeFrom(restoreData[i].Versions);
		}

		return versions;
	}

	private List<RestoreLogData> DetermineRestoreLogs(ReadOnlyArray<LogDescriptor> logDescs)
	{
		TTTrace.Write(database.TraceId);

		List<RestoreLogData> l = new List<RestoreLogData>(logDescs.Length);
		for (int i = 0; i < logDescs.Length; i++)
		{
			RestoreLogData ld = CreateRestoreLogData(i, logDescs[i]);
			if (trace.ShouldTrace(TraceLevel.Verbose))
			{
				trace.Verbose("Log to restore, snapshotFile={0}, logCount={1}.",
					ld.SnapshotFileName ?? string.Empty, ld.LogFileNames.Length);
			}

			TTTrace.Write(database.TraceId, database.Id, i, ld.LogFileNames.Length);

			l.Add(ld);
		}

		return l;
	}

	private ulong DetermineStoppingLogSeqNum(RestoreData[] restoreData)
	{
		ulong stoppingLogSeqNum = UnusedLogSeqNum;

		if (splitTrans.Count != 0)
		{
			foreach (KeyValuePair<ulong, byte> kv in splitTrans)
			{
				ulong lsn = kv.Key;
				if (lsn < stoppingLogSeqNum)
					stoppingLogSeqNum = lsn;
			}
		}

		for (int i = 0; i < restoreData.Length; i++)
		{
			if (restoreData[i].StoppingLogSeqNum < stoppingLogSeqNum)
				stoppingLogSeqNum = restoreData[i].StoppingLogSeqNum;
		}

		return stoppingLogSeqNum;
	}

	private LogState[] ExtractLogStates(RestoreData[] restoreData)
	{
		TTTrace.Write(database.TraceId, database.Id, restoreData.Length);

		List<LogState> l = new List<LogState>();
		for (int i = 0; i < restoreData.Length; i++)
		{
			RestoreData p = restoreData[i];
			LogState state = p.LogStates[p.LogStates.Count - 1];

			TTTrace.Write(database.TraceId, database.Id, state.ActiveFile, state.Header.timestamp,
				state.Header.hasSnapshot, state.LogIndex, state.ActiveFile);
			Checker.AssertTrue(state.LogIndex == i);

			l.Add(state);
		}

		return l.ToArray();
	}

	private void RestoreLog(RestoreData restoreData, RestoreLogData restoreLog)
	{
		TTTrace.Write(database.TraceId, database.Id, restoreLog.LogIndex);

		ulong newStoppingLogSeqNum = RestoreLog(restoreLog,
			restoreData.StoppingLogSeqNum, out long filePosition, out DatabaseVersions versions);

		versions.TTTraceState();
		restoreData.Versions.MergeFrom(versions);

		LogState ls = new LogState()
		{
			ActiveFile = restoreLog.FileIndexes[restoreLog.FileIndexes.Length - 1],
			LogIndex = restoreLog.LogIndex,
			Position = filePosition,
			Header = restoreLog.LogHeaders[restoreLog.LogHeaders.Length - 1],
		};

		TTTrace.Write(database.TraceId, database.Id, ls.LogIndex, ls.Position, ls.ActiveFile, ls.Header.version, newStoppingLogSeqNum);

		restoreData.StoppingLogSeqNum = newStoppingLogSeqNum;
		restoreData.LogStates.Add(ls);
	}

	private ulong RestoreLog(RestoreLogData restoreLog, ulong stoppingLogSeqNum, out long filePosition, out DatabaseVersions versions)
	{
		using (SnapshotFileReader sfr = new SnapshotFileReader(database, workers, restoreLog.SnapshotFileName, restoreLog.LogIndex))
		{
			sfr.Restore(out versions);
		}

		using (LogFileReader reader = new LogFileReader(database.Engine.Trace.Name, restoreLog.LogFileNames[0], restoreLog.LogHeaders[0]))
		{
			ulong newInvalidLogSeqNum = RestoreLogFile(reader, restoreLog.LogIndex, stoppingLogSeqNum, versions, out filePosition);
			if (newInvalidLogSeqNum != UnusedLogSeqNum)
				return newInvalidLogSeqNum;
		}

		// If the creation of snapshot file failed during the previous runtime of the database
		// we can have more than one log file to restore.
		if (restoreLog.LogFileNames.Length == 1)
			return UnusedLogSeqNum;

		TTTrace.Write(database.TraceId, database.Id, restoreLog.LogIndex);
		trace.Debug("More than one log file discovered, logIndex={0}. Creating snapshot...", restoreLog.LogIndex);

		// We have, so far, restored the snapshot and one log file. This is a good place to create the missing
		// snapshot. After that we can just restore the second log file.
		CreateSnapshot(restoreLog.LogDesc, restoreLog.LogIndex, restoreLog.LogHeaders[1], versions, restoreLog.FileIndexes[1]);
		File.Delete(restoreLog.SnapshotFileName);

		using (LogFileReader reader = new LogFileReader(database.Engine.Trace.Name, restoreLog.LogFileNames[1], restoreLog.LogHeaders[1]))
		{
			ulong newInvalidLogSeqNum = RestoreLogFile(reader, restoreLog.LogIndex, stoppingLogSeqNum, versions, out filePosition);
			return newInvalidLogSeqNum;
		}
	}

	private void CreateSnapshot(LogDescriptor logDesc, int logIndex, LogFileHeader header, DatabaseVersions versions, int fileIndex)
	{
		TTTrace.Write(database.TraceId, database.Id, logIndex, fileIndex);

		LogPersister.GetLogNames(database.Engine, logDesc, out string[] logFileNames);
		LogPersister.GetSnapshotNames(database.Engine, logDesc, out string[] snapshotFileNames);

		string snapshotFileName = snapshotFileNames[fileIndex];
		string logFileName = logFileNames[fileIndex];

		SnapshotFileWriter sfw = new SnapshotFileWriter(database, null, versions, logIndex);
		sfw.CreateSnapshot(snapshotFileName);

		string tempFileName = GetTempLogFileName(logFileName);
		File.Move(logFileName, tempFileName);
		LogFileWriter.CreateEmpty(logFileName, header.timestamp, true, logDesc.MaxSize, logDesc.IsPackedFormat);
		File.Delete(tempFileName);
	}

	private void SetLogRestoreActions(int logIndex, ulong stoppingLogSeqNum, DatabaseVersions snapshotVersions,
		out PendingRestoreOperations pendingOperations, out DatabaseVersions[] versions)
	{
		PendingRestoreOperations pendingOps = pendingOperations =
			new PendingRestoreOperations(database.TraceId, database.Engine.MemoryManager);

		versions = new DatabaseVersions[workers.WorkerCount];
		Action<CommonWorkerParam>[] actions = new Action<CommonWorkerParam>[workers.WorkerCount];
		for (int i = 0; i < actions.Length; i++)
		{
			// It is very important that the snapshot version is used by the first worker (while others get clones).
			// When a rewind occurs, all version sets need to be affected (including the snapshot versions) so that
			// the final merge of all versions is correct.
			versions[i] = i == 0 ? snapshotVersions : snapshotVersions.Clone();

			LogWorkerParam p = new LogWorkerParam()
			{
				LogIndex = logIndex,
				Reader = new ChangesetReader(),
				Block = new ChangesetBlock(),
				Versions = versions[i],
				StoppingLogSeqNum = stoppingLogSeqNum
			};

			actions[i] = dp =>
			{
				if (dp.LogWorkerItem.LogItems != null)
					RestoreLogItems(p, dp, pendingOps);
				else
					RestoreLogBlock(p, dp, pendingOps);
			};
		}

		workers.SetActions(actions);
	}

	private void RestoreLogBlock(LogWorkerParam p, CommonWorkerParam dp, PendingRestoreOperations pendingOps)
	{
		using (dp.LogWorkerItem.BlockBuffer)
		{
			byte* buffer = (byte*)dp.LogWorkerItem.BlockBuffer.Value;
			long size = dp.LogWorkerItem.BlockBuffer.Size;

			if (LogFileReader.TryExtractLogItems(database.Engine.MemoryManager, buffer, size, p.LogItems, p.StoppingLogSeqNum))
			{
#if TTTRACE
				for (int i = 0; i < p.LogItems.Count; i++)
				{
					TTTrace.Write(database.TraceId, database.Id, p.LogItems[i].CommitVersion, p.LogItems[i].LogSeqNum,
						p.LogItems[i].AffectedLogsMask, p.LogItems[i].ChangesetCount);
				}
#endif

				dp.LogWorkerItem = new LogWorkerItem() { IsAlignment = dp.LogWorkerItem.IsAlignment, LogItems = p.LogItems };
				RestoreLogItems(p, dp, pendingOps);

				for (int i = 0; i < p.LogItems.Count; i++)
				{
					LogItem logItem = p.LogItems[i];
					p.Versions.TransactionRestored(logItem.GlobalTerm, logItem.LocalTerm,
						logItem.CommitVersion, logItem.LogSeqNum);
					UpdateSplitTransaction(logItem, p.LogIndex);
				}
			}

			p.LogItems.Clear();
		}
	}

	private void RestoreLogItems(LogWorkerParam p, CommonWorkerParam dp, PendingRestoreOperations pendingOps)
	{
		LogWorkerItem item = dp.LogWorkerItem;
		for (int k = 0; k < item.LogItems.Count; k++)
		{
			LogItem logItem = item.LogItems[k];
			TTTrace.Write(database.TraceId, database.Id, item.IsAlignment, logItem.CommitVersion,
				logItem.LogSeqNum, logItem.AffectedLogsMask, logItem.ChangesetCount, logItem.GlobalTerm.Hight,
				logItem.GlobalTerm.Low, logItem.LocalTerm, logItem.Alignment != null ? (int)logItem.Alignment.Type : 0);

			LogChangeset curr = logItem.ChangesetsList;
			for (int i = 0; i < logItem.ChangesetCount; i++)
			{
#if TEST_BUILD
				if (TryGetSlowdown(logItem.CommitVersion, i, out int dur))
					Thread.Sleep(dur);
#endif

				using (curr)
				{
					p.Reader.Init(database.ModelDesc, curr);
					database.Engine.RestoreChangeset(database, p.Block, pendingOps,
						p.Reader, logItem.CommitVersion, item.IsAlignment, p.LogIndex);
				}

				curr = curr.Next;
			}
		}
	}

	private unsafe ulong RestoreLogFile(LogFileReader reader, int logIndex,
		ulong stoppingLogSeqNum, DatabaseVersions versions, out long filePosition)
	{
		TTTrace.Write(database.TraceId, database.Id, logIndex, stoppingLogSeqNum);
		versions.TTTraceState();

		trace.Debug("Restoring log file, logIndex={0}.", logIndex);

		SetLogRestoreActions(logIndex, stoppingLogSeqNum, versions,
			out PendingRestoreOperations restoreOps, out DatabaseVersions[] perWorkerVersions);

		List<LogItem> logItems = new List<LogItem>(8);
		filePosition = 0;

		while (reader.TryReadBlock(out var buffer, out bool isStandard))
		{
			if (isStandard)
			{
				workers.EnqueueWork(new CommonWorkerParam()
				{
					LogWorkerItem = new LogWorkerItem()
					{
						BlockBuffer = buffer,
						IsAlignment = false,
					}
				});
			}
			else
			{
				using (buffer)
				{
					if (!LogFileReader.TryExtractLogItems(database.Engine.MemoryManager,
						(byte*)buffer.Value, buffer.Size, logItems, stoppingLogSeqNum))
					{
						break;
					}
				}

				if (!TryProcessNonParallel(logIndex, reader, logItems, perWorkerVersions, ref stoppingLogSeqNum))
					return stoppingLogSeqNum;
			}

		}

		workers.Drain();
		filePosition = reader.GetLogPosition();

		restoreOps.ValidateEmpty();

		TTTrace.Write(database.TraceId, database.Id, logIndex);
		Checker.AssertTrue(object.ReferenceEquals(versions, perWorkerVersions[0]));
		for (int i = 1; i < perWorkerVersions.Length; i++)
		{
			versions.MergeFrom(perWorkerVersions[i]);
		}

		TTTrace.Write(database.TraceId, database.Id, logIndex, filePosition);
		return UnusedLogSeqNum;
	}

	private bool TryProcessNonParallel(int logIndex, LogFileReader reader, List<LogItem> logItems,
		DatabaseVersions[] perWorkerVersions, ref ulong stoppingLogSeqNum)
	{
		if (logItems[0].Alignment != null)  // Beginning of alignment will come as a single logItem
		{
			if (!TryProcessAlignment(logIndex, reader, logItems, perWorkerVersions, ref stoppingLogSeqNum))
				return false;
		}
		else
		{
			workers.Drain();
			workers.EnqueueWork(new CommonWorkerParam()
			{
				LogWorkerItem = new LogWorkerItem()
				{
					LogItems = new List<LogItem>(logItems),
					IsAlignment = false,
				}
			});

			workers.Drain();

			for (int i = 0; i < logItems.Count; i++)
			{
				LogItem logItem = logItems[i];
				perWorkerVersions.ForEach(x => x.TransactionRestored(logItem.GlobalTerm, logItem.LocalTerm,
					logItem.CommitVersion, logItem.LogSeqNum));
				UpdateSplitTransaction(logItem, logIndex);
			}
		}

		logItems.Clear();

		return true;
	}

	private bool TryProcessAlignment(int logIndex, LogFileReader reader, List<LogItem> logItems,
		DatabaseVersions[] perWorkerVersions, ref ulong stoppingLogSeqNum)
	{
		LogItem beginLogItem = logItems[0];

		TTTrace.Write(database.TraceId, database.Id, logIndex, beginLogItem.CommitVersion,
			beginLogItem.ChangesetCount, (int)logItems[0].Alignment.Type);
		logItems[0].TTTraceState(database.TraceId);

		Checker.AssertTrue(logItems[0].Alignment.Type == AlignmentTransactionType.Beginning);

		// We must not interleave alignment restoration with regular transactions since alignment might
		// update records to a smaller version which would break the parallelization logic (which uses
		// previous version to order updates of a single object). During alignment every object is only
		// touched at most once, so there is no need to order anything.
		workers.Drain();

		// Extract and process rewind if present
		ProcessAlignmentAction(logIndex, logItems, perWorkerVersions, beginLogItem);

		perWorkerVersions.ForEach(x => x.AlignmentRestored(logItems[0].Alignment.GlobalVersions));

		// Load all alignment log items until we encounter End
		bool beginAlign = true;
		while (true)
		{
			for (int i = 0; i < logItems.Count; i++)
			{
				long size = logItems[i].SerializedSize;
				workers.EnqueueWork(new CommonWorkerParam()
				{
					LogWorkerItem = new LogWorkerItem()
					{
						LogItems = new List<LogItem> { logItems[i] },
						IsAlignment = true,
					}
				});

				TTTrace.Write(database.TraceId, database.Id, logItems[i].GlobalTerm.Low, logItems[i].GlobalTerm.Hight,
					logItems[i].LocalTerm, logItems[i].CommitVersion, logItems[i].LogSeqNum, logItems[i].AffectedLogsMask);

				perWorkerVersions.ForEach(x => x.TransactionRestored(logItems[i].GlobalTerm, logItems[i].LocalTerm,
					logItems[i].CommitVersion, logItems[i].LogSeqNum));

				UpdateSplitTransaction(logItems[i], logIndex);
			}

			if (beginAlign)
			{
				workers.Drain();
				beginAlign = false;
			}

			if (logItems[0].Alignment != null && logItems[0].Alignment.Type == AlignmentTransactionType.End)
			{
				TTTrace.Write(database.TraceId, database.Id, logIndex);
				break;
			}

			logItems.Clear();

			if (!reader.TryReadBlock(out var buffer, out _) || !LogFileReader.TryExtractLogItems(database.Engine.MemoryManager,
					(byte*)buffer.Value, buffer.Size, logItems, stoppingLogSeqNum))
			{
				TTTrace.Write(database.TraceId, database.Id, logIndex);
				buffer.Dispose();
				workers.Drain();
				stoppingLogSeqNum = beginLogItem.LogSeqNum;
				return false;
			}

			buffer.Dispose();
		}

		// Again, we need to prevent interleaving of alignment transactions with regular transactions
		workers.Drain();

		return true;
	}

	private void ProcessAlignmentAction(int logIndex, List<LogItem> logItems, DatabaseVersions[] perWorkerVersions, LogItem beginLogItem)
	{
		if (beginLogItem.ChangesetsList != null)
		{
			ChangesetReader reader = new ChangesetReader();
			reader.Init(database.ModelDesc, beginLogItem.ChangesetsList);
			if (reader.TryReadRewindBlock(out ulong rewindVersion))
			{
				perWorkerVersions.ForEach(x => x.TryRewind(rewindVersion));
				TTTrace.Write(database.TraceId, logIndex, rewindVersion, beginLogItem.CommitVersion,
					beginLogItem.LogSeqNum, beginLogItem.AffectedLogsMask, beginLogItem.ChangesetCount);

				beginLogItem.DisposeFirstChangeset();
				logItems[0] = beginLogItem;
			}
			else if (reader.TryReadDropDatabaseBlock())
			{
				perWorkerVersions.ForEach(x => x.Reset());
				TTTrace.Write(database.TraceId, database.Id, logIndex, beginLogItem.CommitVersion,
					beginLogItem.LogSeqNum, beginLogItem.AffectedLogsMask, beginLogItem.ChangesetCount);
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private byte FilterOutSnapshotedLog(LogItem rollbackCommit, int logIndex, byte affectedLogsMask)
	{
		for (int i = 0; i < snapshotLogSeqNums.Length; i++)
		{
			if (i != logIndex && rollbackCommit.LogSeqNum <= snapshotLogSeqNums[i])
			{
				// We do not need log i since it has a snapshot newer than this transaction
				affectedLogsMask &= (byte)~(1 << i);
			}
		}

		return affectedLogsMask;
	}

	private void UpdateSplitTransaction(LogItem item, int logIndex)
	{
		if (Utils.IsPowerOf2(item.AffectedLogsMask & restoredLogMask))  // Only single log affected
			return;

		byte inverseLogMask = (byte)(~(uint)(1 << logIndex));

		TTTrace.Write(database.TraceId, database.Id, logIndex, item.CommitVersion, inverseLogMask, item.AffectedLogsMask);

		lock (splitTrans)
		{
			if (!splitTrans.TryGetValue(item.LogSeqNum, out byte affectedLogsMask))
				affectedLogsMask = FilterOutSnapshotedLog(item, logIndex, item.AffectedLogsMask);

			affectedLogsMask &= inverseLogMask;
			TTTrace.Write(database.TraceId, affectedLogsMask);

			if (affectedLogsMask == 0)
			{
				splitTrans.Remove(item.LogSeqNum);
			}
			else
			{
				splitTrans[item.LogSeqNum] = affectedLogsMask;
			}
		}
	}

	private void Swap(string[] s)
	{
		string temp = s[0];
		s[0] = s[1];
		s[1] = temp;
	}

	private RestoreLogData CreateRestoreLogData(int logIndex, LogDescriptor logDesc)
	{
		LogPersister.GetLogNames(database.Engine, logDesc, out string[] logFileNames);
		LogPersister.GetSnapshotNames(database.Engine, logDesc, out string[] snapshotFileNames);

		if (!File.Exists(logFileNames[0]) && !File.Exists(logFileNames[1]) &&
			!File.Exists(snapshotFileNames[0]) && !File.Exists(snapshotFileNames[1]))
		{
			return CreateEmptyLogForRestore(database, logDesc, logIndex, logFileNames, snapshotFileNames);
		}

		int first = 0, second = 1;
		LogFileHeader head1 = LogFileReader.ReadHeader(logFileNames[0]);
		LogFileHeader head2 = LogFileReader.ReadHeader(logFileNames[1]);
		if (head1.timestamp > head2.timestamp)
		{
			Swap(logFileNames);
			Swap(snapshotFileNames);
			Utils.Exchange(ref head1, ref head2);
			Utils.Exchange(ref first, ref second);
		}

		RevertUnsuccessfulSnapshotCreation(logFileNames[1], snapshotFileNames[1]);

		if (head2.hasSnapshot)
			return CreateSingleFileVariant(logIndex, logDesc, logFileNames, snapshotFileNames, second, head2);

		return CreateMultiFileVariant(logIndex, logDesc, logFileNames, snapshotFileNames, first, second, head1, head2);
	}

	private static RestoreLogData CreateMultiFileVariant(int logIndex, LogDescriptor logDesc, string[] logFileNames,
		string[] snapshotFileNames, int first, int second, LogFileHeader head1, LogFileHeader head2)
	{
		if (File.Exists(snapshotFileNames[1]))
			File.Delete(snapshotFileNames[1]);

		return new RestoreLogData()
		{
			LogIndex = logIndex,
			LogDesc = logDesc,
			LogFileNames = new string[] { logFileNames[0], logFileNames[1] },
			LogHeaders = new LogFileHeader[] { head1, head2 },
			FileIndexes = new int[] { first, second },
			SnapshotFileName = snapshotFileNames[0]
		};
	}

	private static RestoreLogData CreateSingleFileVariant(int logIndex, LogDescriptor logDesc,
		string[] logFileNames, string[] snapshotFileNames, int second, LogFileHeader head2)
	{
		if (File.Exists(snapshotFileNames[0]))
			File.Delete(snapshotFileNames[0]);

		return new RestoreLogData()
		{
			LogIndex = logIndex,
			LogDesc = logDesc,
			LogFileNames = new string[] { logFileNames[1] },
			LogHeaders = new LogFileHeader[] { head2 },
			FileIndexes = new int[] { second },
			SnapshotFileName = snapshotFileNames[1]
		};
	}

	private void RevertUnsuccessfulSnapshotCreation(string logFileName, string snapshotFileName)
	{
		var tempFileName = GetTempLogFileName(logFileName);

		if (File.Exists(tempFileName))
		{
			File.Delete(logFileName);
			if (File.Exists(snapshotFileName))
				File.Delete(snapshotFileName);

			File.Move(tempFileName, logFileName);
		}
	}

	private static string GetTempLogFileName(string logFileName)
	{
		return Path.Combine(Path.GetDirectoryName(logFileName),
					Path.GetFileNameWithoutExtension(logFileName) + "_temp" + Path.GetExtension(logFileName));
	}

	public static LogState CreateEmptyLog(Database database, LogDescriptor logDesc, int logIndex)
	{
		LogPersister.GetLogNames(database.Engine, logDesc, out string[] logFileNames);
		LogPersister.GetSnapshotNames(database.Engine, logDesc, out string[] snapshotFileNames);

		LogFileWriter.CreateEmpty(logFileNames[0], 1, true, logDesc.MaxSize, logDesc.IsPackedFormat);
		LogFileHeader head = LogFileWriter.CreateEmpty(logFileNames[1], 2, true, logDesc.MaxSize, logDesc.IsPackedFormat);
		SnapshotFileWriter.CreateEmpty(database, logIndex, snapshotFileNames[1]);

		return new LogState()
		{
			ActiveFile = 1,
			Header = head,
			LogIndex = logIndex,
			Position = LogFileReader.GetEmptyLogPosition()
		};
	}

	private static RestoreLogData CreateEmptyLogForRestore(Database database, LogDescriptor logDesc,
		int logIndex, string[] logFileNames, string[] snapshotFileNames)
	{
		LogFileWriter.CreateEmpty(logFileNames[0], 1, true, logDesc.MaxSize, logDesc.IsPackedFormat);
		LogFileHeader head = LogFileWriter.CreateEmpty(logFileNames[1], 2, true, logDesc.MaxSize, logDesc.IsPackedFormat);
		SnapshotFileWriter.CreateEmpty(database, logIndex, snapshotFileNames[1]);

		return new RestoreLogData()
		{
			LogFileNames = new string[] { logFileNames[1] },
			SnapshotFileName = snapshotFileNames[1],
			FileIndexes = new int[] { 1 },
			LogDesc = logDesc,
			LogHeaders = new LogFileHeader[] { head },
			LogIndex = logIndex,
		};
	}

	private void TraceDatabaseVersions(DatabaseVersions versions)
	{
		if (trace.ShouldTrace(TraceLevel.Verbose))
		{
			trace.Verbose("Restored database versions:");
			GlobalVersion[] gvs = versions.UnpackClusterVersions(out uint localTerm);
			trace.Verbose("Database versions, logSeqNum={0}, commitVersion={1}, count={2}:",
				versions.LogSeqNum, versions.CommitVersion, gvs.Length);

			for (int j = 0; j < gvs.Length; j++)
			{
				trace.Verbose(gvs[j].ToString());
			}
		}
	}

	internal class RestoreLogData
	{
		public int LogIndex { get; set; }
		public LogDescriptor LogDesc { get; set; }
		public string[] LogFileNames { get; set; }
		public int[] FileIndexes { get; set; }
		public LogFileHeader[] LogHeaders { get; set; }
		public string SnapshotFileName { get; set; }
	}

	internal struct LogState
	{
		public int LogIndex { get; set; }
		public int ActiveFile { get; set; }
		public long Position { get; set; }
		public LogFileHeader Header { get; set; }

		public static void TTTraceState(LogState[] states)
		{
			TTTrace.Write(states.Length);
			for (int i = 0; i < states.Length; i++)
			{
				LogState ls = states[i];
				TTTrace.Write(ls.LogIndex, ls.ActiveFile, ls.Position, ls.Header.version);
			}
		}
	}

	internal class RestoreData
	{
		public List<LogState> LogStates { get; private set; }
		public ulong StoppingLogSeqNum { get; set; }
		public DatabaseVersions Versions { get; set; }

		public RestoreData(Database database)
		{
			Versions = new DatabaseVersions(database);
			LogStates = new List<LogState>(2);
		}
	}

	public struct LogWorkerItem
	{
		public List<LogItem> LogItems { get; set; }
		public bool IsAlignment { get; set; }
		public LogBufferPool.Buffer BlockBuffer { get; set; }
	}

	private class LogWorkerParam
	{
		public int LogIndex { get; set; }
		public ChangesetBlock Block { get; set; }
		public ChangesetReader Reader { get; set; }
		public DatabaseVersions Versions { get; set; }
		public ulong StoppingLogSeqNum { get; set; }
		public List<LogItem> LogItems { get; set; } = new List<LogItem>(8);
	}

#if TEST_BUILD
	public static Dictionary<ulong, Dictionary<int, int>> Slowdowns = new Dictionary<ulong, Dictionary<int, int>>();
	public static void AddSlowdown(ulong commitVersion, int chIndex, int duration)
	{
		if (!Slowdowns.TryGetValue(commitVersion, out Dictionary<int, int> d))
		{
			d = new Dictionary<int, int>();
			Slowdowns.Add(commitVersion, d);
		}

		d[chIndex] = duration;
	}

	private static bool TryGetSlowdown(ulong commitVersion, int chIndex, out int duration)
	{
		duration = 0;
		if (!Slowdowns.TryGetValue(commitVersion, out Dictionary<int, int> d))
			return false;

		return d.TryGetValue(chIndex, out duration);
	}
#endif
}
