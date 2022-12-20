using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal sealed class DatabaseVersions
{
	RWSpinLock sync;
	RWSpinLock writeSync;

	Database database;

	ulong tranId;
	ulong commitVersion;
	ulong readVersion;
	ulong baseStandbyLogSeqNum;
	ulong logSeqNum;

	uint localTerm;
	SimpleGuid globalTerm;
	List<GlobalVersion> globalVersions;

	TransactionOrderedCallback finalizedCallback;

	public DatabaseVersions(Database database)
	{
		TTTrace.Write(database.TraceId, database.Id);

		this.database = database;
		this.localTerm = 0;
		this.readVersion = 1;
		this.commitVersion = 1;
		this.tranId = Database.MaxCommitedVersion + 1;
		this.baseStandbyLogSeqNum = 0;
		this.logSeqNum = 0;

		globalTerm = new SimpleGuid();
		globalVersions = new List<GlobalVersion>(256);
		globalVersions.Add(new GlobalVersion(globalTerm, readVersion));

		finalizedCallback = OnTransactionFinalized;
	}

	public DatabaseVersions(Database database, ulong readVersion, ulong commitVersion,
		ulong logSeqNum, uint localTerm, List<GlobalVersion> globalVersions) :
		this(database, Database.MinTranId, readVersion, commitVersion, logSeqNum, localTerm, globalVersions)
	{
	}

	public DatabaseVersions(Database database, ulong tranId, ulong readVersion,
		ulong commitVersion, ulong logSeqNum, uint localTerm, List<GlobalVersion> globalVersions)
	{
		this.database = database;
		this.tranId = tranId;
		this.readVersion = readVersion;
		this.commitVersion = commitVersion;
		this.baseStandbyLogSeqNum = logSeqNum;
		this.logSeqNum = logSeqNum;
		this.localTerm = localTerm;
		this.globalVersions = globalVersions;
		globalTerm = globalVersions[globalVersions.Count - 1].GlobalTerm;

		finalizedCallback = OnTransactionFinalized;

		TTTraceState();
	}

	public long DatabaseId => database.Id;
	public ulong ReadVersion => readVersion;
	public ulong CommitVersion => commitVersion;
	public ulong LogSeqNum => logSeqNum;
	public uint LocalTerm => localTerm;
	public SimpleGuid GlobalTerm => globalTerm;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ulong GetNextTranId()
	{
		return Interlocked.Increment(ref tranId);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AssignCommitAndLogSeqNum(Transaction tran)
	{
		if (!tran.IsCommitVersionPreAssigned)
		{
			writeSync.EnterWriteLock();
			TTTrace.Write(database.TraceId, database.Id, tran.Id, tran.CommitVersion, tran.StandbyOrderNum, logSeqNum, baseStandbyLogSeqNum, commitVersion);
			baseStandbyLogSeqNum = logSeqNum = logSeqNum + 1;
			tran.SetCommitAndLogSeqNum(++commitVersion, logSeqNum);
			writeSync.ExitWriteLock();
		}
		else
		{
			if (tran.StandbyOrderNum == 0)
			{
				writeSync.EnterWriteLock();
				TTTrace.Write(database.TraceId, database.Id, tran.Id, tran.CommitVersion, tran.StandbyOrderNum, logSeqNum, baseStandbyLogSeqNum, commitVersion);
				baseStandbyLogSeqNum = logSeqNum = logSeqNum + 1;
				tran.SetCommitAndLogSeqNum(tran.CommitVersion, logSeqNum);
				writeSync.ExitWriteLock();
			}
			else
			{
				writeSync.EnterWriteLock();
				TTTrace.Write(database.TraceId, database.Id, tran.Id, tran.CommitVersion, tran.StandbyOrderNum, logSeqNum, baseStandbyLogSeqNum, commitVersion);
				ulong currLogSeqNum = baseStandbyLogSeqNum + tran.StandbyOrderNum;
				if (currLogSeqNum > logSeqNum)
					logSeqNum = currLogSeqNum;

				tran.SetCommitAndLogSeqNum(tran.CommitVersion, currLogSeqNum);
				writeSync.ExitWriteLock();
			}
		}
	}

	public void SetCommitAndLogSeqNum(ulong commitVersion, ulong logSeqNum)
	{
		TTTrace.Write(database.TraceId, database.Id, commitVersion, logSeqNum);
		this.commitVersion = commitVersion;
		this.baseStandbyLogSeqNum = logSeqNum;
		this.logSeqNum = logSeqNum;
	}

	private void OnTransactionFinalized(Transaction tran)
	{
		TTTrace.Write(database.TraceId, database.Id, tran.GlobalTerm.Low, tran.GlobalTerm.Hight, tran.LocalTerm, tran.CommitVersion,
			this.globalTerm.Low, this.globalTerm.Hight, this.localTerm, this.commitVersion, this.readVersion);

		if (!globalTerm.Equals(tran.GlobalTerm))
		{
			ValidateGlobalTermUnique(tran.GlobalTerm);

			RefreshLastGlobalVersion();
			globalVersions.Add(new GlobalVersion(tran.GlobalTerm, 0));
			this.globalTerm = tran.GlobalTerm;

			database.Engine.Trace.Debug("Database versions introducing new global term, dbId={0}, globalTerm={1}, version={2}.",
				database.Id, globalTerm, commitVersion);
		}

		Checker.AssertFalse(!tran.IsAlignment && tran.LocalTerm < localTerm);
		localTerm = tran.LocalTerm;
		readVersion = tran.CommitVersion;
		if (tran.IsCommitVersionPreAssigned)
		{
			Checker.AssertTrue(tran.CommitVersion >= commitVersion);
			commitVersion = tran.CommitVersion;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void FinalizeTransactionCommit(Transaction tran, TransactionCommitOrderer orderer, GlobalVersion[] alignmentGlobalVersions = null)
	{
		TTTrace.Write(database.TraceId, database.Id, tran.GlobalTerm.Low, tran.GlobalTerm.Hight, tran.LocalTerm, tran.Id, tran.CommitVersion,
			globalTerm.Low, globalTerm.Hight, localTerm, commitVersion, readVersion);

		Checker.AssertFalse(tran.CommitVersion < readVersion);

		sync.EnterWriteLock();
		try
		{
			if (tran.IsAlignment || tran.CommitVersion == readVersion + 1)
			{
				if (alignmentGlobalVersions != null)
				{
					TTTrace.Write(database.TraceId, database.Id);
					GlobalVersion.TTTraceState(database.TraceId, alignmentGlobalVersions);
					globalVersions = new List<GlobalVersion>(alignmentGlobalVersions);
					globalTerm = globalVersions[globalVersions.Count - 1].GlobalTerm;
					readVersion = globalVersions[globalVersions.Count - 1].Version;
				}

				orderer.TranCommited(tran, finalizedCallback);
			}
			else
			{
				orderer.AddPending(tran);
			}

		}
		finally
		{
			sync.ExitWriteLock();
		}
	}

	public GlobalVersion[] UnpackClusterVersions(out uint localTerm)
	{
		sync.EnterReadLock();

		try
		{
			GlobalVersion[] res = globalVersions.ToArray();
			res[res.Length - 1] = new GlobalVersion(globalTerm, readVersion);
			localTerm = this.localTerm;
			return res;
		}
		finally
		{
			sync.ExitReadLock();
		}
	}

	public void UnpackLocalTermAndReadVersion(out uint localTerm, out ulong readVersion)
	{
		sync.EnterWriteLock();

		try
		{
			localTerm = this.localTerm;
			readVersion = this.readVersion;
		}
		finally
		{
			sync.ExitWriteLock();
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public DatabaseVersions Clone()
	{
		sync.EnterReadLock();
		writeSync.EnterReadLock();

		try
		{
			List<GlobalVersion> gvs = new List<GlobalVersion>(globalVersions);
			gvs[gvs.Count - 1] = new GlobalVersion(globalTerm, readVersion);
			return new DatabaseVersions(database, tranId, readVersion, commitVersion, baseStandbyLogSeqNum, localTerm, gvs);
		}
		finally
		{
			writeSync.ExitReadLock();
			sync.ExitReadLock();
		}
	}

	public bool TryRewind(ulong version)
	{
		TTTraceState();

		sync.EnterWriteLock();

		try
		{
			RefreshLastGlobalVersion();
			int index = globalVersions.Count;
			while (index > 0)
			{
				if (globalVersions[index - 1].Version <= version)
				{
					if (globalVersions[index - 1].Version == version)
						index--;

					break;
				}

				index--;
			}

			if (index == globalVersions.Count)
				return false;

			if (globalVersions.Count - index - 1 > 0)
				globalVersions.RemoveRange(index + 1, globalVersions.Count - index - 1);

			globalTerm = globalVersions[globalVersions.Count - 1].GlobalTerm;
			readVersion = version;
			commitVersion = version;
			RefreshLastGlobalVersion();

			return true;
		}
		finally
		{
			sync.ExitWriteLock();
		}
	}

	public void MergeFrom(DatabaseVersions other)
	{
		other = other.Clone();
		TTTraceState();
		other.TTTraceState();

		sync.EnterWriteLock();

		try
		{
			tranId = Math.Max(tranId, other.tranId);
			localTerm = Math.Max(localTerm, other.localTerm);
			baseStandbyLogSeqNum = Math.Max(baseStandbyLogSeqNum, other.baseStandbyLogSeqNum);
			logSeqNum = baseStandbyLogSeqNum;

			RefreshLastGlobalVersion();
			other.RefreshLastGlobalVersion();

			globalVersions = MergeVersions(globalVersions, other.globalVersions);
			globalTerm = globalVersions[globalVersions.Count - 1].GlobalTerm;

			readVersion = globalVersions[globalVersions.Count - 1].Version;
			commitVersion = readVersion;
		}
		finally
		{
			sync.ExitWriteLock();
		}
	}

	private static List<GlobalVersion> MergeVersions(List<GlobalVersion> l1, List<GlobalVersion> l2)
	{
		List<GlobalVersion> m = new List<GlobalVersion>(l1.Count + l2.Count);
		int i = 0;
		int j = 0;
		while (i < l1.Count && j < l2.Count)
		{
			if (l1[i].GlobalTerm.Equals(l2[j].GlobalTerm))
			{
				m.Add(new GlobalVersion(l1[i].GlobalTerm, Math.Max(l1[i].Version, l2[j].Version)));
				i++;
				j++;
			}
			else if (l1[i].Version < l2[j].Version)
			{
				m.Add(l1[i]);
				i++;
			}
			else
			{
				m.Add(l2[j]);
				j++;
			}
		}

		for (int k = i; k < l1.Count; k++)
		{
			m.Add(l1[k]);
		}

		for (int k = j; k < l2.Count; k++)
		{
			m.Add(l2[k]);
		}

		return m;
	}

	public void TransactionRestored(SimpleGuid globalTerm, uint localTerm, ulong version, ulong logSeqNum)
	{
		TTTrace.Write(database.TraceId, database.Id, globalTerm.Low, globalTerm.Hight, localTerm, version,
			tranId, logSeqNum, this.globalTerm.Low, this.globalTerm.Hight, this.localTerm, this.baseStandbyLogSeqNum);

		Checker.AssertTrue(globalVersions.Count > 0);
		Checker.AssertTrue(logSeqNum > this.baseStandbyLogSeqNum);

		this.baseStandbyLogSeqNum = logSeqNum;
		this.logSeqNum = logSeqNum;

		if (!globalTerm.Equals(this.globalTerm))
		{
			RefreshLastGlobalVersion();
			globalVersions.Add(new GlobalVersion(globalTerm, version));
			this.globalTerm = globalTerm;
		}

		readVersion = version;
		commitVersion = version;
		this.localTerm = localTerm;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void RefreshLastGlobalVersion()
	{
		globalVersions[globalVersions.Count - 1] = new GlobalVersion(this.globalTerm, readVersion);
	}

	public void AlignmentRestored(GlobalVersion[] alignmentGlobalVersions)
	{
		TTTrace.Write(database.TraceId, database.Id);
		TTTraceState();
		GlobalVersion.TTTraceState(database.TraceId, alignmentGlobalVersions);

		globalVersions = new List<GlobalVersion>(alignmentGlobalVersions);
		globalTerm = globalVersions[globalVersions.Count - 1].GlobalTerm;
		readVersion = globalVersions[globalVersions.Count - 1].Version;
		commitVersion = readVersion;
	}

#if TEST_BUILD
	internal void ValidateOrderAndUniqueness()
	{
		RefreshLastGlobalVersion();
		HashSet<SimpleGuid> s = new HashSet<SimpleGuid>(globalVersions.Count);
		GlobalVersion[] gs = globalVersions.ToArray();
		for (int i = 0; i < gs.Length; i++)
		{
			if (i > 0 && gs[i].Version < gs[i - 1].Version)
				throw new InvalidOperationException();

			if (s.Contains(gs[i].GlobalTerm))
				throw new InvalidOperationException();

			s.Add(gs[i].GlobalTerm);
		}
	}
#endif

	[Conditional("DEBUG")]
	private void ValidateGlobalTermUnique(SimpleGuid globalTerm)
	{
		for (int i = 0; i < globalVersions.Count; i++)
		{
			if (globalVersions[i].Equals(globalTerm))
				throw new InvalidOperationException();
		}
	}

	[Conditional("TTTRACE")]
	public void TTTraceState()
	{
		TTTrace.Write(database.TraceId, database.Id, tranId, globalTerm.Low, globalTerm.Hight, localTerm, readVersion, commitVersion, baseStandbyLogSeqNum, logSeqNum);
		TTTrace.Write(globalVersions.Count);
		for (int i = 0; i < globalVersions.Count; i++)
		{
			GlobalVersion v = globalVersions[i];
			TTTrace.Write(database.TraceId, database.Id, v.GlobalTerm.Low, v.GlobalTerm.Hight, v.Version);
		}
	}
}
