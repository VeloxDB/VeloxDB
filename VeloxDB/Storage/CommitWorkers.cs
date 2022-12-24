using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal sealed class CommitWorkers
{
	StorageEngine engine;
	Database userDatabase;

	int maxMergedTranCount;
	int maxMergedOperationCount;
	int smallTranOperationLimit;

	readonly object globalSync = new object();
	bool readySignaled;
	AutoResetEvent readySignal;

	bool mainWorkerWaiting;
	AutoResetEvent mainReadySignal;

	int activeExecuteCount;
	int globalCount;
	int globalMergedCount;
	long globalOperationCount;
	Transaction[] globalQueue;

	PerCPUGroup[] perCPUGroups;

	List<Thread> workers;

	volatile bool isStopped;
	AutoResetEvent stoppedSignal;

	public CommitWorkers(StorageEngine engine)
	{
		this.engine = engine;
		userDatabase = engine.UserDatabase;

		this.maxMergedTranCount = engine.Settings.MaxMergedTransactionCount;
		this.maxMergedOperationCount = engine.Settings.MaxMergedOperationCount;

		smallTranOperationLimit = this.maxMergedOperationCount / 8;

		perCPUGroups = new PerCPUGroup[CalculateCPUIndex(ProcessorNumber.CoreCount)]; // To ensure no cache line sharing
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			perCPUGroups[CalculateCPUIndex(i)] = new PerCPUGroup(maxMergedTranCount, maxMergedOperationCount);
		}

		globalSync = new ReaderWriterLockSlim();
		readySignal = new AutoResetEvent(false);
		mainReadySignal = new AutoResetEvent(false);

		globalQueue = new Transaction[1024];

		int workerCount = engine.Settings.CommitWorkerCount;
		string name = string.Format("{0}: vlx-CommitWorker", engine.Trace.Name);
		workers = new List<Thread>(workerCount);
		for (int i = 0; i < workerCount; i++)
		{
			Thread t = Utils.RunThreadWithSupressedFlow(Worker, i == 0, name, true);
			if (i == 0)
				t.Priority = ThreadPriority.AboveNormal;

			workers.Add(t);
		}
	}

	public bool HasTransactions
	{
		get
		{
			for (int i = 0; i < ProcessorNumber.CoreCount; i++)
			{
				perCPUGroups[CalculateCPUIndex(i)].EnterLock();
			}

			try
			{
				lock (globalSync)
				{
					if (globalCount > 0)
					{
						TTTrace.Write(engine.TraceId, globalCount);
						return true;
					}

					for (int i = 0; i < ProcessorNumber.CoreCount; i++)
					{
						if (perCPUGroups[CalculateCPUIndex(i)].TransactionCount > 0)
						{
							TTTrace.Write(engine.TraceId, perCPUGroups[CalculateCPUIndex(i)].TransactionCount);
							return true;
						}
					}

					return false;
				}
			}
			finally
			{
				for (int i = 0; i < ProcessorNumber.CoreCount; i++)
				{
					perCPUGroups[CalculateCPUIndex(i)].ExitLock();
				}
			}
		}
	}

	public void Commit(Transaction tran)
	{
		TTTrace.Write(engine.TraceId, tran.Id, tran.Context.TotalAffectedCount);
		Checker.AssertTrue(tran.Source == TransactionSource.Client);

		if (tran.Context.TotalAffectedCount >= smallTranOperationLimit || tran.AsyncCallback == null)
		{
			EnqueueGlobal(tran);
		}
		else
		{
			int procNum = ProcessorNumber.GetCore();
			TTTrace.Write(tran.Engine.TraceId, tran.Id, procNum);
			perCPUGroups[CalculateCPUIndex(procNum)].Add(this, tran);
		}

		userDatabase.TransactionCompleted(tran);
	}
	
	private void Worker(object obj)
	{
		bool isMain = (bool)obj;
		AutoResetEvent signal = isMain ? mainReadySignal : readySignal;

		List<Transaction> l = new List<Transaction>(maxMergedTranCount);
		while (true)
		{
			if (isMain)
			{
				lock (globalSync)
				{
					mainWorkerWaiting = true;
					if (globalCount > 0)
						mainReadySignal.Set();
				}

				CollectPerCPUGroups(l);
			}

			signal.WaitOne();

			if (isStopped)
			{
				stoppedSignal.Set();
				return;
			}

			Dequeue(l, isMain);

			if (l.Count > 0)
			{
				try
				{
					ExecuteWork(l, isMain);
				}
				finally
				{
					Interlocked.Decrement(ref activeExecuteCount);
				}
			}
		}
	}

	private void Dequeue(List<Transaction> l, bool isMain)
	{
		lock (globalSync)
		{
			if (isMain)
			{
				mainWorkerWaiting = false;
			}
			else
			{
				readySignaled = false;
			}

			long oc = 0;
			int tc = 0;

			for (int i = globalCount - 1; i >= 0; i--)
			{
				Transaction tran = globalQueue[i];

				if (tc > 0)
				{
					if (tran.AsyncCallback == null)
						break;

					if (oc + tran.Context.TotalAffectedCount > maxMergedOperationCount ||
						tc + tran.Context.TotalMergedCount > maxMergedTranCount)
					{
						break;
					}
				}

				oc += tran.Context.TotalAffectedCount;
				tc += tran.Context.TotalMergedCount;

				l.Add(tran);
				globalCount--;
				globalMergedCount -= tran.Context.TotalMergedCount;
				globalOperationCount -= tran.Context.TotalAffectedCount;
				globalQueue[i] = null;

				if (tran.AsyncCallback == null)
					break;
			}

			if (globalCount > 0)
			{
				if (mainWorkerWaiting)
					mainReadySignal.Set();
				else
				{
					if (globalOperationCount >= maxMergedOperationCount || globalMergedCount >= maxMergedTranCount)
						SignalNonMainWorker();
				}
			}

			if (l.Count > 0)
				Interlocked.Increment(ref activeExecuteCount);
		}
	}

	private void SignalNonMainWorker()
	{
		if (readySignaled)
			return;

		readySignaled = true;
		readySignal.Set();
	}

	private void CollectPerCPUGroups(List<Transaction> l)
	{
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			Transaction published = perCPUGroups[CalculateCPUIndex(i)].TryTake();
			if (published != null)
				l.Add(published);
		}

		if (l.Count > 0)
		{
			EnqueueGlobal(l);
			l.Clear();
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private int CalculateCPUIndex(int procNum)
	{
		return procNum << 2;	// To avoid cache line sharing in perCPU groups array
	}

	private void ExecuteWork(List<Transaction> l, bool isMain)
	{
		Transaction tran = l[0];
		for (int i = 1; i < l.Count; i++)
		{
			Checker.AssertTrue(l[i].Context != null);
			TTTrace.Write(tran.Engine.TraceId, tran.Id, l[i].Id);
			tran.MergeWith(l[i]);
			l[i].ClearContext();
		}

		l.Clear();

		DatabaseErrorDetail err = null;
		ulong logSeqNum = 0;

		try
		{
			engine.CommitTransactionInternal(tran, out logSeqNum);
			Checker.AssertTrue(tran.Closed);
		}
		catch (DatabaseException e)
		{
			err = e.Detail;
		}

		TTTrace.Write(engine.TraceId, tran.Id, tran.CommitVersion, logSeqNum);

		if (tran.AsyncCallback != null)
		{
			Transaction merged = tran.NextMerged;
			while (merged != null)
			{
				merged.Closed = true;
				merged.SetCommitVersion(tran.CommitVersion);
				merged.AsyncCallback(merged.AsyncCallbackState, err != null ? new DatabaseException(err.Clone()) : null);
				merged = merged.NextMerged;
			}

			tran.ClearContext();
			tran.AsyncCallback(tran.AsyncCallbackState, err != null ? new DatabaseException(err.Clone()) : null);
		}
		else
		{
			Checker.AssertNull(tran.NextMerged);
			tran.Context.AsyncError = err;
			tran.Context.AsyncCommitWaitEvent.Set();
		}
	}

	private void EnqueueGlobal(Transaction tran)
	{
		TransactionContext tc = tran.Context;
		TTTrace.Write(tran.Engine.TraceId, tran.Id);

		lock (globalSync)
		{
			globalMergedCount += tc.TotalMergedCount;
			globalOperationCount += tc.TotalAffectedCount;

			if (globalQueue.Length == globalCount)
				Array.Resize(ref globalQueue, globalQueue.Length * 2);

			globalQueue[globalCount++] = tran;

			if (mainWorkerWaiting)
			{
				mainReadySignal.Set();
			}
			else
			{
				if (globalMergedCount >= maxMergedTranCount || globalOperationCount >= maxMergedOperationCount)
					SignalNonMainWorker();
			}
		}
	}

	private void EnqueueGlobal(List<Transaction> trans)
	{
		lock (globalSync)
		{
			for (int i = 0; i < trans.Count; i++)
			{
				Transaction tran = trans[i];
				TTTrace.Write(tran.Engine.TraceId, tran.Id);
				globalMergedCount += tran.Context.TotalMergedCount;
				globalOperationCount += tran.Context.TotalAffectedCount;

				if (globalQueue.Length == globalCount)
					Array.Resize(ref globalQueue, globalQueue.Length * 2);

				globalQueue[globalCount++] = tran;
			}

			if (mainWorkerWaiting)
			{
				mainReadySignal.Set();
			}
			else
			{
				if (globalMergedCount >= maxMergedTranCount || globalOperationCount >= maxMergedOperationCount)
					SignalNonMainWorker();
			}
		}
	}

	public void Stop()
	{
		lock (globalSync)
		{
			stoppedSignal = new AutoResetEvent(false);
			isStopped = true;
		}

		mainReadySignal.Set();
		stoppedSignal.WaitOne();

		for (int i = 1; i < workers.Count; i++)
		{
			readySignal.Set();
			stoppedSignal.WaitOne();
		}

		for (int i = 0; i < workers.Count; i++)
		{
			workers[i].Join();
		}
	}

	private struct PerCPUGroup
	{
		Transaction head;
		long operationCount;
		GroupLock sync;
		int transactionCount;
		int publishTransactionCount;
		int publishOperationCount;

		public PerCPUGroup(int maxMergedTranCount, int maxMergedOperationCount)
		{
			publishTransactionCount = maxMergedTranCount / 4;
			publishOperationCount = maxMergedOperationCount / 4;

			head = null;
			transactionCount = 0;
			operationCount = 0;
			sync = new GroupLock();
		}

		public int TransactionCount => transactionCount;

		public void EnterLock()
		{
			sync.Enter();
		}

		public void ExitLock()
		{
			sync.Exit();
		}

		public void Add(CommitWorkers commitWorkers, Transaction tran)
		{
			TransactionContext tc = tran.Context;

			Transaction publishTran = null;

			sync.Enter();

			operationCount += tc.TotalAffectedCount;
			transactionCount++;

			if (head == null)
			{
				head = tran;
			}
			else
			{
				TTTrace.Write(head.Engine.TraceId, head.Id, tran.Id);
				head.MergeWith(tran);
				tran.Engine.RemapTransactionSlot(tran, head.Slot);
				tran.ClearContext();
			}

			if (operationCount >= publishOperationCount || transactionCount >= publishTransactionCount)
			{
				TTTrace.Write(tran.Engine.TraceId, tran.Id);
				publishTran = head;
				head = null;
				operationCount = 0;
				transactionCount = 0;
				sync.Exit();
				commitWorkers.EnqueueGlobal(publishTran);
				return;
			}

			if (sync.TryExitIfNotFlagged())
				return;

			TTTrace.Write(tran.Engine.TraceId, tran.Id);
			publishTran = head;
			head = null;
			operationCount = 0;
			transactionCount = 0;
			if (publishTran != null)
				commitWorkers.EnqueueGlobal(publishTran);

			sync.Exit();
		}

		public Transaction TryTake()
		{
			if (sync.TryEnterOrFlag())
			{
				Transaction res = head;
				head = null;
				operationCount = 0;
				transactionCount = 0;
				if (res == null)
				{
					sync.FlagAndExit();
				}
				else
				{
					TTTrace.Write(res.Engine.TraceId, res.Id);
					sync.Exit();
				}

				return res;
			}

			return null;
		}
	}

	[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
	private struct GroupLock
	{
		const int taken = 0x00000001;
		const int flagged = 0x00000002;

		const int yieldAfter = 1024;

		int state;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Enter()
		{
			int count = 0;

			while (true)
			{
				int s = state;
				if ((s & taken) == 0 && Interlocked.CompareExchange(ref state, (s | taken), s) == s)
					return;

				count++;
				YieldIfNeeded(ref count);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void SetFlag()
		{
			state = state | flagged;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool TryEnterOrFlag()
		{
			while (true)
			{
				int s = state;
				if ((s & taken) == 0)
				{
					if (Interlocked.CompareExchange(ref state, (s | taken), s) == s)
						return true;
				}
				else
				{
					if (Interlocked.CompareExchange(ref state, (s | flagged), s) == s)
						return false;
				}
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool TryExitIfNotFlagged()
		{
			while (true)
			{
				int s = state;
				Checker.AssertTrue((s & taken) != 0);
				if ((s & flagged) != 0)
					return false;

				if (Interlocked.CompareExchange(ref state, 0, s) == s)
					return true;
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Exit()
		{
			while (true)
			{
				int s = state;
				Checker.AssertTrue((s & taken) != 0);
				if (Interlocked.CompareExchange(ref state, 0, s) == s)
					return;
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void FlagAndExit()
		{
			while (true)
			{
				int s = state;
				Checker.AssertTrue((s & taken) != 0);
				if (Interlocked.CompareExchange(ref state, flagged, s) == s)
					return;
			}
		}

		[MethodImpl(MethodImplOptions.NoInlining)]
		private static void YieldIfNeeded(ref int count)
		{
			if (count == yieldAfter)
			{
				count = 0;
				Thread.Yield();
			}
		}
	}
}
