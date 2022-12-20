using System;
using System.Collections.Generic;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal sealed class CommitWorkers
{
	readonly object sync = new object();
	SemaphoreSlim freeSpotCounter;

	int first;
	int last;
	int count;
	int capacityMask;
	Transaction[] transactions;
	int maxMergedTranCount;
	int maxMergedOperationCount;
	List<Thread> workers;
	Action<List<Transaction>> action;

	public CommitWorkers(StorageEngine engine, int initCapacity, Action<List<Transaction>> action, int maxItemCount = -1)
	{
		this.maxMergedTranCount = engine.Settings.MaxMergedTransactionCount;
		this.maxMergedOperationCount = engine.Settings.MaxMergedOperationCount;
		this.action = action;

		initCapacity = (int)Utils.GetNextPow2((uint)initCapacity);

		if (maxItemCount != -1)
			freeSpotCounter = new SemaphoreSlim(maxItemCount, maxItemCount);

		capacityMask = initCapacity - 1;
		first = 0;
		last = 0;
		count = 0;
		transactions = new Transaction[initCapacity];

		string name = string.Format("{0}: vlx-CommitWorker", engine.Trace.Name);
		workers = new List<Thread>(engine.Settings.CommitWorkerCount);
		for (int i = 0; i < engine.Settings.CommitWorkerCount; i++)
		{
			workers.Add(Utils.RunThreadWithSupressedFlow(Worker, i, name, true));
		}
	}

	public int Count => count;

	private void Worker(object obj)
	{
		List<Transaction> l = new List<Transaction>(64);
		while (true)
		{
			Dequeue(l);
			if (l.Count == 0)
				return;

			action(l);
			l.Clear();
		}
	}

	public void Enqueue(Transaction tran)
	{
		if (tran != null)
			TTTrace.Write(tran.Engine.TraceId, tran.Id);

		if (freeSpotCounter != null)
			freeSpotCounter.Wait();

		lock (sync)
		{
			if (count == transactions.Length)
				Resize();

			transactions[last] = tran;
			last = (last + 1) & capacityMask;
			count++;

			if (count == 1)
				Monitor.Pulse(sync);
		}
	}

	private void Dequeue(List<Transaction> l)
	{
		int dequeuedCount = 0;
		Monitor.Enter(sync);
		try
		{
			while (count == 0)
			{
				Monitor.Wait(sync);
			}

			Transaction curr = DequeueSingle(ref dequeuedCount);
			if (curr == null)
				return;

			l.Add(curr);

			TransactionContext currCtx = curr.Context;

			int opCount = currCtx.AffectedObjects.Count;
			while (count > 0 && l.Count < maxMergedTranCount && opCount < maxMergedOperationCount)
			{
				Transaction prev = curr;
				TransactionContext prevCtx = currCtx;

				curr = transactions[first];
				if (curr != null)
				{
					currCtx = curr.Context;
					if (curr.Database.Id != prev.Database.Id || currCtx.CommitType != prevCtx.CommitType)
						return;
				}

				l.Add(DequeueSingle(ref dequeuedCount));
				if (curr == null)
					return;

				opCount += currCtx.AffectedObjects.Count;
			}
		}
		finally
		{
			if (count > 0)
				Monitor.Pulse(sync);

			Monitor.Exit(sync);

			if (freeSpotCounter != null && dequeuedCount > 0)
				freeSpotCounter.Release(dequeuedCount);
		}
	}

	private Transaction DequeueSingle(ref int dequeuedCount)
	{
		Transaction curr = transactions[first];
		transactions[first] = null;
		first = (first + 1) & capacityMask;
		count--;
		dequeuedCount++;
		return curr;
	}

	private void Resize(int size = 0)
	{
		long newSize = (long)transactions.Length * 2;
		if (size > newSize)
		{
			newSize = size;
			newSize = (int)Utils.GetNextPow2((uint)newSize);
		}

		Transaction[] newTransactions = new Transaction[newSize];

		for (int i = 0; i < count; i++)
		{
			newTransactions[i] = transactions[first];
			first = (first + 1) & capacityMask;
		}

		transactions = newTransactions;
		capacityMask = transactions.Length - 1;
		first = 0;
		last = count & capacityMask;
		Checker.AssertTrue(last == first || count == ((last - first + transactions.Length) & capacityMask));
	}

	public void Stop()
	{
		for (int i = 0; i < workers.Count; i++)
		{
			Enqueue(null);
		}

		for (int i = 0; i < workers.Count; i++)
		{
			workers[i].Join();
		}
	}
}
