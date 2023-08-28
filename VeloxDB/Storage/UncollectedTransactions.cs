using System;
using System.Runtime.CompilerServices;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal unsafe sealed class UncollectedTransactions
{
	MemoryManager memoryManager;

	int bufferCountLimit;
	int itemCountLimit;

	Action<IntPtr> executor;

	ArrayQueue<Transaction> queue;

	int itemCount;
	int bufferCount;
	ModifiedBufferHeader* firstBuffer;

	long traceId;

	public UncollectedTransactions(MemoryManager memoryManager, StorageEngineSettings sett, Action<IntPtr> executor, long traceId)
	{
		TTTrace.Write(traceId);

		this.traceId = traceId;
		this.executor = executor;
		this.memoryManager = memoryManager;

		bufferCountLimit = sett.GCTranThreshold;
		itemCountLimit = sett.GCItemThreshold;

		int capacity = 1024;
		queue = new ArrayQueue<Transaction>(capacity);

		itemCount = 0;
		bufferCount = 0;
		firstBuffer = null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Reset()
	{
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Insert(Transaction tran)
	{
		TTTrace.Write(traceId, tran.Id, tran.CommitVersion);

		queue.Enqueue(tran);

		for (int i = queue.Count - 2; i >= 0; i--)
		{
			if (queue[i].CommitVersion <= tran.CommitVersion)
			{
				queue[i + 1] = tran;
				return;
			}

			queue[i + 1] = queue[i];
		}

		queue[0] = tran;
	}

	public void Collect(ulong oldestReadVersion)
	{
		TTTrace.Write(traceId, oldestReadVersion, queue.Count);

		while (queue.Count > 0)
		{
			Transaction tran = queue.Peek();
			if (tran.CommitVersion > oldestReadVersion)
				break;

			queue.Dequeue();
			EnqueueCommands(tran, oldestReadVersion);
		}
	}

	public void Flush(ulong databaseReadVersion)
	{
		TTTrace.Write(traceId);

		Collect(databaseReadVersion);

		if (bufferCount == 0)
			return;

		executor.Invoke((IntPtr)firstBuffer);

		itemCount = 0;
		bufferCount = 0;
		firstBuffer = null;
	}

	private void EnqueueCommandBuffers(ulong bufferHandle, ulong readVersion)
	{
		ModifiedBufferHeader* curr = (ModifiedBufferHeader*)memoryManager.GetBuffer(bufferHandle);
		while (curr != null)
		{
			Checker.AssertTrue(curr->nextQueueGroup == null);

			curr->readVersion = readVersion;
			curr->nextQueueGroup = firstBuffer;
			firstBuffer = curr;

			bufferCount++;
			itemCount += curr->count;

			ulong nextHandle = curr->nextBuffer;

			if (itemCount > itemCountLimit || bufferCount > bufferCountLimit)
			{
				executor.Invoke((IntPtr)firstBuffer);
				firstBuffer = null;
				bufferCount = 0;
				itemCount = 0;
			}

			curr = (ModifiedBufferHeader*)memoryManager.GetBuffer(nextHandle);
		}
	}

	private void EnqueueCommands(Transaction tran, ulong readVersion)
	{
		TTTrace.Write(tran.Engine.TraceId, tran.Id, tran.CommitVersion);
		Transaction.CollectableCollections affected = tran.Garbage;

		if (affected.invRefs != 0)
			EnqueueCommandBuffers(affected.invRefs, readVersion);

		if (affected.objects != 0)
			EnqueueCommandBuffers(affected.objects, readVersion);
	}
}
