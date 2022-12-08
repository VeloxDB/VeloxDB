using System;
using System.Runtime.CompilerServices;
using Velox.Common;

namespace Velox.Storage;

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

	ulong lastCollectedVersion;

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

		lastCollectedVersion = 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Reset()
	{
		lastCollectedVersion = 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Insert(Transaction tran)
	{
		TTTrace.Write(traceId, tran.Id, tran.CommitVersion, lastCollectedVersion);

		if (tran.CommitVersion <= lastCollectedVersion)
		{
			EnqueueCommands(tran, lastCollectedVersion);
			return;
		}

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
		TTTrace.Write(traceId, oldestReadVersion, queue.Count, lastCollectedVersion);

		this.lastCollectedVersion = oldestReadVersion;

		while (queue.Count > 0)
		{
			Transaction tran = queue.Peek();
			if (tran.CommitVersion > oldestReadVersion)
				break;

			queue.Dequeue();
			EnqueueCommands(tran, oldestReadVersion);
		}
	}

	public void Flush()
	{
		TTTrace.Write(traceId);

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
		Transaction.CollectableCollections affected = tran.Collectable;

		if (affected.invRefs != 0)
			EnqueueCommandBuffers(affected.invRefs, readVersion);

		if (affected.hashReadLocks != 0)
			EnqueueCommandBuffers(affected.hashReadLocks, readVersion);

		if (affected.objects != 0)
			EnqueueCommandBuffers(affected.objects, readVersion);
	}
}
