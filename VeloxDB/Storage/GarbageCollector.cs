using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Velox.Common;

namespace Velox.Storage;

internal unsafe sealed class GarbageCollector
{
	static readonly IntPtr pauseCommand = (IntPtr)1;
	static readonly IntPtr destroyCommand = IntPtr.Zero;

	Database database;

	ActiveTransations activeTrans;
	UncollectedTransactions uncollectedTrans;

	int workerCount;
	List<Thread> workers;
	JobQueue<IntPtr> commands;

	readonly object drainSync = new object();
	CountdownEvent drainEvent;
	volatile CountedManualResetEvent unpauseEvent;

	public GarbageCollector(Database database)
	{
		TTTrace.Write(database.TraceId);

		this.database = database;

		activeTrans = new ActiveTransations(database.TraceId);
		uncollectedTrans = new UncollectedTransactions(database.Engine.MemoryManager,
			database.Engine.Settings, AddWorkItems, database.TraceId);

		commands = new JobQueue<IntPtr>(1024, JobQueueMode.Normal);

		workerCount = 1;
		if (database.Id == DatabaseId.User && database.Engine.Settings.AllowInternalParallelization)
			workerCount = database.Engine.Settings.GCWorkerCount;

		CreateWorkers();
	}

	public bool HasActiveTransactions()
	{
		return !activeTrans.IsEmpty;
	}

	public void ForEachActiveTransaction(Action<Transaction> action)
	{
		foreach (Transaction tran in activeTrans)
		{
			action(tran);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void InsertTransaction(Transaction tran)
	{
		activeTrans.AddTran(tran);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public Transaction TransactionCompleted(Transaction tran)
	{
		return activeTrans.TransactionCompleted(tran);
	}

	public void PrepareGarbage(Transaction tran, Transaction lastReader, ulong databaseReadVersion)
	{
		TTTrace.Write(database.TraceId, tran.Id, tran.ReadVersion, databaseReadVersion);

		ulong lastReadVersion = lastReader == null ? databaseReadVersion : lastReader.ReadVersion;

		lock (uncollectedTrans)
		{
			uncollectedTrans.Collect(lastReadVersion);

			if (tran.Type == TransactionType.ReadWrite)
			{
				if (tran.IsAlignment)
				{
					uncollectedTrans.Reset();
				}
				else
				{
					uncollectedTrans.Insert(tran);
				}
			}
		}
	}

	public void Flush()
	{
		TTTrace.Write(database.TraceId);
		uncollectedTrans.Flush();
	}

	private void AddWorkItems(IntPtr item)
	{
		Checker.AssertTrue(item != IntPtr.Zero);
		commands.Enqueue(item);
	}

	private void Worker()
	{
		while (true)
		{
			IntPtr p = commands.Dequeue();

			if (p == destroyCommand)
				return;

			if (p == pauseCommand)
			{
				CountedManualResetEvent sd = unpauseEvent;
				drainEvent.Signal();
				sd.Wait();
				continue;
			}

			ModifiedBufferHeader* cp = (ModifiedBufferHeader*)p;
			GarbageCollect(cp);
		}
	}

	private void DestroyWorkers()
	{
		for (int i = 0; i < workers.Count; i++)
		{
			commands.Enqueue(destroyCommand);
		}

		for (int i = 0; i < workers.Count; i++)
		{
			workers[i].Join();
		}

		workers = null;
	}

	private void CreateWorkers()
	{
		workers = new List<Thread>(workerCount);
		for (int i = 0; i < workerCount; i++)
		{
			Thread t = Utils.RunThreadWithSupressedFlow(Worker,
				string.Format("{0}: vlx-GarbageCollectorExecutor", database.Engine.Trace.Name), true, 256 * 1024);
			t.Priority = ThreadPriority.AboveNormal;
			workers.Add(t);
		}
	}

	internal unsafe void GarbageCollect(ModifiedBufferHeader* cp)
	{
		TTTrace.Write(database.TraceId);

		while (cp != null)
		{
			TTTrace.Write(database.TraceId, (byte)cp->modificationType, cp->count);

			if (cp->modificationType == ModifiedType.Class)
				GarbageCollectClasses(cp);
			else if (cp->modificationType == ModifiedType.InverseReference)
				GarbageCollectInvRefs(cp);
			else if (cp->modificationType == ModifiedType.HashReadLock)
				GarbageCollectHashReadLocks(cp);

			// We do not collect ObjectReadLock (there isn't any garbage produced). Best would be to not put those in the queue.

			ModifiedBufferHeader* nextCP = cp->nextQueueGroup;
			database.Engine.MemoryManager.Free(cp->handle);
			cp = nextCP;
		}
	}

	private unsafe void GarbageCollectHashReadLocks(ModifiedBufferHeader* cp)
	{
		TTTrace.Write(database.TraceId);

		HashReadLock* rl = (HashReadLock*)((byte*)cp + ModifiedBufferHeader.Size);
		for (int i = 0; i < cp->count; i++)
		{
			HashKeyReadLocker locker = database.GetHashIndexLocker(rl->hashIndex);
			locker.GarbageCollect(rl->itemHandle, rl->hash, cp->readVersion);
			rl++;
		}
	}

	private unsafe void GarbageCollectInvRefs(ModifiedBufferHeader* cp)
	{
		TTTrace.Write(database.TraceId);

		AffectedInverseReferences* ir = (AffectedInverseReferences*)((byte*)cp + ModifiedBufferHeader.Size);
		for (int i = 0; i < cp->count; i++)
		{
			InverseReferenceMap invRefMap = database.GetInvRefs(ir->classIndex);
			invRefMap.GarbageCollect(ir->id, ir->propertyId, cp->readVersion);
			ir++;
		}
	}

	private unsafe void GarbageCollectClasses(ModifiedBufferHeader* cp)
	{
		TTTrace.Write(database.TraceId);

		AffectedObject* r = (AffectedObject*)((byte*)cp + ModifiedBufferHeader.Size);
		for (int i = 0; i < cp->count; i++)
		{
			Class @class = database.GetClass(r->classIndex).MainClass;
			@class.GarbageCollect(r->id, cp->readVersion);
			r++;
		}
	}

	public void Drain()
	{
		lock (drainSync)
		{
			drainEvent = new CountdownEvent(workers.Count);
			unpauseEvent = new CountedManualResetEvent(workers.Count);

			for (int i = 0; i < workers.Count; i++)
			{
				commands.Enqueue(pauseCommand);
			}

			drainEvent.Wait();

			unpauseEvent.Set();
			drainEvent.Dispose();
			drainEvent = null;
		}
	}

	public void Dispose()
	{
		DestroyWorkers();
		commands.Dispose();
	}

#if TEST_BUILD
	public int ActiveTransactionCount => activeTrans.Count;

	public void GetOldestReaders(DatabaseVersions versions, out ulong readVer)
	{
		Transaction tran = activeTrans.GetOldestReader();
		if (tran == null)
			readVer = versions.ReadVersion;
		else
			readVer = tran.ReadVersion;
	}
#endif

	private sealed class CountedManualResetEvent
	{
		int count;
		ManualResetEvent signal;

		public CountedManualResetEvent(int count)
		{
			signal = new ManualResetEvent(false);
			this.count = count;
		}

		public void Set()
		{
			signal.Set();
		}

		public void Wait()
		{
			signal.WaitOne();
			if (Interlocked.Decrement(ref count) == 0)
			{
				signal.Dispose();
				signal = null;
			}
		}
	}
}
