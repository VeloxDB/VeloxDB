using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal unsafe sealed class GarbageCollector
{
	static readonly IntPtr pauseCommand = (IntPtr)1;
	static readonly IntPtr destroyCommand = IntPtr.Zero;

	Database database;

	NativeInterlocked64 oldestReadVersion;
	UncollectedTransactions uncollectedTrans;

	ActiveTransations activeTrans;

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

		activeTrans = new ActiveTransations();
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

	public void CancelTransactions()
	{
		activeTrans.CancelTransactions();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool TryInsertTransaction(Transaction tran)
	{
		activeTrans.AddTran(tran);
		Thread.MemoryBarrier();
		ulong lastReadVersion = (ulong)this.oldestReadVersion.state;
		if (lastReadVersion <= tran.ReadVersion)
		{
			return true;
		}
		else
		{
			activeTrans.Remove(tran);
			tran.PrevActiveTran = tran.NextActiveTran = null;
			return false;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void TransactionCompleted(Transaction tran)
	{
		activeTrans.Remove(tran);
	}

	public void ProcessGarbage(Transaction tran)
	{
		RefreshOldestReadVersion(tran.CommitVersion, out ulong oldestReadVersion, out bool collect);
		TTTrace.Write(database.TraceId, tran.Id, tran.ReadVersion, tran.CommitVersion, this.oldestReadVersion.state, oldestReadVersion);

		lock (uncollectedTrans)
		{
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

			if (collect)
				uncollectedTrans.Collect(oldestReadVersion);
		}
	}

	private void RefreshOldestReadVersion(ulong referentReadVersion, out ulong oldestReadVersion, out bool collect)
	{
		Transaction oldestReader = activeTrans.GetOldestReader();
		oldestReadVersion = oldestReader == null ? referentReadVersion : oldestReader.ReadVersion;

		collect = false;
		while (true)
		{
			ulong prevOldestReadVersion = (ulong)this.oldestReadVersion.state;
			if (oldestReadVersion > prevOldestReadVersion)
			{
				if (this.oldestReadVersion.CompareExchange((long)oldestReadVersion, (long)prevOldestReadVersion) == (long)prevOldestReadVersion)
				{
					collect = true;
					break;
				}
			}
			else
			{
				break;
			}
		}
	}

	public void Rewind(ulong version)
	{
		if (version < (ulong)oldestReadVersion.state)
			oldestReadVersion.state = (long)version;
	}

	private void CollectImmediate(Transaction tran, ulong handle)
	{
		ModifiedBufferHeader* curr = (ModifiedBufferHeader*)database.Engine.MemoryManager.GetBuffer(handle);
		while (curr != null)
		{
			curr->readVersion = ulong.MaxValue;
			curr->nextQueueGroup = null;
			ulong nextHandle = curr->nextBuffer;
			GarbageCollect(curr);
			curr = (ModifiedBufferHeader*)database.Engine.MemoryManager.GetBuffer(nextHandle);
		}
	}

	public void Flush()
	{
		TTTrace.Write(database.TraceId);

		RefreshOldestReadVersion(database.ReadVersion, out ulong oldestReadVersion, out bool collect);

		lock (uncollectedTrans)
		{
			uncollectedTrans.Flush(oldestReadVersion);
		}
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
		activeTrans.Dispose();
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
