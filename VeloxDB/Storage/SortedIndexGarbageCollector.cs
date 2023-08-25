using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Runtime.InteropServices;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal sealed partial class SortedIndex
{
	public static GarbageCollectorBase CreateGarbageCollector(MemoryManager memoryManager, long traceId)
	{
		return new GarbageCollector(memoryManager, traceId);
	}

	public abstract class GarbageCollectorBase : IDisposable
	{
		public abstract void Dispose();
	}

	/// <summary>
	/// Safely releases nodes from the BTree when those are no longer visible by any threads working with the tree. This is needed
	/// since BTree uses optimistic read locking (where threads do not actually take locks on the nodes).
	/// Each thread, when entering the btree, increases the ref count on the current epoch (making it uncollectable). Any garbage produced
	/// after that time is guaranted to be added either to that epoch or a newer one, thus no node can be released that is still visible by
	/// that thread. Epochs are collected periodically by a timer.
	/// </summary>
	private unsafe sealed class GarbageCollector : GarbageCollectorBase
	{
		const int timerInterval = 5;

		MemoryManager memoryManager;

		long traceId;

		Epoch epoch;
		Epoch garbageHead;
		Epoch garbageTail;

		List<Epoch> pool;

		Thread timer;
		ManualResetEvent finished = new ManualResetEvent(false);

		public GarbageCollector(MemoryManager memoryManager, long traceId)
		{
			this.traceId = traceId;
			this.memoryManager = memoryManager;

			epoch = new Epoch();
			pool = new List<Epoch>();

			timer = new Thread(() => ExecuteTimer());
			timer.Priority = ThreadPriority.AboveNormal;
			timer.Start();
		}

		public void ExecuteTimer()
		{
			Stopwatch s = Stopwatch.StartNew();
			while (true)
			{
				if (finished.WaitOne((int)Math.Max(0, timerInterval - s.ElapsedMilliseconds)))
					return;

				TTTrace.Write(traceId);
				s.Restart();

				if (epoch.HasGarbage)
				{
					Epoch temp = epoch;
					epoch = GetFromPool();
					temp.Disable();

					EnqueueGarbage(temp);
				}

				Collect();
			}
		}

		public void AddGarbage(Node* node)
		{
			TTTrace.Write(traceId, (ulong)node, epoch.GetHashCode());
			epoch.AddGarbage(Node.GetHandle(node), memoryManager);
		}

		public void AddGarbage(Range* range)
		{
			TTTrace.Write(traceId, (ulong)range, epoch.GetHashCode());
			epoch.AddGarbage(Range.GetHandle(range), memoryManager);
		}

		public Epoch ThreadEntered()
		{
			while (true)
			{
				Epoch e = epoch;
				if (e.TryThreadEnter())
				{
					TTTrace.Write(traceId, Thread.CurrentThread.ManagedThreadId);
					return e;
				}
			}
		}

		public void ThreadExited(Epoch epoch)
		{
			TTTrace.Write(traceId, Thread.CurrentThread.ManagedThreadId, epoch.GetHashCode());
			epoch.ThreadExited();
		}

		private void Collect()
		{
			while (true)
			{
				Epoch curr = DequeueGarbage();
				if (curr == null)
					return;

				TTTrace.Write(traceId, curr.GetHashCode());
				curr.Free(memoryManager);
				PutToPool(curr);
			}
		}

		private void EnqueueGarbage(Epoch e)
		{
			if (e.Next != null)
				throw new InvalidOperationException();

			if (garbageHead != null)
				garbageHead.Next = e;

			garbageHead = e;

			if (garbageTail == null)
				garbageTail = garbageHead;
		}

		private Epoch DequeueGarbage()
		{
			if (garbageTail == null || garbageTail.TotalCount() != 0)
				return null;

			Epoch e = garbageTail;
			garbageTail = garbageTail.Next;
			if (garbageTail == null)
				garbageHead = null;

			return e;
		}

		private Epoch GetFromPool()
		{
			lock (pool)
			{
				if (pool.Count == 0)
					return new Epoch();

				Epoch e = pool[pool.Count - 1];
				pool.RemoveAt(pool.Count - 1);
				return e;
			}
		}

		private void PutToPool(Epoch e)
		{
			e.Reset();
			lock (pool)
			{
				pool.Add(e);
			}
		}

		public override void Dispose()
		{
			finished.Set();
			timer.Join();

			EnqueueGarbage(epoch);
			epoch = null;

			Collect();
			if (garbageHead != null || garbageTail != null)
				throw new InvalidOperationException();

			lock (pool)
			{
				for (int i = 0; i < pool.Count; i++)
				{
					pool[i].Dispose();
				}
			}
		}

		public sealed class Epoch : IDisposable
		{
			object dataHandle;
			byte* data;

			public Epoch()
			{
				data = CacheLineMemoryManager.Allocate(sizeof(PerCPUData), out dataHandle);
			}

			public Epoch Next { get; set; }

			public bool HasGarbage
			{
				get
				{
					for (int i = 0; i < ProcessorNumber.CoreCount; i++)
					{
						PerCPUData* lp = (PerCPUData*)CacheLineMemoryManager.GetBuffer(data, i);
						if (lp->head != 0)
							return true;
					}

					return false;
				}
			}

			public int TotalCount()
			{
				int s = 0;
				for (int i = 0; i < ProcessorNumber.CoreCount; i++)
				{
					PerCPUData* lp = (PerCPUData*)CacheLineMemoryManager.GetBuffer(data, i);
					s += (int)lp->refCount.state;
				}

				return s;
			}

			public void Free(MemoryManager memoryManager)
			{
				for (int i = 0; i < ProcessorNumber.CoreCount; i++)
				{
					PerCPUData* lp = (PerCPUData*)CacheLineMemoryManager.GetBuffer(data, i);
					ulong curr = lp->head;
					while (curr != 0)
					{
						ulong next = ((GarbageItem*)memoryManager.GetBuffer(curr))->nextGarbage;
						memoryManager.Free(curr);
						curr = next;
					}

					lp->head = 0xcdcdcdcdcdcdcdcd;
				}
			}

			public void Reset()
			{
				Next = null;
				for (int i = 0; i < ProcessorNumber.CoreCount; i++)
				{
					PerCPUData* lp = (PerCPUData*)CacheLineMemoryManager.GetBuffer(data, i);
					*lp = new PerCPUData();
				}
			}

			public void Disable()
			{
				for (int i = 0; i < ProcessorNumber.CoreCount; i++)
				{
					PerCPUData* lp = (PerCPUData*)CacheLineMemoryManager.GetBuffer(data, i);
					while (true)
					{
						ulong state = (ulong)lp->refCount.state;
						if (lp->refCount.CompareExchange((long)(state | 0x8000000000000000), (long)state) == (long)state)
							break;
					}
				}
			}

			public bool TryThreadEnter()
			{
				int procNum = ProcessorNumber.GetCore();
				PerCPUData* lp = (PerCPUData*)CacheLineMemoryManager.GetBuffer(data, procNum);

				while (true)
				{
					ulong state = (ulong)lp->refCount.state;
					if ((state & 0x8000000000000000) != 0)
						return false;

					ulong newState = (uint)((int)state + 1);
					if (lp->refCount.CompareExchange((long)newState, (long)state) == (long)state)
						return true;
				}
			}

			public void ThreadExited()
			{
				int procNum = ProcessorNumber.GetCore();
				PerCPUData* lp = (PerCPUData*)CacheLineMemoryManager.GetBuffer(data, procNum);

				while (true)
				{
					ulong state = (ulong)lp->refCount.state;
					ulong newState = (state & 0xffffffff00000000) | (uint)((int)state - 1);

					if (lp->refCount.CompareExchange((long)newState, (long)state) == (long)state)
						return;
				}
			}

			public void AddGarbage(ulong itemHandle, MemoryManager memoryManager)
			{
				GarbageItem* item = (GarbageItem*)memoryManager.GetBuffer(itemHandle);

				// It is important to ensure that all writes have completed before adding the item to an epoch which is accomplished
				// by the usage of interlocked CompareExchange operation.

				int procNum = ProcessorNumber.GetCore();
				PerCPUData* lp = (PerCPUData*)CacheLineMemoryManager.GetBuffer(data, procNum);
				NativeInterlocked64* ph = (NativeInterlocked64*)(&lp->head);
				while (true)
				{
					item->nextGarbage = (ulong)ph->state;
					if (ph->CompareExchange((long)item, (long)item->nextGarbage) == (long)item->nextGarbage)
						return;
				}
			}

			public void Dispose()
			{
				CacheLineMemoryManager.Free(dataHandle);
			}
		}

		[StructLayout(LayoutKind.Explicit, Pack = 1, Size = 1)]
		private struct GarbageItem
		{
			[FieldOffset(0)]
			public ulong nextGarbage;
		}

		private unsafe struct PerCPUData
		{
			public ulong head;
			public NativeInterlocked64 refCount;
		}
	}
}
