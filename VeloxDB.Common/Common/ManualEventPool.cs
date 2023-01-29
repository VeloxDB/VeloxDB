using System;
using System.Threading;

namespace VeloxDB.Common;

internal static class ManualEventPool
{
	static readonly object poolSync = new object();
	static int poolCount;
	static int[] pool;

	static readonly object storageSync = new object();
	static int storageCount;
	static RefCountedManualEvent[] storage;

	static ManualEventPool()
	{
		storageCount = 0;
		storage = new RefCountedManualEvent[64];

		poolCount = 64;
		pool = new int[poolCount];
		for (int i = 0; i < poolCount; i++)
		{
			pool[i] = Create();
		}
	}

	public static int Alloc()
	{
		lock (poolSync)
		{
			int handle;
			if (poolCount > 0)
				handle = pool[--poolCount];
			else
				handle = Create();

			Get(handle).Allocated();
			return handle;
		}
	}

	private static void Free(int handle)
	{
		if (Get(handle).IsSignaled())
			throw new InvalidOperationException();

		if (poolCount == pool.Length)
			Array.Resize(ref pool, pool.Length * 2);

		pool[poolCount++] = handle;
	}

	public static RefCountedManualEvent Get(int handle)
	{
		return storage[handle - 1];
	}

	private static int Create()
	{
		lock (storageSync)
		{
			if (storageCount == storage.Length)
			{
				RefCountedManualEvent[] temp = new RefCountedManualEvent[storage.Length * 2];
				for (int i = 0; i < storage.Length; i++)
				{
					temp[i] = storage[i];
				}

				Thread.MemoryBarrier();
				storage = temp;
			}

			RefCountedManualEvent s = new RefCountedManualEvent(storageCount + 1, poolSync);
			storage[storageCount++] = s;
			return storageCount;
		}
	}

	public sealed class RefCountedManualEvent
	{
		readonly object sync;
		int handle;
		ManualResetEventSlim e;
		int refCount;

		public RefCountedManualEvent(int handle, object sync)
		{
			this.sync = sync;
			this.handle = handle;
			refCount = 0;
			e = new ManualResetEventSlim(false);
		}

		public void Allocated()
		{
			refCount = 1;
		}

		public bool TryIncRefCount()
		{
			lock (sync)
			{
				if (refCount == 0)
					return false;

				refCount++;
				return true;
			}
		}

		public void DecRefCount(bool incSucceeded)
		{
			if (!incSucceeded)
				return;

			lock (sync)
			{
				refCount--;
				if (refCount == 0)
				{
					e.Reset();
					ManualEventPool.Free(handle);
				}
			}
		}

		public bool IsSignaled()
		{
			return e.Wait(0);
		}

		public void Wait()
		{
			e.Wait();
		}

		public void Set()
		{
			e.Set();
		}
	}
}