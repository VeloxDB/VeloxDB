using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Threading;

namespace Velox.Common;

internal interface IItemFactory<T>
{
	void Init(T item);
	T Create();
	void Reset(T item);
	void Destroy(T item);
}

internal sealed class ItemPool<T>
{
	static readonly int borrowProbeCount = Math.Min(ProcessorNumber.CoreCount, 8);

	RWSpinLock sync;
	int count;
	T[] pool;

	IItemFactory<T> factory;

	bool closed;

	public ItemPool(int capacity, IItemFactory<T> factory)
	{
		this.factory = factory;

		pool = new T[capacity];

		count = capacity;
		for (int i = 0; i < capacity; i++)
		{
			pool[i] = factory.Create();
		}
	}

	public T Get()
	{
		T item = default(T);

		bool found = false;
		sync.EnterWriteLock();
		try
		{
			if (this.closed)
				throw new ObjectDisposedException(nameof(ItemPool<T>));

			if (count != 0)
			{
				found = true;
				item = pool[--count];
				pool[count] = default(T);
			}
		}
		finally
		{
			sync.ExitWriteLock();
		}

		if (!found)
			item = factory.Create();

		factory.Init(item);
		return item;
	}

	public void Put(T item)
	{
		factory.Reset(item);

		sync.EnterWriteLock();
		try
		{
			if (count < pool.Length)
			{
				pool[count++] = item;
				return;
			}
		}
		finally
		{
			sync.ExitWriteLock();
		}

		factory.Destroy(item);
	}

	public void Close()
	{
		sync.EnterWriteLock();
		try
		{
			if (closed)
				return;

			closed = true;
			for (int i = 0; i < count; i++)
			{
				factory.Destroy(pool[i]);
				pool[i] = default(T);
			}

			pool = EmptyArray<T>.Instance;
			count = 0;
		}
		finally
		{
			sync.ExitWriteLock();
		}
	}
}
