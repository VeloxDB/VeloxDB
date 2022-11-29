using System;

namespace Velox.Common;

internal interface IItemFactory<T> where T : class
{
	void Init(T item);
	T Create();
	void Reset(T item);
	void Destroy(T item);
}

internal sealed class ItemPool<T> where T : class
{
	readonly object sync = new object();

	int count;
	T[] pool;

	IItemFactory<T> factory;

	bool closed;

	public ItemPool(int capacity, IItemFactory<T> factory)
	{
		this.factory = factory;

		count = 0;
		pool = new T[capacity];
	}

	public T Get()
	{
		T item;
		lock (sync)
		{
			if (this.closed)
				throw new ObjectDisposedException(nameof(ItemPool<T>));

			if (count != 0)
			{
				item = pool[--count];
				pool[count] = null;
			}
			else
			{
				item = factory.Create();
			}
		}

		factory.Init(item);
		return item;
	}

	public void Put(T item)
	{
		factory.Reset(item);

		lock (sync)
		{
			if (closed)
			{
				factory.Destroy(item);
				return;
			}

			if (count == pool.Length)
			{
				factory.Destroy(item);
				return;
			}

			pool[count++] = item;
		}
	}

	public void Close()
	{
		lock (sync)
		{
			if (closed)
				return;

			closed = true;
			for (int i = 0; i < count; i++)
			{
				factory.Destroy(pool[i]);
                    pool[i] = null;
			}

			count = 0;
		}
	}
}
