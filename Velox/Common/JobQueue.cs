using System;
using System.Collections.Generic;
using System.Threading;

namespace Velox.Common;

internal enum JobQueueMode
{
	Normal,
	Grouped
}

internal sealed class JobQueue<T> : IDisposable
{
	readonly object sync = new object();
	readonly SemaphoreSlim itemCounter;
	readonly JobQueueMode mode;
	SemaphoreSlim freeSpotCounter;

	int first;
	int last;
	int count;
	int capacityMask;
	T[] items;
	bool disposed;

	public JobQueue(int initCapacity, JobQueueMode mode, int maxItemCount = -1)
	{
		initCapacity = (int)Utils.GetNextPow2((uint)initCapacity);

		this.mode = mode;
		itemCounter = new SemaphoreSlim(0, int.MaxValue);

		if (maxItemCount != -1)
			freeSpotCounter = new SemaphoreSlim(maxItemCount, maxItemCount);

		capacityMask = initCapacity - 1;
		first = 0;
		last = 0;
		count = 0;
		items = new T[initCapacity];
	}

	~JobQueue()
	{
		CleanUp(false);
	}

	public JobQueueMode Mode => mode;
	public int Count => count;

	public void Dispose()
	{
		CleanUp(true);
		System.GC.SuppressFinalize(this);
	}

	public void SetMaxItemCount(int maxItemCount)
	{
		if (freeSpotCounter != null)
		{
			freeSpotCounter.Dispose();
			freeSpotCounter = null;
		}

		if (maxItemCount != -1)
			freeSpotCounter = new SemaphoreSlim(maxItemCount, maxItemCount);
	}

	public void Enqueue(T item)
	{
		TryEnqueue(item, -1);
	}

	public bool TryEnqueue(T item, int timeout)
	{
		Checker.NotDisposed(disposed);

		if (freeSpotCounter != null)
		{
			if (!freeSpotCounter.Wait(timeout))
				return false;
		}

		lock (sync)
		{
			if (count == items.Length)
				Resize();

			items[last] = item;
			last = (last + 1) & capacityMask;
			count++;

			if (mode == JobQueueMode.Grouped && count == 1)
				itemCounter.Release();
		}

		if (mode == JobQueueMode.Normal)
			itemCounter.Release();

		return true;
	}

	public T Dequeue()
	{
		TryDequeue(-1, out T item);
		return item;
	}

	public bool TryDequeue(int timeout, out T item)
	{
		Checker.NotDisposed(disposed);

		if (mode == JobQueueMode.Grouped)
			throw new InvalidOperationException("Queue does not support dequeuing of a single item.");

		if (!itemCounter.Wait(timeout))
		{
			item = default(T);
			return false;
		}

		lock (sync)
		{
			item = items[first];
			items[first] = default(T);
			first = (first + 1) & capacityMask;
			count--;
		}

		if (freeSpotCounter != null)
			freeSpotCounter.Release();

		return true;
	}

	public void DequeueAll(IList<T> storage)
	{
		Checker.NotDisposed(disposed);

		if (mode == JobQueueMode.Normal)
			throw new InvalidOperationException("Queue does not support dequeuing of all items.");

		itemCounter.Wait();

		lock (sync)
		{
			for (int i = 0; i < count; i++)
			{
				storage.Add(items[first]);
				items[first] = default(T);
				first = (first + 1) & capacityMask;
			}

			if (freeSpotCounter != null)
				freeSpotCounter.Release(count);

			first = 0;
			last = 0;
			count = 0;
		}
	}

	private void Resize(int size = 0)
	{
		long newSize = (long)items.Length * 2;
		if (size > newSize)
			newSize = (int)Utils.GetNextPow2((uint)size);

		T[] newItems = new T[newSize];

		for (int i = 0; i < count; i++)
		{
			newItems[i] = items[(first + i) & capacityMask];
		}

		items = newItems;
		capacityMask = items.Length - 1;
		first = 0;
		last = count;
	}

	private void CleanUp(bool disposing)
	{
		if (!disposed)
		{
			if (disposing)
			{
				itemCounter.Dispose();

				if (freeSpotCounter != null)
					freeSpotCounter.Dispose();
			}
		}

		disposed = true;
	}
}
