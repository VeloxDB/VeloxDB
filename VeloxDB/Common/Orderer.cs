using System;
using System.Collections.Generic;
using VeloxDB.Common;

namespace VeloxDB.Common;

internal sealed class Orderer<T>
{
	const uint initSize = 8;

	long traceId;

	Func<T, ulong> valueGetterDelegate;
	Predicate<T> emptyCheckerDelegate;
	ulong lastValue;

	uint capacity;
	uint capacityMask;
	T[] items;

	bool disposed;

	public Orderer(long traceId, ulong nextValue, Func<T, ulong> valueGetterDelegate, Predicate<T> emptyCheckerDelegate)
	{
		TTTrace.Write(traceId, nextValue);

		this.traceId = traceId;
		this.valueGetterDelegate = valueGetterDelegate;
		this.emptyCheckerDelegate = emptyCheckerDelegate;

		lastValue = nextValue - 1;

		capacity = initSize;
		capacityMask = capacity - 1;
		items = new T[initSize];
	}

	public IEnumerable<T> GetUnprocessedItems()
	{
		for (int i = 0; i < items.Length; i++)
		{
			if (!emptyCheckerDelegate(items[i]))
				yield return items[i];
		}
	}

	public void Process(T cmd, List<T> processed)
	{
		ProcessItem(cmd, processed);
	}

	public void Process(List<T> cmds, List<T> processed)
	{
		if (disposed)
			throw new ObjectDisposedException(nameof(Orderer<T>));

		for (int j = 0; j < cmds.Count; j++)
		{
			ProcessItem(cmds[j], processed);
		}
	}

	public void Process(T[] cmds, List<T> processed)
	{
		if (disposed)
			throw new ObjectDisposedException(nameof(Orderer<T>));

		for (int j = 0; j < cmds.Length; j++)
		{
			ProcessItem(cmds[j], processed);
		}
	}

	public bool IsEmpty()
	{
		for (int i = 0; i < items.Length; i++)
		{
			if (!emptyCheckerDelegate(items[i]))
				return false;
		}

		return true;
	}

	public void ResetNextId(ulong nextId)
	{
		TTTrace.Write(traceId, nextId);
		Checker.AssertTrue(IsEmpty());
		this.lastValue = nextId - 1;
	}

	private void ProcessItem(T item, List<T> processed)
	{
		TTTrace.Write(traceId, item.GetHashCode());

		ulong value = valueGetterDelegate(item);
		Checker.AssertTrue(value > lastValue);
		if (value - lastValue > (ulong)items.Length)
			Resize((int)(value - lastValue));

		uint pos = (uint)(value & capacityMask);
		items[pos] = item;

		TTTrace.Write(traceId, value, lastValue, pos, items.Length);

		if (value != lastValue + 1)
			return;

		uint i = pos;
		while (!emptyCheckerDelegate(items[i]))
		{
			T currItem = items[i];
			ulong currItemValue = valueGetterDelegate(currItem);
			lastValue = currItemValue;
			items[i] = default(T);

			TTTrace.Write(traceId, currItemValue);
			processed.Add(currItem);

			i = (i + 1) & capacityMask;
		}
	}

	private void Resize(int size)
	{
		uint newCapacity = capacity;
		while (newCapacity < size)
			newCapacity *= 2;

		T[] newItems = new T[newCapacity];
		uint newCapacityMask = newCapacity - 1;

		for (int i = 0; i < items.Length; i++)
		{
			if (!emptyCheckerDelegate(items[i]))
			{
				int index = (int)(valueGetterDelegate(items[i]) & newCapacityMask);
				newItems[index] = items[i];
			}
		}

		items = newItems;
		capacity = newCapacity;
		capacityMask = newCapacityMask;
	}


	public void Dispose()
	{
		if (disposed)
			return;

		disposed = true;

		Type type = typeof(T);
		if (typeof(IDisposable).IsAssignableFrom(type))
		{
			for (int i = 0; i < items.Length; i++)
			{
				IDisposable d = ((object)items[i] as IDisposable);
				d?.Dispose();
			}
		}
	}
}
