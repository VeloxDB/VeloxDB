using System;
using System.Collections;
using System.Collections.Generic;

namespace Velox.Common;

internal sealed class ArrayQueue<T> : IEnumerable<T>
{
	int count;
	int first;
	int last;
	int capacityMask;
	T[] items;

	Action<T, int> resizeCallback;

	public ArrayQueue(int capacity = 128, Action<T, int> resizeCallback = null)
	{
		if (!Utils.IsPowerOf2(capacity))
			capacity = (int)Utils.GetNextPow2((ulong)capacity);

		this.resizeCallback = resizeCallback;
		capacityMask = capacity - 1;
		items = new T[capacity];
	}

	public int Count => count;
	public T this[int index]
	{
		get => items[(first + index) & capacityMask];
		set => items[(first + index) & capacityMask] = value;
	}

	public T GetAtAbsolute(int index)
	{
		return items[index];
	}

	public void SetAtAbsolute(int index, T value)
	{
		items[index] = value;
	}

	public void Clear()
	{
		for (int i = 0; i < count; i++)
		{
			items[(first + i) & capacityMask] = default(T);
		}

		count = 0;
		first = 0;
		last = 0;
	}

	public int Enqueue(T item)
	{
		if (count == items.Length)
			Resize();

		items[last] = item;
		int index = last;
		last = (last + 1) & capacityMask;
		count++;
		return index;
	}

	public void EnqueueFront(T item)
	{
		if (count == items.Length)
			Resize();

		items[first] = item;
		first = (first - 1) & capacityMask;
		count++;
	}

	public T Peek()
	{
		if (count == 0)
			throw new InvalidOperationException();

		return items[first];
	}

	public T Dequeue()
	{
		if (count == 0)
			throw new InvalidOperationException();

		T res = items[first];
		items[first] = default(T);
		first = (first + 1) & capacityMask;
		count--;
		return res;
	}

	public IEnumerator<T> GetEnumerator()
	{
		for (int i = 0; i < count; i++)
		{
			yield return items[(first + i) & capacityMask];
		}
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		return GetEnumerator();
	}

	private void Resize()
	{
		T[] temp = new T[items.Length * 2];

		for (int i = 0; i < count; i++)
		{
			temp[i] = items[(first + i) & capacityMask];
			resizeCallback?.Invoke(temp[i], i);
		}

		items = temp;
		capacityMask = items.Length - 1;
		first = 0;
		last = count;
	}
}
