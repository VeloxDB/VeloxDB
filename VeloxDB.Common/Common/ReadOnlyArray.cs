using System;
using System.Collections.Generic;
using System.Linq;

namespace VeloxDB.Common;

internal sealed class ReadOnlyArray<T> : IReadOnlyList<T>
{
	public static readonly ReadOnlyArray<T> Empty = new ReadOnlyArray<T>(Array.Empty<T>());

	readonly T[] array;

	public ReadOnlyArray(T[] array)
	{
		if (array.Length == 0)
		{
			this.array = Array.Empty<T>();
			return;
		}

		this.array = array;
	}

	public static ReadOnlyArray<T> FromNullable(IEnumerable<T> list)
	{
		if (list == null || !list.Any())
			return new ReadOnlyArray<T>(Array.Empty<T>());

		return new ReadOnlyArray<T>(list.ToArray());
	}

	public ReadOnlyArray(T[] array, bool cloneArray)
	{
		if (array.Length == 0)
		{
			this.array = Array.Empty<T>();
			return;
		}

		if (cloneArray)
		{
			this.array = new T[array.Length];
			Array.Copy(array, this.array, array.Length);
		}
		else
		{
			this.array = array;
		}
	}

	int IReadOnlyCollection<T>.Count => array.Length;
	public int Length => array.Length;
	public T this[int index] => array[index];

	public T Get(int index)
	{
		return array[index];
	}

	public IEnumerator<T> GetEnumerator()
	{
		for (int i = 0; i < array.Length; i++)
		{
			yield return array[i];
		}
	}

	System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
	{
		return GetEnumerator();
	}

	public T[] ToArray()
	{
		T[] a = new T[array.Length];
		Array.Copy(array, a, array.Length);
		return a;
	}

	public override string ToString()
	{
		return string.Format("Length: {0}", array.Length);
	}
}
