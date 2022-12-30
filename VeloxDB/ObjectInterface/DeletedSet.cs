using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using VeloxDB.Common;

namespace VeloxDB.ObjectInterface;

internal sealed class DeletedSet
{
	const int capacity = 1024;

	int version;
	LongHashSet ids;

	public DeletedSet()
	{
		ids = new LongHashSet(capacity);
	}

	public int Count => ids.Count;
	public bool HasDeleted => ids.Count > 0;
	public LongHashSet Ids => ids;
	public int Version => version;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Add(long id)
	{
		ids.Add(id);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void IncVersion()
	{
		version++;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool Contains(long id)
	{
		if (ids.Count == 0)
			return false;

		return ids.Contains(id);
	}

	public void ForEach(Action<long> action)
	{
		ids.ForEach(action);
	}

	public void Clear()
	{
		version = 0;

		if (ids.Count > capacity)
		{
			ids = new LongHashSet(capacity);
		}
		else
		{
			ids.Clear();
		}
	}
}
