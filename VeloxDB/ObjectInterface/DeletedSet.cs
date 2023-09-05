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
		TTTrace.Write(id, version);
		ids.Add(id);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void IncVersion()
	{
		version++;
		TTTrace.Write(version);
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

	public void Reset()
	{
		TTTrace.Write();
		version = 0;
		Clear();
	}

	public void Clear()
	{
		TTTrace.Write();
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
