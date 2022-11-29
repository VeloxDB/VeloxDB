using System;
using System.Collections.Generic;

namespace Velox.Common;

internal sealed class ReadOnlyHashSet<TKey> : IEnumerable<TKey>
{
	HashSet<TKey> set;

	public ReadOnlyHashSet(HashSet<TKey> set)
	{
		this.set = set;
	}

	public int Count => set.Count;

	public bool ContainsKey(TKey key)
	{
		return set.Contains(key);
	}

	public bool IsSubsetOf(ReadOnlyHashSet<TKey> h)
	{
		return set.IsSubsetOf(h.set);
	}

	public IEnumerator<TKey> GetEnumerator()
	{
		return set.GetEnumerator();
	}

	System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
	{
		return GetEnumerator();
	}

	public override string ToString()
	{
		return string.Format("Count: {0}", set.Count);
	}
}
