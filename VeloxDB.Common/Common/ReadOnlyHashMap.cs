using System;
using System.Collections;
using System.Collections.Generic;

namespace VeloxDB.Common;

internal sealed class ReadOnlyHashMap<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
{
	Dictionary<TKey, TValue> map;

	public ReadOnlyHashMap(Dictionary<TKey, TValue> map)
	{
		this.map = map;
	}

	public int Count => map.Count;

	public bool ContainsKey(TKey key)
	{
		return TryGetValue(key, out TValue value);
	}

	public TValue this[TKey key] => map[key];

	public bool TryGetValue(TKey key, out TValue value)
	{
		return map.TryGetValue(key, out value);
	}

	public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
	{
		foreach (KeyValuePair<TKey, TValue> kv in map)
		{
			yield return kv;
		}
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		return GetEnumerator();
	}

	public IEnumerable<TKey> Keys
	{
		get
		{
			foreach (TKey key in map.Keys)
			{
				yield return key;
			}
		}
	}

	public IEnumerable<TValue> Values
	{
		get
		{
			foreach (TValue value in map.Values)
			{
				yield return value;
			}
		}
	}

	public override string ToString()
	{
		return string.Format("Count: {0}", map.Count);
	}
}
