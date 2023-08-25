using System;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal unsafe abstract class HashIndexReaderBase : IndexReaderBase
{
	HashIndex index;

	public HashIndexReaderBase(Type[] types, string cultureName, bool caseSensitive, ReadOnlyArray<SortOrder> sortOrder) :
		base(types, cultureName, caseSensitive, sortOrder)
	{
	}

	public HashIndex HashIndex => index;

	public override void SetIndex(Index index)
	{
		this.index = (HashIndex)index;
	}
}

internal unsafe abstract class HashIndexReaderBase<TKey1> : HashIndexReaderBase, IHashIndexReader<TKey1>
{
	public HashIndexReaderBase(string cultureName, bool caseSensitive, ReadOnlyArray<SortOrder> sortOrder) :
		base(new Type[] { typeof(TKey1) }, cultureName, caseSensitive, sortOrder)
	{
	}

	public static string PopulateMethodName => nameof(PopulateKeyBuffer);

	[SkipLocalsInit]
	public void GetObjects(Transaction tran, TKey1 key1, ref ObjectReader[] objectReaders, out int count)
	{
		byte* pkey = stackalloc byte[keySize];
		string[] strings = base.comparer.StringPropertyCount == 0 ? null : new string[base.comparer.StringPropertyCount];
		PopulateKeyBuffer(key1, pkey, strings);
		tran.Engine.ReadHashIndex(tran, base.HashIndex, pkey, comparer, strings, ref objectReaders, out count);
	}

	protected abstract void PopulateKeyBuffer(TKey1 key1, byte* pkey, string[] strings);
}

internal unsafe abstract class HashIndexReaderBase<TKey1, TKey2> : HashIndexReaderBase, IHashIndexReader<TKey1, TKey2>
{
	public HashIndexReaderBase(string cultureName, bool caseSensitive, ReadOnlyArray<SortOrder> sortOrder) :
		base(new Type[] { typeof(TKey1), typeof(TKey2) }, cultureName, caseSensitive, sortOrder)
	{
	}

	[SkipLocalsInit]
	public void GetObjects(Transaction tran, TKey1 key1, TKey2 key2, ref ObjectReader[] objectReaders, out int count)
	{
		byte* pkey = stackalloc byte[keySize];
		string[] strings = base.comparer.StringPropertyCount == 0 ? null : new string[base.comparer.StringPropertyCount];
		PopulateKeyBuffer(key1, key2, pkey, strings);
		tran.Engine.ReadHashIndex(tran, base.HashIndex, pkey, base.comparer, strings, ref objectReaders, out count);
	}

	protected abstract void PopulateKeyBuffer(TKey1 key1, TKey2 key2, byte* pkey, string[] strings);
}

internal unsafe abstract class HashIndexReaderBase<TKey1, TKey2, TKey3> : HashIndexReaderBase, IHashIndexReader<TKey1, TKey2, TKey3>
{
	public HashIndexReaderBase(string cultureName, bool caseSensitive, ReadOnlyArray<SortOrder> sortOrder) :
		base(new Type[] { typeof(TKey1), typeof(TKey2), typeof(TKey3) }, cultureName, caseSensitive, sortOrder)
	{
	}

	[SkipLocalsInit]
	public void GetObjects(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ref ObjectReader[] objectReaders, out int count)
	{
		byte* pkey = stackalloc byte[keySize];
		string[] strings = base.comparer.StringPropertyCount == 0 ? null : new string[base.comparer.StringPropertyCount];
		PopulateKeyBuffer(key1, key2, key3, pkey, strings);
		tran.Engine.ReadHashIndex(tran, base.HashIndex, pkey, base.comparer, strings, ref objectReaders, out count);
	}

	protected abstract void PopulateKeyBuffer(TKey1 key1, TKey2 key2, TKey3 key3, byte* pkey, string[] strings);
}

internal unsafe abstract class HashIndexReaderBase<TKey1, TKey2, TKey3, TKey4> : HashIndexReaderBase,
	IHashIndexReader<TKey1, TKey2, TKey3, TKey4>
{
	public HashIndexReaderBase(string cultureName, bool caseSensitive, ReadOnlyArray<SortOrder> sortOrder) :
		base(new Type[] { typeof(TKey1), typeof(TKey2), typeof(TKey3), typeof(TKey4) }, cultureName, caseSensitive, sortOrder)
	{
	}

	[SkipLocalsInit]
	public void GetObjects(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ref ObjectReader[] objectReaders, out int count)
	{
		byte* pkey = stackalloc byte[keySize];
		string[] strings = base.comparer.StringPropertyCount == 0 ? null : new string[base.comparer.StringPropertyCount];
		PopulateKeyBuffer(key1, key2, key3, key4, pkey, strings);
		tran.Engine.ReadHashIndex(tran, base.HashIndex, pkey, base.comparer, strings, ref objectReaders, out count);
	}

	protected abstract void PopulateKeyBuffer(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, byte* pkey, string[] strings);
}
