using System;
using System.Runtime.CompilerServices;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.Storage;

internal interface IHashIndexReader<TKey1>
{
	void GetObjects(Transaction tran, TKey1 key1, ref ObjectReader[] objectReaders, out int count);
}

internal interface IHashIndexReader<TKey1, TKey2>
{
	void GetObjects(Transaction tran, TKey1 key1, TKey2 key2, ref ObjectReader[] objectReaders, out int count);
}

internal interface IHashIndexReader<TKey1, TKey2, TKey3>
{
	void GetObjects(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ref ObjectReader[] objectReaders, out int count);
}

internal interface IHashIndexReader<TKey1, TKey2, TKey3, TKey4>
{
	void GetObjects(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ref ObjectReader[] objectReaders, out int count);
}
