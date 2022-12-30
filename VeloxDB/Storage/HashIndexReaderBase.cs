using System;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal abstract class HashIndexReaderBase
{
	int keySize;
	HashIndex hashIndex;

	protected ComparerPool comparerPool;

	public HashIndexReaderBase(Type[] types)
	{
		keySize = 0;
		KeyProperty[] properties = new KeyProperty[types.Length];

		for (int i = 0; i < types.Length; i++)
		{
			PropertyType propType = PropertyTypesHelper.ManagedTypeToPropertyType(types[i]);
			if (propType == PropertyType.None || PropertyTypesHelper.IsArray(propType))
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.IndexPropertyWrongType));

			properties[i] = new KeyProperty(propType, keySize);
			keySize += PropertyTypesHelper.GetItemSize(propType);
		}

		comparerPool = new ComparerPool(2, properties);
	}

	public HashIndex HashIndex => hashIndex;
	protected int KeySize => keySize;

	public void SetIndex(HashIndex hashIndex)
	{
		this.hashIndex = hashIndex;
	}
}

internal unsafe abstract class HashIndexReaderBase<TKey1> : HashIndexReaderBase, IHashIndexReader<TKey1>
{
	public HashIndexReaderBase() :
		base(new Type[] { typeof(TKey1) })
	{
	}

	public static string PopulateMethodName => nameof(PopulateKeyBuffer);

	[SkipLocalsInit]
	public void GetObjects(Transaction tran, TKey1 key1, ref ObjectReader[] objectReaders, out int count)
	{
		HashComparer a = comparerPool.GetComparer();

		try
		{
			byte* pkey = stackalloc byte[KeySize];
			PopulateKeyBuffer(key1, pkey, a.Strings);
			tran.Engine.ReadHashIndex(tran, base.HashIndex, pkey, a, ref objectReaders, out count);
		}
		finally
		{
			comparerPool.PutComparer(a);
		}
	}

	protected abstract void PopulateKeyBuffer(TKey1 key1, byte* pkey, string[] strings);
}

internal unsafe abstract class HashIndexReaderBase<TKey1, TKey2> : HashIndexReaderBase, IHashIndexReader<TKey1, TKey2>
{
	public HashIndexReaderBase() :
		base(new Type[] { typeof(TKey1), typeof(TKey2) })
	{
	}

	[SkipLocalsInit]
	public void GetObjects(Transaction tran, TKey1 key1, TKey2 key2, ref ObjectReader[] objectReaders, out int count)
	{
		HashComparer a = comparerPool.GetComparer();

		try
		{
			byte* pkey = stackalloc byte[KeySize];
			PopulateKeyBuffer(key1, key2, pkey, a.Strings);
			tran.Engine.ReadHashIndex(tran, base.HashIndex, pkey, a, ref objectReaders, out count);
		}
		finally
		{
			comparerPool.PutComparer(a);
		}
	}

	protected abstract void PopulateKeyBuffer(TKey1 key1, TKey2 key2, byte* pkey, string[] strings);
}

internal unsafe abstract class HashIndexReaderBase<TKey1, TKey2, TKey3> : HashIndexReaderBase, IHashIndexReader<TKey1, TKey2, TKey3>
{
	public HashIndexReaderBase() :
		base(new Type[] { typeof(TKey1), typeof(TKey2), typeof(TKey3) })
	{
	}

	[SkipLocalsInit]
	public void GetObjects(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ref ObjectReader[] objectReaders, out int count)
	{
		HashComparer a = comparerPool.GetComparer();

		try
		{
			byte* pkey = stackalloc byte[KeySize];
			PopulateKeyBuffer(key1, key2, key3, pkey, a.Strings);
			tran.Engine.ReadHashIndex(tran, base.HashIndex, pkey, a, ref objectReaders, out count);
		}
		finally
		{
			comparerPool.PutComparer(a);
		}
	}

	protected abstract void PopulateKeyBuffer(TKey1 key1, TKey2 key2, TKey3 key3, byte* pkey, string[] strings);
}

internal unsafe abstract class HashIndexReaderBase<TKey1, TKey2, TKey3, TKey4> : HashIndexReaderBase, IHashIndexReader<TKey1, TKey2, TKey3, TKey4>
{
	public HashIndexReaderBase() :
		base(new Type[] { typeof(TKey1), typeof(TKey2), typeof(TKey3), typeof(TKey4) })
	{
	}

	[SkipLocalsInit]
	public void GetObjects(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ref ObjectReader[] objectReaders, out int count)
	{
		HashComparer a = comparerPool.GetComparer();

		try
		{
			byte* pkey = stackalloc byte[KeySize];
			PopulateKeyBuffer(key1, key2, key3, key4, pkey, a.Strings);
			tran.Engine.ReadHashIndex(tran, base.HashIndex, pkey, a, ref objectReaders, out count);
		}
		finally
		{
			comparerPool.PutComparer(a);
		}
	}

	protected abstract void PopulateKeyBuffer(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, byte* pkey, string[] strings);
}

internal struct ComparerPool
{
	RWSpinLock sync;
	int count;
	HashComparer[] items;

	KeyProperty[] properties;
	int stringCount;

	public ComparerPool(int initCapacity, KeyProperty[] properties)
	{
		this.properties = properties;

		stringCount = 0;
		for (int i = 0; i < properties.Length; i++)
		{
			if (properties[i].PropertyType == PropertyType.String)
				stringCount++;
		}

		sync = new RWSpinLock();

		count = initCapacity;
		items = new HashComparer[count];
		KeyComparerDesc compDesc = new KeyComparerDesc(properties);
		for (int i = 0; i < count; i++)
		{
			items[i] = new HashComparer(compDesc, new string[stringCount]);
		}
	}

	public HashComparer GetComparer()
	{
		HashComparer s = null;
		sync.EnterWriteLock();
		if (count > 0)
			s = items[--count];

		sync.ExitWriteLock();

		if (s == null)
			s = new HashComparer(new KeyComparerDesc(properties), new string[stringCount]);

		return s;
	}

	public void PutComparer(HashComparer s)
	{
		sync.EnterWriteLock();
		if (items.Length == count)
			Array.Resize(ref items, items.Length * 2);

		items[count++] = s;
		sync.ExitWriteLock();
	}
}
