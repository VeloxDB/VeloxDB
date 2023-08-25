using System;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal unsafe delegate void KeyWriter<TKey1>(TKey1 key1, byte* pkey, string[] strings);
internal unsafe delegate void KeyWriter<TKey1, TKey2>(TKey1 key1, TKey2 key2, byte* pkey, string[] strings);
internal unsafe delegate void KeyWriter<TKey1, TKey2, TKey3>(TKey1 key1, TKey2 key2, TKey3 key3, byte* pkey, string[] strings);
internal unsafe delegate void KeyWriter<TKey1, TKey2, TKey3, TKey4>(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, byte* pkey, string[] strings);

internal abstract class IndexReaderBase
{
	protected int keySize;
	protected KeyComparer comparer;

	public IndexReaderBase(Type[] types, string cultureName, bool caseSensitive, ReadOnlyArray<SortOrder> sortOrder)
	{
		keySize = 0;
		KeyProperty[] properties = new KeyProperty[types.Length];

		for (int i = 0; i < types.Length; i++)
		{
			PropertyType propType = PropertyTypesHelper.ManagedTypeToPropertyType(types[i]);
			if (propType == PropertyType.None || PropertyTypesHelper.IsArray(propType))
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.InvalidIndex));

			properties[i] = new KeyProperty(propType, keySize, sortOrder == null ? SortOrder.Asc : sortOrder[i]);
			keySize += PropertyTypesHelper.GetItemSize(propType);
		}

		KeyComparerDesc keyDesc = new KeyComparerDesc(properties, cultureName, caseSensitive);
		comparer = new KeyComparer(keyDesc);
	}

	public abstract void SetIndex(Index index);
}
