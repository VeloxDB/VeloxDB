using System;
using System.Linq;
using System.Diagnostics;
using VeloxDB.Descriptor;
using VeloxDB.Common;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace VeloxDB.Storage;

internal unsafe sealed class KeyComparer
{
	delegate void Copier(byte* src, KeyComparer srcComparer, byte* dst, string[] requestStrings, ref int reqStrCount, StringStorage stringStorage);
	delegate void Stringifier(StringBuilder sb, byte* src, string[] requestStrings, StringStorage stringStorage);

	delegate ulong HashCodeAdvancer(ulong h, byte* p, KeyComparer comparer, string[] requestStrings, StringStorage stringStorage);

	delegate long Comparer(byte* p1, KeyComparer comparer1, byte* p2,
		KeyComparer comparer2, string[] requestStrings, StringStorage stringStorage);

	// ulong.MaxValue is used when we need maximum handle value, but this is and indicator for a maximum total key.
	// ulong.MaxValue - 1 is also never a valid object handle.
	public const ulong MaxKey = ulong.MaxValue - 1;

	static Copier[] stringStorageCopiers;
	static Copier[] requestStringsCopiers;
	static Stringifier[] stringifiers;
	static HashCodeAdvancer[] hashAdvancers;
	static Comparer[] comparers;

	static readonly Dictionary<Type, Tuple<object, object>> minMaxTable;

	StringComparer stringComparer;
	KeyProperty[] properties;
	int stringPropertyCount;

	int keySize;

	static KeyComparer()
	{
		minMaxTable = new Dictionary<Type, Tuple<object, object>>
		{
			{ typeof(byte), new Tuple<object, object>(byte.MinValue, byte.MaxValue) },
			{ typeof(short), new Tuple<object, object>(short.MinValue, short.MaxValue) },
			{ typeof(int), new Tuple<object, object>(int.MinValue, int.MaxValue) },
			{ typeof(long), new Tuple<object, object>(long.MinValue, long.MaxValue) },
			{ typeof(float), new Tuple<object, object>(float.NegativeInfinity, float.PositiveInfinity) },
			{ typeof(double), new Tuple<object, object>(double.NegativeInfinity, double.PositiveInfinity) },
			{ typeof(bool), new Tuple<object, object>(false, true) },
			{ typeof(DateTime), new Tuple<object, object>(DateTime.MinValue, DateTime.MaxValue) },
			{ typeof(string), new Tuple<object, object>(StringStorage.MinimumString, StringStorage.MaximumString) },
		};

		comparers = new Comparer[(int)PropertyType.String + 1];
		comparers[(int)PropertyType.Byte] = CompareByte;
		comparers[(int)PropertyType.Short] = CompareShort;
		comparers[(int)PropertyType.Int] = CompareInt;
		comparers[(int)PropertyType.Long] = CompareLong;
		comparers[(int)PropertyType.Float] = CompareFloat;
		comparers[(int)PropertyType.Double] = CompareDouble;
		comparers[(int)PropertyType.Bool] = CompareByte;
		comparers[(int)PropertyType.DateTime] = CompareLong;
		comparers[(int)PropertyType.String] = CompareString;

		stringifiers = new Stringifier[(int)PropertyType.String + 1];
		stringifiers[(int)PropertyType.Byte] = StringifyByte;
		stringifiers[(int)PropertyType.Short] = StringifyShort;
		stringifiers[(int)PropertyType.Int] = StringifyInt;
		stringifiers[(int)PropertyType.Long] = StringifyLong;
		stringifiers[(int)PropertyType.Float] = StringifyFloat;
		stringifiers[(int)PropertyType.Double] = StringifyDouble;
		stringifiers[(int)PropertyType.Bool] = StringifyByte;
		stringifiers[(int)PropertyType.DateTime] = StringifyLong;
		stringifiers[(int)PropertyType.String] = StringifyString;

		stringStorageCopiers = new Copier[(int)PropertyType.String + 1];
		stringStorageCopiers[(int)PropertyType.Byte] = Copy1;
		stringStorageCopiers[(int)PropertyType.Short] = Copy2;
		stringStorageCopiers[(int)PropertyType.Int] = Copy4;
		stringStorageCopiers[(int)PropertyType.Long] = Copy8;
		stringStorageCopiers[(int)PropertyType.Float] = Copy4;
		stringStorageCopiers[(int)PropertyType.Double] = Copy8;
		stringStorageCopiers[(int)PropertyType.Bool] = Copy1;
		stringStorageCopiers[(int)PropertyType.DateTime] = Copy8;
		stringStorageCopiers[(int)PropertyType.String] = CopyStringStringStorage;

		requestStringsCopiers = new Copier[(int)PropertyType.String + 1];
		requestStringsCopiers[(int)PropertyType.Byte] = Copy1;
		requestStringsCopiers[(int)PropertyType.Short] = Copy2;
		requestStringsCopiers[(int)PropertyType.Int] = Copy4;
		requestStringsCopiers[(int)PropertyType.Long] = Copy8;
		requestStringsCopiers[(int)PropertyType.Float] = Copy4;
		requestStringsCopiers[(int)PropertyType.Double] = Copy8;
		requestStringsCopiers[(int)PropertyType.Bool] = Copy1;
		requestStringsCopiers[(int)PropertyType.DateTime] = Copy8;
		requestStringsCopiers[(int)PropertyType.String] = CopyStringRequestStrings;

		hashAdvancers = new HashCodeAdvancer[(int)PropertyType.String + 1];
		hashAdvancers[(int)PropertyType.Byte] = Advance1;
		hashAdvancers[(int)PropertyType.Short] = Advance2;
		hashAdvancers[(int)PropertyType.Int] = Advance4;
		hashAdvancers[(int)PropertyType.Long] = Advance8;
		hashAdvancers[(int)PropertyType.Float] = Advance4;
		hashAdvancers[(int)PropertyType.Double] = Advance8;
		hashAdvancers[(int)PropertyType.Bool] = Advance1;
		hashAdvancers[(int)PropertyType.DateTime] = Advance8;
		hashAdvancers[(int)PropertyType.String] = AdvanceStringHashCode;
	}

	public KeyComparer(KeyComparerDesc keyDesc)
	{
		properties = keyDesc.Properties;

		stringPropertyCount = 0;
		for (int i = 0; i < keyDesc.Properties.Length; i++)
		{
			if (keyDesc.Properties[i].PropertyType == PropertyType.String)
				stringPropertyCount++;
		}

		if (stringPropertyCount > 0)
		{
			stringComparer = keyDesc.CultureName == null ?
				(keyDesc.CaseSensitive ? StringComparer.InvariantCulture : StringComparer.InvariantCultureIgnoreCase) :
				StringComparer.Create(new CultureInfo(keyDesc.CultureName), !keyDesc.CaseSensitive);
		}

		keySize = 0;
		for (int i = 0; i < properties.Length; i++)
		{
			keySize += PropertyTypesHelper.GetItemSize(properties[i].PropertyType);
		}
	}

	public int KeySize => keySize;
	public bool HasStringProperties => stringPropertyCount > 0;
	public int StringPropertyCount => stringPropertyCount;

	public static TKey GetKeyMinimum<TKey>()
	{
		if (!minMaxTable.TryGetValue(typeof(TKey), out var key))
			throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.InvalidIndex));

		return (TKey)key.Item1;
	}

	public static TKey GetKeyMaximum<TKey>()
	{
		if (!minMaxTable.TryGetValue(typeof(TKey), out var key))
			throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.InvalidIndex));

		return (TKey)key.Item2;
	}

	public void ReleaseStrings(byte* key, StringStorage stringStorage)
	{
		for (int i = 0; i < properties.Length; i++)
		{
			if (properties[i].PropertyType == PropertyType.String)
			{
				ulong handle = ((ulong*)(key + properties[i].ByteOffset))[0];
				stringStorage.DecRefCount(handle);
			}
		}
	}

	public ulong CalculateHashCode(byte* key, ulong seed, string[] requestStrings, StringStorage stringStorage)
	{
		ulong v = HashUtils.StartHash64(seed);
		for (int i = 0; i < properties.Length; i++)
		{
			HashCodeAdvancer d = hashAdvancers[(int)properties[i].PropertyType];
			v = d(v, key + properties[i].ByteOffset, this, requestStrings, stringStorage);
		}

		return HashUtils.FinishHash64(v);
	}

	public bool Equals(byte* key1, string[] requestStrings, byte* key2, KeyComparer comparer2, StringStorage stringStorage)
	{
		Checker.AssertTrue(comparer2.properties.Length == properties.Length);

		for (int i = 0; i < properties.Length; i++)
		{
			Checker.AssertTrue(comparer2.properties[i].PropertyType == properties[i].PropertyType);

			Comparer c = comparers[(int)properties[i].PropertyType];
			if (c(key1 + properties[i].ByteOffset, this, key2 + comparer2.properties[i].ByteOffset,
				comparer2, requestStrings, stringStorage) != 0)
			{
				return false;
			}
		}

		return true;
	}

	public bool IsBefore(byte* key1, long id1, ulong handle1, string[] requestStrings, byte* key2, long id2, ulong handle2,
		KeyComparer comparer2, StringStorage stringStorage, out bool equal)
	{
		Checker.AssertTrue(comparer2 == null || comparer2.properties.Length == properties.Length);

		equal = false;

		if (handle1 == MaxKey)
			return false;

		if (handle2 == MaxKey)
			return true;

		for (int i = 0; i < properties.Length; i++)
		{
			Checker.AssertTrue(comparer2.properties[i].PropertyType == properties[i].PropertyType);

			Comparer c = comparers[(int)properties[i].PropertyType];
			long v = c(key1 + properties[i].ByteOffset, this, key2 + comparer2.properties[i].ByteOffset,
				comparer2, requestStrings, stringStorage);

			if (v < 0)
				return properties[i].SortOrder == SortOrder.Asc;
			else if (v > 0)
				return properties[i].SortOrder == SortOrder.Desc;
		}

		if (id1 < id2)
			return true;

		if (id1 > id2)
			return false;

		if (handle1 < handle2)
			return true;

		if (handle1 > handle2)
			return false;

		equal = true;
		return false;
	}

	public bool IsAfter(byte* key1, long id1, ulong handle1, string[] requestStrings, byte* key2, long id2, ulong handle2,
		KeyComparer comparer2, StringStorage stringStorage, out bool equal)
	{
		Checker.AssertTrue(comparer2 == null || comparer2.properties.Length == properties.Length);

		equal = false;

		if (handle1 == MaxKey)
			return true;

		if (handle2 == MaxKey)
			return false;

		for (int i = 0; i < properties.Length; i++)
		{
			Checker.AssertTrue(comparer2.properties[i].PropertyType == properties[i].PropertyType);

			Comparer c = comparers[(int)properties[i].PropertyType];
			long v = c(key1 + properties[i].ByteOffset, this, key2 + comparer2.properties[i].ByteOffset,
				comparer2, requestStrings, stringStorage);

			if (v < 0)
				return properties[i].SortOrder == SortOrder.Desc;
			else if (v > 0)
				return properties[i].SortOrder == SortOrder.Asc;
		}

		if (id1 < id2)
			return false;

		if (id1 > id2)
			return true;

		if (handle1 < handle2)
			return false;

		if (handle1 > handle2)
			return true;

		equal = true;
		return false;
	}

	public void CopyWithStringStorage(byte* src, string[] requestStrings, byte* dst, StringStorage stringStorage)
	{
		int dstOffset = 0;
		int reqStrCount = 0;
		for (int i = 0; i < properties.Length; i++)
		{
			Copier c = stringStorageCopiers[(int)properties[i].PropertyType];
			c(src + properties[i].ByteOffset, this, dst + dstOffset, requestStrings, ref reqStrCount, stringStorage);
			dstOffset += (int)PropertyTypesHelper.GetItemSize(properties[i].PropertyType);
		}
	}

	public void CopyWithRequestStrings(byte* src, StringStorage stringStorage, byte* dst, string[] requestStrings)
	{
		int dstOffset = 0;
		int reqStrCount = 0;
		for (int i = 0; i < properties.Length; i++)
		{
			Copier c = requestStringsCopiers[(int)properties[i].PropertyType];
			c(src + properties[i].ByteOffset, this, dst + dstOffset, requestStrings, ref reqStrCount, stringStorage);
			dstOffset += (int)PropertyTypesHelper.GetItemSize(properties[i].PropertyType);
		}

		Checker.AssertTrue(reqStrCount == StringPropertyCount);
	}

	private static long CompareByte(byte* p1, KeyComparer comparer1, byte* p2,
		KeyComparer comparer2, string[] requestStrings, StringStorage stringStorage)
	{
		return (long)*p1 - (long)*p2;
	}

	private static void StringifyByte(StringBuilder sb, byte* p, string[] requestStrings, StringStorage stringStorage)
	{
		sb.Append(*p);
	}

	private static long CompareShort(byte* p1, KeyComparer comparer1, byte* p2,
		KeyComparer comparer2, string[] requestStrings, StringStorage stringStorage)
	{
		return (long)((short*)p1)[0] - (long)((short*)p2)[0];
	}

	private static void StringifyShort(StringBuilder sb, byte* p, string[] requestStrings, StringStorage stringStorage)
	{
		sb.Append(*(short*)p);
	}

	private static long CompareInt(byte* p1, KeyComparer comparer1, byte* p2,
		KeyComparer comparer2, string[] requestStrings, StringStorage stringStorage)
	{
		return (long)((int*)p1)[0] - (long)((int*)p2)[0];
	}

	private static void StringifyInt(StringBuilder sb, byte* p, string[] requestStrings, StringStorage stringStorage)
	{
		sb.Append(*(int*)p);
	}

	private static long CompareLong(byte* p1, KeyComparer comparer1, byte* p2,
		KeyComparer comparer2, string[] requestStrings, StringStorage stringStorage)
	{
		long v1 = ((long*)p1)[0];
		long v2 = ((long*)p2)[0];
		if (v1 < v2)
			return -1;
		else if (v1 > v2)
			return 1;
		else
			return 0;
	}

	private static void StringifyLong(StringBuilder sb, byte* p, string[] requestStrings, StringStorage stringStorage)
	{
		sb.Append(*(long*)p);
	}

	private static long CompareFloat(byte* p1, KeyComparer comparer1, byte* p2,
		KeyComparer comparer2, string[] requestStrings, StringStorage stringStorage)
	{
		float v1 = ((float*)p1)[0];
		float v2 = ((float*)p2)[0];
		if (v1 < v2)
			return -1;
		else if (v1 > v2)
			return 1;
		else
			return 0;
	}

	private static void StringifyFloat(StringBuilder sb, byte* p, string[] requestStrings, StringStorage stringStorage)
	{
		sb.Append(*(float*)p);
	}

	private static long CompareDouble(byte* p1, KeyComparer comparer1, byte* p2,
		KeyComparer comparer2, string[] requestStrings, StringStorage stringStorage)
	{
		double v1 = ((double*)p1)[0];
		double v2 = ((double*)p2)[0];
		if (v1 < v2)
			return -1;
		else if (v1 > v2)
			return 1;
		else
			return 0;
	}

	private static void StringifyDouble(StringBuilder sb, byte* p, string[] requestStrings, StringStorage stringStorage)
	{
		sb.Append(*(double*)p);
	}

	private static long CompareString(byte* p1, KeyComparer comparer1, byte* p2,
		KeyComparer comparer2, string[] requestStrings, StringStorage stringStorage)
	{
		ulong handle1 = ((ulong*)p1)[0];
		ulong handle2 = ((ulong*)p2)[0];
		string s1 = requestStrings != null ? requestStrings[handle1] : stringStorage.GetStringSafe(handle1);
		string s2 = stringStorage.GetStringSafe(handle2);
		if (object.ReferenceEquals(s1, StringStorage.MinimumString))
		{
			return object.ReferenceEquals(s2, StringStorage.MinimumString) ? 0 : -1;
		}
		else if (object.ReferenceEquals(s1, StringStorage.MaximumString))
		{
			return object.ReferenceEquals(s2, StringStorage.MaximumString) ? 0 : 1;
		}
		else if (object.ReferenceEquals(s2, StringStorage.MinimumString))
		{
			return 1;
		}
		else if (object.ReferenceEquals(s2, StringStorage.MaximumString))
		{
			return -1;
		}
		else
		{
			return comparer1.stringComparer.Compare(s1, s2);
		}
	}

	private static void StringifyString(StringBuilder sb, byte* p, string[] requestStrings, StringStorage stringStorage)
	{
		ulong handle = ((ulong*)p)[0];
		string s = requestStrings != null ? requestStrings[handle] : stringStorage.GetStringSafe(handle);
		sb.Append(@"""");
		sb.Append(s);
		sb.Append(@"""");
	}

	private static ulong Advance1(ulong h, byte* p, KeyComparer comparer, string[] requestStrings, StringStorage stringStorage)
	{
		return HashUtils.AdvanceHash64(h, *p);
	}

	private static ulong Advance2(ulong h, byte* p, KeyComparer comparer, string[] requestStrings, StringStorage stringStorage)
	{
		return HashUtils.AdvanceHash64(h, ((ushort*)p)[0]);
	}

	private static ulong Advance4(ulong h, byte* p, KeyComparer comparer, string[] requestStrings, StringStorage stringStorage)
	{
		return HashUtils.AdvanceHash64(h, ((uint*)p)[0]);
	}

	private static ulong Advance8(ulong h, byte* p, KeyComparer comparer, string[] requestStrings, StringStorage stringStorage)
	{
		return HashUtils.AdvanceHash64(h, ((ulong*)p)[0]);
	}

	private static ulong AdvanceStringHashCode(ulong h, byte* p, KeyComparer comparer, string[] requestStrings, StringStorage stringStorage)
	{
		ulong handle = ((ulong*)p)[0];
		string s = requestStrings == null ? stringStorage.GetString(handle) : requestStrings[handle];
		int v = s == null ? 0 : comparer.stringComparer.GetHashCode(s);
		return HashUtils.AdvanceHash64(h, (uint)v);
	}

	private static void Copy1(byte* src, KeyComparer srcComparer, byte* dst, string[] requestStrings, ref int reqStrCount, StringStorage stringStorage)
	{
		*dst = *src;
	}

	private static void Copy2(byte* src, KeyComparer srcComparer, byte* dst, string[] requestStrings, ref int reqStrCount, StringStorage stringStorage)
	{
		*((short*)dst) = *((short*)src);
	}

	private static void Copy4(byte* src, KeyComparer srcComparer, byte* dst, string[] requestStrings, ref int reqStrCount, StringStorage stringStorage)
	{
		*((int*)dst) = *((int*)src);
	}

	private static void Copy8(byte* src, KeyComparer srcComparer, byte* dst, string[] requestStrings, ref int reqStrCount, StringStorage stringStorage)
	{
		*((long*)dst) = *((long*)src);
	}

	private static void CopyStringStringStorage(byte* src, KeyComparer srcComparer, byte* dst, string[] requestStrings, ref int reqStrCount, StringStorage stringStorage)
	{
		ulong srcHandle = ((ulong*)src)[0];
		if (requestStrings == null)
		{
			stringStorage.IncRefCount(srcHandle);
			((ulong*)dst)[0] = srcHandle;
		}
		else
		{
			string s = requestStrings[srcHandle];
			ulong dstHandle = stringStorage.AddStringSpecial(s);
			((ulong*)dst)[0] = dstHandle;
		}
	}

	private static void CopyStringRequestStrings(byte* src, KeyComparer srcComparer, byte* dst, string[] requestStrings, ref int reqStrCount, StringStorage stringStorage)
	{
		ulong srcHandle = ((ulong*)src)[0];
		string s = stringStorage.GetString(srcHandle);
		((ulong*)dst)[0] = (ulong)reqStrCount;
		requestStrings[reqStrCount++] = s;
	}

#if TEST_BUILD
	public void CollectKeyStrings(byte* key, Dictionary<ulong, int> strings)
	{
		for (int i = 0; i < properties.Length; i++)
		{
			if (properties[i].PropertyType == PropertyType.String)
			{
				ulong handle = ((ulong*)(key + properties[i].ByteOffset))[0];
				strings.TryGetValue(handle, out int count);
				strings[handle] = count + 1;
			}
		}
	}
#endif

	public string ShowKey(byte* key, string[] requestStrings, StringStorage stringStorage)
	{
		if (key == null)
			return "(MaxKey)";

		StringBuilder sb = new StringBuilder();
		sb.Append("(");
		for (int i = 0; i < properties.Length; i++)
		{
			Stringifier s = stringifiers[(int)properties[i].PropertyType];
			s(sb, key + properties[i].ByteOffset, requestStrings, stringStorage);
			if (i < properties.Length - 1)
				sb.Append(", ");
		}

		sb.Append(")");

		return sb.ToString();
	}

	[Conditional("TTTRACE")]
	public void TTTraceKeys(long traceId, ulong tranId, int indexId, byte* key,
		string[] requestStrings, StringStorage stringStorage, int source)
	{
		TTTrace.Write(traceId, tranId, source);
		for (int i = 0; i < properties.Length; i++)
		{
			if (properties[i].PropertyType == PropertyType.String)
			{
				uint handle = ((uint*)(key + properties[i].ByteOffset))[0];
				TTTrace.Write(traceId, tranId, indexId, i, source,
					requestStrings != null ? requestStrings[handle] : stringStorage.GetString(handle));
			}
			else
			{
				int size = PropertyTypesHelper.GetItemSize(properties[i].PropertyType);
				if (size == 1)
				{
					TTTrace.Write(traceId, tranId, indexId, i, source, ((byte*)(key + properties[i].ByteOffset))[0]);
				}
				else if (size == 2)
				{
					TTTrace.Write(traceId, tranId, indexId, i, source, ((short*)(key + properties[i].ByteOffset))[0]);
				}
				else if (size == 4)
				{
					TTTrace.Write(traceId, tranId, indexId, i, source, ((int*)(key + properties[i].ByteOffset))[0]);
				}
				else // 8
				{
					TTTrace.Write(traceId, tranId, indexId, i, source, ((long*)(key + properties[i].ByteOffset))[0]);
				}
			}
		}
	}
}
