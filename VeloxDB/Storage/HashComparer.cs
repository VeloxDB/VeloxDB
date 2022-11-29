using System;
using System.Linq;
using System.Diagnostics;
using Velox.Descriptor;
using Velox.Common;
using System.Collections.Generic;

namespace Velox.Storage;

internal unsafe sealed class HashComparer
{
	delegate void Copier(byte* src, HashComparer srcComparer, byte* dst, StringStorage stringStorage);
	delegate ulong HashCodeAdvancer(ulong h, byte* p, HashComparer comparer, StringStorage stringStorage);
	delegate bool EqualityComparer(byte* p1, HashComparer comparer1, byte* p2, HashComparer comparer2, StringStorage stringStorage);

	static Copier[] keyCopiers;
	static HashCodeAdvancer[] hashAdvancers;
	static EqualityComparer[] keyComparers;

	KeyProperty[] properties;
	KeyProperty[] stringProperties;

	string[] strings;

	static HashComparer()
	{
		keyComparers = new EqualityComparer[(int)PropertyType.String + 1];
		keyComparers[(int)PropertyType.Byte] = Equal1;
		keyComparers[(int)PropertyType.Short] = Equal2;
		keyComparers[(int)PropertyType.Int] = Equal4;
		keyComparers[(int)PropertyType.Long] = Equal8;
		keyComparers[(int)PropertyType.Float] = Equal4;
		keyComparers[(int)PropertyType.Double] = Equal8;
		keyComparers[(int)PropertyType.Bool] = Equal1;
		keyComparers[(int)PropertyType.DateTime] = Equal8;
		keyComparers[(int)PropertyType.String] = StringsEqual;

		keyCopiers = new Copier[(int)PropertyType.String + 1];
		keyCopiers[(int)PropertyType.Byte] = Copy1;
		keyCopiers[(int)PropertyType.Short] = Copy2;
		keyCopiers[(int)PropertyType.Int] = Copy4;
		keyCopiers[(int)PropertyType.Long] = Copy8;
		keyCopiers[(int)PropertyType.Float] = Copy4;
		keyCopiers[(int)PropertyType.Double] = Copy8;
		keyCopiers[(int)PropertyType.Bool] = Copy1;
		keyCopiers[(int)PropertyType.DateTime] = Copy8;
		keyCopiers[(int)PropertyType.String] = CopyString;

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

	public HashComparer(KeyComparerDesc keyDesc, string[] strings)
	{
		this.strings = strings;

		properties = keyDesc.Properties;
		stringProperties = properties.Where(x => x.PropertyType == PropertyType.String).ToArray();
	}

	public string[] Strings => strings;

	public bool HasNullStrings(byte* key, StringStorage stringStorage)
	{
		if (strings == null)
		{
			for (int i = 0; i < stringProperties.Length; i++)
			{
				ulong handle = ((ulong*)(key + stringProperties[i].ByteOffset))[0];
				if (stringStorage.GetString(handle) == null)
					return true;
			}

			return false;
		}

		for (int i = 0; i < stringProperties.Length; i++)
		{
			ulong handle = ((ulong*)(key + stringProperties[i].ByteOffset))[0];
			if (strings[handle] == null)
				return true;
		}

		return false;
	}

	public void ReleaseStrings(byte* key, StringStorage stringStorage)
	{
		for (int i = 0; i < stringProperties.Length; i++)
		{
			ulong handle = ((ulong*)(key + stringProperties[i].ByteOffset))[0];
			stringStorage.DecRefCount(handle);
		}
	}

	public ulong CalculateHashCode(byte* key, ulong seed, StringStorage stringStorage)
	{
		ulong v = HashUtils.StartHash64(seed);
		for (int i = 0; i < properties.Length; i++)
		{
			HashCodeAdvancer d = hashAdvancers[(int)properties[i].PropertyType];
			v = d(v, key + properties[i].ByteOffset, this, stringStorage);
		}

		return HashUtils.FinishHash64(v);
	}

	public bool AreKeysEqual(byte* key1, byte* key2, HashComparer comparer2, StringStorage stringStorage)
	{
		Checker.AssertTrue(comparer2.properties.Length == properties.Length);

		for (int i = 0; i < properties.Length; i++)
		{
			Checker.AssertTrue(comparer2.properties[i].PropertyType == properties[i].PropertyType);

			EqualityComparer c = keyComparers[(int)properties[i].PropertyType];
			if (!c(key1 + properties[i].ByteOffset, this, key2 + comparer2.properties[i].ByteOffset, comparer2, stringStorage))
				return false;
		}

		return true;
	}

	public void CopyKeyWithStringRetention(byte* src, byte* dst, StringStorage stringStorage)
	{
		int dstOffset = 0;
		for (int i = 0; i < properties.Length; i++)
		{
			Copier c = keyCopiers[(int)properties[i].PropertyType];
			c(src + properties[i].ByteOffset, this, dst + dstOffset, stringStorage);
			dstOffset += (int)PropertyTypesHelper.GetItemSize(properties[i].PropertyType);
		}
	}

	private static bool Equal1(byte* p1, HashComparer comparer1, byte* p2, HashComparer comparer2, StringStorage stringStorage)
	{
		return *p1 == *p2;
	}

	private static bool Equal2(byte* p1, HashComparer comparer1, byte* p2, HashComparer comparer2, StringStorage stringStorage)
	{
		return ((short*)p1)[0] == ((short*)p2)[0];
	}

	private static bool Equal4(byte* p1, HashComparer comparer1, byte* p2, HashComparer comparer2, StringStorage stringStorage)
	{
		return ((int*)p1)[0] == ((int*)p2)[0];
	}

	private static bool Equal8(byte* p1, HashComparer comparer1, byte* p2, HashComparer comparer2, StringStorage stringStorage)
	{
		return ((long*)p1)[0] == ((long*)p2)[0];
	}

	private static bool StringsEqual(byte* p1, HashComparer comparer1, byte* p2, HashComparer comparer2, StringStorage stringStorage)
	{
		ulong handle1 = ((ulong*)p1)[0];
		ulong handle2 = ((ulong*)p2)[0];
		string s1 = comparer1.Strings != null ? comparer1.Strings[handle1] : stringStorage.GetString(handle1);
		string s2 = comparer2.Strings != null ? comparer2.Strings[handle2] : stringStorage.GetString(handle2);
		return StringComparer.Ordinal.Equals(s1, s2);
	}

	private static ulong Advance1(ulong h, byte* p, HashComparer comparer, StringStorage stringStorage)
	{
		return HashUtils.AdvanceHash64(h, *p);
	}

	private static ulong Advance2(ulong h, byte* p, HashComparer comparer, StringStorage stringStorage)
	{
		return HashUtils.AdvanceHash64(h, ((ushort*)p)[0]);
	}

	private static ulong Advance4(ulong h, byte* p, HashComparer comparer, StringStorage stringStorage)
	{
		return HashUtils.AdvanceHash64(h, ((uint*)p)[0]);
	}

	private static ulong Advance8(ulong h, byte* p, HashComparer comparer, StringStorage stringStorage)
	{
		return HashUtils.AdvanceHash64(h, ((ulong*)p)[0]);
	}

	private static ulong AdvanceStringHashCode(ulong h, byte* p, HashComparer comparer, StringStorage stringStorage)
	{
		ulong handle = ((ulong*)p)[0];
		string s = comparer.Strings == null ? stringStorage.GetString(handle) : comparer.Strings[handle];
		int v = s == null ? 0 : s.GetHashCode();
		return HashUtils.AdvanceHash64(h, (uint)v);
	}

	private static void Copy1(byte* src, HashComparer srcComparer, byte* dst, StringStorage stringAllocator)
	{
		*dst = *src;
	}

	private static void Copy2(byte* src, HashComparer srcComparer, byte* dst, StringStorage stringAllocator)
	{
		*((short*)dst) = *((short*)src);
	}

	private static void Copy4(byte* src, HashComparer srcComparer, byte* dst, StringStorage stringAllocator)
	{
		*((int*)dst) = *((int*)src);
	}

	private static void Copy8(byte* src, HashComparer srcComparer, byte* dst, StringStorage stringAllocator)
	{
		*((long*)dst) = *((long*)src);
	}

	private static void CopyString(byte* src, HashComparer srcComparer, byte* dst, StringStorage stringAllocator)
	{
		ulong srcHandle = ((ulong*)src)[0];
		if (srcComparer.strings == null)
		{
			stringAllocator.IncRefCount(srcHandle);
			((ulong*)dst)[0] = srcHandle;
		}
		else
		{
			string s = srcComparer.strings[srcHandle];
			ulong dstHandle = stringAllocator.AddString(s);
			((ulong*)dst)[0] = dstHandle;
		}
	}

#if TEST_BUILD
	public void CollectKeyStrings(byte* key, Dictionary<ulong, int> strings)
	{
		if (this.strings != null)
			throw new InvalidOperationException();

		for (int i = 0; i < stringProperties.Length; i++)
		{
			ulong handle = ((ulong*)(key + stringProperties[i].ByteOffset))[0];
			strings.TryGetValue(handle, out int count);
			strings[handle] = count + 1;
		}
	}
#endif

	[Conditional("TTTRACE")]
	public void TTTraceKeys(long traceId, ulong tranId, int hashIndexId, byte* key, StringStorage stringStorage, int source)
	{
		TTTrace.Write(traceId, tranId, source);
		for (int i = 0; i < properties.Length; i++)
		{
			if (properties[i].PropertyType == PropertyType.String)
			{
				uint handle = ((uint*)(key + properties[i].ByteOffset))[0];
				TTTrace.Write(traceId, tranId, hashIndexId, i, source, strings != null ? strings[handle] : stringStorage.GetString(handle));
			}
			else
			{
				int size = PropertyTypesHelper.GetItemSize(properties[i].PropertyType);
				if (size == 1)
				{
					TTTrace.Write(traceId, tranId, hashIndexId, i, source, ((byte*)(key + properties[i].ByteOffset))[0]);
				}
				else if (size == 2)
				{
					TTTrace.Write(traceId, tranId, hashIndexId, i, source, ((short*)(key + properties[i].ByteOffset))[0]);
				}
				else if (size == 4)
				{
					TTTrace.Write(traceId, tranId, hashIndexId, i, source, ((int*)(key + properties[i].ByteOffset))[0]);
				}
				else // 8
				{
					TTTrace.Write(traceId, tranId, hashIndexId, i, source, ((long*)(key + properties[i].ByteOffset))[0]);
				}
			}
		}
	}
}
