using System;
using System.Collections.Generic;
using Velox.Descriptor;

namespace Velox.Common;

internal unsafe static class PropertyTypesHelper
{
	static readonly Dictionary<string, PropertyType> typeMap;
	static readonly int[] sizeInObject;
	static readonly int[] elementSizes;
	static readonly Dictionary<Type, PropertyType> managedTypeToPropertyType;
	static readonly Type[] propertyTypeToManagedType;

	static PropertyTypesHelper()
	{
		PropertyType[] propVals = Enum.GetValues<PropertyType>();
		typeMap = new Dictionary<string, PropertyType>(propVals.Length);
		foreach (PropertyType propType in propVals)
		{
			typeMap.Add(propType.ToString(), propType);
		}

		sizeInObject = new int[Utils.MaxEnumValue(typeof(PropertyType)) + 1];
		sizeInObject[(int)PropertyType.Byte] = 1;
		sizeInObject[(int)PropertyType.Short] = 2;
		sizeInObject[(int)PropertyType.Int] = 4;
		sizeInObject[(int)PropertyType.Long] = 8;
		sizeInObject[(int)PropertyType.Float] = 4;
		sizeInObject[(int)PropertyType.Double] = 8;
		sizeInObject[(int)PropertyType.String] = 8;
		sizeInObject[(int)PropertyType.Bool] = 1;
		sizeInObject[(int)PropertyType.DateTime] = 8;
		sizeInObject[(int)PropertyType.ByteArray] = 8;
		sizeInObject[(int)PropertyType.ShortArray] = 8;
		sizeInObject[(int)PropertyType.IntArray] = 8;
		sizeInObject[(int)PropertyType.LongArray] = 8;
		sizeInObject[(int)PropertyType.FloatArray] = 8;
		sizeInObject[(int)PropertyType.DoubleArray] = 8;
		sizeInObject[(int)PropertyType.StringArray] = 8;
		sizeInObject[(int)PropertyType.BoolArray] = 8;
		sizeInObject[(int)PropertyType.DateTimeArray] = 8;

		elementSizes = new int[Utils.MaxEnumValue(typeof(PropertyType)) + 1];
		elementSizes[(int)PropertyType.Byte] = 1;
		elementSizes[(int)PropertyType.Short] = 2;
		elementSizes[(int)PropertyType.Int] = 4;
		elementSizes[(int)PropertyType.Long] = 8;
		elementSizes[(int)PropertyType.Float] = 4;
		elementSizes[(int)PropertyType.Double] = 8;
		elementSizes[(int)PropertyType.Bool] = 1;
		elementSizes[(int)PropertyType.DateTime] = 8;
		elementSizes[(int)PropertyType.ByteArray] = 1;
		elementSizes[(int)PropertyType.ShortArray] = 2;
		elementSizes[(int)PropertyType.IntArray] = 4;
		elementSizes[(int)PropertyType.LongArray] = 8;
		elementSizes[(int)PropertyType.FloatArray] = 4;
		elementSizes[(int)PropertyType.DoubleArray] = 8;
		elementSizes[(int)PropertyType.BoolArray] = 1;
		elementSizes[(int)PropertyType.DateTimeArray] = 8;

		managedTypeToPropertyType = new Dictionary<Type, PropertyType>(18);
		managedTypeToPropertyType.Add(typeof(byte), PropertyType.Byte);
		managedTypeToPropertyType.Add(typeof(short), PropertyType.Short);
		managedTypeToPropertyType.Add(typeof(int), PropertyType.Int);
		managedTypeToPropertyType.Add(typeof(long), PropertyType.Long);
		managedTypeToPropertyType.Add(typeof(float), PropertyType.Float);
		managedTypeToPropertyType.Add(typeof(double), PropertyType.Double);
		managedTypeToPropertyType.Add(typeof(bool), PropertyType.Bool);
		managedTypeToPropertyType.Add(typeof(DateTime), PropertyType.DateTime);
		managedTypeToPropertyType.Add(typeof(string), PropertyType.String);

		propertyTypeToManagedType = new Type[Utils.MaxEnumValue(typeof(PropertyType)) + 1];
		propertyTypeToManagedType[(int)PropertyType.Byte] = typeof(byte);
		propertyTypeToManagedType[(int)PropertyType.Short] = typeof(short);
		propertyTypeToManagedType[(int)PropertyType.Int] = typeof(int);
		propertyTypeToManagedType[(int)PropertyType.Long] = typeof(long);
		propertyTypeToManagedType[(int)PropertyType.Float] = typeof(float);
		propertyTypeToManagedType[(int)PropertyType.Double] = typeof(double);
		propertyTypeToManagedType[(int)PropertyType.Bool] = typeof(bool);
		propertyTypeToManagedType[(int)PropertyType.DateTime] = typeof(DateTime);
		propertyTypeToManagedType[(int)PropertyType.String] = typeof(string);
		propertyTypeToManagedType[(int)PropertyType.ByteArray] = typeof(byte[]);
		propertyTypeToManagedType[(int)PropertyType.ShortArray] = typeof(short[]);
		propertyTypeToManagedType[(int)PropertyType.IntArray] = typeof(int[]);
		propertyTypeToManagedType[(int)PropertyType.LongArray] = typeof(long[]);
		propertyTypeToManagedType[(int)PropertyType.FloatArray] = typeof(float[]);
		propertyTypeToManagedType[(int)PropertyType.DoubleArray] = typeof(double[]);
		propertyTypeToManagedType[(int)PropertyType.StringArray] = typeof(string[]);
		propertyTypeToManagedType[(int)PropertyType.BoolArray] = typeof(bool[]);
		propertyTypeToManagedType[(int)PropertyType.DateTimeArray] = typeof(DateTime[]);
	}

	public static PropertyType GetPropertyType(string s)
	{
		return typeMap[s];
	}

	public static int GetItemSize(PropertyType type)
	{
		return sizeInObject[(int)type];
	}

	public static int GetElementSize(PropertyType type)
	{
		Checker.AssertTrue(type != PropertyType.String && type != PropertyType.StringArray);
		return elementSizes[(int)type];
	}

	public static bool IsTypeValid(PropertyType type)
	{
		return type >= PropertyType.Byte && type <= PropertyType.String ||
			type >= PropertyType.ByteArray && type <= PropertyType.StringArray;
	}

	public static bool IsSimpleValue(PropertyType type)
	{
		return type <= PropertyType.String;
	}

	public static bool IsArray(PropertyType type)
	{
		return type >= PropertyType.ByteArray;
	}

	public static PropertyType ManagedTypeToPropertyType(Type type)
	{
		if (type.IsEnum)
			type = type.GetEnumUnderlyingType();

		if (!managedTypeToPropertyType.TryGetValue(type, out var ptype))
			return PropertyType.None;

		return ptype;
	}

	public static Type PropertyTypeToManagedType(PropertyType type)
	{
		return propertyTypeToManagedType[(int)type];
	}

	public static void WriteDefaultPropertyValue(PropertyDescriptor property, IntPtr p)
	{
		switch (property.PropertyType)
		{
			case PropertyType.Byte:
				((byte*)p)[0] = (byte)property.DefaultValue;
				return;

			case PropertyType.Short:
				((short*)p)[0] = (short)property.DefaultValue;
				return;

			case PropertyType.Int:
				((int*)p)[0] = (int)property.DefaultValue;
				return;

			case PropertyType.Long:
				((long*)p)[0] = (long)property.DefaultValue;
				return;

			case PropertyType.Float:
				((float*)p)[0] = (float)property.DefaultValue;
				return;

			case PropertyType.Double:
				((double*)p)[0] = (double)property.DefaultValue;
				return;

			case PropertyType.Bool:
				((bool*)p)[0] = (bool)property.DefaultValue;
				return;

			case PropertyType.DateTime:
				((DateTime*)p)[0] = (DateTime)property.DefaultValue;
				return;

			default:
				throw new ArgumentException();
		}
	}

	public static byte[] DBUnpackByteArray(byte* buffer)
	{
		int len = ((int*)buffer)[0];
		if (len == 0)
			return Array.Empty<byte>();

		byte[] res = new byte[len];
		fixed (byte* vp = res)
		{
			Utils.CopyMemory(buffer + 4, vp, len);
		}

		return res;
	}

	public static short[] DBUnpackShortArray(byte* buffer)
	{
		int len = ((int*)buffer)[0];
		if (len == 0)
			return Array.Empty<short>();

		short[] res = new short[len];
		fixed (short* vp = res)
		{
			Utils.CopyMemory(buffer + 4, (byte*)vp, len << 1);
		}

		return res;
	}

	public static int[] DBUnpackIntArray(byte* buffer)
	{
		int len = ((int*)buffer)[0];
		if (len == 0)
			return Array.Empty<int>();

		int[] res = new int[len];
		fixed (int* vp = res)
		{
			Utils.CopyMemory(buffer + 4, (byte*)vp, len << 2);
		}

		return res;
	}

	public static long[] DBUnpackLongArray(byte* buffer)
	{
		int len = ((int*)buffer)[0];
		if (len == 0)
			return Array.Empty<long>();

		long[] res = new long[len];
		fixed (long* vp = res)
		{
			Utils.CopyMemory(buffer + 4, (byte*)vp, len << 3);
		}

		return res;
	}

	public static long* DBUnpackLongArray(byte* buffer, out int count)
	{
		count = ((int*)buffer)[0];
		if (count == 0)
			return null;

		return (long*)(buffer + 4);
	}

	public static bool DBCheckLongArrayForValue(byte* buffer, LongHashSet values, out long value)
	{
		int count = ((int*)buffer)[0];
		if (count == 0)
		{
			value = 0;
			return false;
		}

		long* lp = (long*)(buffer + 4);

		for (int i = 0; i < count; i++)
		{
			if (values.Contains(lp[i]))
			{
				value = lp[i];
				return true;
			}
		}

		value = 0;
		return false;
	}

	public static void DBUnpackLongArray(byte* buffer, long* lp)
	{
		int len = ((int*)buffer)[0];
		if (len == 0)
			return;

		Utils.CopyMemory(buffer + 4, (byte*)lp, len << 3);
	}

	public static float[] DBUnpackFloatArray(byte* buffer)
	{
		int len = ((int*)buffer)[0];
		if (len == 0)
			return Array.Empty<float>();

		float[] res = new float[len];
		fixed (float* vp = res)
		{
			Utils.CopyMemory(buffer + 4, (byte*)vp, len << 2);
		}

		return res;
	}

	public static double[] DBUnpackDoubleArray(byte* buffer)
	{
		int len = ((int*)buffer)[0];
		if (len == 0)
			return Array.Empty<double>();

		double[] res = new double[len];
		fixed (double* vp = res)
		{
			Utils.CopyMemory(buffer + 4, (byte*)vp, len << 3);
		}

		return res;
	}

	public static string[] DBUnpackStringArray(byte* buffer)
	{
		int len = ((int*)buffer)[0];
		if (len == 0)
			return Array.Empty<string>();

		int c = 4;
		string[] res = new string[len];
		for (int i = 0; i < len; i++)
		{
			int strLen = *((int*)(buffer + c));
			c += 4;

			res[i] = new string((char*)(buffer + c), 0, strLen);
			c += (strLen << 1);
		}

		return res;
	}

	public static int DBUnpackStringArraySize(byte* buffer)
	{
		int len = ((int*)buffer)[0];
		int c = 4;

		for (int i = 0; i < len; i++)
		{
			int strLen = *((int*)(buffer + c));
			c += 4;
			c += (strLen << 1);
		}

		return c;
	}

	public static bool[] DBUnpackBoolArray(byte* buffer)
	{
		int len = ((int*)buffer)[0];
		if (len == 0)
			return Array.Empty<bool>();

		bool[] res = new bool[len];
		fixed (bool* vp = res)
		{
			Utils.CopyMemory(buffer + 4, (byte*)vp, len);
		}

		return res;
	}

	public static DateTime[] DBUnpackDateTimeArray(byte* buffer)
	{
		int len = ((int*)buffer)[0];
		if (len == 0)
			return Array.Empty<DateTime>();

		int c = 4;
		DateTime[] res = new DateTime[len];
		for (int i = 0; i < len; i++)
		{
			res[i] = *((DateTime*)&(buffer[c]));
			c += 8;
		}

		return res;
	}
}
