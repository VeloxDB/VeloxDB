using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using VeloxDB.Common;

namespace VeloxDB.Protocol;

internal enum BuiltInType
{
	None = 0,
	SByte = 1,
	Byte = 2,
	Short = 3,
	UShort = 4,
	Int = 5,
	UInt = 6,
	Long = 7,
	ULong = 8,
	Float = 9,
	Double = 10,
	Decimal = 11,
	Bool = 12,
	String = 13,
	DateTime = 14,
	TimeSpan = 15,
	Guid = 16,
	MaxValue = 16,
}

internal static class BuiltInTypesHelper
{
	static readonly Dictionary<Type, BuiltInType> directMap;
	static readonly Type[] revereseMap;
	static readonly MethodInfo[] writeMethods;
	static readonly MethodInfo[] writeArrayMethods;
	static readonly MethodInfo[] writeListMethods;
	static readonly MethodInfo[] readMethods;
	static readonly MethodInfo[] readArrayMethods;
	static readonly MethodInfo[] readEnumArrayMethods;
	static readonly MethodInfo[] readListMethods;
	static readonly MethodInfo[] readEnumListMethods;
	static readonly OpCode[] storeIndInstructions;
	static readonly OpCode[] loadIndInstructions;
	static readonly byte[] sizes;

	static BuiltInTypesHelper()
	{
		directMap = new Dictionary<Type, BuiltInType>(Utils.MaxEnumValue(typeof(BuiltInType)) + 1);
		directMap.Add(typeof(sbyte), BuiltInType.SByte);
		directMap.Add(typeof(byte), BuiltInType.Byte);
		directMap.Add(typeof(short), BuiltInType.Short);
		directMap.Add(typeof(ushort), BuiltInType.UShort);
		directMap.Add(typeof(int), BuiltInType.Int);
		directMap.Add(typeof(uint), BuiltInType.UInt);
		directMap.Add(typeof(long), BuiltInType.Long);
		directMap.Add(typeof(ulong), BuiltInType.ULong);
		directMap.Add(typeof(float), BuiltInType.Float);
		directMap.Add(typeof(double), BuiltInType.Double);
		directMap.Add(typeof(decimal), BuiltInType.Decimal);
		directMap.Add(typeof(bool), BuiltInType.Bool);
		directMap.Add(typeof(string), BuiltInType.String);
		directMap.Add(typeof(DateTime), BuiltInType.DateTime);
		directMap.Add(typeof(TimeSpan), BuiltInType.TimeSpan);
		directMap.Add(typeof(Guid), BuiltInType.Guid);

		revereseMap = new Type[Utils.MaxEnumValue(typeof(BuiltInType)) + 1];
		revereseMap[(int)BuiltInType.SByte] = typeof(sbyte);
		revereseMap[(int)BuiltInType.Byte] = typeof(byte);
		revereseMap[(int)BuiltInType.Short] = typeof(short);
		revereseMap[(int)BuiltInType.UShort] = typeof(ushort);
		revereseMap[(int)BuiltInType.Int] = typeof(int);
		revereseMap[(int)BuiltInType.UInt] = typeof(uint);
		revereseMap[(int)BuiltInType.Long] = typeof(long);
		revereseMap[(int)BuiltInType.ULong] = typeof(ulong);
		revereseMap[(int)BuiltInType.Float] = typeof(float);
		revereseMap[(int)BuiltInType.Double] = typeof(double);
		revereseMap[(int)BuiltInType.Decimal] = typeof(decimal);
		revereseMap[(int)BuiltInType.Bool] = typeof(bool);
		revereseMap[(int)BuiltInType.String] = typeof(string);
		revereseMap[(int)BuiltInType.DateTime] = typeof(DateTime);
		revereseMap[(int)BuiltInType.TimeSpan] = typeof(TimeSpan);
		revereseMap[(int)BuiltInType.Guid] = typeof(Guid);

		writeMethods = new MethodInfo[Utils.MaxEnumValue(typeof(BuiltInType)) + 1];
		writeMethods[(int)BuiltInType.SByte] = Methods.WriteSByteMethod;
		writeMethods[(int)BuiltInType.Byte] = Methods.WriteByteMethod;
		writeMethods[(int)BuiltInType.Short] = Methods.WriteShortMethod;
		writeMethods[(int)BuiltInType.UShort] = Methods.WriteUShortMethod;
		writeMethods[(int)BuiltInType.Int] = Methods.WriteIntMethod;
		writeMethods[(int)BuiltInType.UInt] = Methods.WriteUIntMethod;
		writeMethods[(int)BuiltInType.Long] = Methods.WriteLongMethod;
		writeMethods[(int)BuiltInType.ULong] = Methods.WriteULongMethod;
		writeMethods[(int)BuiltInType.Float] = Methods.WriteFloatMethod;
		writeMethods[(int)BuiltInType.Double] = Methods.WriteDoubleMethod;
		writeMethods[(int)BuiltInType.Bool] = Methods.WriteBoolMethod;
		writeMethods[(int)BuiltInType.Decimal] = Methods.WriteDecimalMethod;
		writeMethods[(int)BuiltInType.String] = Methods.WriteStringMethod;
		writeMethods[(int)BuiltInType.DateTime] = Methods.WriteDateTimeMethod;
		writeMethods[(int)BuiltInType.TimeSpan] = Methods.WriteTimeSpanMethod;
		writeMethods[(int)BuiltInType.Guid] = Methods.WriteGuidMethod;

		writeArrayMethods = new MethodInfo[Utils.MaxEnumValue(typeof(BuiltInType)) + 1];
		writeArrayMethods[(int)BuiltInType.SByte] = Methods.WriteSByteArrayMethod;
		writeArrayMethods[(int)BuiltInType.Byte] = Methods.WriteByteArrayMethod;
		writeArrayMethods[(int)BuiltInType.Short] = Methods.WriteShortArrayMethod;
		writeArrayMethods[(int)BuiltInType.UShort] = Methods.WriteUShortArrayMethod;
		writeArrayMethods[(int)BuiltInType.Int] = Methods.WriteIntArrayMethod;
		writeArrayMethods[(int)BuiltInType.UInt] = Methods.WriteUIntArrayMethod;
		writeArrayMethods[(int)BuiltInType.Long] = Methods.WriteLongArrayMethod;
		writeArrayMethods[(int)BuiltInType.ULong] = Methods.WriteULongArrayMethod;
		writeArrayMethods[(int)BuiltInType.Float] = Methods.WriteFloatArrayMethod;
		writeArrayMethods[(int)BuiltInType.Double] = Methods.WriteDoubleArrayMethod;
		writeArrayMethods[(int)BuiltInType.Bool] = Methods.WriteBoolArrayMethod;
		writeArrayMethods[(int)BuiltInType.Decimal] = Methods.WriteDecimalArrayMethod;
		writeArrayMethods[(int)BuiltInType.String] = Methods.WriteStringArrayMethod;
		writeArrayMethods[(int)BuiltInType.DateTime] = Methods.WriteDateTimeArrayMethod;
		writeArrayMethods[(int)BuiltInType.TimeSpan] = Methods.WriteTimeSpanArrayMethod;
		writeArrayMethods[(int)BuiltInType.Guid] = Methods.WriteGuidArrayMethod;

		writeListMethods = new MethodInfo[Utils.MaxEnumValue(typeof(BuiltInType)) + 1];
		writeListMethods[(int)BuiltInType.SByte] = Methods.WriteSByteListMethod;
		writeListMethods[(int)BuiltInType.Byte] = Methods.WriteByteListMethod;
		writeListMethods[(int)BuiltInType.Short] = Methods.WriteShortListMethod;
		writeListMethods[(int)BuiltInType.UShort] = Methods.WriteUShortListMethod;
		writeListMethods[(int)BuiltInType.Int] = Methods.WriteIntListMethod;
		writeListMethods[(int)BuiltInType.UInt] = Methods.WriteUIntListMethod;
		writeListMethods[(int)BuiltInType.Long] = Methods.WriteLongListMethod;
		writeListMethods[(int)BuiltInType.ULong] = Methods.WriteULongListMethod;
		writeListMethods[(int)BuiltInType.Float] = Methods.WriteFloatListMethod;
		writeListMethods[(int)BuiltInType.Double] = Methods.WriteDoubleListMethod;
		writeListMethods[(int)BuiltInType.Bool] = Methods.WriteBoolListMethod;
		writeListMethods[(int)BuiltInType.Decimal] = Methods.WriteDecimalListMethod;
		writeListMethods[(int)BuiltInType.String] = Methods.WriteStringListMethod;
		writeListMethods[(int)BuiltInType.DateTime] = Methods.WriteDateTimeListMethod;
		writeListMethods[(int)BuiltInType.TimeSpan] = Methods.WriteTimeSpanListMethod;
		writeListMethods[(int)BuiltInType.Guid] = Methods.WriteGuidListMethod;

		readMethods = new MethodInfo[Utils.MaxEnumValue(typeof(BuiltInType)) + 1];
		readMethods[(int)BuiltInType.SByte] = Methods.ReadSByteMethod;
		readMethods[(int)BuiltInType.Byte] = Methods.ReadByteMethod;
		readMethods[(int)BuiltInType.Short] = Methods.ReadShortMethod;
		readMethods[(int)BuiltInType.UShort] = Methods.ReadUShortMethod;
		readMethods[(int)BuiltInType.Int] = Methods.ReadIntMethod;
		readMethods[(int)BuiltInType.UInt] = Methods.ReadUIntMethod;
		readMethods[(int)BuiltInType.Long] = Methods.ReadLongMethod;
		readMethods[(int)BuiltInType.ULong] = Methods.ReadULongMethod;
		readMethods[(int)BuiltInType.Float] = Methods.ReadFloatMethod;
		readMethods[(int)BuiltInType.Double] = Methods.ReadDoubleMethod;
		readMethods[(int)BuiltInType.Bool] = Methods.ReadBoolMethod;
		readMethods[(int)BuiltInType.Decimal] = Methods.ReadDecimalMethod;
		readMethods[(int)BuiltInType.String] = Methods.ReadStringMethod;
		readMethods[(int)BuiltInType.DateTime] = Methods.ReadDateTimeMethod;
		readMethods[(int)BuiltInType.TimeSpan] = Methods.ReadTimeSpanMethod;
		readMethods[(int)BuiltInType.Guid] = Methods.ReadGuidMethod;

		readArrayMethods = new MethodInfo[Utils.MaxEnumValue(typeof(BuiltInType)) + 1];
		readArrayMethods[(int)BuiltInType.SByte] = Methods.ReadSByteArrayMethod;
		readArrayMethods[(int)BuiltInType.Byte] = Methods.ReadByteArrayMethod;
		readArrayMethods[(int)BuiltInType.Short] = Methods.ReadShortArrayMethod;
		readArrayMethods[(int)BuiltInType.UShort] = Methods.ReadUShortArrayMethod;
		readArrayMethods[(int)BuiltInType.Int] = Methods.ReadIntArrayMethod;
		readArrayMethods[(int)BuiltInType.UInt] = Methods.ReadUIntArrayMethod;
		readArrayMethods[(int)BuiltInType.Long] = Methods.ReadLongArrayMethod;
		readArrayMethods[(int)BuiltInType.ULong] = Methods.ReadULongArrayMethod;
		readArrayMethods[(int)BuiltInType.Float] = Methods.ReadFloatArrayMethod;
		readArrayMethods[(int)BuiltInType.Double] = Methods.ReadDoubleArrayMethod;
		readArrayMethods[(int)BuiltInType.Bool] = Methods.ReadBoolArrayMethod;
		readArrayMethods[(int)BuiltInType.Decimal] = Methods.ReadDecimalArrayMethod;
		readArrayMethods[(int)BuiltInType.String] = Methods.ReadStringArrayMethod;
		readArrayMethods[(int)BuiltInType.DateTime] = Methods.ReadDateTimeArrayMethod;
		readArrayMethods[(int)BuiltInType.TimeSpan] = Methods.ReadTimeSpanArrayMethod;
		readArrayMethods[(int)BuiltInType.Guid] = Methods.ReadGuidArrayMethod;

		readEnumArrayMethods = new MethodInfo[(int)BuiltInType.ULong + 1];
		readEnumArrayMethods[(int)BuiltInType.SByte] = Methods.ReadSByteArrayFactMethod;
		readEnumArrayMethods[(int)BuiltInType.Byte] = Methods.ReadByteArrayFactMethod;
		readEnumArrayMethods[(int)BuiltInType.Short] = Methods.ReadShortArrayFactMethod;
		readEnumArrayMethods[(int)BuiltInType.UShort] = Methods.ReadUShortArrayFactMethod;
		readEnumArrayMethods[(int)BuiltInType.Int] = Methods.ReadIntArrayFactMethod;
		readEnumArrayMethods[(int)BuiltInType.UInt] = Methods.ReadUIntArrayFactMethod;
		readEnumArrayMethods[(int)BuiltInType.Long] = Methods.ReadLongArrayFactMethod;
		readEnumArrayMethods[(int)BuiltInType.ULong] = Methods.ReadULongArrayFactMethod;

		readListMethods = new MethodInfo[Utils.MaxEnumValue(typeof(BuiltInType)) + 1];
		readListMethods[(int)BuiltInType.SByte] = Methods.ReadSByteListMethod;
		readListMethods[(int)BuiltInType.Byte] = Methods.ReadByteListMethod;
		readListMethods[(int)BuiltInType.Short] = Methods.ReadShortListMethod;
		readListMethods[(int)BuiltInType.UShort] = Methods.ReadUShortListMethod;
		readListMethods[(int)BuiltInType.Int] = Methods.ReadIntListMethod;
		readListMethods[(int)BuiltInType.UInt] = Methods.ReadUIntListMethod;
		readListMethods[(int)BuiltInType.Long] = Methods.ReadLongListMethod;
		readListMethods[(int)BuiltInType.ULong] = Methods.ReadULongListMethod;
		readListMethods[(int)BuiltInType.Float] = Methods.ReadFloatListMethod;
		readListMethods[(int)BuiltInType.Double] = Methods.ReadDoubleListMethod;
		readListMethods[(int)BuiltInType.Bool] = Methods.ReadBoolListMethod;
		readListMethods[(int)BuiltInType.Decimal] = Methods.ReadDecimalListMethod;
		readListMethods[(int)BuiltInType.String] = Methods.ReadStringListMethod;
		readListMethods[(int)BuiltInType.DateTime] = Methods.ReadDateTimeListMethod;
		readListMethods[(int)BuiltInType.TimeSpan] = Methods.ReadTimeSpanListMethod;
		readListMethods[(int)BuiltInType.Guid] = Methods.ReadGuidListMethod;

		readEnumListMethods = new MethodInfo[(int)BuiltInType.ULong + 1];
		readEnumListMethods[(int)BuiltInType.SByte] = Methods.ReadSByteListFactMethod;
		readEnumListMethods[(int)BuiltInType.Byte] = Methods.ReadByteListFactMethod;
		readEnumListMethods[(int)BuiltInType.Short] = Methods.ReadShortListFactMethod;
		readEnumListMethods[(int)BuiltInType.UShort] = Methods.ReadUShortListFactMethod;
		readEnumListMethods[(int)BuiltInType.Int] = Methods.ReadIntListFactMethod;
		readEnumListMethods[(int)BuiltInType.UInt] = Methods.ReadUIntListFactMethod;
		readEnumListMethods[(int)BuiltInType.Long] = Methods.ReadLongListFactMethod;
		readEnumListMethods[(int)BuiltInType.ULong] = Methods.ReadULongListFactMethod;

		storeIndInstructions = new OpCode[Utils.MaxEnumValue(typeof(BuiltInType)) + 1];
		storeIndInstructions[(int)BuiltInType.SByte] = OpCodes.Stind_I1;
		storeIndInstructions[(int)BuiltInType.Byte] = OpCodes.Stind_I1;
		storeIndInstructions[(int)BuiltInType.Short] = OpCodes.Stind_I2;
		storeIndInstructions[(int)BuiltInType.UShort] = OpCodes.Stind_I2;
		storeIndInstructions[(int)BuiltInType.Int] = OpCodes.Stind_I4;
		storeIndInstructions[(int)BuiltInType.UInt] = OpCodes.Stind_I4;
		storeIndInstructions[(int)BuiltInType.Long] = OpCodes.Stind_I8;
		storeIndInstructions[(int)BuiltInType.ULong] = OpCodes.Stind_I8;
		storeIndInstructions[(int)BuiltInType.Float] = OpCodes.Stind_R4;
		storeIndInstructions[(int)BuiltInType.Double] = OpCodes.Stind_R8;
		storeIndInstructions[(int)BuiltInType.Bool] = OpCodes.Stind_I1;
		storeIndInstructions[(int)BuiltInType.Decimal] = OpCodes.Stobj;
		storeIndInstructions[(int)BuiltInType.String] = OpCodes.Stind_Ref;
		storeIndInstructions[(int)BuiltInType.DateTime] = OpCodes.Stobj;
		storeIndInstructions[(int)BuiltInType.TimeSpan] = OpCodes.Stobj;
		storeIndInstructions[(int)BuiltInType.Guid] = OpCodes.Stobj;

		loadIndInstructions = new OpCode[Utils.MaxEnumValue(typeof(BuiltInType)) + 1];
		loadIndInstructions[(int)BuiltInType.SByte] = OpCodes.Ldind_I1;
		loadIndInstructions[(int)BuiltInType.Byte] = OpCodes.Ldind_U1;
		loadIndInstructions[(int)BuiltInType.Short] = OpCodes.Ldind_I2;
		loadIndInstructions[(int)BuiltInType.UShort] = OpCodes.Ldind_U2;
		loadIndInstructions[(int)BuiltInType.Int] = OpCodes.Ldind_I4;
		loadIndInstructions[(int)BuiltInType.UInt] = OpCodes.Ldind_U4;
		loadIndInstructions[(int)BuiltInType.Long] = OpCodes.Ldind_I8;
		loadIndInstructions[(int)BuiltInType.ULong] = OpCodes.Ldind_I8;
		loadIndInstructions[(int)BuiltInType.Float] = OpCodes.Ldind_R4;
		loadIndInstructions[(int)BuiltInType.Double] = OpCodes.Ldind_R8;
		loadIndInstructions[(int)BuiltInType.Bool] = OpCodes.Ldind_U1;
		loadIndInstructions[(int)BuiltInType.Decimal] = OpCodes.Ldobj;
		loadIndInstructions[(int)BuiltInType.String] = OpCodes.Ldind_Ref;
		loadIndInstructions[(int)BuiltInType.DateTime] = OpCodes.Ldobj;
		loadIndInstructions[(int)BuiltInType.TimeSpan] = OpCodes.Ldobj;
		loadIndInstructions[(int)BuiltInType.Guid] = OpCodes.Ldobj;

		sizes = new byte[Utils.MaxEnumValue(typeof(BuiltInType)) + 1];
		sizes[(int)BuiltInType.SByte] = 1;
		sizes[(int)BuiltInType.Byte] = 1;
		sizes[(int)BuiltInType.Short] = 2;
		sizes[(int)BuiltInType.UShort] = 2;
		sizes[(int)BuiltInType.Int] = 4;
		sizes[(int)BuiltInType.UInt] = 4;
		sizes[(int)BuiltInType.Long] = 8;
		sizes[(int)BuiltInType.ULong] = 8;
		sizes[(int)BuiltInType.Float] = 4;
		sizes[(int)BuiltInType.Double] = 8;
		sizes[(int)BuiltInType.Bool] = 1;
		sizes[(int)BuiltInType.Decimal] = 16;
		sizes[(int)BuiltInType.String] = 0;
		sizes[(int)BuiltInType.DateTime] = 8;
		sizes[(int)BuiltInType.TimeSpan] = 8;
		sizes[(int)BuiltInType.Guid] = 0;
	}

	public static bool IsSimple(BuiltInType type)
	{
		return type != BuiltInType.None && type != BuiltInType.String && type != BuiltInType.Guid;
	}

	public static int GetSimpleSize(BuiltInType type)
	{
		return sizes[(int)type];
	}

	public static BuiltInType To(Type type)
	{
		if (type.IsEnum)
			type = Enum.GetUnderlyingType(type);

		return directMap[type];
	}

	public static Type From(BuiltInType builtInType)
	{
		return revereseMap[(int)builtInType];
	}

	public static bool IsValidValue(BuiltInType builtInType)
	{
		return builtInType >= BuiltInType.None && builtInType <= BuiltInType.MaxValue;
	}

	public static bool IsBuiltInType(Type type)
	{
		if (type.IsEnum)
			type = Enum.GetUnderlyingType(type);

		return directMap.ContainsKey(type);
	}

	public static MethodInfo GetWriteMethod(BuiltInType type)
	{
		return writeMethods[(int)type];
	}

	public static MethodInfo GetWriteArrayMethod(BuiltInType type)
	{
		return writeArrayMethods[(int)type];
	}

	public static MethodInfo GetWriteListMethod(BuiltInType type)
	{
		return writeListMethods[(int)type];
	}

	public static MethodInfo GetReadMethod(BuiltInType type)
	{
		return readMethods[(int)type];
	}

	public static MethodInfo GetReadArrayMethod(BuiltInType type)
	{
		return readArrayMethods[(int)type];
	}

	public static MethodInfo GetReadEnumArrayMethod(BuiltInType type)
	{
		return readEnumArrayMethods[(int)type];
	}

	public static MethodInfo GetReadListMethod(BuiltInType type)
	{
		return readListMethods[(int)type];
	}

	public static MethodInfo GetReadEnumListMethod(BuiltInType type)
	{
		return readEnumListMethods[(int)type];
	}

	public static OpCode GetIndStoreInstruction(BuiltInType type)
	{
		return storeIndInstructions[(int)type];
	}

	public static OpCode GetIndLoadInstruction(BuiltInType type)
	{
		return loadIndInstructions[(int)type];
	}

	public static bool IsInitInPlace(BuiltInType type)
	{
		return type == BuiltInType.Decimal || type == BuiltInType.DateTime || type == BuiltInType.TimeSpan || type == BuiltInType.Guid;
	}

	public static void GenerateLoadDefaultValue(ILGenerator il, BuiltInType type, out bool initInPlace)
	{
		initInPlace = false;
		if (type <= BuiltInType.UInt || type == BuiltInType.Bool)
		{
			il.Emit(OpCodes.Ldc_I4_0);
		}
		else if (type <= BuiltInType.ULong)
		{
			il.Emit(OpCodes.Ldc_I8, 0);
		}
		else if (type == BuiltInType.Float)
		{
			il.Emit(OpCodes.Ldc_R4, 0.0f);
		}
		else if (type == BuiltInType.Double)
		{
			il.Emit(OpCodes.Ldc_R8, 0.0);
		}
		else if (type == BuiltInType.Decimal || type == BuiltInType.DateTime || type == BuiltInType.TimeSpan || type == BuiltInType.Guid)
		{
			initInPlace = true;
			il.Emit(OpCodes.Initobj, From(type));
		}
		else if (type == BuiltInType.String)
		{
			il.Emit(OpCodes.Ldnull);
		}
	}
}
