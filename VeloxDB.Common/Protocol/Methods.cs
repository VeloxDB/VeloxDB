using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using VeloxDB.Networking;

namespace VeloxDB.Protocol;

internal static class Methods
{
	public static readonly MethodInfo WriteSByteMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteSByte), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteByteMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteByte), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteShortMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteShort), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteUShortMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteUShort), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteIntMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteInt), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteUIntMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteUInt), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteLongMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteLong), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteULongMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteULong), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteFloatMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteFloat), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteDoubleMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteDouble), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteBoolMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteBool), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteDecimalMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteDecimal), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteStringMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteString), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteDateTimeMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteDateTime), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteTimeSpanMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteTimeSpan), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteGuidMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteGuid), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteSByteArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteSByteArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteByteArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteByteArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteShortArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteShortArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteUShortArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteUShortArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteIntArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteIntArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteUIntArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteUIntArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteLongArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteLongArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteULongArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteULongArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteFloatArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteFloatArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteDoubleArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteDoubleArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteBoolArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteBoolArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteDecimalArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteDecimalArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteStringArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteStringArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteDateTimeArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteDateTimeArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteTimeSpanArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteTimeSpanArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteGuidArrayMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteGuidArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteSByteListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteSByteList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteByteListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteByteList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteShortListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteShortList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteUShortListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteUShortList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteIntListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteIntList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteUIntListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteUIntList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteLongListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteLongList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteULongListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteULongList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteFloatListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteFloatList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteDoubleListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteDoubleList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteBoolListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteBoolList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteDecimalListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteDecimalList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteStringListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteStringList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteDateTimeListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteDateTimeList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteTimeSpanListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteTimeSpanList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo WriteGuidListMethod = typeof(MessageWriter).GetMethod(nameof(MessageWriter.WriteGuidList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);

	public static readonly MethodInfo ReadSByteMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadSByte), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadByteMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadByte), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadShortMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadShort), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadUShortMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadUShort), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadIntMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadInt), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadUIntMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadUInt), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadLongMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadLong), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadULongMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadULong), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadFloatMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadFloat), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadDoubleMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadDouble), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadBoolMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadBool), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadDecimalMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadDecimal), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadStringMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadString), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadDateTimeMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadDateTime), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadTimeSpanMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadTimeSpan), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadGuidMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadGuid), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadSByteArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadSByteArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadByteArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadByteArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadShortArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadShortArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadUShortArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadUShortArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadIntArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadIntArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadUIntArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadUIntArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadLongArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadLongArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadULongArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadULongArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadFloatArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadFloatArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadDoubleArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadDoubleArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadBoolArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadBoolArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadDecimalArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadDecimalArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadStringArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadStringArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadDateTimeArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadDateTimeArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadTimeSpanArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadTimeSpanArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadGuidArrayMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadGuidArray), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadSByteArrayFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadSByteArrayFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadByteArrayFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadByteArrayFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadShortArrayFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadShortArrayFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadUShortArrayFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadUShortArrayFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadIntArrayFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadIntArrayFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadUIntArrayFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadUIntArrayFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadLongArrayFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadLongArrayFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadULongArrayFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadULongArrayFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadSByteListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadSByteList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadByteListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadByteList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadShortListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadShortList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadUShortListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadUShortList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadIntListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadIntList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadUIntListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadUIntList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadLongListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadLongList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadULongListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadULongList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadFloatListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadFloatList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadDoubleListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadDoubleList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadBoolListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadBoolList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadDecimalListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadDecimalList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadStringListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadStringList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadDateTimeListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadDateTimeList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadTimeSpanListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadTimeSpanList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadGuidListMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadGuidList), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadSByteListFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadSByteListFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadByteListFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadByteListFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadShortListFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadShortListFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadUShortListFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadUShortListFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadIntListFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadIntListFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadUIntListFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadUIntListFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadLongListFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadLongListFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo ReadULongListFactMethod = typeof(MessageReader).GetMethod(nameof(MessageReader.ReadULongListFact), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);

	public static readonly MethodInfo ArrayLengthMethod = typeof(Array).GetProperty(nameof(Array.Length), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance).GetGetMethod();
	public static readonly MethodInfo SerializePolymorphMethod = typeof(Methods).GetMethod(nameof(SerializePolymorphType), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Static);
	public static readonly MethodInfo SerializerContextTryWriteMethod = typeof(SerializerContext).GetMethod(nameof(SerializerContext.TryWriteInstance), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo SerializerContextGetMethod = typeof(SerializerContext).GetProperty(nameof(SerializerContext.Instance), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Static).GetGetMethod();
	public static readonly MethodInfo SerializerContextInitMethod = typeof(SerializerContext).GetMethod(nameof(SerializerContext.Init), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo SerializerContextSerQueuedMethod = typeof(SerializerContext).GetMethod(nameof(SerializerContext.SerializeQueued), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo DeserializePolymorphMethod = typeof(Methods).GetMethod(nameof(DeserializePolymorphType), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Static);
	public static readonly MethodInfo DeserializerContextTryReadMethod = typeof(DeserializerContext).GetMethod(nameof(DeserializerContext.TryReadInstance), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo DeserializerContextAddMethod = typeof(DeserializerContext).GetMethod(nameof(DeserializerContext.AddInstance), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo DeserializerContextGetMethod = typeof(DeserializerContext).GetMethod(nameof(DeserializerContext.GetInstance), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Static);
	public static readonly MethodInfo DeserializerContextResetMethod = typeof(DeserializerContext).GetMethod(nameof(DeserializerContext.Reset), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly MethodInfo DeserializerContextDeserQueuedMethod = typeof(DeserializerContext).GetMethod(nameof(DeserializerContext.DeserializeQueued), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);

	public static readonly FieldInfo MessageReaderOffsetFld = typeof(MessageReader).GetField(nameof(MessageReader.offset), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly FieldInfo MessageReaderSizeFld = typeof(MessageReader).GetField(nameof(MessageReader.size), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly FieldInfo MessageReaderBufferFld = typeof(MessageReader).GetField(nameof(MessageReader.buffer), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly FieldInfo MessageWriterOffsetFld = typeof(MessageWriter).GetField(nameof(MessageWriter.offset), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly FieldInfo MessageWriterSizeFld = typeof(MessageWriter).GetField(nameof(MessageWriter.capacity), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
	public static readonly FieldInfo MessageWriterBufferFld = typeof(MessageWriter).GetField(nameof(MessageWriter.buffer), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);

	public static readonly MethodInfo ConnReleaseReaderMethod = typeof(Connection).GetMethod(nameof(Connection.ReleaseReader), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);

	public static void GenerateForLoop(ILGenerator il, Action<LocalBuilder> bodyGenerator, LocalBuilder lenVar)
	{
		Label endLoopLabel = il.DefineLabel();
		Label beginLoopLabel = il.DefineLabel();

		LocalBuilder iVar = il.DeclareLocal(typeof(int));
		il.Emit(OpCodes.Ldc_I4_0);
		il.Emit(OpCodes.Stloc, iVar);

		il.MarkLabel(beginLoopLabel);

		il.Emit(OpCodes.Ldloc, iVar);
		il.Emit(OpCodes.Ldloc, lenVar);
		il.Emit(OpCodes.Bge, endLoopLabel);

		bodyGenerator(iVar);

		il.Emit(OpCodes.Ldloc, iVar);
		il.Emit(OpCodes.Ldc_I4_1);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Stloc, iVar);

		il.Emit(OpCodes.Br, beginLoopLabel);

		il.MarkLabel(endLoopLabel);
	}

	public static void SerializePolymorphType(MessageWriter writer, object value, SerializerContext context, int depth)
	{
		if (value == null)
		{
			writer.WriteUShort(0);
			return;
		}

		TypeSerializerEntry e = context.SerializerManager.GetTypeSerializer(value.GetType());
		if (e == null)
			throw new DbAPIMismatchException();

		writer.WriteUShort(e.TypeDesc.Id);
		e.Delegate(writer, value, context, depth);
	}

	public static object DeserializePolymorphType(MessageReader reader,
		DeserializerContext context, Delegate[] deserializerTable, int depth)
	{
		ushort typeId = reader.ReadUShort();
		if (typeId == 0)
			return null;

		if (typeId >= deserializerTable.Length || deserializerTable[typeId] == null)
			throw new DbAPIMismatchException();

		ProtocolDeserializeDelegate d = (deserializerTable[typeId] as ProtocolDeserializeDelegate);
		if (d != null)
		{
			return d(reader, context, deserializerTable, depth);
		}
		else
		{
			ProtocolSkipDelegate s = (ProtocolSkipDelegate)deserializerTable[typeId];
			s(reader, context, deserializerTable, depth);
			return null;
		}
	}
}
