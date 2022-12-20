using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal unsafe sealed class ChangesetReader
{
	public const byte ShortNonNullStringFlag = 0xC0;
	public const byte LongNonNullStringFlag = 0x80;

	int currLog;
	LogChangeset[] logChangesets;

	DataModelDescriptor modelDesc;

	int offset;
	int bufferLength;
	byte* buffer;

	int bufferCount;
	byte* head;

	public ChangesetReader()
	{
	}

	public void Init(DataModelDescriptor modelDesc, Changeset changeset)
	{
		this.modelDesc = modelDesc;
		this.logChangesets = changeset.LogChangesets;
		this.currLog = -1;
		buffer = null;

		if (logChangesets.Length > 0)
			TakeNextLog();
	}

	public void Init(DataModelDescriptor model, LogChangeset logChangeset)
	{
		this.modelDesc = model;
		this.logChangesets = new LogChangeset[] { logChangeset };
		this.currLog = -1;
		buffer = null;
		TakeNextLog();
	}

	public void Clear()
	{
		modelDesc = null;
	}

	public static void SkipBlock(ChangesetReader reader, ChangesetBlock block)
	{
		for (int i = 0; i < block.OperationCount; i++)
		{
			reader.ReadLong();  // Operation header

			reader.ReadLong();
			for (int j = 1; j < block.PropertyCount; j++)
			{
				ChangesetBlockProperty prop = block.GetProperty(j);
				reader.SkipValue(prop.Type);
			}
		}
	}

	public bool TryReadRewindBlock(out ulong rewindVersion)
	{
		if (EndOfLog())
			TakeNextLog();

		// Rewind is always the only operation in the changeset
		if (offset + 1 > bufferLength || buffer[offset] != (byte)OperationType.Rewind)
		{
			rewindVersion = 0;
			return false;
		}

		ReadByte();
		rewindVersion = (ulong)ReadLong();
		return true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ReadSimpleValue(byte* buffer, int size)
	{
		EnsureSpace(size);

		if (size == 8)
		{
			*((long*)buffer) = *(long*)(this.buffer + offset);
			TTTrace.Write(*((long*)buffer));
			offset += 8;
		}
		else if (size == 4)
		{
			*((int*)buffer) = *(int*)(this.buffer + offset);
			TTTrace.Write(*((int*)buffer));
			offset += 4;
		}
		else if (size == 2)
		{
			*((short*)buffer) = *(short*)(this.buffer + offset);
			TTTrace.Write(*((short*)buffer));
			offset += 2;
		}
		else
		{
			*buffer = *(this.buffer + offset++);
			TTTrace.Write(*((byte*)buffer));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte ReadByte()
	{
		EnsureSpace(1);
		return buffer[offset++];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public short ReadShort()
	{
		EnsureSpace(2);
		short v = *((short*)(buffer + offset));
		offset += 2;
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int ReadInt()
	{
		EnsureSpace(4);
		int v = *((int*)(buffer + offset));
		offset += 4;
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public OperationHeader GetOperationHeader()
	{
		EnsureSpace(8);
		ulong* p = ((ulong*)(buffer + offset));
		offset += 8;
		return new OperationHeader(p);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long ReadLong()
	{
		EnsureSpace(8);
		long v = *((long*)(buffer + offset));
		offset += 8;
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public float ReadFloat()
	{
		EnsureSpace(4);
		float v = *((float*)(buffer + offset));
		offset += 4;
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public double ReadDouble()
	{
		EnsureSpace(8);
		double v = *((double*)(buffer + offset));
		offset += 8;
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool ReadBool()
	{
		EnsureSpace(1);
		byte v = *((byte*)(buffer + offset));
		offset += 1;
		return v != 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public DateTime ReadDateTime()
	{
		EnsureSpace(8);
		long v = *((long*)(buffer + offset));
		offset += 8;
		return DateTime.FromBinary(v);
	}

	public void ReadBytes(byte* ptr, int length)
	{
		int r = 0;
		while (r < length)
		{
			EnsureSpace(1);
			int cr = Math.Min(length - r, bufferLength - offset);
			Utils.CopyMemory(buffer + offset, ptr + r, cr);
			r += cr;
			offset += cr;
		}
	}

	public int ReadStringArraySize(out bool isNull)
	{
		return ReadStringArraySize(out isNull, out _);
	}

	public int ReadStringArraySize(out bool isNull, out bool isDefined)
	{
		if (ReadByte() == 0)
		{
			isDefined = ReadByte() != 0;
			isNull = true;
			return 0;
		}

		isDefined = true;
		isNull = false;
		return ReadInt();
	}

	public string ReadString()
	{
		return ReadString(out _);
	}

	public string ReadString(out bool isDefined)
	{
		int length = ReadLength(out bool isNull, out isDefined);
		if (isNull)
		{
			TTTrace.Write();
			return null;
		}

		if (length == 0)
		{
			TTTrace.Write();
			return string.Empty;
		}

		int n = ReadInt();
		string[] ss = logChangesets[currLog].Strings;
		if (ss != null)
		{
			SkipBytes(length * 2);
			TTTrace.Write(ss[n]);
			return ss[n];
		}
		else
		{
			string s = new string(' ', length);
			fixed (char* cp = s)
			{
				ReadBytes((byte*)cp, length * 2);
			}

			TTTrace.Write(s);
			return s;
		}
	}

	public int ReadLength(out bool isNull)
	{
		return ReadLength(out isNull, out _);
	}

	public int ReadLength(out bool isNull, out bool isDefined)
	{
		byte v = ReadByte();
		if ((v & ShortNonNullStringFlag) == ShortNonNullStringFlag)
		{
			isNull = false;
			isDefined = true;
			return v & (~ShortNonNullStringFlag);
		}

		if (v == LongNonNullStringFlag)
		{
			isNull = false;
			isDefined = true;
			return ReadInt();
		}

		isNull = true;
		isDefined = ReadByte() != 0;
		return 0;
	}

	public static OperationType PeekOperationType(LogChangeset ch)
	{
		if (ch == null)
			return OperationType.None;

		byte* buffer = ch.Buffers;
		int offset = ChangesetBufferHeader.Size;
		return (OperationType)buffer[offset];
	}

	public bool ReadBlock(bool validateNonExistent, ChangesetBlock block, bool isRestoring = false)
	{
		if (EndOfLog())
			TakeNextLog();

		OperationType opType = (OperationType)ReadByte();
		if (opType < OperationType.Insert || opType > OperationType.DefaultValue)
			throw new DatabaseException(DatabaseErrorDetail.CreateInvalidChangeset());

		if (opType == OperationType.Rewind)
		{
			block.InitRewind((ulong)ReadLong());
			return isRestoring || currLog == 0;
		}

		short classId = ReadShort();
		ClassDescriptor classDesc = modelDesc.GetClass(classId);
		if (classDesc == null)
		{
			if (validateNonExistent)
				throw new DatabaseException(DatabaseErrorDetail.CreateInvalidChangeset());
		}

		int operationCount = (ushort)ReadShort();
		int propertyCount = ReadShort();

		if (propertyCount < 0)
			throw new DatabaseException(DatabaseErrorDetail.CreateInvalidChangeset());

		block.Init(opType, classDesc, propertyCount, operationCount);

		ReadBlockProperties(validateNonExistent, block, classDesc, propertyCount);

		return true;
	}

	public void SkipValue(PropertyType type)
	{
		if (PropertyTypesHelper.IsSimpleValue(type))
		{
			if (type == PropertyType.String)
			{
				SkipString();
			}
			else
			{
				SkipSimpleValue(PropertyTypesHelper.GetItemSize(type));
			}
		}
		else if (type == PropertyType.StringArray)
		{
			SkipStringArray();
		}
		else
		{
			SkipArray(type);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SkipBytes(int size)
	{
		int r = 0;
		while (r < size)
		{
			EnsureSpace(1);
			int cr = Math.Min(size - r, bufferLength - offset);
			r += cr;
			offset += cr;
		}
	}

	public void SkipString()
	{
		int length = ReadLength(out bool isNull);
		if (!isNull && length > 0)
		{
			ReadInt();
			SkipBytes(length * 2);
		}
	}

	public void SkipArray(PropertyType type)
	{
		int len = ReadLength(out bool isNull);
		if (!isNull)
		{
			int size = len * PropertyTypesHelper.GetElementSize(type);
			SkipBytes(size);
		}
	}

	public void SkipStringArray()
	{
		int size = ReadStringArraySize(out bool isNull);
		if (!isNull)
			SkipBytes(size);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SkipSimpleValue(int size)
	{
		EnsureSpace(size);
		offset += size;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* GetSimpleValue(int size)
	{
		EnsureSpace(size);
		byte* res = buffer + offset;
		offset += size;
		return res;
	}

	private void TakeNextLog()
	{
		currLog++;
		LogChangeset ch = logChangesets[currLog];
		TTTrace.Write(ch.LogIndex);

		bufferCount = ch.BufferCount;
		head = ch.Buffers;

		buffer = ch.Buffers;
		bufferLength = (int)((ChangesetBufferHeader*)buffer)->size;
		offset = ChangesetBufferHeader.Size;
	}

	private void ReadBlockProperties(bool validateNonExistent, ChangesetBlock block, ClassDescriptor classDesc, int propertyCount)
	{
		for (int i = 1; i < propertyCount; i++)
		{
			int propId = ReadInt();
			PropertyType propType = (PropertyType)ReadByte();

			if (propId == -1 || !PropertyTypesHelper.IsTypeValid(propType))
				throw new DatabaseException(DatabaseErrorDetail.CreateInvalidChangeset());

			int propIndex = classDesc != null ? classDesc.GetPropertyIndex(propId) : -1;

			if (validateNonExistent && propIndex == -1)
				throw new DatabaseException(DatabaseErrorDetail.CreateInvalidChangeset());

			block.AddProperty(propIndex, propType);
		}
	}

	private void EnsureSpace(int size)
	{
		if (offset + size <= bufferLength)
			return;

		if (((ChangesetBufferHeader*)buffer)->next == null)
			throw new DatabaseException(DatabaseErrorDetail.CreateInvalidChangeset());

		buffer = (byte*)((ChangesetBufferHeader*)buffer)->next;
		bufferLength = (int)((ChangesetBufferHeader*)buffer)->size;
		offset = ChangesetBufferHeader.Size;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool EndOfLog()
	{
		return buffer == null || (((ChangesetBufferHeader*)buffer)->next == null && offset == bufferLength);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool EndOfStream()
	{
		if (EndOfLog())
			return currLog == logChangesets.Length - 1;

		return false;
	}

	public ReaderPosition GetPosition()
	{
		return new ReaderPosition() { Buffer = buffer, BufferLength = bufferLength, Offset = offset };
	}

	public void SetPosition(ReaderPosition position)
	{
		this.buffer = position.Buffer;
		this.bufferLength = position.BufferLength;
		this.offset = position.Offset;
	}
}

internal unsafe struct OperationHeader
{
	public const ulong NotLastInTranFlag = 0x8000000000000000;

	ulong* p;

	public OperationHeader(ulong* p)
	{
		this.p = p;
	}

	public ulong* Pointer => p;
	public ulong PreviousVersion => *p & 0x7fffffffffffffff;
	public bool IsLastInTransaction => (*p & 0x8000000000000000) == 0;
	public bool IsFirstInTransaction => PreviousVersion != 0;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WritePreviousVersion(ulong v)
	{
		*p = v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetNotLastInTransaction()
	{
		*p |= 0x8000000000000000;
	}
}


internal unsafe struct ReaderPosition
{
	public int Offset { get; set; }
	public int BufferLength { get; set; }
	public byte* Buffer { get; set; }
}
