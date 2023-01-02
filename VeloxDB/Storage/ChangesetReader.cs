using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
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
			reader.GetOperationHeader();
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
		if (offset + size <= bufferLength)
		{
			if (size == 8)
			{
				*((long*)buffer) = *(long*)(this.buffer + offset);
				offset += 8;
				TTTrace.Write(*((long*)buffer));
			}
			else if (size == 4)
			{
				*((int*)buffer) = *(int*)(this.buffer + offset);
				offset += 4;
				TTTrace.Write(*((int*)buffer));
			}
			else if (size == 2)
			{
				*((short*)buffer) = *(short*)(this.buffer + offset);
				offset += 2;
				TTTrace.Write(*((short*)buffer));
			}
			else
			{
				*buffer = *(this.buffer + offset++);
				TTTrace.Write(*((byte*)buffer));
			}

			return;
		}

		ReadBytes(buffer, size);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte ReadByte()
	{
		if (offset + 1 <= bufferLength)
			return buffer[offset++];

		byte b;
		ReadBytes((byte*)&b, 1);
		return b;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public short ReadShort()
	{
		if (offset + 2 <= bufferLength)
		{
			short v = *((short*)(buffer + offset));
			offset += 2;
			return v;
		}

		short b;
		ReadBytes((byte*)&b, 2);
		return b;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int ReadInt()
	{
		if (offset + 4 <= bufferLength)
		{
			int v = *((int*)(buffer + offset));
			offset += 4;
			return v;
		}

		int b;
		ReadBytes((byte*)&b, 4);
		return b;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public OperationHeader GetOperationHeader()
	{
		EnsureSpace(1);
		if (offset + 8 <= bufferLength)
		{
			ulong* p = ((ulong*)(buffer + offset));
			offset += 8;
			return new OperationHeader(p);
		}

		return GetOperationHeaderSplit();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private OperationHeader GetOperationHeaderSplit()
	{
		byte* p1 = buffer + offset;
		int size1 = bufferLength - offset;
		offset += size1;
		EnsureSpace(8 - size1);
		byte* p2 = buffer + offset;
		offset += 8 - size1;
		return new OperationHeader(p1, size1, p2);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long ReadLong()
	{
		if (offset + 8 <= bufferLength)
		{
			long v = *((long*)(buffer + offset));
			offset += 8;
			return v;
		}

		long b;
		ReadBytes((byte*)&b, 8);
		return b;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public float ReadFloat()
	{
		if (offset + 4 <= bufferLength)
		{
			EnsureSpace(4);
			float v = *((float*)(buffer + offset));
			offset += 4;
			return v;
		}

		float b;
		ReadBytes((byte*)&b, 4);
		return b;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public double ReadDouble()
	{
		if (offset + 8 <= bufferLength)
		{
			EnsureSpace(8);
			double v = *((double*)(buffer + offset));
			offset += 8;
			return v;
		}

		double b;
		ReadBytes((byte*)&b, 8);
		return b;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool ReadBool()
	{
		if (offset + 1 <= bufferLength)
		{
			byte v = *((byte*)(buffer + offset));
			offset += 1;
			return v != 0;
		}

		bool b;
		ReadBytes((byte*)&b, 1);
		return b;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public DateTime ReadDateTime()
	{
		if (offset + 8 <= bufferLength)
		{
			long v = *((long*)(buffer + offset));
			offset += 8;
			return DateTime.FromBinary(v);
		}

		long b;
		ReadBytes((byte*)&b, 8);
		return DateTime.FromBinary(b);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
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
		if (opType < OperationType.Insert || opType > OperationType.DropClass)
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

		int operationCount = ReadByte();
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

	[MethodImpl(MethodImplOptions.NoInlining)]
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
		long l;
		ReadSimpleValue((byte*)&l, size);
	}

	private void TakeNextLog()
	{
		currLog++;
		LogChangeset ch = logChangesets[currLog];
		TTTrace.Write(ch.LogIndex);

		bufferCount = ch.BufferCount;
		head = ch.Buffers;

		buffer = ch.Buffers;
		bufferLength = ((ChangesetBufferHeader*)buffer)->size;
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

	int size1;
	ulong* p1;
	byte* p2;

	ulong value;

	static OperationHeader()
	{
		ushort v = 0x0001;
		byte* b = (byte*)&v;
		if (b[0] == 0)
			throw new NotSupportedException("Big endian systems are not supported.");
	}

	public OperationHeader(ulong* p)
	{
		this.p1 = p;
		this.size1 = sizeof(ulong);
		this.p2 = null;
		value = *p1;
	}

	public OperationHeader(byte* p1, int size1, byte* p2)
	{
		this.p1 = (ulong*)p1;
		this.size1 = size1;
		this.p2 = p2;

		ulong v;
		Utils.CopyMemory(p1, (byte*)&v, size1);
		Utils.CopyMemory(p2, (byte*)&v + size1, sizeof(ulong) - size1);
		value = v;
	}

	public bool IsSplit => p2 != null;
	public byte* NotLastInTransactionPointer => (byte*)p1;
	public ulong PreviousVersion => value >> 1;
	public bool IsLastInTransaction => (value & 0x01) == 0;
	public bool IsFirstInTransaction => PreviousVersion != 0;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WritePreviousVersion(ulong v)
	{
		value = v = v << 1;
		if (p2 == null)
		{
			*p1 = value;
		}
		else
		{
			Utils.CopyMemory((byte*)&v, (byte*)p1, size1);
			Utils.CopyMemory((byte*)&v + size1, p2, sizeof(ulong) - size1);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void SetNotLastInTransaction(byte* p)
	{
		*p |= 0x01;
	}
}


internal unsafe struct ReaderPosition
{
	public int Offset { get; set; }
	public int BufferLength { get; set; }
	public byte* Buffer { get; set; }
}
