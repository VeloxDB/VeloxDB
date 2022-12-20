using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using VeloxDB.Common;

namespace VeloxDB.Networking;

internal unsafe delegate bool ProcessChunkDelegate(int size, ref object state, ref byte* buffer, ref int capacity);

internal unsafe sealed class MessageWriter
{
	internal const short HeaderVersion = 1;
	internal const int Header1Size = sizeof(int) + sizeof(int) + sizeof(long) + sizeof(byte);

	internal const int SmallArrayLength = 253;

	// These fields are public because they are used in IL code generation (inlined)
	public int capacity;
	public int offset;
	public byte* buffer;

	object state;
	ulong messageId;
	ProcessChunkDelegate processor;
	bool isFirst;

#if TEST_BUILD
	public static bool InvalidLastChunk { get; set; }
#endif

	internal MessageWriter()
	{
	}

	internal void Init(object state, byte* buffer, int capacity, ProcessChunkDelegate processor, ulong messageId)
	{
		this.buffer = buffer;
		this.state = state;
		this.capacity = capacity;
		this.processor = processor;
		this.messageId = messageId;
		this.isFirst = true;
		this.offset = 0;

		this.offset += sizeof(int); // Chunk size
		WriteInt(HeaderVersion);
		WriteULong(messageId);      // Message id
		this.offset++;              // Chunk type
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteSByte(sbyte v)
	{
		if (offset + sizeof(sbyte) > capacity)
			EmptyBuffer();

		*((sbyte*)(buffer + offset)) = v;
		offset += sizeof(sbyte);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteByte(byte v)
	{
		if (offset + sizeof(byte) > capacity)
			EmptyBuffer();

		*((byte*)(buffer + offset)) = v;
		offset += sizeof(byte);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteShort(short v)
	{
		if (offset + sizeof(short) > capacity)
			EmptyBuffer();

		*((short*)(buffer + offset)) = v;
		offset += sizeof(short);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteUShort(ushort v)
	{
		if (offset + sizeof(ushort) > capacity)
			EmptyBuffer();

		*((ushort*)(buffer + offset)) = v;
		offset += sizeof(ushort);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteInt(int v)
	{
		if (offset + sizeof(int) > capacity)
			EmptyBuffer();

		*((int*)(buffer + offset)) = v;
		offset += sizeof(int);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteUInt(uint v)
	{
		if (offset + sizeof(uint) > capacity)
			EmptyBuffer();

		*((uint*)(buffer + offset)) = v;
		offset += sizeof(uint);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteLong(long v)
	{
		if (offset + sizeof(long) > capacity)
			EmptyBuffer();

		*((long*)(buffer + offset)) = v;
		offset += sizeof(long);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteULong(ulong v)
	{
		if (offset + sizeof(ulong) > capacity)
			EmptyBuffer();

		*((ulong*)(buffer + offset)) = v;
		offset += sizeof(ulong);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteFloat(float v)
	{
		if (offset + sizeof(float) > capacity)
			EmptyBuffer();

		*((float*)(buffer + offset)) = v;
		offset += sizeof(float);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDouble(double v)
	{
		if (offset + sizeof(double) > capacity)
			EmptyBuffer();

		*((double*)(buffer + offset)) = v;
		offset += sizeof(double);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDecimal(decimal v)
	{
		if (offset + sizeof(decimal) > capacity)
			EmptyBuffer();

		*((decimal*)(buffer + offset)) = v;
		offset += sizeof(decimal);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteBool(bool v)
	{
		if (offset + sizeof(byte) > capacity)
			EmptyBuffer();

		*((byte*)(buffer + offset)) = v ? (byte)1 : (byte)0;
		offset += sizeof(byte);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteString(string v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(char) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (char* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteStringInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteStringInternal(string v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (char* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(char));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDateTime(DateTime v)
	{
		if (offset + sizeof(long) > capacity)
			EmptyBuffer();

		*((long*)(buffer + offset)) = v.ToBinary();
		offset += sizeof(long);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteTimeSpan(TimeSpan v)
	{
		if (offset + sizeof(long) > capacity)
			EmptyBuffer();

		*((long*)(buffer + offset)) = v.Ticks;
		offset += sizeof(long);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteGuid(Guid v)
	{
		byte[] b = v.ToByteArray();
		WriteByteArray(b);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteSByteArray(sbyte[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(sbyte) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (sbyte* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteSByteArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteSByteArrayInternal(sbyte[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (sbyte* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(sbyte));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteByteArray(byte[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(byte) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (byte* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteByteArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteByteArrayInternal(byte[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (byte* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(byte));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteShortArray(short[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(short) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (short* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteShortArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteShortArrayInternal(short[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (short* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(short));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteUShortArray(ushort[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(ushort) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (ushort* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteUShortArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteUShortArrayInternal(ushort[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (ushort* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(ushort));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteIntArray(int[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(int) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (int* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteIntArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteIntArrayInternal(int[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (int* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(int));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteUIntArray(uint[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(uint) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (uint* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteUIntArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteUIntArrayInternal(uint[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (uint* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(uint));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteLongArray(long[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(long) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (long* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteLongArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteLongArrayInternal(long[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (long* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(long));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteULongArray(ulong[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(ulong) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (ulong* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteULongArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteULongArrayInternal(ulong[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (ulong* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(ulong));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteFloatArray(float[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(float) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (float* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteFloatArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteFloatArrayInternal(float[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (float* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(float));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDoubleArray(double[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(double) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (double* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteDoubleArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteDoubleArrayInternal(double[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (double* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(double));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteBoolArray(bool[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(bool) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (bool* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteBoolArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteBoolArrayInternal(bool[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (bool* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(bool));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDecimalArray(decimal[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(decimal) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (decimal* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteDecimalArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteDecimalArrayInternal(decimal[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (decimal* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(decimal));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteStringArray(string[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);
		for (int i = 0; i < v.Length; i++)
		{
			WriteString(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDateTimeArray(DateTime[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(DateTime) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (DateTime* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteDateTimeArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteDateTimeArrayInternal(DateTime[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);

		fixed (DateTime* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(DateTime));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteTimeSpanArray(TimeSpan[] v)
	{
		if (v != null)
		{
			long size = v.Length * sizeof(TimeSpan) + 1;
			if (offset + size <= capacity && v.Length <= SmallArrayLength)
			{
				*(buffer + offset) = (byte)(v.Length + 2);
				fixed (TimeSpan* p = v)
				{
					Utils.CopyMemory((byte*)p, buffer + offset + 1, size - 1);
				}

				offset += (int)size;
				return;
			}
		}

		WriteTimeSpanArrayInternal(v);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WriteTimeSpanArrayInternal(TimeSpan[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteLong(v.LongLength);

		fixed (TimeSpan* p = v)
		{
			WriteBuffer((byte*)p, v.Length * sizeof(TimeSpan));
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteGuidArray(Guid[] v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Length);
		for (int i = 0; i < v.Length; i++)
		{
			WriteGuid(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteSByteList(List<sbyte> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteSByte(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteByteList(List<byte> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteByte(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteShortList(List<short> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteShort(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteUShortList(List<ushort> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteUShort(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteIntList(List<int> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteInt(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteUIntList(List<uint> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteUInt(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteLongList(List<long> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteLong(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteULongList(List<ulong> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteULong(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteFloatList(List<float> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteFloat(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDoubleList(List<double> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteDouble(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteBoolList(List<bool> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteBool(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDecimalList(List<decimal> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteDecimal(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteStringList(List<string> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteString(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDateTimeList(List<DateTime> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteDateTime(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteTimeSpanList(List<TimeSpan> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteTimeSpan(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteGuidList(List<Guid> v)
	{
		if (v == null)
		{
			WriteByte(0);
			return;
		}

		WriteByte(1);
		WriteInt(v.Count);
		for (int i = 0; i < v.Count; i++)
		{
			WriteGuid(v[i]);
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	public void WriteBuffer(byte* bp, long size)
	{
		long leftToWrite = size;
		while (leftToWrite > 0)
		{
			long toWrite = Math.Min(leftToWrite, capacity - offset);
			Utils.CopyMemory(bp + size - leftToWrite, buffer + offset, toWrite);
			offset += (int)toWrite;

			leftToWrite -= toWrite;
			if (leftToWrite > 0)
				EmptyBuffer();
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Reset()
	{
		buffer = null;
		processor = null;
		state = null;
		isFirst = false;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	public void EmptyBuffer(bool isLast = false)
	{
		ChunkFlags flags = isLast ? ChunkFlags.Last : ChunkFlags.None;
		if (isFirst)
			flags |= ChunkFlags.First;

#if TEST_BUILD
		if (InvalidLastChunk && isLast)
			*(long*)(buffer + sizeof(int) + sizeof(int)) = long.MaxValue;
#endif

		*((int*)buffer) = offset;
		*(buffer + sizeof(int) + sizeof(int) + sizeof(long)) = (byte)flags;

		if (processor(offset, ref state, ref buffer, ref capacity))
		{
			if (!isLast)
			{
				isFirst = false;
				offset = sizeof(int);       // Skip chunk size
				WriteInt(HeaderVersion);
				WriteULong(messageId);      // Message id
				this.offset++;              // Is last chunk
			}
		}
		else
		{
			// else we just got an upgrade of the small first chunk to a large one, nothing to do here
		}
	}
}
