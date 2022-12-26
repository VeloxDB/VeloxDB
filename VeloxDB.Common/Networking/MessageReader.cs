using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using VeloxDB.Common;

namespace VeloxDB.Networking;

internal unsafe delegate MessageChunk MessageReaderCallback(MessageChunk chunk);

internal unsafe sealed class MessageReader
{
	// These fields are public because they are used in IL code generation (inlined)
	public int size;
	public int offset;
	public byte* buffer;

	MessageReaderCallback callback;
	MessageChunk chunk;

	bool isReleased;

	internal MessageReader()
	{
	}

	internal void Init(MessageChunk chunk, MessageReaderCallback callback)
	{
		this.chunk = chunk;
		this.callback = callback;

		chunk.ReadHeader();

		size = chunk.ChunkSize;
		offset = chunk.HeaderSize;
		buffer = chunk.PBuffer;
	}

	public bool IsEndReached => offset == size;
	public MessageChunk Chunk => chunk;
	public bool IsReleased { get => isReleased; set => isReleased = value; }

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public sbyte ReadSByte()
	{
		if (offset + sizeof(sbyte) > size)
			ProvideBuffer();

		sbyte v = *((sbyte*)(buffer + offset));
		offset += sizeof(sbyte);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte ReadByte()
	{
		if (offset + sizeof(byte) > size)
			ProvideBuffer();

		byte v = *((byte*)(buffer + offset));
		offset += sizeof(byte);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public short ReadShort()
	{
		if (offset + sizeof(short) > size)
			ProvideBuffer();

		short v = *((short*)(buffer + offset));
		offset += sizeof(short);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ushort ReadUShort()
	{
		if (offset + sizeof(ushort) > size)
			ProvideBuffer();

		ushort v = *((ushort*)(buffer + offset));
		offset += sizeof(ushort);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int ReadInt()
	{
		if (offset + sizeof(int) > size)
			ProvideBuffer();

		int v = *((int*)(buffer + offset));
		offset += sizeof(int);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public uint ReadUInt()
	{
		if (offset + sizeof(uint) > size)
			ProvideBuffer();

		uint v = *((uint*)(buffer + offset));
		offset += sizeof(uint);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long ReadLong()
	{
		if (offset + sizeof(long) > size)
			ProvideBuffer();

		long v = *((long*)(buffer + offset));
		offset += sizeof(long);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ulong ReadULong()
	{
		if (offset + sizeof(ulong) > size)
			ProvideBuffer();

		ulong v = *((ulong*)(buffer + offset));
		offset += sizeof(ulong);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public float ReadFloat()
	{
		if (offset + sizeof(float) > size)
			ProvideBuffer();

		float v = *((float*)(buffer + offset));
		offset += sizeof(float);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public double ReadDouble()
	{
		if (offset + sizeof(double) > size)
			ProvideBuffer();

		double v = *((double*)(buffer + offset));
		offset += sizeof(double);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool ReadBool()
	{
		if (offset + sizeof(byte) > size)
			ProvideBuffer();

		bool v = *((byte*)(buffer + offset)) == 1;
		offset += sizeof(byte);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public decimal ReadDecimal()
	{
		if (offset + sizeof(decimal) > size)
			ProvideBuffer();

		decimal v = *((decimal*)(buffer + offset));
		offset += sizeof(decimal);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public string ReadString()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(char);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(char);
				string v = new string((char*)(buffer + offset + 1), 0, length);
				offset += size + 1;
				return v;
			}
		}

		return ReadStringInternal();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private string ReadStringInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		string v = new string(' ', length);
		fixed (char* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(char));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public DateTime ReadDateTime()
	{
		if (offset + sizeof(long) > size)
			ProvideBuffer();

		long v = *((long*)(buffer + offset));
		offset += sizeof(long);
		return DateTime.FromBinary(v);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public TimeSpan ReadTimeSpan()
	{
		if (offset + sizeof(long) > size)
			ProvideBuffer();

		long v = *((long*)(buffer + offset));
		offset += sizeof(long);
		return new TimeSpan(v);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public Guid ReadGuid()
	{
		byte[] b = ReadByteArray();
		return new Guid(b);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public sbyte[] ReadSByteArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(sbyte);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(sbyte);
				sbyte[] v = new sbyte[length];
				fixed (sbyte* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadSByteArrayInternal();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public sbyte[] ReadSByteArrayFact(Func<int, sbyte[]> fact)
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(sbyte);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(sbyte);
				sbyte[] v = fact(length);
				fixed (sbyte* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadSByteArrayInternalFact(fact);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private sbyte[] ReadSByteArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		sbyte[] v = new sbyte[length];
		fixed (sbyte* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(sbyte));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private sbyte[] ReadSByteArrayInternalFact(Func<int, sbyte[]> fact)
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		sbyte[] v = fact(length);
		fixed (sbyte* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(sbyte));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte[] ReadByteArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(byte);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(byte);
				byte[] v = new byte[length];
				fixed (byte* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadByteArrayInternal();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte[] ReadByteArrayFact(Func<int, byte[]> fact)
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(byte);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(byte);
				byte[] v = fact(length);
				fixed (byte* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadByteArrayInternalFact(fact);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private byte[] ReadByteArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		byte[] v = new byte[length];
		fixed (byte* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(byte));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private byte[] ReadByteArrayInternalFact(Func<int, byte[]> fact)
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		byte[] v = fact(length);
		fixed (byte* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(byte));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public short[] ReadShortArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(short);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(short);
				short[] v = new short[length];
				fixed (short* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadShortArrayInternal();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public short[] ReadShortArrayFact(Func<int, short[]> fact)
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(short);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(short);
				short[] v = fact(length);
				fixed (short* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadShortArrayInternalFact(fact);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private short[] ReadShortArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		short[] v = new short[length];
		fixed (short* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(short));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private short[] ReadShortArrayInternalFact(Func<int, short[]> fact)
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		short[] v = fact(length);
		fixed (short* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(short));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ushort[] ReadUShortArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(ushort);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(ushort);
				ushort[] v = new ushort[length];
				fixed (ushort* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadUShortArrayInternal();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ushort[] ReadUShortArrayFact(Func<int, ushort[]> fact)
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(ushort);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(ushort);
				ushort[] v = fact(length);
				fixed (ushort* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadUShortArrayInternalFact(fact);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private ushort[] ReadUShortArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		ushort[] v = new ushort[length];
		fixed (ushort* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(ushort));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private ushort[] ReadUShortArrayInternalFact(Func<int, ushort[]> fact)
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		ushort[] v = fact(length);
		fixed (ushort* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(ushort));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int[] ReadIntArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(int);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(int);
				int[] v = new int[length];
				fixed (int* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadIntArrayInternal();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int[] ReadIntArrayFact(Func<int, int[]> fact)
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(int);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(int);
				int[] v = fact(length);
				fixed (int* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadIntArrayInternalFact(fact);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private int[] ReadIntArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		int[] v = new int[length];
		fixed (int* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(int));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private int[] ReadIntArrayInternalFact(Func<int, int[]> fact)
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		int[] v = fact(length);
		fixed (int* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(int));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public uint[] ReadUIntArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(uint);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(uint);
				uint[] v = new uint[length];
				fixed (uint* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadUIntArrayInternal();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public uint[] ReadUIntArrayFact(Func<int, uint[]> fact)
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(uint);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(uint);
				uint[] v = fact(length);
				fixed (uint* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadUIntArrayInternalFact(fact);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private uint[] ReadUIntArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		uint[] v = new uint[length];
		fixed (uint* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(uint));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private uint[] ReadUIntArrayInternalFact(Func<int, uint[]> fact)
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		uint[] v = fact(length);
		fixed (uint* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(uint));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long[] ReadLongArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(long);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(long);
				long[] v = new long[length];
				fixed (long* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadLongArrayInternal();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long[] ReadLongArrayFact(Func<int, long[]> fact)
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(long);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(long);
				long[] v = fact(length);
				fixed (long* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadLongArrayInternalFact(fact);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private long[] ReadLongArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		long[] v = new long[length];
		fixed (long* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(long));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private long[] ReadLongArrayInternalFact(Func<int, long[]> fact)
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		long[] v = fact(length);
		fixed (long* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(long));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ulong[] ReadULongArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(ulong);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(ulong);
				ulong[] v = new ulong[length];
				fixed (ulong* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadULongArrayInternal();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ulong[] ReadULongArrayFact(Func<int, ulong[]> fact)
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(ulong);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(ulong);
				ulong[] v = fact(length);
				fixed (ulong* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadULongArrayInternalFact(fact);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private ulong[] ReadULongArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		ulong[] v = new ulong[length];
		fixed (ulong* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(ulong));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private ulong[] ReadULongArrayInternalFact(Func<int, ulong[]> fact)
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		ulong[] v = fact(length);
		fixed (ulong* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(ulong));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public float[] ReadFloatArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(float);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(float);
				float[] v = new float[length];
				fixed (float* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadFloatArrayInternal();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private float[] ReadFloatArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		float[] v = new float[length];
		fixed (float* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(float));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public double[] ReadDoubleArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(double);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(double);
				double[] v = new double[length];
				fixed (double* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadDoubleArrayInternal();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private double[] ReadDoubleArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		double[] v = new double[length];
		fixed (double* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(double));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool[] ReadBoolArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(bool);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(bool);
				bool[] v = new bool[length];
				fixed (bool* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadBoolArrayInternal();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool[] ReadBoolArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		bool[] v = new bool[length];
		fixed (bool* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(bool));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public decimal[] ReadDecimalArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * sizeof(decimal);
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * sizeof(decimal);
				decimal[] v = new decimal[length];
				fixed (decimal* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadDecimalArrayInternal();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private decimal[] ReadDecimalArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		decimal[] v = new decimal[length];
		fixed (decimal* p = v)
		{
			ReadBuffer((byte*)p, length * sizeof(decimal));
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public string[] ReadStringArray()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		string[] v = new string[count];
		for (int i = 0; i < count; i++)
		{
			v[i] = ReadString();
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public DateTime[] ReadDateTimeArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * 8;
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * 8;
				DateTime[] v = new DateTime[length];
				fixed (DateTime* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadDateTimeArrayInternal();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private DateTime[] ReadDateTimeArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		DateTime[] v = new DateTime[length];
		fixed (DateTime* p = v)
		{
			ReadBuffer((byte*)p, length * 8);
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public TimeSpan[] ReadTimeSpanArray()
	{
		const int s = 1 + MessageWriter.SmallArrayLength * 8;
		if (offset + s <= size)
		{
			byte b = *(buffer + offset);
			if (b >= 2)
			{
				int length = b - 2;
				int size = length * 8;
				TimeSpan[] v = new TimeSpan[length];
				fixed (TimeSpan* p = v)
				{
					Utils.CopyMemory(buffer + offset + 1, (byte*)p, size);
				}

				offset += size + 1;
				return v;
			}
		}

		return ReadTimeSpanArrayInternal();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private TimeSpan[] ReadTimeSpanArrayInternal()
	{
		byte b = ReadByte();
		if (b == 0)
			return null;

		int length;
		if (b == 1)
			length = ReadInt();
		else
			length = b - 2;

		TimeSpan[] v = new TimeSpan[length];
		fixed (TimeSpan* p = v)
		{
			ReadBuffer((byte*)p, length * 8);
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public Guid[] ReadGuidArray()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		Guid[] v = new Guid[count];
		for (int i = 0; i < count; i++)
		{
			v[i] = ReadGuid();
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<sbyte> ReadSByteList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<sbyte> v = new List<sbyte>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadSByte());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<sbyte> ReadSByteListFact(Func<int, List<sbyte>> fact)
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<sbyte> v = fact(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadSByte());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<byte> ReadByteList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<byte> v = new List<byte>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadByte());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<byte> ReadByteListFact(Func<int, List<byte>> fact)
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<byte> v = fact(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadByte());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<short> ReadShortList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<short> v = new List<short>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadShort());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<short> ReadShortListFact(Func<int, List<short>> fact)
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<short> v = fact(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadShort());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<ushort> ReadUShortList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<ushort> v = new List<ushort>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadUShort());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<ushort> ReadUShortListFact(Func<int, List<ushort>> fact)
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<ushort> v = fact(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadUShort());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<int> ReadIntList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<int> v = new List<int>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadInt());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<int> ReadIntListFact(Func<int, List<int>> fact)
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<int> v = fact(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadInt());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<uint> ReadUIntList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<uint> v = new List<uint>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadUInt());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<uint> ReadUIntListFact(Func<int, List<uint>> fact)
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<uint> v = fact(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadUInt());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<long> ReadLongList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<long> v = new List<long>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadLong());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<long> ReadLongListFact(Func<int, List<long>> fact)
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<long> v = fact(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadLong());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<ulong> ReadULongList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<ulong> v = new List<ulong>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadULong());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<ulong> ReadULongListFact(Func<int, List<ulong>> fact)
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<ulong> v = fact(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadULong());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<float> ReadFloatList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<float> v = new List<float>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadFloat());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<double> ReadDoubleList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<double> v = new List<double>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadDouble());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<bool> ReadBoolList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<bool> v = new List<bool>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadBool());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<decimal> ReadDecimalList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<decimal> v = new List<decimal>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadDecimal());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<string> ReadStringList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<string> v = new List<string>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadString());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<DateTime> ReadDateTimeList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<DateTime> v = new List<DateTime>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadDateTime());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<TimeSpan> ReadTimeSpanList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<TimeSpan> v = new List<TimeSpan>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadTimeSpan());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public List<Guid> ReadGuidList()
	{
		if (ReadByte() == 0)
			return null;

		int count = ReadInt();
		List<Guid> v = new List<Guid>(count);
		for (int i = 0; i < count; i++)
		{
			v.Add(ReadGuid());
		}

		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Reset()
	{
		buffer = null;
		callback = null;
		chunk = null;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	public void ReadBuffer(byte* bp, int size)
	{
		int leftToRead = size;
		while (leftToRead > 0)
		{
			int toRead = Math.Min(leftToRead, this.size - offset);
			Utils.CopyMemory(buffer + offset, bp + size - leftToRead, toRead);
			offset += toRead;

			leftToRead -= toRead;
			if (leftToRead > 0)
				ProvideBuffer();
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void ProvideBuffer()
	{
		if (callback == null)
			throw new CorruptMessageException();

		MessageChunk temp = chunk;
		chunk = null;			// Relinquish the chunk, callback method will dispose of it

		chunk = callback(temp);
		chunk.ReadHeader();
		size = chunk.ChunkSize;
		offset = chunk.HeaderSize;
		buffer = chunk.PBuffer;
	}
}
