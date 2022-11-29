using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Velox.Common;

internal unsafe sealed class SegmentBinaryReader
{
	long size;
	byte* originalBuffer;
	byte* buffer;

	public SegmentBinaryReader(byte* buffer, long size)
	{
		this.originalBuffer = buffer;
		this.buffer = buffer;
		this.size = size;
	}

	public long CurrOffset => (long)(buffer - originalBuffer);

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte ReadByte()
	{
		Checker.AssertTrue((long)(buffer - originalBuffer) + sizeof(byte) <= size);

		byte v = *buffer;
		buffer++;
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public short ReadShort()
	{
		Checker.AssertTrue((long)(buffer - originalBuffer) + sizeof(short) <= size);

		short v = *((short*)buffer);
		buffer += sizeof(short);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int ReadInt()
	{
		Checker.AssertTrue((long)(buffer - originalBuffer) + sizeof(int) <= size);

		int v = *((int*)buffer);
		buffer += sizeof(int);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long ReadLong()
	{
		Checker.AssertTrue((long)(buffer - originalBuffer) + sizeof(long) <= size);

		long v = *((long*)buffer);
		buffer += sizeof(long);
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* SkipBytes(int size)
	{
		Checker.AssertTrue((long)(buffer - originalBuffer) + size <= this.size);

		byte* v = buffer;
		buffer += size;
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ReadBytes(byte* dst, long size)
	{
		Checker.AssertTrue((long)(buffer - originalBuffer) + size <= this.size);

		Utils.CopyMemory(buffer, dst, size);
		buffer += size;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public string ReadString()
	{
		if (ReadByte() == 0)
			return null;

		int length = ReadInt();
		Checker.AssertTrue((long)(buffer - originalBuffer) + length * 2 <= this.size);

		string s = new string(' ', length);
		fixed (char* cp = s)
		{
			Utils.CopyMemory(buffer, (byte*)cp, length * 2);
			buffer += length * 2;
		}

		return s;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SkipString()
	{
		if (ReadByte() == 0)
			return;

		int length = ReadInt();
		Checker.AssertTrue((long)(buffer - originalBuffer) + length * 2 <= this.size);

		buffer += length * 2;
	}

	[Conditional("DEBUG")]
	public void ValidateFinishedReading()
	{
		Checker.AssertTrue((long)(buffer - originalBuffer) == size);
	}
}
