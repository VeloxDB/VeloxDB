using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using static System.Math;

namespace VeloxDB.Common;

internal unsafe sealed class SegmentBinaryWriter : IDisposable
{
	readonly int segmentSize;

	List<IntPtr> segments;
	int offset;
	byte* segment;

	public SegmentBinaryWriter(int segmentSize)
	{
		this.segmentSize = segmentSize;
		segments = new List<IntPtr>();
		offset = segmentSize;
	}

	public List<IntPtr> Segments => segments;
	public long Size => (long)(segments.Count - 1) * segmentSize + offset;

	public long SizeFrom(Position p) => (long)(segments.Count - 1 - p.SegmentIndex) * segmentSize + offset - p.Offset - sizeof(long);

	[MethodImpl(MethodImplOptions.NoInlining)]
	public Position ReserveSpace(int length)
	{
		AllocSegmentIfNeeded();
		Position p = new Position(offset, segments.Count - 1);

		while (length > 0)
		{
			AllocSegmentIfNeeded();
			int c = (int)Min(length, segmentSize - offset);
			length -= c;
			offset += c;
		}

		return p;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Write(byte v)
	{
		AllocSegmentIfNeeded();
		*(segment + offset) = v;
		offset++;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Write(int v)
	{
		if (offset <= segmentSize - 4)
		{
			*((int*)(segment + offset)) = v;
			offset += 4;
			return;
		}

		Write((byte*)&v, 4);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Write(long v)
	{
		if (offset <= segmentSize - 8)
		{
			*((long*)(segment + offset)) = v;
			offset += 8;
			return;
		}

		Write((byte*)&v, 8);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	public void Write(byte* buffer, int length)
	{
		while (length > 0)
		{
			AllocSegmentIfNeeded();
			int c = (int)Min(length, segmentSize - offset);
			Utils.CopyMemory(buffer, segment + offset, c);
			buffer += c;
			length -= c;
			offset += c;
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	public void Write(Position pos, byte* buffer, int length)
	{
		int currOffset = pos.Offset;
		int currSegmentIndex = pos.SegmentIndex;
		while (length > 0)
		{
			int c = (int)Min(length, segmentSize - currOffset);
			Utils.CopyMemory(buffer, (byte*)segments[currSegmentIndex] + currOffset, c);
			buffer += c;
			length -= c;
			currSegmentIndex++;
			currOffset = 0;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Write(string s)
	{
		if (s == null)
		{
			Write((byte)0);
			return;
		}

		Write((byte)1);

		Write(s.Length);
		fixed (char* pc = s)
		{
			Write((byte*)pc, s.Length * 2);
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void AllocSegmentIfNeeded()
	{
		if (offset < segmentSize)
			return;

		segment = (byte*)NativeAllocator.Allocate(segmentSize);
		segments.Add((IntPtr)segment);
		offset = 0;
	}

	public long WriteToFile(NativeFile file)
	{
		if (segments.Count == 0)
			return 0;

		long s = 0;
		for (int i = 0; i < segments.Count - 1; i++)
		{
			s += segmentSize;
			file.Write(segments[i], segmentSize);
		}

		s += offset;
		file.Write(segments[segments.Count - 1], offset);

		return s;
	}

	public void Dispose()
	{
		for (int i = 0; i < segments.Count; i++)
		{
			NativeAllocator.Free(segments[i]);
		}
	}

	public struct Position
	{
		public int Offset { get; private set; }
		public int SegmentIndex { get; private set; }

		public Position(int offset, int segmentIndex)
		{
			this.Offset = offset;
			this.SegmentIndex = segmentIndex;
		}
	}
}
