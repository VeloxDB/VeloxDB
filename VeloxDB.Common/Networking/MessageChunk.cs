using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Networking;

[Flags]
internal enum ChunkFlags : byte
{
	None = 0x00,
	First = 0x01,
	Last = 0x02,
	TheOnlyOne = First | Last
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = Size)]
internal struct MessageChunkHeader
{
	internal const short HeaderVersion = 1;
	internal const int Size = sizeof(int) + sizeof(int) + sizeof(ulong) + sizeof(byte);

	public int size;
	public int version;
	public ulong messageId;
	public ChunkFlags flags;
}

internal unsafe sealed class MessageChunk : IDisposable
{
	public const int SmallBufferSize = 1024 * 4;
	public const int LargeBufferSize = 1024 * 64;

	int bufferSize;
	byte* pbuffer;

	MessageWriter writer;
	MessageReader reader;
	ChunkAwaiter awaiter;
	int headerSize;
	int poolIndex;
	int refCount;
	bool disposed;

	public MessageChunk(int bufferSize)
	{
		this.bufferSize = bufferSize;
		this.writer = new MessageWriter();
		this.reader = new MessageReader();
		pbuffer = (byte*)AlignedAllocator.Allocate(bufferSize, false);
	}

	public int BufferSize => bufferSize;
	public byte* PBuffer => pbuffer;
	public MessageWriter Writer => writer;
	public MessageReader Reader => reader;
	public ulong MessageId => ((MessageChunkHeader*)pbuffer)->messageId;
	public int ChunkSize => ((MessageChunkHeader*)pbuffer)->size;
	public int HeaderSize => headerSize;
	public ChunkAwaiter NextChunkAwaiter { get => awaiter; set => awaiter = value; }
	public bool IsFirst => (Flags & ChunkFlags.First) != ChunkFlags.None;
	public bool IsLast => (Flags & ChunkFlags.Last) != ChunkFlags.None;
	public bool IsTheOnlyOne => Flags == ChunkFlags.TheOnlyOne;
	public int PoolIndex { get => poolIndex; set => poolIndex = value; }

	private ChunkFlags Flags => ((MessageChunkHeader*)pbuffer)->flags;

	~MessageChunk()
	{
		throw new CriticalDatabaseException();
	}

	public unsafe void ReadHeader()
	{
		int headerVersion = ((int*)pbuffer)[1];
		if (headerVersion > MessageChunkHeader.HeaderVersion)
			throw new UnsupportedHeaderException();

		ReaderHeader1();
	}

	private void ReaderHeader1()
	{
		headerSize = MessageChunkHeader.Size;
	}

	public void Reset()
	{
		reader.Reset();
		writer.Reset();
		awaiter = null;
		refCount = 0;
	}

	public void SwapReaders(MessageChunk chunk)
	{
		reader.SwapStates(chunk.reader);
		MessageReader temp = reader;
		reader = chunk.reader;
		chunk.reader = temp;
	}

	public void SwapWriters(MessageChunk chunk)
	{
		MessageWriter temp = writer;
		writer = chunk.writer;
		chunk.writer = temp;
	}

	public void CopyContent(MessageChunk chunk)
	{
		Utils.CopyMemory(chunk.PBuffer, PBuffer, chunk.ChunkSize);
		ReadHeader();
	}

	public void SetupAutomaticCleanup(int refCount)
	{
		this.refCount = refCount;
	}

	public void DecRefCount(int amount, MessageChunkPool chunkPool)
	{
		if (Interlocked.Add(ref refCount, -amount) == 0)
			chunkPool.Put(this);
	}

	public void Dispose()
	{
		GC.SuppressFinalize(this);
		CleanUp(true);
	}

	private void CleanUp(bool disposing)
	{
		if (!disposed)
		{
			AlignedAllocator.Free((IntPtr)pbuffer);
			disposed = true;
		}
	}
}
