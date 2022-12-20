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

internal unsafe sealed class MessageChunk : IDisposable
{
	public const int SmallBufferSize = 1024 * 4;
	public const int LargeBufferSize = 1024 * 64;

	int bufferSize;
	byte* pbuffer;

	MessageWriter writer;
	MessageReader reader;
	ulong messageId;
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
	public ulong MessageId => messageId;
	public int ChunkSize => *((int*)pbuffer);
	public int HeaderSize => headerSize;
	public ChunkAwaiter Awaiter { get => awaiter; set => awaiter = value; }
	public bool IsFirst => (*PFlags & ChunkFlags.First) != ChunkFlags.None;
	public bool IsLast => (*PFlags & ChunkFlags.Last) != ChunkFlags.None;
	public bool IsTheOnlyOne => *PFlags == ChunkFlags.TheOnlyOne;
	public int PoolIndex { get => poolIndex; set => poolIndex = value; }

	private ChunkFlags* PFlags => (ChunkFlags*)(&pbuffer[sizeof(int) + sizeof(int) + sizeof(long)]);

	~MessageChunk()
	{
		CleanUp(false);
	}

	public unsafe void ReadHeader()
	{
		int headerVersion = ((int*)pbuffer)[1];
		if (headerVersion > MessageWriter.HeaderVersion)
			throw new UnsupportedHeaderException();

		ReaderHeader1();
	}

	private void ReaderHeader1()
	{
		messageId = *((ulong*)(pbuffer + sizeof(int) + sizeof(int)));
		headerSize = MessageWriter.Header1Size;
	}

	public void Reset()
	{
		reader.Reset();
		writer.Reset();
		awaiter = null;
		messageId = 0;
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
		CleanUp(true);
		GC.SuppressFinalize(this);
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
