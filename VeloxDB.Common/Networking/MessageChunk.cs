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
	internal const int Size = sizeof(int) + sizeof(int) + sizeof(ulong) + sizeof(int) + sizeof(byte);

	public int size;
	public int version;
	public ulong messageId;
	public int chunkNum;
	public ChunkFlags flags;

	public bool IsFirst => (flags & ChunkFlags.First) != ChunkFlags.None;
	public bool IsLast => (flags & ChunkFlags.Last) != ChunkFlags.None;
	public bool IsTheOnlyOne => flags == ChunkFlags.TheOnlyOne;
}

internal unsafe sealed partial class MessageChunk : IDisposable
{
	public const int SmallBufferSize = 1024 * 4;
	public const int LargeBufferSize = 1024 * 64;

	int bufferSize;
	byte* pbuffer;

	int headerSize;
	int poolIndex;
	int refCount;
	bool isInPool;
	bool disposed;

	public MessageChunk(int bufferSize)
	{
		TrackCreationStack();

		this.bufferSize = bufferSize;
		pbuffer = (byte*)AlignedAllocator.Allocate(bufferSize, false);
	}

	public int BufferSize => bufferSize;
	public byte* PBuffer => pbuffer;
	public ulong MessageId => ((MessageChunkHeader*)pbuffer)->messageId;
	public int ChunkSize => ((MessageChunkHeader*)pbuffer)->size;
	public int ChunkNum => ((MessageChunkHeader*)pbuffer)->chunkNum;
	public int HeaderSize => headerSize;
	public bool IsFirst => ((MessageChunkHeader*)pbuffer)->IsFirst;
	public bool IsLast => ((MessageChunkHeader*)pbuffer)->IsLast;
	public bool IsTheOnlyOne => ((MessageChunkHeader*)pbuffer)->IsTheOnlyOne;
	public int PoolIndex { get => poolIndex; set => poolIndex = value; }
	public bool IsInPool { get => isInPool; set => isInPool = value; }
	public MessageChunkHeader* Header => (MessageChunkHeader*)pbuffer;

	~MessageChunk()
	{
#if HUNT_CHG_LEAKS
		throw new CriticalDatabaseException();
#else
		CleanUp(false);
#endif
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
		refCount = 0;
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
