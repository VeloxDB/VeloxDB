 using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using Velox.Common;

namespace Velox.Networking;

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

	SocketAsyncEventArgs args;
	int bufferSize;
	byte[] buffer;
	byte* pbuffer;

	MessageWriter writer;
	MessageReader reader;
	long messageId;
	int chunkSize;
	ChunkAwaiter awaiter;
	int headerSize;
	GCHandle pinnedBuffer;
	bool disposed;

	public MessageChunk(EventHandler<SocketAsyncEventArgs> completedHandler, int bufferSize)
	{
		this.bufferSize = bufferSize;

		if (bufferSize == SmallBufferSize)
		{
			this.pbuffer = (byte*)NativeAllocator.Allocate(SmallBufferSize);
		}
		else
		{
			this.buffer = GC.AllocateArray<byte>(bufferSize, true);
			this.args = new SocketAsyncEventArgs();
			this.Args.UserToken = this;
			this.Args.SetBuffer(this.buffer, 0, bufferSize);
			if (completedHandler != null)
				this.Args.Completed += completedHandler;

			pinnedBuffer = GCHandle.Alloc(this.buffer, GCHandleType.Pinned);
			this.pbuffer = (byte*)pinnedBuffer.AddrOfPinnedObject();
		}

		this.writer = new MessageWriter();
		this.reader = new MessageReader();
	}

	public int BufferSize => bufferSize;
	public SocketAsyncEventArgs Args => args;
	public byte* PBuffer => pbuffer;
	public MessageWriter Writer => writer;
	public MessageReader Reader => reader;
	public long MessageId => messageId;
	public int ChunkSize => chunkSize;
	public int HeaderSize => headerSize;
	public ChunkAwaiter Awaiter { get => awaiter; set => awaiter = value; }
	public bool IsFirst => (*PFlags & ChunkFlags.First) != ChunkFlags.None;
	public bool IsLast => (*PFlags & ChunkFlags.Last) != ChunkFlags.None;
	public bool IsTheOnlyOne => *PFlags == ChunkFlags.TheOnlyOne;

	private ChunkFlags* PFlags => (ChunkFlags*)(&pbuffer[sizeof(int) + sizeof(int) + sizeof(long)]);

	~MessageChunk()
	{
		CleanUp(false);
	}

	public unsafe void UpdateSize()
	{
		chunkSize = *((int*)pbuffer);
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
		messageId = ((long*)(pbuffer + sizeof(int) + sizeof(int)))[0];
		headerSize = MessageWriter.Header1Size;
	}

	internal void Init()
	{
	}

	public void Reset()
	{
		reader.Reset();
		writer.Reset();
		args?.SetBuffer(0, bufferSize);
		awaiter = null;
		messageId = 0;
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

	public void ReturnToPool(ItemPool<MessageChunk> smallChunkPool, ItemPool<MessageChunk> largeChunkPool)
	{
		if (BufferSize == SmallBufferSize)
			smallChunkPool.Put(this);
		else
			largeChunkPool.Put(this);
	}

	public void CopyContent(MessageChunk chunk)
	{
		Utils.CopyMemory(chunk.PBuffer, PBuffer, chunk.ChunkSize);
		UpdateSize();
		ReadHeader();
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
			if (disposing && args != null)
				args.Dispose();

			if (pinnedBuffer.IsAllocated)
				pinnedBuffer.Free();
			else
				NativeAllocator.Free((IntPtr)pbuffer);

			disposed = true;
		}
	}
}

internal sealed class MessageChunkFactory : IItemFactory<MessageChunk>
{
	int bufferSize;
	EventHandler<SocketAsyncEventArgs> completedHandler;

	public MessageChunkFactory(EventHandler<SocketAsyncEventArgs> completedHandler, int bufferSize)
	{
		this.completedHandler = completedHandler;
		this.bufferSize = bufferSize;
	}

	public void Init(MessageChunk item)
	{
		item.Init();
	}

	public MessageChunk Create()
	{
		return new MessageChunk(completedHandler, bufferSize);
	}

	public void Reset(MessageChunk item)
	{
		item.Reset();
	}

	public void Destroy(MessageChunk item)
	{
		item.Dispose();
	}
}
