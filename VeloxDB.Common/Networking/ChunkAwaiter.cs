using System;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Networking;

internal sealed class ChunkAwaiter : IDisposable
{
	const int chunkTimeout = 10 * 1000;

	AutoResetEvent signal;
	MessageChunk chunk;
	bool disposed;
	ChunkAwaiter nextAwaiter;
	WaitHandle[] waitHandles;

	public ChunkAwaiter(ManualResetEvent abortWait)
	{
		signal = new AutoResetEvent(false);
		waitHandles = new WaitHandle[2] { signal, abortWait };
	}

	public MessageChunk Chunk => chunk;

	public ChunkAwaiter NextAwaiter { get => nextAwaiter; set => nextAwaiter = value; }

	public void SetChunk(MessageChunk chunk)
	{
		this.chunk = chunk;
		signal.Set();
	}

	public void WaitChunk()
	{
		try
		{
			int n = WaitHandle.WaitAny(waitHandles, chunkTimeout);
			if (n == WaitHandle.WaitTimeout)
				throw new ChunkTimeoutException();
			else if (n == 1)
				throw new CommunicationObjectAbortedException(AbortedPhase.Communication);
		}
		catch (Exception e) when (e is ObjectDisposedException || e is AbandonedMutexException)
		{
			throw new CommunicationObjectAbortedException(AbortedPhase.Communication);
		}
	}

	public void Reset()
	{
		nextAwaiter = null;
		chunk = null;
		signal.Reset();
	}

	~ChunkAwaiter()
	{
		CleanUp(false);
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
			if (disposing)
				signal.Dispose();

			disposed = true;
		}
	}
}

internal sealed class ChunkAwaiterFactory : IItemFactory<ChunkAwaiter>
{
	ManualResetEvent abortWait;

	public ChunkAwaiterFactory(ManualResetEvent abortWait)
	{
		this.abortWait = abortWait;
	}

	public void Init(ChunkAwaiter item)
	{
	}

	public ChunkAwaiter Create()
	{
		return new ChunkAwaiter(abortWait);
	}

	public void Reset(ChunkAwaiter item)
	{
		item.Reset();
	}

	public void Destroy(ChunkAwaiter item)
	{
		item.Dispose();
	}
}
