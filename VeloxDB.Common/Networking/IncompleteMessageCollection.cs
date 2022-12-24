using System;
using System.Collections.Generic;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Networking;

internal sealed class IncompleteMessageCollection : IDisposable
{
	readonly object sync = new object();

	Dictionary<ulong, ChunkAwaiter> map;
	ItemPool<ChunkAwaiter> awaiterPool;
	MessageChunkPool chunkPool;
	ManualResetEvent abortWait;
	Action stopReceiving;
	Action continueReceiving;
	int maxQueuedChunkCount;
	int currChunkCount;
	bool disposed;

	public IncompleteMessageCollection(MessageChunkPool chunkPool, int maxQueuedChunkCount, Action stopReceiving, Action continueReceiving)
	{
		this.chunkPool = chunkPool;
		this.stopReceiving = stopReceiving;
		this.continueReceiving = continueReceiving;
		this.maxQueuedChunkCount = maxQueuedChunkCount;

		abortWait = new ManualResetEvent(false);

		map = new Dictionary<ulong, ChunkAwaiter>(16);
		awaiterPool = new ItemPool<ChunkAwaiter>(Connection.AwaiterPoolCount, new ChunkAwaiterFactory(abortWait));
	}

	public unsafe void ChunkReceived(MessageChunk chunk, out bool chunkConsumed)
	{
		Checker.AssertFalse(chunk.MessageId == 0);

		chunkConsumed = false;
		if (chunk.IsTheOnlyOne)
			return;

		// We are going to need an awaiter to be able to wait for the next chunk if this chunk is not the last one.
		if (!chunk.IsLast)
			chunk.NextChunkAwaiter = awaiterPool.Get();

		lock (sync)
		{
			if (chunk.IsFirst)
			{
				map.Add(chunk.MessageId, chunk.NextChunkAwaiter);
			}
			else
			{
				// If this is not the first chunk there should be an awaiter that is waiting for this chunk unless
				// waiting for this message has been aborted (due to timeout or connection being closed...)
				ChunkAwaiter awaiter = GetLastAwaiter(chunk.MessageId);
				if (awaiter == null)
				{
					throw new CorruptMessageException();
				}
				else
				{
					currChunkCount++;
					awaiter.SetChunk(chunk);
					awaiter.NextAwaiter = chunk.NextChunkAwaiter;

					if (currChunkCount == maxQueuedChunkCount)
						stopReceiving();
				}

				chunkConsumed = true;
			}
		}
	}

	public void WaitNextChunk(MessageChunk chunk, out MessageChunk nextChunk)
	{
		Checker.AssertFalse(chunk.MessageId == 0);

		try
		{
			chunk.NextChunkAwaiter.WaitChunk();
		}
		catch (Exception)
		{
			// Multiple different exceptions can occur that would cause the waiting for the chunk to be interrupted.
			// We don't care here which one occurred since those will be processed outside. We just need to clean up
			// this message.

			lock (sync)
			{
				CancelMessageInternal(chunk.MessageId, true);
				nextChunk = null;
			}

			throw;
		}

		lock (sync)
		{
			ChunkAwaiter awaiter = map[chunk.MessageId];
			Checker.AssertTrue(object.ReferenceEquals(awaiter, chunk.NextChunkAwaiter));

			nextChunk = awaiter.Chunk;
			nextChunk.NextChunkAwaiter = awaiter.NextAwaiter;
			if (nextChunk.NextChunkAwaiter == null)
			{
				map.Remove(chunk.MessageId);
			}
			else
			{
				map[chunk.MessageId] = nextChunk.NextChunkAwaiter;
			}

			awaiterPool.Put(awaiter);

			currChunkCount--;
			if (currChunkCount == maxQueuedChunkCount - 1)
				continueReceiving();
		}
	}

	private unsafe void CancelMessageInternal(ulong msgId, bool notifyReciver)
	{
		if (map.TryGetValue(msgId, out ChunkAwaiter curr))
		{
			while (curr != null)
			{
				if (curr.Chunk != null)
				{
					chunkPool.Put(curr.Chunk);
					currChunkCount--;
					if (currChunkCount == maxQueuedChunkCount - 1 && notifyReciver)
						continueReceiving();
				}

				ChunkAwaiter temp = curr.NextAwaiter;
				awaiterPool.Put(curr);
				curr = temp;
			}

			map.Remove(msgId);
		}
	}

	private ChunkAwaiter GetLastAwaiter(ulong msgId)
	{
		if (!map.TryGetValue(msgId, out ChunkAwaiter awaiter))
			return null;

		while (awaiter.NextAwaiter != null)
		{
			awaiter = awaiter.NextAwaiter;
		}

		return awaiter;
	}

	public void AbortWaiters()
	{
		abortWait.Set();
	}

	public void Dispose()
	{
		lock (sync)
		{
			if (disposed)
				return;

			disposed = true;

			foreach (ulong msgId in new List<ulong>(map.Keys))
			{
				CancelMessageInternal(msgId, false);
			}

			awaiterPool.Close();
		}
	}
}
