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

	// ChunkPreReceived
	// Uvesti da awaiter zna koji chunkNum ceka
	// Ukoliko je ovo poslednji chunk, ne radi se nista
	// Nas chunkNum mora biti za jedan veci od zadnjeg u mapi
	// Uvezemo awaiter ali ne signaliziramo trenutni
	// Posle u ChunkReceived kada trazimo awaiter, trazimo sledbenih onog koji ceka nas
	//	Ovo moze biti null u kom slucaju stavimo novi awaiter ili moze biti awaiter

	public unsafe void PreChunkReceived(MessageChunkHeader* chunkHeader)
	{
		if (chunkHeader->IsLast)
			return;

		ChunkAwaiter awaiter = awaiterPool.Get();
		awaiter.WaitingChunkNum = chunkHeader->chunkNum + 1;

		lock (sync)
		{
			if (chunkHeader->IsFirst)
			{
				map.Add(chunkHeader->messageId, awaiter);
			}
			else
			{
				// If this is not the first chunk there should be an awaiter that is waiting for this chunk unless
				// waiting for this message has been aborted (due to timeout or connection being closed...)
				ChunkAwaiter prevAwaiter = GetLastAwaiter(chunkHeader->messageId);
				if (prevAwaiter == null || prevAwaiter.WaitingChunkNum != chunkHeader->chunkNum)
					throw new CorruptMessageException();

				currChunkCount++;
				prevAwaiter.NextAwaiter = awaiter;

				if (currChunkCount == maxQueuedChunkCount)
					stopReceiving();
			}
		}
	}

	public unsafe void ChunkReceived(MessageChunk chunk, out bool chunkConsumed)
	{
		Checker.AssertFalse(chunk.MessageId == 0);

		chunkConsumed = false;
		if (chunk.IsFirst)
			return;

		ChunkAwaiter awaiter;
		lock (sync)
		{
			awaiter = GetAwaiter(chunk.MessageId, chunk.ChunkNum);
		}

		if (awaiter == null)
			throw new CorruptMessageException();

		awaiter.SetChunk(chunk);
		chunkConsumed = true;
	}

	public MessageChunk WaitNextChunk(ulong messageId, int chunkNum)
	{
		ChunkAwaiter awaiter;

		try
		{
			lock (sync)
			{
				if (!map.TryGetValue(messageId, out awaiter))
					throw new CorruptMessageException();
			}

			Checker.AssertTrue(awaiter.WaitingChunkNum == chunkNum);
			awaiter.WaitChunk();
		}
		catch (Exception)
		{
			// Multiple different exceptions can occur that would cause the waiting for the chunk to be interrupted.
			// We don't care here which one occurred since those will be processed outside. We just need to clean up
			// this message.

			lock (sync)
			{
				CancelMessageInternal(messageId, true);
			}

			throw;
		}

		MessageChunk nextChunk = awaiter.Chunk;
		Checker.AssertTrue(nextChunk.ChunkNum == chunkNum);

		lock (sync)
		{
			if (awaiter.NextAwaiter == null)
			{
				map.Remove(messageId);
			}
			else
			{
				map[messageId] = awaiter.NextAwaiter;
			}

			currChunkCount--;
			if (currChunkCount == maxQueuedChunkCount - 1)
				continueReceiving();
		}

		awaiterPool.Put(awaiter);
		return nextChunk;
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

	private ChunkAwaiter GetAwaiter(ulong msgId, int chunkNum)
	{
		if (!map.TryGetValue(msgId, out ChunkAwaiter awaiter))
			return null;

		while (awaiter != null)
		{
			if (awaiter.WaitingChunkNum == chunkNum)
				return awaiter;

			awaiter = awaiter.NextAwaiter;
		}

		return null;
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
