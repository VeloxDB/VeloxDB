using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Reflection;
using VeloxDB.Common;

namespace VeloxDB.Networking;

internal sealed unsafe class MessageChunkPool
{
	Pool smallPool;
	Pool largePool;

	public MessageChunkPool(int poolSize)
	{
		CalculateCapacities(poolSize, out int smallChunkCount, out int largeChunkCount);
		smallPool = new Pool(smallChunkCount, MessageChunk.SmallBufferSize);
		largePool = new Pool(largeChunkCount, MessageChunk.LargeBufferSize);
	}

	public MessageChunk GetSmall(int procNum = -1)
	{
		MessageChunk chunk = smallPool.Get(procNum);
		if (!chunk.IsInPool)
			throw new CriticalDatabaseException();

		chunk.TrackPoolRetreival();
		chunk.IsInPool = false;

		return chunk;
	}

	public MessageChunk GetLarge(int procNum = -1)
	{
		MessageChunk chunk = largePool.Get(procNum);
		if (!chunk.IsInPool)
			throw new CriticalDatabaseException();

		chunk.TrackPoolRetreival();
		chunk.IsInPool = false;

		return chunk;
	}

	public void Put(MessageChunk chunk)
	{
		if (chunk.IsInPool)
			throw new CriticalDatabaseException();

		chunk.TrackPoolReturn();
		chunk.IsInPool = true;

		if (chunk.BufferSize == MessageChunk.SmallBufferSize)
			smallPool.Put(chunk);
		else
			largePool.Put(chunk);
	}

	public void ResizeIfLarger(int poolSize)
	{
		CalculateCapacities(poolSize, out int smallChunkCount, out int largeChunkCount);
		smallPool.ResizeIfLarger(smallChunkCount);
		largePool.ResizeIfLarger(largeChunkCount);
	}

	private static void CalculateCapacities(int poolSize, out int smallChunkCount, out int largeChunkCount)
	{
		smallChunkCount = Math.Max(ProcessorNumber.CoreCount, (int)(poolSize * 0.5) / MessageChunk.SmallBufferSize);
		largeChunkCount = Math.Max(ProcessorNumber.CoreCount, (int)(poolSize * 0.5) / MessageChunk.LargeBufferSize);
	}

	internal sealed class Pool
	{
		PoolData* poolData;
		object poolDataHandle;
		MessageChunk[][] pools;
		int chunkSize;
		bool disposed;

		public Pool(int capacity, int chunkSize)
		{
			this.chunkSize = chunkSize;

			int perCoreCapacity = capacity / ProcessorNumber.CoreCount;
			if (perCoreCapacity * ProcessorNumber.CoreCount != capacity)
				perCoreCapacity++;

			pools = new MessageChunk[ProcessorNumber.CoreCount][];
			for (int i = 0; i < pools.Length; i++)
			{
				pools[i] = new MessageChunk[perCoreCapacity];
				for (int j = 0; j < perCoreCapacity; j++)
				{
					pools[i][j] = new MessageChunk(chunkSize) { PoolIndex = i, IsInPool = true };
				}
			}

			poolData = (PoolData*)CacheLineMemoryManager.Allocate(sizeof(PoolData), out poolDataHandle);
			for (int i = 0; i < ProcessorNumber.CoreCount; i++)
			{
				PoolData* pd = (PoolData*)CacheLineMemoryManager.GetBuffer(poolData, i);
				pd->count = perCoreCapacity;
				pd->sync = new RWSpinLock();
			}
		}

		~Pool()
		{
			CacheLineMemoryManager.Free(poolDataHandle);
		}

		public MessageChunk Get(int procNum = -1)
		{
			if (procNum == -1)
				procNum = ProcessorNumber.GetCore();

			MessageChunk chunk = GetInternal(procNum);
			if (chunk == null)
				chunk = Borrow(procNum);

			if (chunk == null)
				chunk = new MessageChunk(chunkSize) { PoolIndex = procNum, IsInPool = true };

			return chunk;
		}

		private MessageChunk GetInternal(int procNum)
		{
			PoolData* pd = (PoolData*)CacheLineMemoryManager.GetBuffer(poolData, procNum);
			MessageChunk[] pool = pools[procNum];

			pd->sync.EnterWriteLock();
			try
			{
				if (disposed)
					return new MessageChunk(chunkSize) { IsInPool = true };

				if (pd->count > 0)
				{
					MessageChunk chunk = pool[--pd->count];
					pool[pd->count] = null;
					return chunk;
				}
			}
			finally
			{
				pd->sync.ExitWriteLock();
			}

			return null;
		}

		private MessageChunk Borrow(int procNum)
		{
			procNum = procNum + (1 - (procNum & 0x01) * 2); // In SMT CPUs this will yield the "other" logical core

			for (int i = 0; i < ProcessorNumber.CoreCount; i++)
			{
				if (procNum >= ProcessorNumber.CoreCount)
					procNum = 0;

				PoolData* pd = (PoolData*)CacheLineMemoryManager.GetBuffer(poolData, procNum);
				if (pd->count > 0)
				{
					MessageChunk chunk = GetInternal(procNum);
					if (chunk != null)
						return chunk;
				}

				procNum++;
			}

			return null;
		}

		public void Put(MessageChunk chunk)
		{
			PoolData* pd = (PoolData*)CacheLineMemoryManager.GetBuffer(poolData, chunk.PoolIndex);
			MessageChunk[] pool = pools[chunk.PoolIndex];

			chunk.Reset();

			pd->sync.EnterWriteLock();
			try
			{
				if (disposed)
				{
					chunk.Dispose();
					return;
				}

				if (pd->count < pool.Length)
				{
					pool[pd->count++] = chunk;
					return;
				}
			}
			finally
			{
				pd->sync.ExitWriteLock();
			}

			chunk.Dispose();
		}

		public void ResizeIfLarger(int capacity)
		{
			int perCoreCapacity = capacity / ProcessorNumber.CoreCount;
			if (perCoreCapacity * ProcessorNumber.CoreCount != capacity)
				perCoreCapacity++;

			poolData->sync.EnterWriteLock();
			try
			{
				if (disposed)
					return;

				if (pools[0].Length >= perCoreCapacity)
					return;

				int diff = perCoreCapacity - pools[0].Length;

				for (int i = 1; i < ProcessorNumber.CoreCount; i++)
				{
					PoolData* pd = (PoolData*)CacheLineMemoryManager.GetBuffer(poolData, i);
					pd->sync.EnterWriteLock();
				}

				try
				{
					for (int i = 0; i < pools.Length; i++)
					{
						MessageChunk[] newPool = new MessageChunk[perCoreCapacity];
						Array.Copy(pools[i], newPool, pools[i].Length);
						pools[i] = newPool;

						PoolData* pd = (PoolData*)CacheLineMemoryManager.GetBuffer(poolData, i);
						for (int j = 0; j < diff; j++)
						{
							pools[i][pd->count++] = new MessageChunk(chunkSize) { PoolIndex = i, IsInPool = true };
						}
					}
				}
				finally
				{
					for (int i = 1; i < ProcessorNumber.CoreCount; i++)
					{
						PoolData* pd = (PoolData*)CacheLineMemoryManager.GetBuffer(poolData, i);
						pd->sync.ExitWriteLock();
					}
				}
			}
			finally
			{
				poolData->sync.ExitWriteLock();
			}
		}

		public void Dispose()
		{
			for (int i = 0; i < ProcessorNumber.CoreCount; i++)
			{
				PoolData* pd = (PoolData*)CacheLineMemoryManager.GetBuffer(poolData, i);
				pd->sync.EnterWriteLock();
			}

			try
			{
				if (disposed)
					return;

				disposed = true;

				for (int i = 0; i < pools.Length; i++)
				{
					PoolData* pd = (PoolData*)CacheLineMemoryManager.GetBuffer(poolData, i);
					for (int j = 0; j < pd->count; j++)
					{
						pools[i][j].Dispose();
					}
				}
			}
			finally
			{
				for (int i = 0; i < ProcessorNumber.CoreCount; i++)
				{
					PoolData* pd = (PoolData*)CacheLineMemoryManager.GetBuffer(poolData, i);
					pd->sync.ExitWriteLock();
				}
			}
		}

		private struct PoolData
		{
			public RWSpinLock sync;
			public int count;
		}
	}

	public void Dispose()
	{
		smallPool.Dispose();
		largePool.Dispose();
	}
}
