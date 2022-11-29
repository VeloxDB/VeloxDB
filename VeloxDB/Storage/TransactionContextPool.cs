using System;
using System.Runtime.CompilerServices;
using Velox.Common;

namespace Velox.Storage;

internal unsafe class TransactionContextPool : IDisposable
{
	// This offset is used to guarantee that each per-core pool array does not share cache lines with other pools
	// which would cause false cache line sharing in the CPU, ruining parallelism.
	int perCoreOffset = 16;

	StorageEngine engine;

	readonly object sync = new object();

	int sharedCount;
	TransactionContext[] sharedPool;

	object allocHandle;
	MultiSpinLock poolLocks;
	int* poolCounts;
	TransactionContext[][] pools;

	int poolCapacity;

	int slotCounter;

	public TransactionContextPool(StorageEngine engine)
	{
		TTTrace.Write(engine.TraceId);

		this.engine = engine;

		poolCapacity = Math.Min(Transaction.MaxConcurrentTrans / ProcessorNumber.CoreCount, 32);

		poolCounts = (int*)CacheLineMemoryManager.Allocate(sizeof(int), out allocHandle);
		pools = new TransactionContext[ProcessorNumber.CoreCount][];
		for (int i = 0; i < pools.Length; i++)
		{
			int* pcount = ((int*)((byte*)poolCounts + (i << AlignedAllocator.CacheLineSizeLog)));
			*pcount = poolCapacity;
			pools[i] = new TransactionContext[poolCapacity + perCoreOffset * 2];
			for (int j = 0; j < poolCapacity; j++)
			{
				pools[i][perCoreOffset + j] = new TransactionContext(engine, i, (ushort)(++slotCounter));
			}
		}

		poolLocks = new MultiSpinLock(true);

		sharedCount = 0;
		sharedPool = new TransactionContext[128];
	}

	public void ResetAlignmentMode()
	{
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			poolLocks.Enter(i);
			try
			{
				int* pcount = ((int*)((byte*)poolCounts + (i << AlignedAllocator.CacheLineSizeLog)));
				for (int j = 0; j < *pcount; j++)
				{
					TransactionContext tc = pools[i][perCoreOffset + j];
					tc.ResetAlignmentMode();
				}
			}
			finally
			{
				poolLocks.Exit(i);
			}
		}

		lock (sync)
		{
			for (int i = 0; i < sharedCount; i++)
			{
				sharedPool[i].ResetAlignmentMode();
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public TransactionContext Get()
	{
		int lockHandle = poolLocks.Enter();
		try
		{
			int* pcount = ((int*)((byte*)poolCounts + (lockHandle << AlignedAllocator.CacheLineSizeLog)));
			if (*pcount > 0)
				return pools[lockHandle][perCoreOffset + (--(*pcount))];
		}
		finally
		{
			poolLocks.Exit(lockHandle);
		}

		return GetShared();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Put(TransactionContext tc)
	{
		int lockHandle = tc.PoolIndex;

		if (lockHandle == -1)
		{
			PutShared(tc);
			return;
		}

		poolLocks.Enter(lockHandle);
		try
		{
			int* pcount = ((int*)((byte*)poolCounts + (lockHandle << AlignedAllocator.CacheLineSizeLog)));
			pools[lockHandle][perCoreOffset + ((*pcount)++)] = tc;
		}
		finally
		{
			poolLocks.Exit(lockHandle);
		}
	}

	private TransactionContext GetShared()
	{
		int slot;
		lock (sync)
		{
			if (slotCounter == Transaction.MaxConcurrentTrans)
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.ConcurrentTranLimitExceeded));

			if (sharedCount != 0)
				return sharedPool[--sharedCount];

			slot = ++slotCounter;
		}

		return new TransactionContext(engine, -1, (ushort)slot);
	}

	private void PutShared(TransactionContext tc)
	{
		lock (sync)
		{
			if (sharedCount == sharedPool.Length)
				Array.Resize(ref sharedPool, sharedPool.Length * 2);

			sharedPool[sharedCount++] = tc;
		}
	}

#if TEST_BUILD
	public void Validate()
	{
		TTTrace.Write(engine.TraceId);

		for (int i = 0; i < pools.Length; i++)
		{
			int* pcount = ((int*)((byte*)poolCounts + (i << AlignedAllocator.CacheLineSizeLog)));
			if (*pcount != poolCapacity)
				throw new InvalidOperationException();
		}
	}
#endif

	public void Dispose()
	{
		TTTrace.Write(engine.TraceId);

		for (int i = 0; i < pools.Length; i++)
		{
			int* pcount = ((int*)((byte*)poolCounts + (i << AlignedAllocator.CacheLineSizeLog)));
			for (int j = 0; j < *pcount; j++)
			{
				pools[i][perCoreOffset + j].Dispose();
			}
		}

		CacheLineMemoryManager.Free(allocHandle);
		poolLocks.Dispose();

		for (int i = 0; i < sharedCount; i++)
		{
			sharedPool[i].Dispose();
		}
	}
}
