using System;
using System.Runtime.CompilerServices;
using Velox.Common;

namespace Velox.Storage;

internal unsafe class TransactionContextPool : IDisposable
{
	StorageEngine engine;

	object allocHandle;
	MultiSpinLock poolLocks;
	int* poolCounts;
	TransactionContext[][] pools;

	int poolCapacity;

	public TransactionContextPool(StorageEngine engine)
	{
		TTTrace.Write(engine.TraceId);

		this.engine = engine;

		poolCapacity = Math.Max(Transaction.MaxConcurrentTrans / ProcessorNumber.CoreCount, 32);

		poolCounts = (int*)CacheLineMemoryManager.Allocate(sizeof(int), out allocHandle);
		pools = new TransactionContext[ProcessorNumber.CoreCount][];
		int slotCounter = 0;
		for (int i = 0; i < pools.Length; i++)
		{
			int* pcount = (int*)CacheLineMemoryManager.GetBuffer(poolCounts, i);
			*pcount = poolCapacity;
			pools[i] = new TransactionContext[poolCapacity];
			for (int j = 0; j < poolCapacity; j++)
			{
				pools[i][j] = new TransactionContext(engine, i, (ushort)(++slotCounter));
			}
		}

		poolLocks = new MultiSpinLock();
	}

	public void ResetAlignmentMode()
	{
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			poolLocks.Enter(i);
			try
			{
				int* pcount = (int*)CacheLineMemoryManager.GetBuffer(poolCounts, i);
				for (int j = 0; j < *pcount; j++)
				{
					TransactionContext tc = pools[i][j];
					tc.ResetAlignmentMode();
				}
			}
			finally
			{
				poolLocks.Exit(i);
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public TransactionContext Get()
	{
		TransactionContext tc = null;
		int lockHandle = poolLocks.Enter();
		try
		{
			int* pcount = (int*)CacheLineMemoryManager.GetBuffer(poolCounts, lockHandle);
			if (*pcount > 0)
				tc = pools[lockHandle][--(*pcount)];
		}
		finally
		{
			poolLocks.Exit(lockHandle);
		}

		if (tc == null)
			tc = Borrow(lockHandle);

		tc.Allocate();
		return tc;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Put(TransactionContext tc)
	{
		int lockHandle = tc.PoolIndex;

		poolLocks.Enter(lockHandle);
		try
		{
			int* pcount = (int*)CacheLineMemoryManager.GetBuffer(poolCounts, lockHandle);
			Checker.AssertTrue(*pcount < poolCapacity);
			pools[lockHandle][(*pcount)++] = tc;
		}
		finally
		{
			poolLocks.Exit(lockHandle);
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private TransactionContext Borrow(int index)
	{
		for (int i = 0; i < pools.Length; i++)
		{
			index++;
			if (index == pools.Length)
				index = 0;

			int* pcount = (int*)CacheLineMemoryManager.GetBuffer(poolCounts, index);
			if (*pcount > 0)
			{
				poolLocks.Enter(index);
				try
				{
					if (*pcount > 0)
						return pools[index][--(*pcount)];
				}
				finally
				{
					poolLocks.Exit(index);
				}
			}
		}

		throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.ConcurrentTranLimitExceeded));
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
			int* pcount = (int*)CacheLineMemoryManager.GetBuffer(poolCounts, i);
			for (int j = 0; j < *pcount; j++)
			{
				pools[i][j].Dispose();
			}
		}

		CacheLineMemoryManager.Free(allocHandle);
		poolLocks.Dispose();
	}
}
