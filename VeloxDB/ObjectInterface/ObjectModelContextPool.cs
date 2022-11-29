using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Velox.Common;
using Velox.Storage;

namespace Velox.ObjectInterface;

internal unsafe class ObjectModelContextPool
{
	const int perCoreCount = 8;

	// This offset is used to guarantee that each per-core pool array does not share cache lines with other pools
	// which would cause false cache line sharing in the CPU, ruining parallelism.
	int perCoreOffset = 16;

	StorageEngine engine;

	readonly object sync = new object();

	int globalCount;
	ObjectModelContext[] globalPool;

	object cacheAllocOwner;
	int* perCoreCounts;
	ObjectModelContext[][] perCorePools;
	MultiSpinLock perCoreSync;

	IdRange idRange;

	public ObjectModelContextPool(StorageEngine engine)
	{
		TTTrace.Write(engine.TraceId);

		this.engine = engine;

		idRange = new IdRange(engine);

		globalCount = 0;
		globalPool = new ObjectModelContext[128];

		perCoreCounts = (int*)CacheLineMemoryManager.Allocate(4, out cacheAllocOwner);
		perCorePools = new ObjectModelContext[ProcessorNumber.CoreCount][];
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			int* pc = ((int*)((byte*)perCoreCounts + (i << AlignedAllocator.CacheLineSizeLog)));
			*pc = perCoreCount;
			perCorePools[i] = new ObjectModelContext[perCoreCount + perCoreOffset * 2];
			for (int j = 0; j < perCoreCount; j++)
			{
				perCorePools[i][perCoreOffset + j] = new ObjectModelContext(engine, idRange, i);
			}
		}

		perCoreSync = new MultiSpinLock(true);
	}

	internal StorageEngine Engine => engine;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ObjectModelContext GetContext()
	{
		int lockHandle = perCoreSync.Enter();
		try
		{
			int procNum = lockHandle;
			int* pc = ((int*)((byte*)perCoreCounts + (procNum << AlignedAllocator.CacheLineSizeLog)));
			if (*pc > 0)
			{
				ObjectModelContext mc = perCorePools[procNum][perCoreOffset + *pc - 1];
				Checker.AssertTrue(mc.PhysCorePool == procNum);
				*pc = *pc - 1;
				return mc;
			}
		}
		finally
		{
			perCoreSync.Exit(lockHandle);
		}

		return GetGlobalContext();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void PutContext(ObjectModelContext mc)
	{
		int procNum = mc.PhysCorePool;

		if (procNum == -1)
		{
			PutGlobalContext(mc);
			return;
		}

		perCoreSync.Enter(procNum);
		try
		{
			int* pc = ((int*)((byte*)perCoreCounts + (procNum << AlignedAllocator.CacheLineSizeLog)));
			Checker.AssertTrue(*pc < perCoreCount);
			perCorePools[procNum][perCoreOffset + *pc] = mc;
			*pc = *pc + 1;
		}
		finally
		{
			perCoreSync.Exit(procNum);
		}
	}

	private ObjectModelContext GetGlobalContext()
	{
		lock (sync)
		{
			if (globalCount == 0)
				return new ObjectModelContext(engine, idRange, -1);

			ObjectModelContext tc = globalPool[--globalCount];
			Checker.AssertTrue(tc.PhysCorePool == -1);
			return tc;
		}
	}

	private void PutGlobalContext(ObjectModelContext tc)
	{
		lock (sync)
		{
			if (globalCount == globalPool.Length)
				Array.Resize(ref globalPool, globalPool.Length * 2);

			globalPool[globalCount++] = tc;
		}
	}

	public void Dispose()
	{
		CacheLineMemoryManager.Free(cacheAllocOwner);
		perCoreSync.Dispose();
	}
}
