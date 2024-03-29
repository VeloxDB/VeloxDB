﻿using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using VeloxDB.Common;
using VeloxDB.Storage;

namespace VeloxDB.ObjectInterface;

internal unsafe class ObjectModelContextPool
{
#if DEBUG
	static readonly int perCoreCount = 8;
#else
	static readonly int perCoreCount = 32;
#endif

	const int readerPoolCapacity = 1024;

	StorageEngine engine;

	readonly object sync = new object();

	int globalCount;
	ObjectModelContext[] globalPool;

	object cacheAllocOwner;
	int* perCoreCounts;
	ObjectModelContext[][] perCorePools;
	MultiSpinLock perCoreSync;

	IdRange idRange;

	readonly object readerPoolSync = new object();
	List<ObjectReader[]> readerPool;

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
			int* pc = (int*)CacheLineMemoryManager.GetBuffer(perCoreCounts, i);
			*pc = perCoreCount;
			perCorePools[i] = new ObjectModelContext[perCoreCount];
			for (int j = 0; j < perCoreCount; j++)
			{
				perCorePools[i][j] = new ObjectModelContext(this, engine, idRange, i);
			}
		}

		readerPool = new List<ObjectReader[]>(readerPoolCapacity);

		perCoreSync = new MultiSpinLock();
	}

	internal StorageEngine Engine => engine;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ObjectModelContext GetContext()
	{
		int lockHandle = perCoreSync.Enter();
		try
		{
			int procNum = lockHandle;
			int* pc = (int*)CacheLineMemoryManager.GetBuffer(perCoreCounts, procNum);
			if (*pc > 0)
			{
				ObjectModelContext mc = perCorePools[procNum][*pc - 1];
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
			int* pc = (int*)CacheLineMemoryManager.GetBuffer(perCoreCounts, procNum);
			Checker.AssertTrue(*pc < perCoreCount);
			perCorePools[procNum][*pc] = mc;
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
			if (globalCount > 0)
			{
				ObjectModelContext tc = globalPool[--globalCount];
				Checker.AssertTrue(tc.PhysCorePool == -1);
				return tc;
			}
		}

		return new ObjectModelContext(this, engine, idRange, -1);
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

	public ObjectReader[] GetObjectReaders()
	{
		lock (readerPoolSync)
		{
			if (readerPool.Count > 0)
			{
				int index = readerPool.Count - 1;
				ObjectReader[] r = readerPool[index];
				readerPool.RemoveAt(index);
				return r;
			}
		}

		return new ObjectReader[ObjectModelContext.ObjectReadersCapacity];
	}

	public void PutObjectReaders(ObjectReader[] r)
	{
		lock (readerPoolSync)
		{
			if (readerPool.Count == readerPoolCapacity)
				return;

			readerPool.Add(r);
		}
	}

	public void Dispose()
	{
		CacheLineMemoryManager.Free(cacheAllocOwner);
		perCoreSync.Dispose();
	}
}
