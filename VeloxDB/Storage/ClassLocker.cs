using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal unsafe sealed class ClassLocker : IDisposable
{
	ushort classIndex;

	object writerStatesHandle;
	WriterState* writerStates;

	uint readerCount;
	ulong commitedReadLockVer;

	MemoryManager memoryManager;

	public ClassLocker(StorageEngine engine, ushort classIndex)
	{
		this.classIndex = classIndex;
		this.memoryManager = engine.MemoryManager;
		writerStates = (WriterState*)CacheLineMemoryManager.Allocate(sizeof(WriterState), out writerStatesHandle);
	}

	public void ModelUpdated(ushort classIndex)
	{
		this.classIndex = classIndex;
	}

	/// <summary>
	/// This method must not be called in parallel with any other methods of this class.
	/// </summary>
	public bool TryTakeReadLock(Transaction tran)
	{
		TransactionContext tc = tran.Context;

		if (ClassIndexMultiSet.Contains(tc.LockedClasses, classIndex))
		{
			TTTrace.Write(classIndex);
			return true;
		}

		int count = ProcessorNumber.CoreCount;
		long writerCount = 0;
		for (int i = 0; i < count; i++)
		{
			WriterState* state = GetWriter(i);
			writerCount += state->count;
			TTTrace.Write(tran.Id, state->count, state->commitedVersion, tran.ReadVersion);
			if ((ulong)state->commitedVersion > tran.ReadVersion)
				return false;
		}

		if (writerCount > 1)
			return false;

		if (writerCount > 0 && !ClassIndexMultiSet.Contains(tc.WrittenClasses, classIndex))
			return false;

		readerCount++;
		TTTrace.Write(tran.Id, readerCount);
		ClassIndexMultiSet* tis = tc.LockedClasses;
		ClassIndexMultiSet.TryAdd(memoryManager, ref tis, classIndex);
		if (tis != tc.LockedClasses)
			tc.LockedClasses = tis;

		return true;
	}

	/// <summary>
	/// This method must not be called in parallel with TryTakeReadLock and Commit/RollbackReadLockTransaction.
	/// </summary>
	public bool TryAddWriter(Transaction tran)
	{
		TTTrace.Write(tran.Id, commitedReadLockVer, tran.ReadVersion, readerCount);

		TransactionContext tc = tran.Context;
		if (commitedReadLockVer > tran.ReadVersion || readerCount > 1 ||
			(readerCount > 0 && !ClassIndexMultiSet.Contains(tc.LockedClasses, classIndex)))
		{
			return false;
		}

		WriterState* state = GetWriter(ProcessorNumber.GetCore());
		state->IncrementWriterCount();

		ClassIndexMultiSet* writtenClassesMap = tc.WrittenClasses;
		bool success = ClassIndexMultiSet.TryAdd(memoryManager, ref writtenClassesMap, classIndex);
		Checker.AssertTrue(success);
		if (writtenClassesMap != tc.WrittenClasses)
			tc.WrittenClasses = writtenClassesMap;

		return true;
	}

	/// <summary>
	/// This method must not be called in parallel with any other methods of this class.
	/// </summary>
	public void CommitReadLock(ulong commitVersion)
	{
		TTTrace.Write(commitVersion, readerCount);

		readerCount--;
		Checker.AssertTrue(readerCount >= 0);
		if (commitVersion > commitedReadLockVer)
			commitedReadLockVer = commitVersion;
	}

	/// <summary>
	/// This method must not be called in parallel with any other methods of this class.
	/// </summary>
	public void RollbackReadLock()
	{
		TTTrace.Write(readerCount);

		readerCount--;
		Checker.AssertTrue(readerCount >= 0);
	}

	/// <summary>
	/// This method must not be called in parallel with TryTakeReadLock and FinishReadLockTransaction.
	/// </summary>
	public void CommitWrite(ulong commitVersion)
	{
		WriterState* state = GetWriter(ProcessorNumber.GetCore());
		state->CommitWrite((long)commitVersion);
	}

	/// <summary>
	/// This method must not be called in parallel with TryTakeReadLock and FinishReadLockTransaction.
	/// </summary>
	public void RollbackWrite()
	{
		WriterState* state = GetWriter(ProcessorNumber.GetCore());
		state->RollbackWrite();
	}

	/// <summary>
	/// This method must not be called in parallel with any other methods of this class.
	/// </summary>
	public void Rewind(ulong version)
	{
		commitedReadLockVer = 0;
		int count = ProcessorNumber.CoreCount;
		for (int i = 0; i < count; i++)
		{
			WriterState* state = GetWriter(i);
			state->commitedVersion = 0;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private WriterState* GetWriter(int index)
	{
		return (WriterState*)CacheLineMemoryManager.GetBuffer(writerStates, index);
	}

#if TEST_BUILD
	public void Validate()
	{
		if (readerCount > 0)
			throw new InvalidOperationException();

		int count = ProcessorNumber.CoreCount;
		long writerCount = 0;
		for (int i = 0; i < count; i++)
		{
			WriterState* state = GetWriter(i);
			writerCount += state->count;
		}

		if (writerCount > 0)
			throw new InvalidOperationException();
	}
#endif

	public void Dispose()
	{
		CacheLineMemoryManager.Free(writerStatesHandle);
		writerStates = null;
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal struct WriterState
{
	public long count;
	public long commitedVersion;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void IncrementWriterCount()
	{
		Interlocked.Increment(ref count);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void CommitWrite(long newVersion)
	{
		Interlocked.Decrement(ref count);
		while (true)
		{
			long temp = commitedVersion;
			if (newVersion < temp || Interlocked.CompareExchange(ref commitedVersion, newVersion, temp) == temp)
				return;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void RollbackWrite()
	{
		Interlocked.Decrement(ref count);
	}
}
