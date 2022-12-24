using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Velox.Common;

namespace Velox.Storage.Persistence;

internal unsafe class LogBufferPool
{
	IntPtr buffer;
	int bufferSize;

	object perCPUPoolsHandle;
	PerCPUPool* perCPUPools;

	int count;
	IntPtr[] pool;

	Func<bool> tryTransferDelegate;

	bool disposed;

	public LogBufferPool(int bufferSize, int capacity)
	{
		this.bufferSize = bufferSize;

		buffer = AlignedAllocator.Allocate(capacity * bufferSize);

		IntPtr p = buffer;
		pool = new IntPtr[capacity];
		for (int i = 0; i < capacity; i++)
		{
			pool[i] = p;
			p += bufferSize;
		}

		count = capacity;

		perCPUPools = (PerCPUPool*)CacheLineMemoryManager.Allocate(sizeof(PerCPUPool), out perCPUPoolsHandle);
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			PerCPUPool* perCPUPool = (PerCPUPool*)CacheLineMemoryManager.GetBuffer(perCPUPools, i);
			perCPUPool->pool = (IntPtr*)AlignedAllocator.Allocate(capacity * sizeof(IntPtr));
		}

		tryTransferDelegate = TryTransfer;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public Buffer Get(long size)
	{
		Checker.AssertTrue(size <= bufferSize);

		// This should be rare since usually restore workers are not the bottleneck, but instead log file reader is.
		if (count == 0)
			SpinWait.SpinUntil(tryTransferDelegate);

		return new Buffer(this, pool[--count], size);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void Put(IntPtr buffer)
	{
		int procNum = ProcessorNumber.GetCore();
		PerCPUPool* perCPUPool = (PerCPUPool*)CacheLineMemoryManager.GetBuffer(perCPUPools, procNum);
		perCPUPool->sync.EnterWriteLock();
		try
		{
			perCPUPool->pool[perCPUPool->count++] = buffer;
		}
		finally
		{
			perCPUPool->sync.ExitWriteLock();
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool TryTransfer()
	{
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			PerCPUPool* perCPUPool = (PerCPUPool*)CacheLineMemoryManager.GetBuffer(perCPUPools, i);
			perCPUPool->sync.EnterWriteLock();
			try
			{
				for (int j = 0; j < perCPUPool->count; j++)
				{
					pool[count++] = perCPUPool->pool[--perCPUPool->count];
				}
			}
			finally
			{
				perCPUPool->sync.ExitWriteLock();
			}
		}

		return count > 0;
	}

	public void Dispose()
	{
		if (disposed)
			return;

		disposed = true;

		int perCPUCount = 0;
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			PerCPUPool* perCPUPool = (PerCPUPool*)CacheLineMemoryManager.GetBuffer(perCPUPools, i);
			perCPUCount += perCPUPool->count;
			AlignedAllocator.Free((IntPtr)perCPUPool->pool);
		}

		Checker.AssertTrue(perCPUCount + count == pool.Length);

		CacheLineMemoryManager.Free(perCPUPoolsHandle);
		perCPUPools = null;

		AlignedAllocator.Free(buffer);
		buffer = IntPtr.Zero;
	}

	private struct PerCPUPool
	{
		public RWSpinLock sync;
		public int count;
		public IntPtr* pool;
	}

	public struct Buffer : IDisposable
	{
		LogBufferPool owner;

#if DEBUG
		LeakDetector leakDetector;
#endif

		public long Size { get; private set; }
		public IntPtr Value { get; private set; }

		public Buffer(LogBufferPool owner, IntPtr value, long size)
		{
			this.owner = owner;
			this.Value = value;
			this.Size = size;

#if DEBUG
			leakDetector = null;
#endif
		}

		public Buffer(long size)
		{
			this.owner = null;
			this.Value = NativeAllocator.Allocate(size);
			this.Size = size;

#if DEBUG
			leakDetector = new LeakDetector();
#endif
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Dispose()
		{
			if (owner == null)
			{
				if (Value != IntPtr.Zero)
				{
#if DEBUG
					Checker.AssertNotNull(leakDetector);
					GC.SuppressFinalize(leakDetector);
#endif
					NativeAllocator.Free(Value);
					Value = IntPtr.Zero;
				}

				return;
			}

			owner.Put(Value);
			Value = IntPtr.Zero;
		}

		private sealed class LeakDetector
		{
			~LeakDetector()
			{
				throw new CriticalDatabaseException();
			}
		}
	}
}
