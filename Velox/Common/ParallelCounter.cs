using System;

namespace Velox.Common;

/// <summary>
/// Helper class that can be used to keep track of the number of occurances of an event in a highly parallel manner. It avoids accessing
/// same CPU cache lines by different CPU cores, for increased parallelism. Use this class for high performane benchamring.
/// </summary>
public unsafe sealed class ParallelCounter
{
	object handle;
	byte* buffer;

	/// <summary>
	/// Creates a new instance of the ParallelCounter class.
	/// </summary>
	public ParallelCounter()
	{
		buffer = CacheLineMemoryManager.Allocate(sizeof(NativeInterlocked64), out handle);
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			long* p = (long*)CacheLineMemoryManager.GetBuffer(buffer, i);
			*p = 0;
		}
	}

	/// <summary>
	/// </summary>
	~ParallelCounter()
	{
		CacheLineMemoryManager.Free(handle);
	}

	/// <summary>
	/// The current count.
	/// </summary>
	public long Count
	{
		get
		{
			long s = 0;
			for (int i = 0; i < ProcessorNumber.CoreCount; i++)
			{
				long* p = (long*)CacheLineMemoryManager.GetBuffer(buffer, i);
				s += *p;
			}

			return s;
		}
	}

	/// <summary>
	/// Increases the count by one.
	/// </summary>
	/// <returns>The incremented value.</returns>
	public long Inc()
	{
		int procNum = ProcessorNumber.GetCore();
		NativeInterlocked64* p = (NativeInterlocked64*)CacheLineMemoryManager.GetBuffer(buffer, procNum);
		return p->Increment();
	}
}
