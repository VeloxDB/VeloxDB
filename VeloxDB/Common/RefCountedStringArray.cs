using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Velox.Common;

internal sealed class RefCountedStringArray
{
	const int chunkSizeLog = 14;
	const ulong chunkSize = 1 << chunkSizeLog;
	const ulong chunkSizeMask = chunkSize - 1;

	readonly object sync = new object();

	ulong length;
	ulong chunkCount;
	RefCountedString[][] chunks;

	public RefCountedStringArray()
	{
		chunkCount = 0;
		length = 0;
		chunks = new RefCountedString[8192][];
	}

	public ulong Length => length;

	public RefCountedString this[ulong index]
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		get
		{
			ulong chunk = index >> chunkSizeLog;
			ulong offset = index & chunkSizeMask;
			return chunks[chunk][offset];
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		set
		{
			ulong chunk = index >> chunkSizeLog;
			ulong offset = index & chunkSizeMask;
			chunks[chunk][offset] = value;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long InterlockedIncrementRefCount(ulong index)
	{
		ulong chunk = index >> chunkSizeLog;
		ulong offset = index & chunkSizeMask;
		return chunks[chunk][offset].InterlockedIncrementRefCount();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long InterlockedDecrementRefCount(ulong index)
	{
		ulong chunk = index >> chunkSizeLog;
		ulong offset = index & chunkSizeMask;
		return chunks[chunk][offset].InterlockedDecrementRefCount();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetVersion(ulong index, ulong version)
	{
		ulong chunk = index >> chunkSizeLog;
		ulong offset = index & chunkSizeMask;
		chunks[chunk][offset].Version = version;
	}

	public void AddChunk(out ulong start, out ulong count)
	{
		lock (sync)
		{
			start = length;
			count = chunkSize;

			if (chunkCount == (ulong)chunks.Length)
			{
				// We need to resize the array in a thread safe manner (meaning concurrent
				// access of individual strings on a given index need to work)
				RefCountedString[][] newChunks = new RefCountedString[chunks.Length * 2][];
				Array.Copy(chunks, newChunks, chunks.Length);

#if !X86_64
			// This is not needed on x86/x64 since neither loads nor stores can be reordered after stores.
			Thread.MemoryBarrier();
#endif

				chunks = newChunks;
			}

			chunks[chunkCount++] = new RefCountedString[chunkSize];
			length += chunkSize;
		}
	}

	internal struct RefCountedString
	{
		string value;
		long refCount;
		ulong version;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public RefCountedString(string value, long refCount)
		{
			this.value = value;
			this.refCount = refCount;
			this.version = ulong.MaxValue;
		}

		public string Value => value;
		public long RefCount => refCount;
		public ulong Next { get => (ulong)refCount; set => refCount = (long)value; }
		public ulong Version { get => version; set => version = value; }

		public long InterlockedIncrementRefCount()
		{
			return Interlocked.Increment(ref refCount);
		}

		public long InterlockedDecrementRefCount()
		{
			return Interlocked.Decrement(ref refCount);
		}
	}
}
