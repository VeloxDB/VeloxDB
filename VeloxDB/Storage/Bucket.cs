using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Velox.Common;

namespace Velox.Storage;

/// <summary>
/// This is a very tricky union structure. It holds both handle to the buffer and lock object in 8 bytes. Lock
/// object is very special in that it only uses single (highest order) bit for locking (does not affect other bits)
/// but also, when lock is taken, resets the value of the highest order bit (zero). This allows one to write to the
/// objectHandle field directly once the lock has been acquired without affecting the status of the lock.
/// </summary>
[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Bucket.Size)]
internal unsafe struct Bucket
{
	public const int Size = 8;

	[FieldOffset(0)]
	private ulong objectHandle;

	[FieldOffset(0)]
	private SingleBitSpinLock64 sync;

	public void Init()
	{
		sync = new SingleBitSpinLock64(false);
	}

	public ulong Handle
	{
		get => sync.Value;
		set
		{
			Checker.AssertTrue((value & 0x8000000000000000) == 0);
			sync.Value = value;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static ulong* LockAccess(Bucket* bn)
	{
		((SingleBitSpinLock64*)&bn->sync)->EnterWriteLock();
		return &bn->objectHandle;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void UnlockAccess(Bucket* bn)
	{
		((SingleBitSpinLock64*)&bn->sync)->ExitWriteLock();
	}
}
