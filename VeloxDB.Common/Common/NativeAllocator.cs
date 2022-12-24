using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;

namespace VeloxDB.Common;

internal static class NativeAllocator
{
	const int largeHeapAllocTreshold = 1024 * 16;   // Experimentally determined on Windows 10/Windows Server 2016
	const int virtualAllocTreshold = 1024 * 1024;   // Experimentally determined on Windows 10/Windows Server 2016

	const ulong corruptionDetectionValue = 0xa56f3a7d99f0e061;

	[DllImport("kernel32.dll", SetLastError = true, ExactSpelling = true)]
	[SuppressUnmanagedCodeSecurity]
	static extern IntPtr VirtualAllocEx(IntPtr hProcess, IntPtr lpAddress, IntPtr dwSize,
		AllocationType flAllocationType, MemoryProtection flProtect);

	[DllImport("kernel32.dll", SetLastError = true, ExactSpelling = true)]
	[SuppressUnmanagedCodeSecurity]
	static extern bool VirtualFreeEx(IntPtr hProcess, IntPtr lpAddress, int dwSize, AllocationType dwFreeType);

	[DllImport("kernel32.dll", SetLastError = true)]
	[SuppressUnmanagedCodeSecurity]
	static extern IntPtr GetCurrentProcess();

	[DllImport("kernel32.dll")]
	[SuppressUnmanagedCodeSecurity]
	private static extern IntPtr GetCurrentThread();

	[DllImport("kernel32.dll", SetLastError = true)]
	[return: MarshalAs(UnmanagedType.Bool)]
	[SuppressUnmanagedCodeSecurity]
	static extern bool DuplicateHandle(IntPtr hSourceProcessHandle, IntPtr hSourceHandle,
		IntPtr hTargetProcessHandle, out IntPtr lpTargetHandle, uint dwDesiredAccess,
		[MarshalAs(UnmanagedType.Bool)] bool bInheritHandle, uint dwOptions);

	const int DUPLICATE_SAME_ACCESS = 0x00000002;

	static readonly object largeHeapAllocSync = new object();
	static readonly IntPtr processHandle;

	private static long totalAllocated;

	[Flags]
	public enum AllocationType : uint
	{
		Commit = 0x1000,
		Reserve = 0x2000,
		Release = 0x8000
	}

	[Flags]
	public enum MemoryProtection : uint
	{
		ReadWrite = 0x04
	}

	static NativeAllocator()
	{
		if (OperatingSystem.IsWindows())
		{
			if (!DuplicateHandle(GetCurrentProcess(), GetCurrentProcess(), GetCurrentProcess(),
				out processHandle, 0, false, DUPLICATE_SAME_ACCESS))
			{
				Win32Exception we = new Win32Exception(Marshal.GetLastWin32Error());
				throw new NativeException(we.Message, we.ErrorCode);
			}
		}
	}

	public static long TotalAllocated => totalAllocated;

#if HUNT_CORRUPT
	static Dictionary<IntPtr, string> intPtrs = new Dictionary<IntPtr, string>(1024 * 128);

	public unsafe static void Verify()
	{
		lock (intPtrs)
		{
			foreach (IntPtr item in intPtrs.Keys)
			{
				ulong* up = (ulong*)item;
				if (up[0] != corruptionDetectionValue)
					throw new CriticalDatabaseException();

				long size = (long)up[1];
				up = (ulong*)((byte*)item + size - 8);

				if (up[0] != corruptionDetectionValue)
					throw new CriticalDatabaseException();
			}
		}
	}
#endif

	public unsafe static IntPtr Allocate(long size, bool zeroedOut = false)
	{
#if TEST_BUILD
		size += 24;
#endif

		IntPtr p;
		if (OperatingSystem.IsWindows())
		{
			p = AllocateWindows(size);
		}
		else
		{
			p = Marshal.AllocHGlobal(new IntPtr(size));
			if (p == IntPtr.Zero)
				throw new OutOfMemoryException();
		}

		if (zeroedOut)
			Utils.ZeroMemory((byte*)p, size);

#if TEST_BUILD
		if (!zeroedOut)
			Utils.FillMemory((byte*)p, size, 0xcd);

		ulong* up = (ulong*)p;
		up[1] = (ulong)size;
		up[0] = corruptionDetectionValue;
		up = (ulong*)((byte*)p + size - 8);
		up[0] = corruptionDetectionValue;
#if HUNT_CORRUPT
		lock (intPtrs)
		{
			intPtrs.Add(p, new StackTrace(true).ToString());
		}
#endif
		return p + 16;
#else
		return p;
#endif
	}

	public unsafe static void Free(IntPtr p)
	{
#if TEST_BUILD
#if HUNT_CORRUPT
		lock (intPtrs)
		{
			intPtrs.Remove(p - 16, out string st);
		}
#endif
		ulong* up = (ulong*)(p - 16);
		if (up[0] != corruptionDetectionValue)
			throw new CriticalDatabaseException();

		long size = (long)up[1];
		up = (ulong*)((byte*)p + size - 24);

		if (up[0] != corruptionDetectionValue)
			throw new CriticalDatabaseException();

		p = p - 16;
		Utils.FillMemory((byte*)p, size, 0xec);
#endif

		if (OperatingSystem.IsWindows())
		{
			FreeWindows(p);
		}
		else
		{
			Marshal.FreeHGlobal(p);
		}
	}

	private unsafe static IntPtr AllocateWindows(long size)
	{
		size += sizeof(long);
		Interlocked.Add(ref totalAllocated, size);

		IntPtr p;
		WindowsAllocationType allocType;
		if (size < largeHeapAllocTreshold)
		{
			p = Marshal.AllocHGlobal(new IntPtr(size));
			allocType = WindowsAllocationType.SmallHeap;
		}
		else if (size < virtualAllocTreshold)
		{
			lock (largeHeapAllocSync)
			{
				p = Marshal.AllocHGlobal(new IntPtr(size));
			}

			allocType = WindowsAllocationType.LargeHeap;
		}
		else
		{
			p = VirtualAllocEx(processHandle, IntPtr.Zero, new IntPtr(size),
				AllocationType.Commit | AllocationType.Reserve, MemoryProtection.ReadWrite);
			allocType = WindowsAllocationType.VirtualAlloc;
		}

		if (p == IntPtr.Zero)
			throw new OutOfMemoryException();

		*((long*)p) = (long)allocType | (long)(size << 2);
		return IntPtr.Add(p, 8);
	}

	private unsafe static void FreeWindows(IntPtr p)
	{
		p = IntPtr.Add(p, -8);

		WindowsAllocationType allocType = (WindowsAllocationType)(*(long*)p & 0x03);
		long size = *(long*)p >> 2;

		Interlocked.Add(ref totalAllocated, -size);

		if (allocType == WindowsAllocationType.SmallHeap)
		{
			Marshal.FreeHGlobal(p);
		}
		else if (allocType == WindowsAllocationType.LargeHeap)
		{
			lock (largeHeapAllocSync)
			{
				Marshal.FreeHGlobal(p);
			}
		}
		else
		{
			VirtualFreeEx(processHandle, p, 0, AllocationType.Release);
		}
	}

	private enum WindowsAllocationType : long
	{
		SmallHeap = 1,
		LargeHeap = 2,
		VirtualAlloc = 3
	}
}
