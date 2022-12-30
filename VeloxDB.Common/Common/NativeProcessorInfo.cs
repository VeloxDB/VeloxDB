using System;
using System.Linq;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security;

namespace VeloxDB.Common;

internal unsafe static class NativeProcessorInfo
{
	static readonly Processor processor;

#if TEST_BUILD
	public static bool ForceSingleCore;
#endif

	static NativeProcessorInfo()
	{
		if (OperatingSystem.IsWindows())
		{
			processor = new WindowsProcessor();
			return;
		}

		if (OperatingSystem.IsLinux())
		{
			processor = new LinuxProcessor();
			return;
		}

		if (OperatingSystem.IsMacOS())
		{
			processor = new OSXProcessor();
			return;
		}

		throw Utils.OSNotSupportedException();
	}

	public static int PhysicalCoreCount
	{
		get
		{
#if TEST_BUILD
			if (ForceSingleCore)
				return 1;
#endif

			return processor.PhysicalCoreCount;
		}
	}

	public static int LogicalCoreCount
	{
		get
		{
#if TEST_BUILD
			if (ForceSingleCore)
				return 1;
#endif

			return processor.LogicalCoreCount;
		}
	}

	public static int GetExecutingPhysicalCore()
	{
#if TEST_BUILD
		if (ForceSingleCore)
			return 0;
#endif

		return processor.GetExecutingPhysicalCore();
	}

	public static int GetExecutingLogicalCore()
	{
#if TEST_BUILD
		if (ForceSingleCore)
			return 0;
#endif

		return processor.GetExecutingLogicalCore();
	}

	public static uint GetCurrentNativeThreadId() => processor.GetCurrentNativeThreadId();
	public static uint MaxCacheLineSize => processor.MaxCacheLineSize;
}

internal abstract class Processor
{
	public abstract uint MaxCacheLineSize { get; }
	public abstract int PhysicalCoreCount { get; }
	public abstract int LogicalCoreCount { get; }
	public abstract int GetExecutingPhysicalCore();
	public abstract int GetExecutingLogicalCore();
	public abstract uint GetCurrentNativeThreadId();
}

internal unsafe sealed class WindowsProcessor : Processor
{
	[DllImport("kernel32.dll", SetLastError = true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern bool GetLogicalProcessorInformationEx(LOGICAL_PROCESSOR_RELATIONSHIP RelationshipType,
		IntPtr Buffer, ref uint ReturnLength);

	[DllImport("kernel32.dll", SetLastError = true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern int GetCurrentProcessorNumberEx(out PROCESSOR_NUMBER ProcNumber);

	[DllImport("kernel32.dll")]
	[SuppressUnmanagedCodeSecurity]
	private static extern uint GetCurrentThreadId();

	private enum PROCESSOR_CACHE_TYPE
	{
		CacheUnified = 0,
		CacheInstruction = 1,
		CacheData = 2,
		CacheTrace = 3
	}

	[StructLayout(LayoutKind.Sequential)]
	private struct PROCESSOR_NUMBER
	{
		public short Group;
		public byte Number;
		public byte Reserved;
	}

	[StructLayout(LayoutKind.Sequential)]
	private struct GROUP_AFFINITY
	{
		public UIntPtr Mask;
		public short Group;
		public fixed short Reserved[3];
	}

	[StructLayout(LayoutKind.Sequential)]
	private struct CACHE_RELATIONSHIP
	{
		public byte Level;
		public byte Associativity;
		public short LineSize;
		public uint CacheSize;
		public PROCESSOR_CACHE_TYPE Type;
		public fixed byte Reserved[20];
		public GROUP_AFFINITY GroupMask;
	}

	[StructLayout(LayoutKind.Sequential)]
	private struct PROCESSOR_RELATIONSHIP
	{
		public byte Flags;
		public fixed byte Reserved[21];
		public short GroupCount;
		public GROUP_AFFINITY GroupMask;
	}

	[StructLayout(LayoutKind.Sequential)]
	private struct PROCESSOR_GROUP_INFO
	{
		public byte MaximumProcessorCount;
		public byte ActiveProcessorCount;
		public fixed byte Reserved[38];
		UIntPtr ActiveProcessorMask;
	}

	[StructLayout(LayoutKind.Sequential)]
	private struct GROUP_RELATIONSHIP
	{
		public short MaximumGroupCount;
		public short ActiveGroupCount;
		public fixed byte Reserved[20];
		public PROCESSOR_GROUP_INFO GroupInfo;
	}

	private enum LOGICAL_PROCESSOR_RELATIONSHIP
	{
		RelationProcessorCore = 0,
		RelationNumaNode = 1,
		RelationCache = 2,
		RelationProcessorPackage = 3,
		RelationGroup = 4,
		RelationAll = 0xffff
	}

	[StructLayout(LayoutKind.Explicit)]
	private struct SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX
	{
		[FieldOffset(0)]
		public LOGICAL_PROCESSOR_RELATIONSHIP Relationship;

		[FieldOffset(4)]
		public uint Size;

		[FieldOffset(8)]
		public PROCESSOR_RELATIONSHIP Processor;

		[FieldOffset(8)]
		public GROUP_RELATIONSHIP Group;

		[FieldOffset(8)]
		public CACHE_RELATIONSHIP Cache;
	}

	private const int ERROR_INSUFFICIENT_BUFFER = 122;

	int coresPerGroup;
	short[] logicalToPhsicalMapping;

	int physicalCoreCount;
	int logicalCoreCount;
	uint maxCacheLineSize;

	public WindowsProcessor()
	{
		coresPerGroup = IntPtr.Size * 8;

		SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX* pcores = GetProcessorInfo(
			LOGICAL_PROCESSOR_RELATIONSHIP.RelationProcessorCore, out uint coresSize);

		Dictionary<int, int> d = new Dictionary<int, int>();
		uint currOffset = 0;
		physicalCoreCount = 0;
		int maxLogCore = -1;
		while (currOffset < coresSize)
		{
			SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX* curr =
				(SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*)((byte*)pcores + currOffset);

			List<int> l = GetLogicalCoresFromMask(curr->Processor.GroupMask.Mask);
			int group = curr->Processor.GroupMask.Group;
			for (int i = 0; i < l.Count; i++)
			{
				int logCore = group * coresPerGroup + l[i];
				maxLogCore = Math.Max(logCore, maxLogCore);
				d.Add(logCore, physicalCoreCount);
			}

			currOffset += curr->Size;
			physicalCoreCount++;
		}

		logicalToPhsicalMapping = new short[maxLogCore + 1];
		logicalCoreCount = logicalToPhsicalMapping.Length;
		foreach (KeyValuePair<int, int> kv in d)
		{
			logicalToPhsicalMapping[kv.Key] = (short)kv.Value;
		}

		SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX* pcaches = GetProcessorInfo(
			LOGICAL_PROCESSOR_RELATIONSHIP.RelationCache, out uint cacheStructSize);

		currOffset = 0;
		maxCacheLineSize = 32;
		while (currOffset < cacheStructSize)
		{
			SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX* curr =
				(SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*)((byte*)pcaches + currOffset);

			maxCacheLineSize = Math.Max(maxCacheLineSize, (uint)curr->Cache.LineSize);
			currOffset += curr->Size;
		}
	}

	public override uint MaxCacheLineSize => maxCacheLineSize;
	public override int PhysicalCoreCount => physicalCoreCount;
	public override int LogicalCoreCount => logicalCoreCount;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public override uint GetCurrentNativeThreadId()
	{
		return GetCurrentThreadId();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public override int GetExecutingPhysicalCore()
	{
		GetCurrentProcessorNumberEx(out PROCESSOR_NUMBER procNum);
		return logicalToPhsicalMapping[procNum.Group * coresPerGroup + procNum.Number];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public override int GetExecutingLogicalCore()
	{
		GetCurrentProcessorNumberEx(out PROCESSOR_NUMBER procNum);
		return procNum.Group * coresPerGroup + procNum.Number;
	}

	private List<int> GetLogicalCoresFromMask(UIntPtr mask)
	{
		long lmask = (long)mask;
		List<int> l = new List<int>(2);
		for (int i = 0; i < coresPerGroup; i++)
		{
			if ((((long)1 << i) & lmask) != 0)
				l.Add(i);
		}

		return l;
	}

	private static SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX* GetProcessorInfo(LOGICAL_PROCESSOR_RELATIONSHIP relation,
		out uint size)
	{
		size = 0;
		GetLogicalProcessorInformationEx(relation, IntPtr.Zero, ref size);
		if (Marshal.GetLastWin32Error() != ERROR_INSUFFICIENT_BUFFER)
			throw new Win32Exception(Marshal.GetLastWin32Error());

		IntPtr ptr = Marshal.AllocHGlobal((int)size);
		GetLogicalProcessorInformationEx(relation, ptr, ref size);
		return (SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*)ptr;
	}
}

internal sealed class LinuxProcessor : Processor
{
	long SYS_gettid;
	int physicalCoreCount;
	uint maxCacheLineSize;
	short[] logicalToPhsicalCore;

	public LinuxProcessor()
	{
		InitSysCallConstants();
		ParseSysFS();
	}

	private void ParseSysFS()
	{
		Dictionary<short, short> logToPhys = new Dictionary<short, short>();
		HashSet<short> cores = new HashSet<short>();

		uint maxCacheLineSize = 32;
		foreach (string dir in Directory.GetDirectories("/sys/devices/system/cpu", "cpu*"))
		{
			short logicalId;
			if (!short.TryParse(Path.GetFileName(dir).Substring(3), out logicalId))
			{
				continue;
			}

			string coreIdPath = Path.Combine(dir, "topology/core_id");
			short physicalId = short.Parse(File.ReadAllText(coreIdPath));

			logToPhys.Add(logicalId, physicalId);
			cores.Add(physicalId);

			string cacheSizePath = Path.Combine(dir, "cache/index0/coherency_line_size");

			if (File.Exists(cacheSizePath) && uint.TryParse(File.ReadAllText(cacheSizePath), out uint cacheSize))
			{
				maxCacheLineSize = Math.Max(maxCacheLineSize, cacheSize);
			}
		}

		this.physicalCoreCount = cores.Count;
		this.maxCacheLineSize = maxCacheLineSize;
		this.logicalToPhsicalCore = new short[logToPhys.Keys.Max() + 1];

		foreach (var pair in logToPhys)
		{
			this.logicalToPhsicalCore[pair.Key] = pair.Value;
		}
	}

	private void InitSysCallConstants()
	{
		if (RuntimeInformation.ProcessArchitecture == Architecture.X64)
		{
			SYS_gettid = 186;
			return;
		}

		if (RuntimeInformation.ProcessArchitecture == Architecture.X86)
		{
			SYS_gettid = 224;
			return;
		}

		if (RuntimeInformation.ProcessArchitecture == Architecture.Arm64 ||
			RuntimeInformation.ProcessArchitecture == Architecture.Arm)
		{
			SYS_gettid = 178;
			return;
		}
	}

	public override uint MaxCacheLineSize => maxCacheLineSize;
	public override int PhysicalCoreCount => physicalCoreCount;
	public override int LogicalCoreCount => logicalToPhsicalCore.Length;

	public override uint GetCurrentNativeThreadId()
	{
		return (uint)syscall(SYS_gettid);
	}

	public override int GetExecutingPhysicalCore()
	{
		int result = sched_getcpu();
		if (result == -1)
		{
			throw new NotSupportedException("sched_getcpu is not supported on this platform");
		}

		return logicalToPhsicalCore[result];
	}

	public override int GetExecutingLogicalCore()
	{
		int result = sched_getcpu();
		if (result == -1)
		{
			throw new NotSupportedException("sched_getcpu is not supported on this platform");
		}

		return result;
	}

	private const string libc = "libc";

	[DllImport(libc, SetLastError = true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern long syscall(long number);

	[DllImport(libc, SetLastError = true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern int sched_getcpu();
}

internal sealed class OSXProcessor : Processor
{
	long SYS_thread_selfid;
	int physicalCoreCount;
	uint maxCacheLineSize;
	public override uint MaxCacheLineSize => maxCacheLineSize;
	public override int PhysicalCoreCount => 1; // OSC currently does not support getting CPU core
	public override int LogicalCoreCount => 1;  // OSC currently does not support getting CPU core

	public OSXProcessor()
	{
		SYS_thread_selfid = 372;
		physicalCoreCount = BitConverter.ToInt32(GetSystemInfo("hw.physicalcpu_max"), 0);
		maxCacheLineSize = BitConverter.ToUInt32(GetSystemInfo("hw.cachelinesize"), 0);
	}

	[DllImport("libSystem.dylib", SetLastError = true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern long syscall(long number);

	[DllImport("libSystem.dylib", SetLastError = true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern int sysctlbyname(string property, byte[] output, ref Int64 oldLen, IntPtr newp, uint newlen);

	private byte[] GetSystemInfo(string property)
	{
		Int64 len = 0L;
		byte[] val;
		int status = sysctlbyname(property, null, ref len, IntPtr.Zero, 0);
		if (status != 0)
		{
			throw new NotSupportedException($"sysctlbyname({property}) Buffer Length is not supported on this platform");
		}
		val = new byte[(Int64)len];
		status = sysctlbyname(property, val, ref len, IntPtr.Zero, 0);
		if (status != 0)
		{
			throw new NotSupportedException($"sysctlbyname({property}) is not supported on this platform");
		}
		return val;
	}

	public override uint GetCurrentNativeThreadId()
	{
		return (uint)syscall(SYS_thread_selfid);
	}

	public override int GetExecutingPhysicalCore()
	{
		// TODO
		// Not implemented. OSX doesn't have cpuid helper function. We can make unamnaged DLL with ASM wrapper and
		// PInvoke it or embedd ASM into c#
		return 0;
	}

	public override int GetExecutingLogicalCore()
	{
		// TODO
		// Not implemented. OSX doesn't have cpuid helper function. We can make unamnaged DLL with ASM wrapper and
		// PInvoke it or embedd ASM into c#
		return 0;
	}
}
