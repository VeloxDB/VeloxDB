namespace Client;

using System;
using System.Runtime.InteropServices;
using System.Security;

internal sealed class LinuxProcessor
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

	public uint MaxCacheLineSize => maxCacheLineSize;
	public int PhysicalCoreCount => physicalCoreCount;
	public int LogicalCoreCount => logicalToPhsicalCore.Length;

	public uint GetCurrentNativeThreadId()
	{
		return (uint)syscall(SYS_gettid);
	}

	public int GetExecutingPhysicalCore()
	{
		int result = sched_getcpu();
		if (result == -1)
		{
			throw new NotSupportedException("sched_getcpu is not supported on this platform");
		}

		return logicalToPhsicalCore[result];
	}

	public int GetExecutingLogicalCore()
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
