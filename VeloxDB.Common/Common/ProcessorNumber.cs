using System;
using System.Runtime.CompilerServices;

namespace VeloxDB.Common;

internal class ProcessorNumber
{
	const int reuseInterval = 16;

	[ThreadStatic]
	public static ProcessorNumber instance; // Accessing thread local storage is faster than retrieving the current CPU core

	static int coreCount = NativeProcessorInfo.LogicalCoreCount;

	short count;
	short procNum;

	public static int CoreCount => coreCount;

	private ProcessorNumber()
	{
		count = 0;
		procNum = (short)NativeProcessorInfo.GetExecutingLogicalCore();
	}

	public int Core
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		get
		{
			count++;
			if (count >= reuseInterval)
				RefreshInternal();

			return procNum;
		}
	}

	public static ProcessorNumber Instance
	{
		get
		{
			ProcessorNumber pn = instance;
			if (pn != null)
				return pn;

			pn = new ProcessorNumber();
			instance = pn;
			return pn;
		}
	}

	private void RefreshInternal()
	{
		procNum = (short)NativeProcessorInfo.GetExecutingLogicalCore();
		count = 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static int GetCore()
	{
		return Instance.Core;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Refresh()
	{
		Instance.RefreshInternal();
	}
}
