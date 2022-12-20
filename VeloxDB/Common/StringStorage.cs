using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace VeloxDB.Common;

internal unsafe sealed partial class StringStorage : IDisposable
{
	public const int ReservedCount = 2;
	const int freeListLimit = 1024 * 16;

	RefCountedStringArray values;
	PerCPUData*[] perCPUData;

	FreeStringSlotLists freeLists;

	public StringStorage()
	{
		values = new RefCountedStringArray();

		freeLists = new FreeStringSlotLists();

		IntPtr[] p = AlignedAllocator.AllocateMultiple(PerCPUData.Size, ProcessorNumber.CoreCount, true);
		perCPUData = new PerCPUData*[ProcessorNumber.CoreCount];
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			perCPUData[i] = (PerCPUData*)p[i];
			*perCPUData[i] = new PerCPUData(values);
		}

		ulong h = perCPUData[0]->AddString(values, null, 1, freeLists);
		Checker.AssertTrue(h == 0);

		h = perCPUData[0]->AddString(values, string.Empty, 1, freeLists);
		Checker.AssertTrue(h == 1);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool IsNullOrEmpty(ulong handle)
	{
		return handle < ReservedCount;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ulong AddString(string s)
	{
		if (s == null)
			return 0;

		if (s.Length == 0)
			return 1;

		int procNum = ProcessorNumber.GetCore();
		ulong handle = perCPUData[procNum]->AddString(values, s, 1, freeLists);

		TTTrace.Write(handle);
		return handle;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ulong AddStringUnused(string s)
	{
		if (s == null)
			return 0;

		if (s.Length == 0)
			return 1;

		int procNum = ProcessorNumber.GetCore();
		ulong handle = perCPUData[procNum]->AddString(values, s, 0, freeLists);

		TTTrace.Write(handle);
		return handle;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ulong GetStringVersion(ulong handle)
	{
		return values[handle].Version;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetStringVersion(ulong handle, ulong version)
	{
		if (handle < ReservedCount)
			return;

		values.SetVersion(handle, version);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public string GetString(ulong handle)
	{
		return values[handle].Value;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool IsRefCountZero(ulong handle)
	{
		if (handle < ReservedCount)
			return false;

		return values[handle].RefCount == 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void IncRefCount(ulong handle)
	{
		if (handle < ReservedCount)
			return;

		TTTrace.Write(handle);
		values.InterlockedIncrementRefCount(handle);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void DecRefCount(ulong handle)
	{
		if (handle < ReservedCount)
			return;

		TTTrace.Write(handle);
		if (values.InterlockedDecrementRefCount(handle) == 0)
		{
			int procNum = ProcessorNumber.GetCore();
			perCPUData[procNum]->Remove(values, handle, freeLists);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Remove(ulong handle)
	{
		if (handle < ReservedCount)
			return;

		TTTrace.Write(handle);
		int currProcNum = ProcessorNumber.GetCore();
		perCPUData[currProcNum]->Remove(values, handle, freeLists);
	}

	public void Dispose()
	{
		AlignedAllocator.Free((IntPtr)perCPUData[0]);
	}

	[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
	private unsafe partial struct PerCPUData
	{
		public const int Size = StringSlotList.Size + 4;

		StringSlotList freeList;
		RWSpinLock sync;

		public PerCPUData(RefCountedStringArray values)
		{
			sync = new RWSpinLock();
			freeList = StringSlotList.Empty;
			AllocSlots(values);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public ulong AddString(RefCountedStringArray values, string s, long initRefCount, FreeStringSlotLists sharedFreeLists)
		{
			sync.EnterWriteLock();

			if (freeList.list == ulong.MaxValue)
			{
				freeList = sharedFreeLists.Get();
				if (freeList.list == ulong.MaxValue)
					AllocSlots(values);
			}

			ulong handle = freeList.list;
			freeList.list = values[freeList.list].Next;
			freeList.count--;
			values[handle] = new RefCountedStringArray.RefCountedString(s, initRefCount);

			sync.ExitWriteLock();

			return handle;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Remove(RefCountedStringArray values, ulong handle, FreeStringSlotLists sharedFreeLists)
		{
			sync.EnterWriteLock();

			values[handle] = new RefCountedStringArray.RefCountedString(null, (long)freeList.list);
			freeList.list = handle;
			freeList.count++;

			if (freeList.count == freeListLimit)
			{
				sharedFreeLists.Add(freeList);
				freeList = StringSlotList.Empty;
			}

			sync.ExitWriteLock();
		}

		private void AllocSlots(RefCountedStringArray values)
		{
			values.AddChunk(out ulong start, out ulong count);

			freeList.list = start;
			freeList.count = (long)count;
			for (ulong i = start; i < start + count; i++)
			{
				values[i] = new RefCountedStringArray.RefCountedString(null, (long)(i + 1));
			}

			values[start + count - 1] = new RefCountedStringArray.RefCountedString(null, unchecked((long)ulong.MaxValue));
		}
	}

	[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
	private struct StringSlotList
	{
		public const int Size = 16;

		static StringSlotList empty = new StringSlotList(false);

		public ulong list;
		public long count;

		private StringSlotList(bool dummy)
		{
			list = ulong.MaxValue;
			count = 0;
		}

		public static StringSlotList Empty => empty;
	}

	private partial class FreeStringSlotLists
	{
		readonly object sync = new object();

		int count;
		StringSlotList[] lists;

		public FreeStringSlotLists()
		{
			lists = new StringSlotList[64];
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Add(StringSlotList list)
		{
			lock (sync)
			{
				if (lists.Length == count)
					Array.Resize(ref lists, lists.Length * 2);

				lists[count++] = list;
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public StringSlotList Get()
		{
			if (count == 0)
				return StringSlotList.Empty;

			lock (sync)
			{
				if (count == 0)
					return StringSlotList.Empty;

				return lists[--count];
			}
		}
	}
}
