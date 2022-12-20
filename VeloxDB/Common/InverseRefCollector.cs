using System;
using System.Threading;
using VeloxDB.Storage;

namespace VeloxDB.Common;

internal unsafe sealed class InverseRefCollector
{
	readonly object sync = new object();
	readonly ReaderWriterLockSlim drainSync = new ReaderWriterLockSlim();

	NativeList list;
	long[] groupCounts;

	public InverseRefCollector(NativeList list)
	{
		this.list = list;
		groupCounts = new long[GroupingReferenceSorter.GroupCount];
	}

	public NativeList List => list;
	public unsafe long[] GroupCounts => groupCounts;

	public void Add(NativeList other, int* groupCounts)
	{
		NativeList temp = null;
		byte* bp;
		lock (sync)
		{
			if (groupCounts != null)
			{
				for (int i = 0; i < GroupingReferenceSorter.GroupCount; i++)
				{
					this.groupCounts[i] += groupCounts[i];
				}
			}

			if (list.Count + other.Count > list.Capacity)
			{
				// Drain threads that reserved the range but haven't yet copied the data in it
				drainSync.EnterWriteLock();
				drainSync.ExitWriteLock();

				temp = list;
				list = new NativeList(Math.Max(list.Capacity * 2, list.Capacity + other.Count), list.ItemSize, list.Count);
			}

			drainSync.EnterReadLock(); // Prevent resize from the point of taking the range to the point where data is copied.
			bp = list.AddRangeNoResize(other.Count);
		}

		if (temp != null)
			Utils.CopyMemory(temp.Buffer, list.Buffer, temp.Count * temp.ItemSize);

		Utils.CopyMemory(other.Buffer, bp, other.Count * other.ItemSize);
		drainSync.ExitReadLock();
	}
}
