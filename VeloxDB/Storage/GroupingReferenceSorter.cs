using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Velox.Common;

namespace Velox.Storage;

unsafe class GroupingReferenceSorter : IDisposable
{
	const int groupBitCount = 8;
	public const int GroupCount = 1 << groupBitCount;
	const int groupMask = GroupCount - 1;

	long count;
	InverseReferenceOperation* refs;

	Group* groups;

	public static int GetGroup(InverseReferenceOperation* p)
	{
		return (int)(((ulong)p->directReference * HashUtils.PrimeMultiplier64) & groupMask);
	}

	public void SetRefs(InverseReferenceOperation* refs, long count, long[] groupCounts)
	{
		this.count = count;
		this.refs = refs;

		long s = 0;
		groups = (Group*)Marshal.AllocHGlobal(new IntPtr(Group.Size * GroupCount));
		for (int i = 0; i < GroupCount; i++)
		{
			s += groupCounts[i];
			groups[i].p = (InverseReferenceOperation*)AlignedAllocator.Allocate(groupCounts[i] * InverseReferenceOperation.Size, false);
			groups[i].count = 0;
		}

		if (s != count)
			throw new CriticalDatabaseException();
	}

	public void Dispose()
	{
		if (groups == null)
			return;

		for (int i = 0; i < GroupCount; i++)
		{
			AlignedAllocator.Free((IntPtr)groups[i].p);
		}

		AlignedAllocator.Free((IntPtr)groups);
		groups = null;
	}

	public void Sort()
	{
		Range[] ranges = SplitRange(count, 1, Environment.ProcessorCount);

		Task[] tasks = new Task[ranges.Length];
		for (int i = 0; i < ranges.Length; i++)
		{
			Task t = new Task(o =>
			{
				Range r = ranges[(int)o];
				MGroup*[] mgs = new MGroup*[GroupCount];
				for (int j = 0; j < mgs.Length; j++)
				{
					mgs[j] = MGroup.Create();
				}

				for (long j = r.Offset; j < r.Offset + r.Count; j++)
				{
					InverseReferenceOperation* pc = refs + j;
					int n = GetGroup(pc);
					mgs[n]->Add(pc, groups + n);
				}

				for (int j = 0; j < mgs.Length; j++)
				{
					mgs[j]->Empty(groups + j);
					MGroup.Destroy(mgs[j]);
				}
			}, i);

			tasks[i] = t;
			t.Start();
		}

		Task.WaitAll(tasks);

		long[] groupOffsets = new long[GroupCount];
		int s = 0;
		for (int i = 0; i < GroupCount; i++)
		{
			groupOffsets[i] = s;
			s += groups[i].count;
		}

		tasks = new Task[GroupCount];
		for (int i = 0; i < GroupCount; i++)
		{
			Task t = new Task(o =>
			{
				int n = (int)o;
				ReferenceSorter s = new ReferenceSorter(InverseComparer.Instance);
				Group* pg = groups + n;
				s.Sort(pg->p, pg->count, true);

				long offset = groupOffsets[n];
				for (int j = 0; j < pg->count; j++)
				{
					InverseReferenceOperation* rc = pg->p + j;
					refs[offset++] = *rc;
				}
			}, i);

			tasks[i] = t;
			t.Start();
		}

		Task.WaitAll(tasks);
	}

	public static Range[] SplitRange(long capacity, long countPerRange, int maxRanges)
	{
		if (capacity == 0)
			return new Range[0];

		long rangeCount = Math.Min(maxRanges, Math.Min(capacity, capacity / countPerRange + 1));
		double cpr = (double)capacity / rangeCount;
		Range[] ranges = new Range[rangeCount];
		double s = 0.0f;
		for (int i = 0; i < rangeCount; i++)
		{
			double r1 = s;
			double r2 = s + cpr;
			ranges[i] = new Range((long)r1, (long)r2 - (long)r1);
			s = r2;
		}

		ranges[ranges.Length - 1].Count = capacity - ranges[ranges.Length - 1].Offset;

		return ranges;
	}

	[StructLayout(LayoutKind.Sequential, Pack = 1, Size = Group.Size)]
	private struct Group
	{
		public const int Size = 12;

		public InverseReferenceOperation* p;
		public int count;

		public InverseReferenceOperation* SafeAdd(int count)
		{
			return p + (Interlocked.Add(ref this.count, count) - count);
		}
	}

	private struct MGroup
	{
		const int capacity = 16;

		public InverseReferenceOperation* p;
		public int count;

		public static MGroup* Create()
		{
			MGroup* p = (MGroup*)AlignedAllocator.Allocate(16 + capacity * InverseReferenceOperation.Size, false);
			p->count = 0;
			p->p = (InverseReferenceOperation*)((byte*)p + 16);

			return p;
		}

		public static void Destroy(MGroup* p)
		{
			AlignedAllocator.Free((IntPtr)p);
		}

		public void Add(InverseReferenceOperation* c, Group* group)
		{
			p[count++] = *c;
			if (count == capacity)
				Empty(group);
		}

		public void Empty(Group* group)
		{
			if (count == 0)
				return;

			InverseReferenceOperation* pc = group->SafeAdd(count);
			for (int i = 0; i < count; i++)
			{
				pc[i] = p[i];
			}

			count = 0;
		}
	}

	public struct Range
	{
		public long Offset { get; set; }
		public long Count { get; set; }

		public Range(long offset, long count)
		{
			this.Offset = offset;
			this.Count = count;
		}
	}
}
