using System;
using System.Linq;
using System.Collections.Generic;
using VeloxDB.Common;

namespace VeloxDB.Common;

#if TEST_BUILD
internal unsafe sealed partial class StringStorage
{
	internal void ValidateFreeList()
	{
		Dictionary<ulong, ulong> free = new Dictionary<ulong, ulong>(4096);
		for (int i = 0; i < perCPUData.Length; i++)
		{
			perCPUData[i]->ValidateFreeList(free, values);
		}
	}

	internal List<ulong> GetFreeSlots()
	{
		List<ulong> l = new List<ulong>(128);
		for (int i = 0; i < perCPUData.Length; i++)
		{
			l.AddRange(perCPUData[i]->GetFreeSlots(values));
		}

		return l;
	}

	internal void ValidateRefCounts(Dictionary<ulong, int> refCountValues)
	{
		Dictionary<ulong, int> d = CollectAndValidateRefCounts();
		foreach (KeyValuePair<ulong, int> kv in d)
		{
			if (kv.Key >= 2)
			{
				if (!refCountValues.TryGetValue(kv.Key, out int rc) || rc != kv.Value)
					throw new InvalidOperationException();
			}

			refCountValues.Remove(kv.Key);
		}

		if (refCountValues.Count > 0)
			throw new InvalidOperationException(string.Format("Invalid string id {0}.", refCountValues.Keys.First()));
	}

	internal Dictionary<ulong, int> CollectAndValidateRefCounts()
	{
		Dictionary<ulong, int> d = new Dictionary<ulong, int>(1024);
		HashSet<ulong> free = new HashSet<ulong>(1024);
		for (int i = 0; i < perCPUData.Length; i++)
		{
			perCPUData[i]->CollectFree(values, free);
		}

		freeLists.CollectFree(values, free);

		if (values[0].Value != null || values[1].Value != string.Empty)
			throw new InvalidOperationException();

		for (ulong i = 0; i < values.Length; i++)
		{
			string v = values[i].Value;
			int refCount = (int)values[i].RefCount;

			if (!free.Contains(i))
			{
				if (i >= ReservedCount)
				{
					if (v == null && refCount > 0 || refCount == 0 && v != null)
						throw new InvalidOperationException();
				}

				d.Add(i, refCount);
			}
		}

		return d;
	}

	private partial class FreeStringSlotLists
	{
		internal void CollectFree(RefCountedStringArray values, HashSet<ulong> free)
		{
			for (int i = 0; i < count; i++)
			{
				ulong currFree = lists[i].list;
				int c = 0;
				while (currFree != ulong.MaxValue)
				{
					if (free.Contains(currFree))
						throw new InvalidOperationException();

					free.Add(currFree);
					currFree = values[currFree].Next;
					c++;
				}

				Checker.AssertTrue(c == freeListLimit);
			}
		}
	}

	private unsafe partial struct PerCPUData
	{
		internal void ValidateFreeList(Dictionary<ulong, ulong> free, RefCountedStringArray values)
		{
			ulong currFree = freeList.list;
			long c = 0;
			while (currFree != ulong.MaxValue)
			{
				if (currFree < ReservedCount)
					throw new InvalidOperationException();

				free.Add(currFree, 0);
				currFree = values[currFree].Next;
				c++;
			}

			Checker.AssertTrue(c == freeList.count);
		}

		internal List<ulong> GetFreeSlots(RefCountedStringArray values)
		{
			List<ulong> l = new List<ulong>(128);
			ulong t = freeList.list;
			while (t != ulong.MaxValue)
			{
				l.Add(t);
				t = (ulong)values[t].Next;
			}

			return l;
		}

		internal void CollectFree(RefCountedStringArray values, HashSet<ulong> free)
		{
			ulong currFree = freeList.list;
			while (currFree != ulong.MaxValue)
			{
				if (free.Contains(currFree))
					throw new InvalidOperationException();

				free.Add(currFree);
				currFree = values[currFree].Next;
			}
		}
	}
}
#endif
