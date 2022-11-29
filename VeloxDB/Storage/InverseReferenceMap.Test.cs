using System;
using System.Collections.Generic;
using System.Diagnostics;
using Velox.Common;

namespace Velox.Storage;

internal unsafe sealed partial class InverseReferenceMap
{
	[Conditional("TEST_BUILD")]
	public void ValidateUsedBucketCount()
	{
		int cbc = CountUsedBuckets();
		if (cbc != resizeCounter.Count)
			throw new InvalidOperationException();
	}

	public int CountUsedBuckets()
	{
		int c = 0;
		for (int i = 0; i < capacity; i++)
		{
			Bucket* bn = buckets + i;
			if (bn->Handle != 0)
				c++;
		}

		return c;
	}

#if TEST_BUILD
	public void ValidateGarbage(ulong readVersion, bool checkEmpty)
	{
		TTTrace.Write(database.TraceId);

		List<uint> res = new List<uint>();
		for (int i = 0; i < capacity; i++)
		{
			Bucket* bn = buckets + i;

			ulong handle = bn->Handle;
			InvRefBaseItem* bitem = (InvRefBaseItem*)memoryManager.GetBuffer(bn->Handle);

			while (bitem != null)
			{
				InvRefBaseItem* bitem2 = bitem;
				while (bitem2 != null && bitem->Version > readVersion)
				{
					bitem2 = (InvRefBaseItem*)memoryManager.GetBuffer(bitem2->nextBase);
				}

				if (bitem2 != null)
				{
					if (bitem2->IsDeleted)
						throw new InvalidOperationException();

					if (checkEmpty && (bitem2->nextDelta | (uint)bitem2->Count | bitem2->readerInfo.lockCount_slotCount) == 0 &&
						bitem2->readerInfo.CommReadLockVer <= readVersion)
					{
						throw new InvalidOperationException();
					}

					if (bitem2->nextBase != 0)
						throw new InvalidOperationException();
				}

				bitem = (InvRefBaseItem*)memoryManager.GetBuffer(bitem->nextCollision);
			}
		}
	}

	public bool ContainsIdAndPropIdOrDeleted(long id, int propId)
	{
		long bucket = CalculateBucket(id, propId);
		Bucket* bn = buckets + bucket;

		InvRefBaseItem* item = null;

		ulong curr = bn->Handle;
		while (curr != 0)
		{
			InvRefBaseItem* currItem = (InvRefBaseItem*)memoryManager.GetBuffer(curr);
			if (currItem->id == id && currItem->IsDeleted)
				return true;

			if (currItem->id == id && currItem->propertyId == propId)
				item = currItem;

			curr = currItem->nextCollision;
		}

		return item != null;
	}

	public void ValidateInverseRefs(Transaction tran, Dictionary<ValueTuple<long, int>, List<long>> invRefs)
	{
		TTTrace.Write(database.TraceId);

		int refCount;
		long[] refs = new long[128];

		int currUsedBucketCount = 0;

		for (int i = 0; i < capacity; i++)
		{
			Bucket* bn = buckets + i;
			InvRefBaseItem* bitem = (InvRefBaseItem*)memoryManager.GetBuffer(bn->Handle);
			if (bitem != null)
				currUsedBucketCount++;

			while (bitem != null)
			{
				bool isTracked;
				GetReferences(tran, bitem->id, bitem->propertyId, ref refs, out refCount, out isTracked);

				InvRefBaseItem* vitem = bitem;
				while (vitem != null)
				{
					if (vitem->readerInfo.LockCount > 0)
						throw new InvalidOperationException();

					if (Database.IsUncommited(vitem->Version))
						throw new InvalidOperationException();

					vitem = (InvRefBaseItem*)memoryManager.GetBuffer(vitem->nextBase);
				}

				ValidateSingleInverseRef(invRefs, refCount, refs, bitem, isTracked);

				bitem = (InvRefBaseItem*)memoryManager.GetBuffer(bitem->nextCollision);
			}
		}

		if (resizeCounter.Count != currUsedBucketCount)
		{
			throw new InvalidOperationException();
		}
	}

	private static void ValidateSingleInverseRef(Dictionary<ValueTuple<long, int>, List<long>> invRefs, int refCount, long[] refs, InvRefBaseItem* bitem, bool isTracked)
	{
		if (isTracked)
		{
			if (refCount == 0)
			{
				if (invRefs.ContainsKey(new ValueTuple<long, int>(bitem->id, bitem->propertyId)))
					throw new InvalidOperationException();
			}
			else
			{
				List<long> validRefs = invRefs[new ValueTuple<long, int>(bitem->id, bitem->propertyId)];
				invRefs.Remove(new ValueTuple<long, int>(bitem->id, bitem->propertyId));
				if (!SetsEqual(refs, refCount, validRefs))
					throw new InvalidOperationException();
			}
		}
		else
		{
			if (refCount == 0)
			{
				if (invRefs.ContainsKey(new ValueTuple<long, int>(bitem->id, bitem->propertyId)))
					throw new InvalidOperationException();
			}
			else
			{
				List<long> validRefs = invRefs[new ValueTuple<long, int>(bitem->id, bitem->propertyId)];
				invRefs.Remove(new ValueTuple<long, int>(bitem->id, bitem->propertyId));
				if (refCount != validRefs.Count)
					throw new InvalidOperationException();
			}
		}
	}

	private static bool SetsEqual(long[] refs, int refCount, List<long> validRefs)
	{
		if (validRefs.Count != refCount)
			return false;

		HashSet<long> hs = new HashSet<long>();
		for (int i = 0; i < refCount; i++)
		{
			hs.Add(refs[i]);
		}

		hs.ExceptWith(validRefs);
		return hs.Count == 0;
	}
#endif
}
