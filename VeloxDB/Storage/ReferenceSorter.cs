using System;
using System.Runtime.CompilerServices;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal interface IInverseReferenceComparer
{
	unsafe bool IsLessThan(InverseReferenceOperation* p, InverseReferenceOperation pivotRef);
	unsafe bool IsGreaterThan(InverseReferenceOperation* p, InverseReferenceOperation pivotRef);
}

internal sealed class PropagatedComparer : IInverseReferenceComparer
{
	public static readonly PropagatedComparer Instance = new PropagatedComparer();

	public unsafe bool IsLessThan(InverseReferenceOperation* p, InverseReferenceOperation pivotRef)
	{
		if (p->inverseReference < pivotRef.inverseReference)
			return true;

		if (p->inverseReference > pivotRef.inverseReference)
			return false;

		if (p->Type < pivotRef.Type)
			return true;

		if (p->Type > pivotRef.Type)
			return false;

		// We need to sort by property id as well because if propagation type is only SetToNull for this inverseReference
		// we want to generate propagation operations property by property.
		if (p->PropertyId < pivotRef.PropertyId)
			return true;

		if (p->PropertyId > pivotRef.PropertyId)
			return false;

		return p->directReference < pivotRef.directReference;
	}

	public unsafe bool IsGreaterThan(InverseReferenceOperation* p, InverseReferenceOperation pivotRef)
	{
		if (p->inverseReference > pivotRef.inverseReference)
			return true;

		if (p->inverseReference < pivotRef.inverseReference)
			return false;

		if (p->Type > pivotRef.Type)
			return true;

		if (p->Type < pivotRef.Type)
			return false;

		// We need to sort by property id as well because if propagation type is only SetToNull for this inverseReference
		// we want to generate propagation operations property by property.
		if (p->PropertyId > pivotRef.PropertyId)
			return true;

		if (p->PropertyId < pivotRef.PropertyId)
			return false;

		return p->directReference > pivotRef.directReference;
	}
}

internal sealed class InverseComparer : IInverseReferenceComparer
{
	public static readonly InverseComparer Instance = new InverseComparer();

	public unsafe bool IsLessThan(InverseReferenceOperation* p, InverseReferenceOperation pivotRef)
	{
		if (p->directReference < pivotRef.directReference)
			return true;

		if (p->directReference > pivotRef.directReference)
			return false;

		long v1 = p->PropId_opType;
		long v2 = pivotRef.PropId_opType;
		if (v1 != v2)
			return v1 < v2;

		return p->inverseReference < pivotRef.inverseReference;
	}

	public unsafe bool IsGreaterThan(InverseReferenceOperation* p, InverseReferenceOperation pivotRef)
	{
		if (p->directReference > pivotRef.directReference)
			return true;

		if (p->directReference < pivotRef.directReference)
			return false;

		long v1 = p->PropId_opType;
		long v2 = pivotRef.PropId_opType;
		if (v1 != v2)
			return v1 > v2;

		return p->inverseReference > pivotRef.inverseReference;
	}
}

internal unsafe sealed class ReferenceSorter
{
	const int parallelLimit = 32 * 1024;
	const int insertionLimit = 16;

	ManualResetEvent finished;
	volatile int activeWorkerCount;

	long count;
	InverseReferenceOperation* refs;

	IInverseReferenceComparer comparer;
	bool forceNonParallel;

	public ReferenceSorter(IInverseReferenceComparer comparer)
	{
		this.comparer = comparer;
		finished = new ManualResetEvent(false);
	}

	public void Sort(InverseReferenceOperation* refs, long count, bool forceNonParallel = false)
	{
		if (count <= 1)
			return;

		this.count = count;
		this.refs = refs;
		this.forceNonParallel = forceNonParallel;

		activeWorkerCount = 0;
		Sort(0, count - 1);
		if (activeWorkerCount != 0)
			finished.WaitOne();
	}

	private void Sort(long left, long right)
	{
		if (left >= right)
			return;

		long count = right - left + 1;
		if (count <= insertionLimit)
		{
			InsertionSort(left, right);
		}
		else if (count <= parallelLimit || forceNonParallel)
		{
			Quicksort(left, right);
		}
		else
		{
			if (activeWorkerCount == 0)
				finished.Reset();

			Interlocked.Increment(ref activeWorkerCount);
			ThreadPool.UnsafeQueueUserWorkItem(SortWorker, new Utils.Range(left, right - left + 1));
		}
	}

	private void Quicksort(long left, long right)
	{
		long low = left, high = right;
		InverseReferenceOperation pivotRef = refs[(left + right) >> 1];

		while (low <= high)
		{
			while (comparer.IsLessThan(refs + low, pivotRef))
				low++;

			while (comparer.IsGreaterThan(refs + high, pivotRef))
				high--;

			if (low <= high)
			{
				ExchangeAtIndexes(low, high);
				low++;
				high--;
			}
		}

		Sort(left, high);
		Sort(low, right);
	}

	private void InsertionSort(long left, long right)
	{
		for (long i = left + 1; i <= right; i++)
		{
			InverseReferenceOperation x = refs[i];

			long j = i;
			while (j > 0 && comparer.IsGreaterThan(refs + (j - 1), x))
			{
				refs[j] = refs[j - 1];
				j--;
			}

			refs[j] = x;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void ExchangeAtIndexes(long i, long j)
	{
		InverseReferenceOperation temp = refs[i];
		refs[i] = refs[j];
		refs[j] = temp;
	}

	private void SortWorker(object state)
	{
		Utils.Range item = (Utils.Range)state;
		Quicksort(item.Offset, item.Offset + item.Count - 1);
		if (Interlocked.Decrement(ref activeWorkerCount) == 0)
			finished.Set();
	}

	public void Dispose()
	{
		finished.Dispose();
	}
}
