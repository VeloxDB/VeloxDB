using System;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal unsafe abstract class SortedIndexReaderBase : IndexReaderBase
{
	protected SortedIndex index;

	public SortedIndexReaderBase(Type[] types, string cultureName, bool caseSensitive, ReadOnlyArray<SortOrder> sortOrder) :
		base(types, cultureName, caseSensitive, sortOrder)
	{
	}

	public SortedIndex SortedIndex => index;
	protected SortedIndexDescriptor IndexDesc => index.SortedIndexDesc;

	protected abstract void OnIndexSet();
	public override void SetIndex(Index index)
	{
		this.index = (SortedIndex)index;
		OnIndexSet();
	}
}

internal unsafe abstract class SortedIndexReaderBase<TKey1> : SortedIndexReaderBase, ISortedIndexReader<TKey1>
{
	KeyWriter<TKey1> keyWriter;
	TKey1 key1First, key1Last;

	public SortedIndexReaderBase(string cultureName, bool caseSensitive, ReadOnlyArray<SortOrder> sortOrder) :
		base(new Type[] { typeof(TKey1) }, cultureName, caseSensitive, sortOrder)
	{
		keyWriter = PopulateKeyBuffer;
	}

	public static string PopulateMethodName => nameof(PopulateKeyBuffer);

	public RangeScan GetEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, long.MinValue, ulong.MinValue, false, key1, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetBefore(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, long.MinValue, ulong.MinValue, false, key1, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, long.MinValue, ulong.MinValue, false, key1, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfter(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, long.MaxValue, ulong.MaxValue, true, key1Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, long.MinValue, ulong.MinValue, false, key1Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetEntireRange(Transaction tran, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, long.MinValue, ulong.MinValue, false, key1Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetRange(Transaction tran, TKey1 startKey1, bool isStartOpen, TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection)
	{
		long startId = isStartOpen ? long.MaxValue : long.MinValue;
		ulong startHandle = isStartOpen ? ulong.MaxValue : ulong.MinValue;

		long endId = isEndOpen ? long.MinValue : long.MaxValue;
		ulong endHandle = isEndOpen ? ulong.MinValue : ulong.MaxValue;

		return GetRange(tran, startKey1, startId, startHandle, isStartOpen, endKey1, endId, endHandle, isEndOpen, scanDirection);
	}

	protected override void OnIndexSet()
	{
		key1First = IndexDesc.PropertySortOrder[0] == SortOrder.Asc ?
			KeyComparer.GetKeyMinimum<TKey1>() : KeyComparer.GetKeyMaximum<TKey1>();
		key1Last = IndexDesc.PropertySortOrder[0] == SortOrder.Asc ?
			KeyComparer.GetKeyMaximum<TKey1>() : KeyComparer.GetKeyMinimum<TKey1>();
	}

	private RangeScan GetRange(Transaction tran, TKey1 startKey1, long startId, ulong startHandle, bool isStartOpen,
		TKey1 endKey1, long endId, ulong endHandle, bool isEndOpen, ScanDirection scanDirection)
	{
		RangeScan<TKey1> scan = new RangeScan<TKey1>(tran, base.comparer, base.SortedIndex, scanDirection == ScanDirection.Forward);
		scan.SetRange(startKey1, startId, startHandle, isStartOpen, endKey1, endId, endHandle, isEndOpen, keyWriter);
		return scan;
	}

	protected abstract void PopulateKeyBuffer(TKey1 key1, byte* pkey, string[] strings);
}

internal unsafe abstract class SortedIndexReaderBase<TKey1, TKey2> : SortedIndexReaderBase, ISortedIndexReader<TKey1, TKey2>
{
	KeyWriter<TKey1, TKey2> keyWriter;
	TKey1 key1First, key1Last;
	TKey2 key2First, key2Last;

	public SortedIndexReaderBase(string cultureName, bool caseSensitive, ReadOnlyArray<SortOrder> sortOrder) :
		base(new Type[] { typeof(TKey1), typeof(TKey2) }, cultureName, caseSensitive, sortOrder)
	{
		keyWriter = PopulateKeyBuffer;
	}

	public RangeScan GetEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2First, long.MinValue, ulong.MinValue, false,
			key1, key2Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, long.MinValue, ulong.MinValue, false,
			key1, key2, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetBefore(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, long.MinValue, ulong.MinValue, false,
			key1, key2First, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetBefore(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, long.MinValue, ulong.MinValue, false,
			key1, key2, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetPartialBefore(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2First, long.MinValue, ulong.MinValue, false,
			key1, key2, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, long.MinValue, ulong.MinValue, false,
			key1, key2Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, long.MinValue, ulong.MinValue, false,
			key1, key2, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2First, long.MinValue, ulong.MinValue, false,
			key1, key2, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfter(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2Last, long.MaxValue, ulong.MaxValue, true,
			key1Last, key2Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfter(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, long.MaxValue, ulong.MaxValue, true,
			key1Last, key2Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialAfter(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, long.MaxValue, ulong.MaxValue, true,
			key1, key2Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2First, long.MinValue, ulong.MinValue, false,
			key1Last, key2Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, long.MinValue, ulong.MinValue, false,
			key1Last, key2Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, long.MinValue, ulong.MinValue, false,
			key1, key2Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetEntireRange(Transaction tran, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, long.MinValue, ulong.MinValue, false,
			key1Last, key2Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetRange(Transaction tran, TKey1 startKey1, bool isStartOpen, TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection)
	{
		TKey2 startKey2 = isStartOpen ? key2Last : key2First;
		long startId = isStartOpen ? long.MaxValue : long.MinValue;
		ulong startHandle = isStartOpen ? ulong.MaxValue : ulong.MinValue;

		TKey2 endKey2 = isEndOpen ? key2First : key2Last;
		long endId = isEndOpen ? long.MinValue : long.MaxValue;
		ulong endHandle = isEndOpen ? ulong.MinValue : ulong.MaxValue;

		return GetRange(tran, startKey1, startKey2, startId, startHandle, isStartOpen,
			endKey1, endKey2, endId, endHandle, isEndOpen, scanDirection);
	}

	public RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, bool isEndOpen, ScanDirection scanDirection)
	{
		long startId = isStartOpen ? long.MaxValue : long.MinValue;
		ulong startHandle = isStartOpen ? ulong.MaxValue : ulong.MinValue;

		long endId = isEndOpen ? long.MinValue : long.MaxValue;
		ulong endHandle = isEndOpen ? ulong.MinValue : ulong.MaxValue;

		return GetRange(tran, startKey1, startKey2, startId, startHandle, isStartOpen,
			endKey1, endKey2, endId, endHandle, isEndOpen, scanDirection);
	}

	protected override void OnIndexSet()
	{
		key1First = IndexDesc.PropertySortOrder[0] == SortOrder.Asc ?
			KeyComparer.GetKeyMinimum<TKey1>() : KeyComparer.GetKeyMaximum<TKey1>();
		key1Last = IndexDesc.PropertySortOrder[0] == SortOrder.Asc ?
			KeyComparer.GetKeyMaximum<TKey1>() : KeyComparer.GetKeyMinimum<TKey1>();

		key2First = IndexDesc.PropertySortOrder[1] == SortOrder.Asc ?
			KeyComparer.GetKeyMinimum<TKey2>() : KeyComparer.GetKeyMaximum<TKey2>();
		key2Last = IndexDesc.PropertySortOrder[1] == SortOrder.Asc ?
			KeyComparer.GetKeyMaximum<TKey2>() : KeyComparer.GetKeyMinimum<TKey2>();
	}

	private RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, long startId, ulong startHandle, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, long endId, ulong endHandle, bool isEndOpen, ScanDirection scanDirection)
	{
		RangeScan<TKey1, TKey2> scan = new RangeScan<TKey1, TKey2>(tran, base.comparer,
			base.SortedIndex, scanDirection == ScanDirection.Forward);
		scan.SetRange(startKey1, startKey2, startId, startHandle, isStartOpen, endKey1, endKey2, endId, endHandle, isEndOpen, keyWriter);
		return scan;
	}

	protected abstract void PopulateKeyBuffer(TKey1 key1, TKey2 key2, byte* pkey, string[] strings);
}

internal unsafe abstract class SortedIndexReaderBase<TKey1, TKey2, TKey3> : SortedIndexReaderBase, ISortedIndexReader<TKey1, TKey2, TKey3>
{
	KeyWriter<TKey1, TKey2, TKey3> keyWriter;
	TKey1 key1First, key1Last;
	TKey2 key2First, key2Last;
	TKey3 key3First, key3Last;

	public SortedIndexReaderBase(string cultureName, bool caseSensitive, ReadOnlyArray<SortOrder> sortOrder) :
		base(new Type[] { typeof(TKey1), typeof(TKey2), typeof(TKey3) }, cultureName, caseSensitive, sortOrder)
	{
		keyWriter = PopulateKeyBuffer;
	}

	public RangeScan GetEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2First, key3First, long.MinValue, ulong.MinValue, false,
			key1, key2Last, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetBefore(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, long.MinValue, ulong.MinValue, false,
			key1, key2First, key3First, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetBefore(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3First, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetBefore(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetPartialBefore(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2First, key3First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3First, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetPartialBefore(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, long.MinValue, ulong.MinValue, false,
			key1, key2Last, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2First, key3First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfter(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2Last, key3Last, long.MaxValue, ulong.MaxValue, true,
			key1Last, key2Last, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfter(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3Last, long.MaxValue, ulong.MaxValue, true,
			key1Last, key2Last, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfter(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, long.MaxValue, ulong.MaxValue, true,
			key1Last, key2Last, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialAfter(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3Last, long.MaxValue, ulong.MaxValue, true,
			key1, key2Last, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialAfter(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, long.MaxValue, ulong.MaxValue, true,
			key1, key2, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2First, key3First, long.MinValue, ulong.MinValue, false,
			key1Last, key2Last, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3First, long.MinValue, ulong.MinValue, false,
			key1Last, key2Last, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, long.MinValue, ulong.MinValue, false,
			key1Last, key2Last, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3First, long.MinValue, ulong.MinValue, false,
			key1, key2Last, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, long.MinValue, ulong.MinValue, false,
			key1, key2, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetEntireRange(Transaction tran, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, long.MinValue, ulong.MinValue, false,
			key1Last, key2Last, key3Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetRange(Transaction tran, TKey1 startKey1, bool isStartOpen, TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection)
	{
		TKey2 startKey2 = isStartOpen ? key2Last : key2First;
		TKey3 startKey3 = isStartOpen ? key3Last : key3First;
		long startId = isStartOpen ? long.MaxValue : long.MinValue;
		ulong startHandle = isStartOpen ? ulong.MaxValue : ulong.MinValue;

		TKey2 endKey2 = isEndOpen ? key2First : key2Last;
		TKey3 endKey3 = isEndOpen ? key3First : key3Last;
		long endId = isEndOpen ? long.MinValue : long.MaxValue;
		ulong endHandle = isEndOpen ? ulong.MinValue : ulong.MaxValue;

		return GetRange(tran, startKey1, startKey2, startKey3, startId, startHandle, isStartOpen,
			endKey1, endKey2, endKey3, endId, endHandle, isEndOpen, scanDirection);
	}

	public RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, bool isEndOpen, ScanDirection scanDirection)
	{
		TKey3 startKey3 = isStartOpen ? key3Last : key3First;
		long startId = isStartOpen ? long.MaxValue : long.MinValue;
		ulong startHandle = isStartOpen ? ulong.MaxValue : ulong.MinValue;

		TKey3 endKey3 = isEndOpen ? key3First : key3Last;
		long endId = isEndOpen ? long.MinValue : long.MaxValue;
		ulong endHandle = isEndOpen ? ulong.MinValue : ulong.MaxValue;

		return GetRange(tran, startKey1, startKey2, startKey3, startId, startHandle, isStartOpen,
			endKey1, endKey2, endKey3, endId, endHandle, isEndOpen, scanDirection);
	}

	public RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, TKey3 startKey3, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, bool isEndOpen, ScanDirection scanDirection)
	{
		long startId = isStartOpen ? long.MaxValue : long.MinValue;
		ulong startHandle = isStartOpen ? ulong.MaxValue : ulong.MinValue;

		long endId = isEndOpen ? long.MinValue : long.MaxValue;
		ulong endHandle = isEndOpen ? ulong.MinValue : ulong.MaxValue;

		return GetRange(tran, startKey1, startKey2, startKey3, startId, startHandle, isStartOpen,
			endKey1, endKey2, endKey3, endId, endHandle, isEndOpen, scanDirection);
	}

	protected override void OnIndexSet()
	{
		key1First = IndexDesc.PropertySortOrder[0] == SortOrder.Asc ?
			KeyComparer.GetKeyMinimum<TKey1>() : KeyComparer.GetKeyMaximum<TKey1>();
		key1Last = IndexDesc.PropertySortOrder[0] == SortOrder.Asc ?
			KeyComparer.GetKeyMaximum<TKey1>() : KeyComparer.GetKeyMinimum<TKey1>();

		key2First = IndexDesc.PropertySortOrder[1] == SortOrder.Asc ?
			KeyComparer.GetKeyMinimum<TKey2>() : KeyComparer.GetKeyMaximum<TKey2>();
		key2Last = IndexDesc.PropertySortOrder[1] == SortOrder.Asc ?
			KeyComparer.GetKeyMaximum<TKey2>() : KeyComparer.GetKeyMinimum<TKey2>();

		key3First = IndexDesc.PropertySortOrder[2] == SortOrder.Asc ?
			KeyComparer.GetKeyMinimum<TKey3>() : KeyComparer.GetKeyMaximum<TKey3>();
		key3Last = IndexDesc.PropertySortOrder[2] == SortOrder.Asc ?
			KeyComparer.GetKeyMaximum<TKey3>() : KeyComparer.GetKeyMinimum<TKey3>();
	}

	private RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, TKey3 startKey3, long startId, ulong startHandle, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, long endId, ulong endHandle, bool isEndOpen, ScanDirection scanDirection)
	{
		RangeScan<TKey1, TKey2, TKey3> scan = new RangeScan<TKey1, TKey2, TKey3>(tran, base.comparer,
			base.SortedIndex, scanDirection == ScanDirection.Forward);
		scan.SetRange(startKey1, startKey2, startKey3, startId, startHandle, isStartOpen,
			endKey1, endKey2, endKey3, endId, endHandle, isEndOpen, keyWriter);
		return scan;
	}

	protected abstract void PopulateKeyBuffer(TKey1 key1, TKey2 key2, TKey3 key3, byte* pkey, string[] strings);
}

internal unsafe abstract class SortedIndexReaderBase<TKey1, TKey2, TKey3, TKey4> : SortedIndexReaderBase,
	ISortedIndexReader<TKey1, TKey2, TKey3, TKey4>
{
	KeyWriter<TKey1, TKey2, TKey3, TKey4> keyWriter;
	TKey1 key1First, key1Last;
	TKey2 key2First, key2Last;
	TKey3 key3First, key3Last;
	TKey4 key4First, key4Last;

	public SortedIndexReaderBase(string cultureName, bool caseSensitive, ReadOnlyArray<SortOrder> sortOrder) :
		base(new Type[] { typeof(TKey1), typeof(TKey2), typeof(TKey3), typeof(TKey4) }, cultureName, caseSensitive, sortOrder)
	{
		keyWriter = PopulateKeyBuffer;
	}

	public RangeScan GetEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2First, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, key4, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, key4, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetBefore(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2First, key3First, key4First, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetBefore(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3First, key4First, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetBefore(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, key4First, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetBefore(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, key4, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetPartialBefore(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2First, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3First, key4First, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetPartialBefore(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, key4First, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetPartialBefore(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, key4, long.MinValue, ulong.MinValue, true, scanDirection);
	}

	public RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, key4, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2First, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, key4, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfter(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, true,
			key1Last, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfter(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3Last, key4Last, long.MaxValue, ulong.MaxValue, true,
			key1Last, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfter(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, key4Last, long.MaxValue, ulong.MaxValue, true,
			key1Last, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfter(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, key4, long.MaxValue, ulong.MaxValue, true,
			key1Last, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialAfter(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3Last, key4Last, long.MaxValue, ulong.MaxValue, true,
			key1, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialAfter(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, key4Last, long.MaxValue, ulong.MaxValue, true,
			key1, key2, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialAfter(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, key4, long.MaxValue, ulong.MaxValue, true,
			key1, key2, key3, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2First, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1Last, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1Last, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, key4First, long.MinValue, ulong.MinValue, false,
			key1Last, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, key4, long.MinValue, ulong.MinValue, false,
			key1Last, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, key4First, long.MinValue, ulong.MinValue, false,
			key1, key2, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetPartialAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		return GetRange(tran, key1, key2, key3, key4, long.MinValue, ulong.MinValue, false,
			key1, key2, key3, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetEntireRange(Transaction tran, ScanDirection scanDirection)
	{
		return GetRange(tran, key1First, key2First, key3First, key4First, long.MinValue, ulong.MinValue, false,
			key1Last, key2Last, key3Last, key4Last, long.MaxValue, ulong.MaxValue, false, scanDirection);
	}

	public RangeScan GetRange(Transaction tran, TKey1 startKey1, bool isStartOpen, TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection)
	{
		TKey2 startKey2 = isStartOpen ? key2Last : key2First;
		TKey3 startKey3 = isStartOpen ? key3Last : key3First;
		TKey4 startKey4 = isStartOpen ? key4Last : key4First;
		long startId = isStartOpen ? long.MaxValue : long.MinValue;
		ulong startHandle = isStartOpen ? ulong.MaxValue : ulong.MinValue;

		TKey2 endKey2 = isEndOpen ? key2First : key2Last;
		TKey3 endKey3 = isEndOpen ? key3First : key3Last;
		TKey4 endKey4 = isEndOpen ? key4First : key4Last;
		long endId = isEndOpen ? long.MinValue : long.MaxValue;
		ulong endHandle = isEndOpen ? ulong.MinValue : ulong.MaxValue;

		return GetRange(tran, startKey1, startKey2, startKey3, startKey4, startId, startHandle, isStartOpen,
			endKey1, endKey2, endKey3, endKey4, endId, endHandle, isEndOpen, scanDirection);
	}

	public RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, bool isEndOpen, ScanDirection scanDirection)
	{
		TKey3 startKey3 = isStartOpen ? key3Last : key3First;
		TKey4 startKey4 = isStartOpen ? key4Last : key4First;
		long startId = isStartOpen ? long.MaxValue : long.MinValue;
		ulong startHandle = isStartOpen ? ulong.MaxValue : ulong.MinValue;

		TKey3 endKey3 = isEndOpen ? key3First : key3Last;
		TKey4 endKey4 = isEndOpen ? key4First : key4Last;
		long endId = isEndOpen ? long.MinValue : long.MaxValue;
		ulong endHandle = isEndOpen ? ulong.MinValue : ulong.MaxValue;

		return GetRange(tran, startKey1, startKey2, startKey3, startKey4, startId, startHandle, isStartOpen,
			endKey1, endKey2, endKey3, endKey4, endId, endHandle, isEndOpen, scanDirection);
	}

	public RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, TKey3 startKey3, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, bool isEndOpen, ScanDirection scanDirection)
	{
		TKey4 startKey4 = isStartOpen ? key4Last : key4First;
		long startId = isStartOpen ? long.MaxValue : long.MinValue;
		ulong startHandle = isStartOpen ? ulong.MaxValue : ulong.MinValue;

		TKey4 endKey4 = isEndOpen ? key4First : key4Last;
		long endId = isEndOpen ? long.MinValue : long.MaxValue;
		ulong endHandle = isEndOpen ? ulong.MinValue : ulong.MaxValue;

		return GetRange(tran, startKey1, startKey2, startKey3, startKey4, startId, startHandle, isStartOpen,
			endKey1, endKey2, endKey3, endKey4, endId, endHandle, isEndOpen, scanDirection);
	}

	public RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, TKey3 startKey3, TKey4 startKey4, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, TKey4 endKey4, bool isEndOpen, ScanDirection scanDirection)
	{
		long startId = isStartOpen ? long.MaxValue : long.MinValue;
		ulong startHandle = isStartOpen ? ulong.MaxValue : ulong.MinValue;

		long endId = isEndOpen ? long.MinValue : long.MaxValue;
		ulong endHandle = isEndOpen ? ulong.MinValue : ulong.MaxValue;

		return GetRange(tran, startKey1, startKey2, startKey3, startKey4, startId, startHandle, isStartOpen,
			endKey1, endKey2, endKey3, endKey4, endId, endHandle, isEndOpen, scanDirection);
	}

	protected override void OnIndexSet()
	{
		key1First = IndexDesc.PropertySortOrder[0] == SortOrder.Asc ?
			KeyComparer.GetKeyMinimum<TKey1>() : KeyComparer.GetKeyMaximum<TKey1>();
		key1Last = IndexDesc.PropertySortOrder[0] == SortOrder.Asc ?
			KeyComparer.GetKeyMaximum<TKey1>() : KeyComparer.GetKeyMinimum<TKey1>();

		key2First = IndexDesc.PropertySortOrder[1] == SortOrder.Asc ?
			KeyComparer.GetKeyMinimum<TKey2>() : KeyComparer.GetKeyMaximum<TKey2>();
		key2Last = IndexDesc.PropertySortOrder[1] == SortOrder.Asc ?
			KeyComparer.GetKeyMaximum<TKey2>() : KeyComparer.GetKeyMinimum<TKey2>();

		key3First = IndexDesc.PropertySortOrder[2] == SortOrder.Asc ?
			KeyComparer.GetKeyMinimum<TKey3>() : KeyComparer.GetKeyMaximum<TKey3>();
		key3Last = IndexDesc.PropertySortOrder[2] == SortOrder.Asc ?
			KeyComparer.GetKeyMaximum<TKey3>() : KeyComparer.GetKeyMinimum<TKey3>();

		key4First = IndexDesc.PropertySortOrder[3] == SortOrder.Asc ?
			KeyComparer.GetKeyMinimum<TKey4>() : KeyComparer.GetKeyMaximum<TKey4>();
		key4Last = IndexDesc.PropertySortOrder[3] == SortOrder.Asc ?
			KeyComparer.GetKeyMaximum<TKey4>() : KeyComparer.GetKeyMinimum<TKey4>();
	}

	private RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, TKey3 startKey3,
		TKey4 startKey4, long startId, ulong startHandle, bool isStartOpen, TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, TKey4 endKey4,
		long endId, ulong endHandle, bool isEndOpen, ScanDirection scanDirection)
	{
		RangeScan<TKey1, TKey2, TKey3, TKey4> scan = new RangeScan<TKey1, TKey2, TKey3, TKey4>(tran, base.comparer,
			base.SortedIndex, scanDirection == ScanDirection.Forward);
		scan.SetRange(startKey1, startKey2, startKey3, startKey4, startId, startHandle, isStartOpen,
			endKey1, endKey2, endKey3, endKey4, endId, endHandle, isEndOpen, keyWriter);
		return scan;
	}

	protected abstract void PopulateKeyBuffer(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, byte* pkey, string[] strings);
}
