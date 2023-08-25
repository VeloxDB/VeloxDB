using System;

namespace VeloxDB.Storage;

/// <summary>
/// Specifies the direction in which a scan through an index should be perfomed.
/// </summary>
public enum ScanDirection
{
	/// <summary>
	/// The scan is performed in the direction that corresponds to the sorting order of the index.
	/// </summary>
	Forward,

	/// <summary>
	/// The scan is performed in the direction that is oposite of the sorting order of the index.
	/// </summary>
	Backward,
}

internal interface ISortedIndexReader<TKey1>
{
	RangeScan GetEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetBefore(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetAfter(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetEntireRange(Transaction tran, ScanDirection scanDirection);
	RangeScan GetRange(Transaction tran, TKey1 startKey1, bool isStartOpen, TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection);
}

internal interface ISortedIndexReader<TKey1, TKey2>
{
	RangeScan GetEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetBefore(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetBefore(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetPartialBefore(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetPartialBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetAfter(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetAfter(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetPartialAfter(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetPartialAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetEntireRange(Transaction tran, ScanDirection scanDirection);
	RangeScan GetRange(Transaction tran, TKey1 startKey1, bool isStartOpen,
		TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection);
	RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, bool isEndOpen, ScanDirection scanDirection);
}

internal interface ISortedIndexReader<TKey1, TKey2, TKey3>
{
	RangeScan GetEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetBefore(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetBefore(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetBefore(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetPartialBefore(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetPartialBefore(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetPartialBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetPartialBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetAfter(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetAfter(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetAfter(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetPartialAfter(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetPartialAfter(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetPartialAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetPartialAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetEntireRange(Transaction tran, ScanDirection scanDirection);
	RangeScan GetRange(Transaction tran, TKey1 startKey1, bool isStartOpen,
		TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection);
	RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, bool isEndOpen, ScanDirection scanDirection);
	RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, TKey3 startKey3, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, bool isEndOpen, ScanDirection scanDirection);
}

internal interface ISortedIndexReader<TKey1, TKey2, TKey3, TKey4>
{
	RangeScan GetEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection);
	RangeScan GetBefore(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetBefore(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetBefore(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetBefore(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection);
	RangeScan GetPartialBefore(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetPartialBefore(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetPartialBefore(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection);
	RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection);
	RangeScan GetPartialBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetPartialBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetPartialBeforeOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection);
	RangeScan GetAfter(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetAfter(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetAfter(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetAfter(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection);
	RangeScan GetPartialAfter(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetPartialAfter(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetPartialAfter(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection);
	RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, ScanDirection scanDirection);
	RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection);
	RangeScan GetPartialAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, ScanDirection scanDirection);
	RangeScan GetPartialAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection);
	RangeScan GetPartialAfterOrEqual(Transaction tran, TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection);
	RangeScan GetEntireRange(Transaction tran, ScanDirection scanDirection);
	RangeScan GetRange(Transaction tran, TKey1 startKey1, bool isStartOpen,
		TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection);
	RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, bool isEndOpen, ScanDirection scanDirection);
	RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, TKey3 startKey3, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, bool isEndOpen, ScanDirection scanDirection);
	RangeScan GetRange(Transaction tran, TKey1 startKey1, TKey2 startKey2, TKey3 startKey3, TKey4 startKey4, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, TKey4 endKey4, bool isEndOpen, ScanDirection scanDirection);
}
