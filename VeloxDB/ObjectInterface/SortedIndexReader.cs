using System;
using System.Collections.Generic;
using VeloxDB.Storage;

namespace VeloxDB.ObjectInterface;

/// <summary>
/// 
/// </summary>
/// <typeparam name="T"></typeparam>
/// <typeparam name="TKey1"></typeparam>
public sealed class SortedIndexReader<T, TKey1> where T : DatabaseObject
{
	internal const int MaxLocalChanges = 4;

	ObjectModel model;
	Func<T, StringComparer, TKey1, bool> keyComparer;
	ISortedIndexReader<TKey1> storageReader;
	IndexData indexData;

	internal SortedIndexReader(ObjectModel model, IndexData indexData)
	{
		keyComparer = indexData.KeyComparer as Func<T, StringComparer, TKey1, bool>;
		if (keyComparer == null)
			throw new ArgumentException("Invalid hash index class and/or key types.");

		this.model = model;
		this.indexData = indexData;
		storageReader = model.Transaction.Engine.GetSortedIndex<TKey1>(indexData.IndexDescriptor.Id);
	}

	public T GetEqual(TKey1 key1)
	{
		model.ValidateThread();
		if (model.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		bool includeLocal = true;
		if (model.GetTypeChangeCount(indexData.ClassData) > MaxLocalChanges)
		{
			includeLocal = false;
			model.ApplyChanges();
		}

		ObjectReaderList l = model.Context.GetObjectReaderList();
		try
		{
			RangeScan scan = storageReader.GetEqual(model.Transaction, key1, ScanDirection.Forward);

			if (scan.Next(l, ObjectReaderList.Capacity))
			{
				DatabaseObject obj = model.GetObjectOrCreate(l[0], indexData.ClassData);
				if (obj != null && obj.State == DatabaseObjectState.Read)
					return (T)(object)obj;
			}
		}
		finally
		{
			model.Context.PutObjectReaderList(l);
		}

		if (includeLocal)
		{
			ChangeList.TypeIterator ti = model.EnumerateLocalChanges(indexData.ClassData);
			while (ti.HasMore)
			{
				DatabaseObject obj = ti.GetNextAndMove();
				if (!obj.IsDeleted && keyComparer((T)(object)obj, indexData.StringComparer, key1))
					return (T)(object)obj;
			}
		}

		return default;
	}

	public IEnumerable<T> GetEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBefore(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfter(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetEntireRange(ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEntireRange(model.Transaction, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetRange(TKey1 startKey1, bool isStartOpen, TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, isStartOpen, endKey1, isEndOpen, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}
}

/// <summary>
/// 
/// </summary>
/// <typeparam name="T"></typeparam>
/// <typeparam name="TKey1"></typeparam>
/// <typeparam name="TKey2"></typeparam>
public sealed class SortedIndexReader<T, TKey1, TKey2> where T : DatabaseObject
{
	internal const int MaxLocalChanges = 4;

	ObjectModel model;
	Func<T, StringComparer, TKey1, TKey2, bool> keyComparer;
	ISortedIndexReader<TKey1, TKey2> storageReader;
	IndexData indexData;

	internal SortedIndexReader(ObjectModel model, IndexData indexData)
	{
		keyComparer = indexData.KeyComparer as Func<T, StringComparer, TKey1, TKey2, bool>;
		if (keyComparer == null)
			throw new ArgumentException("Invalid hash index class and/or key types.");

		this.model = model;
		this.indexData = indexData;
		storageReader = model.Transaction.Engine.GetSortedIndex<TKey1, TKey2>(indexData.IndexDescriptor.Id);
	}

	public T GetEqual(TKey1 key1, TKey2 key2)
	{
		model.ValidateThread();
		if (model.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		bool includeLocal = true;
		if (model.GetTypeChangeCount(indexData.ClassData) > MaxLocalChanges)
		{
			includeLocal = false;
			model.ApplyChanges();
		}

		ObjectReaderList l = model.Context.GetObjectReaderList();
		try
		{
			RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, ScanDirection.Forward);

			if (scan.Next(l, ObjectReaderList.Capacity))
			{
				DatabaseObject obj = model.GetObjectOrCreate(l[0], indexData.ClassData);
				if (obj != null && obj.State == DatabaseObjectState.Read)
					return (T)(object)obj;
			}
		}
		finally
		{
			model.Context.PutObjectReaderList(l);
		}

		if (includeLocal)
		{
			ChangeList.TypeIterator ti = model.EnumerateLocalChanges(indexData.ClassData);
			while (ti.HasMore)
			{
				DatabaseObject obj = ti.GetNextAndMove();
				if (!obj.IsDeleted && keyComparer((T)(object)obj, indexData.StringComparer, key1, key2))
					return (T)(object)obj;
			}
		}

		return default;
	}

	public IEnumerable<T> GetEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBefore(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBefore(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialBefore(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBefore(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialBeforeOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBeforeOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfter(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfter(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialAfter(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfter(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialAfterOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfterOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetEntireRange(ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEntireRange(model.Transaction, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetRange(TKey1 startKey1, bool isStartOpen, TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, isStartOpen, endKey1, isEndOpen, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetRange(TKey1 startKey1, TKey2 startKey2, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, startKey2, isStartOpen,
			endKey1, endKey2, isEndOpen, scanDirection);

		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}
}

/// <summary>
/// 
/// </summary>
/// <typeparam name="T"></typeparam>
/// <typeparam name="TKey1"></typeparam>
/// <typeparam name="TKey2"></typeparam>
/// <typeparam name="TKey3"></typeparam>
public sealed class SortedIndexReader<T, TKey1, TKey2, TKey3> where T : DatabaseObject
{
	internal const int MaxLocalChanges = 4;

	ObjectModel model;
	Func<T, StringComparer, TKey1, TKey2, TKey3, bool> keyComparer;
	ISortedIndexReader<TKey1, TKey2, TKey3> storageReader;
	IndexData indexData;

	internal SortedIndexReader(ObjectModel model, IndexData indexData)
	{
		keyComparer = indexData.KeyComparer as Func<T, StringComparer, TKey1, TKey2,  TKey3, bool>;
		if (keyComparer == null)
			throw new ArgumentException("Invalid hash index class and/or key types.");

		this.model = model;
		this.indexData = indexData;
		storageReader = model.Transaction.Engine.GetSortedIndex<TKey1, TKey2, TKey3>(indexData.IndexDescriptor.Id);
	}

	public T GetEqual(TKey1 key1, TKey2 key2, TKey3 key3)
	{
		model.ValidateThread();
		if (model.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		bool includeLocal = true;
		if (model.GetTypeChangeCount(indexData.ClassData) > MaxLocalChanges)
		{
			includeLocal = false;
			model.ApplyChanges();
		}

		ObjectReaderList l = model.Context.GetObjectReaderList();
		try
		{
			RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, key3, ScanDirection.Forward);

			if (scan.Next(l, ObjectReaderList.Capacity))
			{
				DatabaseObject obj = model.GetObjectOrCreate(l[0], indexData.ClassData);
				if (obj != null && obj.State == DatabaseObjectState.Read)
					return (T)(object)obj;
			}
		}
		finally
		{
			model.Context.PutObjectReaderList(l);
		}

		if (includeLocal)
		{
			ChangeList.TypeIterator ti = model.EnumerateLocalChanges(indexData.ClassData);
			while (ti.HasMore)
			{
				DatabaseObject obj = ti.GetNextAndMove();
				if (!obj.IsDeleted && keyComparer((T)(object)obj, indexData.StringComparer, key1, key2, key3))
					return (T)(object)obj;
			}
		}

		return default;
	}

	public IEnumerable<T> GetEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBefore(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBefore(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBefore(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialBefore(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBefore(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialBefore(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBefore(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialBeforeOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBeforeOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialBeforeOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBeforeOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfter(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfter(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfter(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialAfter(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfter(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialAfter(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfter(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialAfterOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfterOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialAfterOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfterOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetEntireRange(ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEntireRange(model.Transaction, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetRange(TKey1 startKey1, bool isStartOpen, TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, isStartOpen, endKey1, isEndOpen, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetRange(TKey1 startKey1, TKey2 startKey2, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, startKey2, isStartOpen,
			endKey1, endKey2, isEndOpen, scanDirection);

		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetRange(TKey1 startKey1, TKey2 startKey2, TKey3 startKey3, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, startKey2, startKey3, isStartOpen,
			endKey1, endKey2, endKey3, isEndOpen, scanDirection);

		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}
}

/// <summary>
/// 
/// </summary>
/// <typeparam name="T"></typeparam>
/// <typeparam name="TKey1"></typeparam>
/// <typeparam name="TKey2"></typeparam>
/// <typeparam name="TKey3"></typeparam>
/// <typeparam name="TKey4"></typeparam>
public sealed class SortedIndexReader<T, TKey1, TKey2, TKey3, TKey4> where T : DatabaseObject
{
	internal const int MaxLocalChanges = 4;

	ObjectModel model;
	Func<T, StringComparer, TKey1, TKey2, TKey3, TKey4, bool> keyComparer;
	ISortedIndexReader<TKey1, TKey2, TKey3, TKey4> storageReader;
	IndexData indexData;

	internal SortedIndexReader(ObjectModel model, IndexData indexData)
	{
		keyComparer = indexData.KeyComparer as Func<T, StringComparer, TKey1, TKey2, TKey3, TKey4, bool>;
		if (keyComparer == null)
			throw new ArgumentException("Invalid hash index class and/or key types.");

		this.model = model;
		this.indexData = indexData;
		storageReader = model.Transaction.Engine.GetSortedIndex<TKey1, TKey2, TKey3, TKey4>(indexData.IndexDescriptor.Id);
	}

	public T GetEqual(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4)
	{
		model.ValidateThread();
		if (model.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		bool includeLocal = true;
		if (model.GetTypeChangeCount(indexData.ClassData) > MaxLocalChanges)
		{
			includeLocal = false;
			model.ApplyChanges();
		}

		ObjectReaderList l = model.Context.GetObjectReaderList();
		try
		{
			RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, key3, key4, ScanDirection.Forward);

			if (scan.Next(l, ObjectReaderList.Capacity))
			{
				DatabaseObject obj = model.GetObjectOrCreate(l[0], indexData.ClassData);
				if (obj != null && obj.State == DatabaseObjectState.Read)
					return (T)(object)obj;
			}
		}
		finally
		{
			model.Context.PutObjectReaderList(l);
		}

		if (includeLocal)
		{
			ChangeList.TypeIterator ti = model.EnumerateLocalChanges(indexData.ClassData);
			while (ti.HasMore)
			{
				DatabaseObject obj = ti.GetNextAndMove();
				if (!obj.IsDeleted && keyComparer((T)(object)obj, indexData.StringComparer, key1, key2, key3, key4))
					return (T)(object)obj;
			}
		}

		return default;
	}

	public IEnumerable<T> GetEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetEqual(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBefore(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBefore(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBefore(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBefore(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialBefore(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBefore(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialBefore(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBefore(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialBefore(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBefore(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialBeforeOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBeforeOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialBeforeOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBeforeOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialBeforeOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBeforeOrEqual(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfter(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfter(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfter(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfter(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialAfter(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfter(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialAfter(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfter(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialAfter(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfter(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialAfterOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfterOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialAfterOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfterOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetPartialAfterOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfterOrEqual(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetEntireRange(ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEntireRange(model.Transaction, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetRange(TKey1 startKey1, bool isStartOpen, TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, isStartOpen, endKey1, isEndOpen, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetRange(TKey1 startKey1, TKey2 startKey2, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, startKey2, isStartOpen,
			endKey1, endKey2, isEndOpen, scanDirection);

		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetRange(TKey1 startKey1, TKey2 startKey2, TKey3 startKey3, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, startKey2, startKey3, isStartOpen,
			endKey1, endKey2, endKey3, isEndOpen, scanDirection);

		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	public IEnumerable<T> GetRange(TKey1 startKey1, TKey2 startKey2, TKey3 startKey3, TKey4 startKey4, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, TKey4 endKey4, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, startKey2, startKey3, startKey4, isStartOpen,
			endKey1, endKey2, endKey3, endKey4, isEndOpen, scanDirection);

		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}
}
