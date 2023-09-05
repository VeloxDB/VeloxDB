using System;
using System.Collections.Generic;
using VeloxDB.Storage;

namespace VeloxDB.ObjectInterface;

/// <summary>
/// Reader for a single property sorted index. Use this class to lookup a <see cref="DatabaseObject"/> using sorted index.
/// </summary>
/// <typeparam name="T">Type of <see cref="DatabaseObject"/> being looked up.</typeparam>
/// <typeparam name="TKey1">Type of the key property.</typeparam>
/// <seealso href="../articles/guide/data_model.html#hash-indexes">VeloxDB The definitive guide: Sorted indexes</seealso>
/// <seealso cref="SortedIndexAttribute"/>
/// <seealso cref="ObjectModel.GetSortedIndex{T, TKey1}(string)"/>
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

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1.
	/// This method returns at most a single object with a given key and is ideal for querying unique indexes.
	/// </summary>
	/// <param name="key1">The value of the key property.</param>
	/// <returns>An object with a given key, if one exists, null otherwise.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
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

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1.
	/// </summary>
	/// <param name="key1">The value of the key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &lt; keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBefore(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &lt;= keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &gt; keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfter(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &lt;= keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index.
	/// </summary>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEntireRange(ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEntireRange(model.Transaction, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index that belong to the given interval.
	/// </summary>
	/// <param name="startKey1">The value of the key property for the range start.</param>
	/// <param name="isStartOpen">Indicates whether the start side of the interval is open or closed.</param>
	/// <param name="endKey1">The value of the key property for the range end.</param>
	/// <param name="isEndOpen">Indicates whether the end side of the interval is open or closed.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetRange(TKey1 startKey1, bool isStartOpen, TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, isStartOpen, endKey1, isEndOpen, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}
}

/// <summary>
/// Reader for sorted index with two properties as a key. Use this class to lookup a <see cref="DatabaseObject"/> using sorted index.
/// </summary>
/// <typeparam name="T">Type of <see cref="DatabaseObject"/> being looked up.</typeparam>
/// <typeparam name="TKey1">Type of the first key property.</typeparam>
/// <typeparam name="TKey2">Type of the second key property.</typeparam>
/// <seealso href="../articles/guide/data_model.html#hash-indexes">VeloxDB The definitive guide: Sorted indexes</seealso>
/// <seealso cref="SortedIndexAttribute"/>
/// <seealso cref="ObjectModel.GetSortedIndex{T, TKey1, TKey2}(string)"/>
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

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 == keyProperty2.
	/// This method returns at most a single object with a given key and is ideal for querying unique indexes.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <returns>An object with a given key, if one exists, null otherwise.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
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

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 = keyProperty2.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &lt; keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBefore(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2) &lt; (keyProperty1, keyProperty2).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBefore(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 &lt; keyProperty2.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialBefore(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBefore(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &lt;= keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2) &lt;= (keyProperty1, keyProperty2).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 &lt;= keyProperty2.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialBeforeOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBeforeOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &gt;= keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfter(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2) &gt; (keyProperty1, keyProperty2).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfter(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 == keyProperty1 AND key2 &gt; keyProperty2.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialAfter(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfter(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &gt;= keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2) &gt;= (keyProperty1, keyProperty2).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 == keyProperty1 AND key2 &gt;= keyProperty2.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialAfterOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfterOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index.
	/// </summary>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEntireRange(ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEntireRange(model.Transaction, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index that belong to the given interval.
	/// </summary>
	/// <param name="startKey1">The value of the first key property for the range start.</param>
	/// <param name="isStartOpen">Indicates whether the start side of the interval is open or closed.</param>
	/// <param name="endKey1">The value of the first key property for the range end.</param>
	/// <param name="isEndOpen">Indicates whether the end side of the interval is open or closed.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetRange(TKey1 startKey1, bool isStartOpen, TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, isStartOpen, endKey1, isEndOpen, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index that belong to the given interval.
	/// </summary>
	/// <param name="startKey1">The value of the first key property for the range start.</param>
	/// <param name="startKey2">The value of the second key property for the range start.</param>
	/// <param name="isStartOpen">Indicates whether the start side of the interval is open or closed.</param>
	/// <param name="endKey1">The value of the first key property for the range end.</param>
	/// <param name="endKey2">The value of the second key property for the range end.</param>
	/// <param name="isEndOpen">Indicates whether the end side of the interval is open or closed.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
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
/// Reader for sorted index with three properties as a key. Use this class to lookup a <see cref="DatabaseObject"/> using sorted index.
/// </summary>
/// <typeparam name="T">Type of <see cref="DatabaseObject"/> being looked up.</typeparam>
/// <typeparam name="TKey1">Type of the first key property.</typeparam>
/// <typeparam name="TKey2">Type of the second key property.</typeparam>
/// <typeparam name="TKey3">Type of the third key property.</typeparam>
/// <seealso href="../articles/guide/data_model.html#hash-indexes">VeloxDB The definitive guide: Sorted indexes</seealso>
/// <seealso cref="SortedIndexAttribute"/>
/// <seealso cref="ObjectModel.GetSortedIndex{T, TKey1, TKey2, TKey3}(string)"/>
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

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 == keyProperty2 AND key3 == keyProperty3.
	/// This method returns at most a single object with a given key and is ideal for querying unique indexes.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <returns>An object with a given key, if one exists, null otherwise.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
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

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 == keyProperty2.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 == keyProperty2 AND key3 == keyProperty3.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &lt; keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBefore(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2) &lt; (keyProperty1, keyProperty2).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBefore(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2, key3) &lt; (keyProperty1, keyProperty2, keyProperty3).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBefore(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 == keyProperty1 AND key2 &lt; keyProperty2.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialBefore(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBefore(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 == keyProperty1 AND key2 == keyProperty2 AND key3 &lt; keyProperty3.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialBefore(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBefore(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &lt;= keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2) &lt;= (keyProperty1, keyProperty2).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2, key3) &lt;= (keyProperty1, keyProperty2, keyProperty3).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 == keyProperty1 AND key2 &lt;= keyProperty2.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialBeforeOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBeforeOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 == keyProperty1 AND key2 == keyProperty2 AND key3 &lt;= keyProperty3.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialBeforeOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBeforeOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &gt; keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfter(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2) &gt; (keyProperty1, keyProperty2).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfter(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2, key3) &gt; (keyProperty1, keyProperty2, keyProperty3).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfter(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 == keyProperty1 AND key2 &gt; keyProperty2.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialAfter(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfter(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 == keyProperty1 AND key2 == keyProperty2 AND key3 &gt; keyProperty3.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialAfter(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfter(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &gt;= keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2) &gt;= (keyProperty1, keyProperty2).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2, key3) &gt;= (keyProperty1, keyProperty2, keyProperty3).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}


	/// <summary>
	/// Searches the sorted index using the condition key1 == keyProperty1 AND key2 &gt;= keyProperty2.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialAfterOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfterOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 == keyProperty1 AND key2 == keyProperty2 AND key3 &gt;= keyProperty3.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialAfterOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfterOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index.
	/// </summary>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEntireRange(ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEntireRange(model.Transaction, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index that belong to the given interval.
	/// </summary>
	/// <param name="startKey1">The value of the first key property for the range start.</param>
	/// <param name="isStartOpen">Indicates whether the start side of the interval is open or closed.</param>
	/// <param name="endKey1">The value of the first key property for the range end.</param>
	/// <param name="isEndOpen">Indicates whether the end side of the interval is open or closed.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetRange(TKey1 startKey1, bool isStartOpen, TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, isStartOpen, endKey1, isEndOpen, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index that belong to the given interval.
	/// </summary>
	/// <param name="startKey1">The value of the first key property for the range start.</param>
	/// <param name="startKey2">The value of the second key property for the range start.</param>
	/// <param name="isStartOpen">Indicates whether the start side of the interval is open or closed.</param>
	/// <param name="endKey1">The value of the first key property for the range end.</param>
	/// <param name="endKey2">The value of the second key property for the range end.</param>
	/// <param name="isEndOpen">Indicates whether the end side of the interval is open or closed.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetRange(TKey1 startKey1, TKey2 startKey2, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, startKey2, isStartOpen,
			endKey1, endKey2, isEndOpen, scanDirection);

		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index that belong to the given interval.
	/// </summary>
	/// <param name="startKey1">The value of the first key property for the range start.</param>
	/// <param name="startKey2">The value of the second key property for the range start.</param>
	/// <param name="startKey3">The value of the third key property for the range start.</param>
	/// <param name="isStartOpen">Indicates whether the start side of the interval is open or closed.</param>
	/// <param name="endKey1">The value of the first key property for the range end.</param>
	/// <param name="endKey2">The value of the second key property for the range end.</param>
	/// <param name="endKey3">The value of the third key property for the range end.</param>
	/// <param name="isEndOpen">Indicates whether the end side of the interval is open or closed.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
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
/// Reader for sorted index with four properties as a key. Use this class to lookup a <see cref="DatabaseObject"/> using sorted index.
/// </summary>
/// <typeparam name="T">Type of <see cref="DatabaseObject"/> being looked up.</typeparam>
/// <typeparam name="TKey1">Type of the first key property.</typeparam>
/// <typeparam name="TKey2">Type of the second key property.</typeparam>
/// <typeparam name="TKey3">Type of the third key property.</typeparam>
/// <typeparam name="TKey4">Type of the fourth key property.</typeparam>
/// <seealso href="../articles/guide/data_model.html#hash-indexes">VeloxDB The definitive guide: Sorted indexes</seealso>
/// <seealso cref="SortedIndexAttribute"/>
/// <seealso cref="ObjectModel.GetSortedIndex{T, TKey1, TKey2}(string)"/>
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

	/// <summary>
	/// Searches the sorted index using the condition
	/// key1 = keyProperty1 AND key2 == keyProperty2 AND key3 = keyProperty3 AND key4 == keyProperty4.
	/// This method returns at most a single object with a given key and is ideal for querying unique indexes.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="key4">The value of the fourth key property.</param>
	/// <returns>An object with a given key, if one exists, null otherwise.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
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

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 = keyProperty2.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 = keyProperty2 AND key3 = keyProperty3.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition
	/// key1 = keyProperty1 AND key2 = keyProperty2 AND key3 = keyProperty3 AND key4 = keyProperty4.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="key4">The value of the fourth key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEqual(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEqual(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &lt; keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBefore(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2) &lt; (keyProperty1, keyProperty2).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBefore(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2, key3) &lt; (keyProperty1, keyProperty2, keyProperty3).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>v
	public IEnumerable<T> GetBefore(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2, key3, key4) &lt; (keyProperty1, keyProperty2, keyProperty3, keyProperty4).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="key4">The value of the fourth key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBefore(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBefore(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 &lt; keyProperty2.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialBefore(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBefore(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 = keyProperty2 AND key3 &lt; keyProperty3.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialBefore(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBefore(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition
	/// key1 = keyProperty1 AND key2 = keyProperty2 AND key3 = keyProperty3 AND key4 &lt; keyProperty4.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="key4">The value of the fourth key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialBefore(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBefore(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &lt;= keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2) &lt;= (keyProperty1, keyProperty2).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2, key3) &lt;= (keyProperty1, keyProperty2, keyProperty3).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2, key3, key4) &lt;= (keyProperty1, keyProperty2, keyProperty3, keyProperty4).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="key4">The value of the fourth key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetBeforeOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetBeforeOrEqual(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 &lt;= keyProperty2.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialBeforeOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBeforeOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 = keyProperty2 AND key3 &lt;= keyProperty3.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialBeforeOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBeforeOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition
	/// key1 = keyProperty1 AND key2 = keyProperty2 AND key3 = keyProperty3 AND key4 &lt;= keyProperty4.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="key4">The value of the fourth key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialBeforeOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialBeforeOrEqual(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &gt; keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfter(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2) &gt; (keyProperty1, keyProperty2).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfter(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2, key3) &gt; (keyProperty1, keyProperty2, keyProperty3).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfter(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition
	/// (key1, key2, key3, key4) &gt; (keyProperty1, keyProperty2, keyProperty3, keyProperty4).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="key4">The value of the fourth key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfter(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfter(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 &gt; keyProperty2.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialAfter(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfter(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 = keyProperty2 AND key3 &gt; keyProperty3.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialAfter(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfter(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition
	/// key1 = keyProperty1 AND key2 = keyProperty2 AND key3 = keyProperty3 AND key4 &gt; keyProperty4.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="key4">The value of the fourth key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialAfter(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfter(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 &gt;= keyProperty1.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2) &gt;= (keyProperty1, keyProperty2).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition (key1, key2, key3) &gt;= (keyProperty1, keyProperty2, keyProperty3).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition
	/// (key1, key2, key3, key4) &gt;= (keyProperty1, keyProperty2, keyProperty3, keyProperty4).
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="key4">The value of the fourth key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetAfterOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetAfterOrEqual(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 &gt;= keyProperty2.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialAfterOrEqual(TKey1 key1, TKey2 key2, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfterOrEqual(model.Transaction, key1, key2, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition key1 = keyProperty1 AND key2 = keyProperty2 AND key3 &gt;= keyProperty3.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialAfterOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfterOrEqual(model.Transaction, key1, key2, key3, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Searches the sorted index using the condition
	/// key1 = keyProperty1 AND key2 = keyProperty2 AND key3 = keyProperty3 AND key4 &gt;= keyProperty4.
	/// Keep in mind that signs &lt; and &gt; are relative to the sorting order of the property in the index,
	/// meaning before and after in the sort order.
	/// </summary>
	/// <param name="key1">The value of the first key property.</param>
	/// <param name="key2">The value of the second key property.</param>
	/// <param name="key3">The value of the third key property.</param>
	/// <param name="key4">The value of the fourth key property.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetPartialAfterOrEqual(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetPartialAfterOrEqual(model.Transaction, key1, key2, key3, key4, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index.
	/// </summary>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetEntireRange(ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetEntireRange(model.Transaction, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index that belong to the given interval.
	/// </summary>
	/// <param name="startKey1">The value of the first key property for the range start.</param>
	/// <param name="isStartOpen">Indicates whether the start side of the interval is open or closed.</param>
	/// <param name="endKey1">The value of the first key property for the range end.</param>
	/// <param name="isEndOpen">Indicates whether the end side of the interval is open or closed.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetRange(TKey1 startKey1, bool isStartOpen, TKey1 endKey1, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, isStartOpen, endKey1, isEndOpen, scanDirection);
		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index that belong to the given interval.
	/// </summary>
	/// <param name="startKey1">The value of the first key property for the range start.</param>
	/// <param name="startKey2">The value of the second key property for the range start.</param>
	/// <param name="isStartOpen">Indicates whether the start side of the interval is open or closed.</param>
	/// <param name="endKey1">The value of the first key property for the range end.</param>
	/// <param name="endKey2">The value of the second key property for the range end.</param>
	/// <param name="isEndOpen">Indicates whether the end side of the interval is open or closed.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetRange(TKey1 startKey1, TKey2 startKey2, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, startKey2, isStartOpen,
			endKey1, endKey2, isEndOpen, scanDirection);

		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index that belong to the given interval.
	/// </summary>
	/// <param name="startKey1">The value of the first key property for the range start.</param>
	/// <param name="startKey2">The value of the second key property for the range start.</param>
	/// <param name="startKey3">The value of the third key property for the range start.</param>
	/// <param name="isStartOpen">Indicates whether the start side of the interval is open or closed.</param>
	/// <param name="endKey1">The value of the first key property for the range end.</param>
	/// <param name="endKey2">The value of the second key property for the range end.</param>
	/// <param name="endKey3">The value of the third key property for the range end.</param>
	/// <param name="isEndOpen">Indicates whether the end side of the interval is open or closed.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	public IEnumerable<T> GetRange(TKey1 startKey1, TKey2 startKey2, TKey3 startKey3, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, bool isEndOpen, ScanDirection scanDirection)
	{
		if (model.HasLocalChanges(indexData.ClassData))
			model.ApplyChanges();

		RangeScan scan = storageReader.GetRange(model.Transaction, startKey1, startKey2, startKey3, isStartOpen,
			endKey1, endKey2, endKey3, isEndOpen, scanDirection);

		return new ObjectModel.RangeScanEnumerable<T>(model, indexData.ClassData, scan);
	}

	/// <summary>
	/// Returns all the objects in the index that belong to the given interval.
	/// </summary>
	/// <param name="startKey1">The value of the first key property for the range start.</param>
	/// <param name="startKey2">The value of the second key property for the range start.</param>
	/// <param name="startKey3">The value of the third key property for the range start.</param>
	/// <param name="startKey4">The value of the fourth key property for the range start.</param>
	/// <param name="isStartOpen">Indicates whether the start side of the interval is open or closed.</param>
	/// <param name="endKey1">The value of the first key property for the range end.</param>
	/// <param name="endKey2">The value of the second key property for the range end.</param>
	/// <param name="endKey3">The value of the third key property for the range end.</param>
	/// <param name="endKey4">The value of the fourth key property for the range end.</param>
	/// <param name="isEndOpen">Indicates whether the end side of the interval is open or closed.</param>
	/// <param name="scanDirection">Indicates a direction in which to scan the index.</param>
	/// <returns>All objects from the index that satisfy the required condition.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
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
