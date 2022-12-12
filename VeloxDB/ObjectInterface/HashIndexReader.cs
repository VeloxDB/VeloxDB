using System;
using System.Collections;
using System.Collections.Generic;
using Velox.Descriptor;
using Velox.Storage;

namespace Velox.ObjectInterface;

/// <summary>
/// Reader for a single property hash index. Use this class to lookup a <see cref="DatabaseObject"/> using hash index.
/// </summary>
/// <typeparam name="T">Type of <see cref="DatabaseObject"/> being looked up.</typeparam>
/// <typeparam name="TKey1">Type of the key.</typeparam>
/// <seealso href="../articles/guide/data_model.html#hash-indexes">VeloxDB The definitive guide: Hash indexes</seealso>
/// <seealso cref="HashIndexAttribute"/>
/// <seealso cref="ObjectModel.GetHashIndex{T, TKey1}(string)"/>
public sealed class HashIndexReader<T, TKey1> where T : DatabaseObject
{
	ObjectModel model;
	Func<T, TKey1, bool> keyComparer;
	IHashIndexReader<TKey1> storageReader;
	ClassData classData;

	internal HashIndexReader(ObjectModel model, short id, ClassData classData, Func<T, TKey1, bool> keyComparer)
	{
		this.model = model;
		this.keyComparer = keyComparer;
		this.classData = classData;
		storageReader = model.Transaction.Engine.GetHashIndex<TKey1>(id);
	}

	/// <summary>
	/// Gets an object by key.
	/// </summary>
	/// <param name="key1">The key to lookup in the hash index.</param>
	/// <returns>Requested object if found, otherwise `null`.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="HashIndexReader{T, TKey1}"/> has been disposed.</exception>
	public T GetObject(TKey1 key1)
	{
		model.ValidateThread();
		if (model.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		ObjectReader[] rs = model.Context.ObjectReaders;
		storageReader.GetObjects(model.Transaction, key1, ref rs, out int count);

		for (int i = 0; i < count; i++)
		{
			DatabaseObject obj = model.GetObjectOrCreate(rs[i], classData);
			if (obj != null && obj.State == DatabaseObjectState.Read)
				return (T)(object)obj;
		}

		ChangeList.TypeIterator ti = model.EnumerateLocalChanges<T>();
		while (ti.HasMore)
		{
			DatabaseObject obj = ti.GetNextAndMove();
			if (!obj.IsDeleted && keyComparer((T)(object)obj, key1))
				return (T)(object)obj;
		}

		return default(T);
	}

	/// <summary>
	/// Gets all objects by key.
	/// </summary>
	/// <param name="key1">The key to lookup in the hash index.</param>
	/// <returns>A List of objects that match the key. If no object matches, empty list is returned.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="HashIndexReader{T, TKey1}"/> has been disposed.</exception>
	public List<T> GetObjects(TKey1 key1)
	{
		model.ValidateThread();
		if (model.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		ObjectReader[] rs = model.Context.ObjectReaders;
		storageReader.GetObjects(model.Transaction, key1, ref rs, out int count);

		List<T> l = new List<T>(count + 2);
		for (int i = 0; i < count; i++)
		{
			DatabaseObject obj = model.GetObjectOrCreate(rs[i], classData);
			if (obj != null && obj.State == DatabaseObjectState.Read)
				l.Add((T)(object)obj);
		}

		ChangeList.TypeIterator ti = model.EnumerateLocalChanges<T>();
		while (ti.HasMore)
		{
			DatabaseObject obj = ti.GetNextAndMove();
			if (!obj.IsDeleted && keyComparer((T)(object)obj, key1))
				l.Add((T)(object)obj);
		}

		return l;
	}
}

/// <summary>
/// Reader for a 2 property composite key hash index. Use this class to lookup a <see cref="DatabaseObject"/> using hash index.
/// </summary>
/// <typeparam name="T">Type of <see cref="DatabaseObject"/> being looked up.</typeparam>
/// <typeparam name="TKey1">Type of the first key.</typeparam>
/// <typeparam name="TKey2">Type of the second key.</typeparam>
/// <seealso href="../articles/guide/data_model.html#hash-indexes">VeloxDB The definitive guide: Hash indexes</seealso>
/// <seealso cref="HashIndexAttribute"/>
/// <seealso cref="ObjectModel.GetHashIndex{T, TKey1}(string)"/>

public sealed class HashIndexReader<T, TKey1, TKey2> where T : DatabaseObject
{
	ObjectModel model;
	Func<T, TKey1, TKey2, bool> keyComparer;
	IHashIndexReader<TKey1, TKey2> storageReader;
	ClassData classData;

	internal HashIndexReader(ObjectModel model, short id, ClassData classData, Func<T, TKey1, TKey2, bool> keyComparer)
	{
		this.model = model;
		this.keyComparer = keyComparer;
		this.classData = classData;
		storageReader = model.Transaction.Engine.GetHashIndex<TKey1, TKey2>(id);
	}

	/// <summary>
	/// Gets an object by composite key.
	/// </summary>
	/// <param name="key1">The first key.</param>
	/// <param name="key2">The second key.</param>
	/// <returns>Requested object if found, otherwise `null`.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="HashIndexReader{T, TKey1}"/> has been disposed.</exception>
	public T GetObject(TKey1 key1, TKey2 key2)
	{
		model.ValidateThread();
		if (model.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		ObjectReader[] rs = model.Context.ObjectReaders;
		storageReader.GetObjects(model.Transaction, key1, key2, ref rs, out int count);

		for (int i = 0; i < count; i++)
		{
			DatabaseObject obj = model.GetObjectOrCreate(rs[i], classData);
			if (obj != null && obj.State == DatabaseObjectState.Read)
				return (T)(object)obj;
		}

		ChangeList.TypeIterator ti = model.EnumerateLocalChanges<T>();
		while (ti.HasMore)
		{
			DatabaseObject obj = ti.GetNextAndMove();
			if (!obj.IsDeleted && keyComparer((T)(object)obj, key1, key2))
				return (T)(object)obj;
		}

		return default(T);
	}

	/// <summary>
	/// Gets all objects by key.
	/// </summary>
	/// <param name="key1">The first key.</param>
	/// <param name="key2">The second key.</param>
	/// <returns>A List of objects that match the key. If no object matches, empty list is returned.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="HashIndexReader{T, TKey1}"/> has been disposed.</exception>
	public List<T> GetObjects(TKey1 key1, TKey2 key2)
	{
		model.ValidateThread();
		if (model.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		ObjectReader[] rs = model.Context.ObjectReaders;
		storageReader.GetObjects(model.Transaction, key1, key2, ref rs, out int count);

		List<T> l = new List<T>(count + 2);
		for (int i = 0; i < count; i++)
		{
			DatabaseObject obj = model.GetObjectOrCreate(rs[i], classData);
			if (obj != null && obj.State == DatabaseObjectState.Read)
				l.Add((T)(object)obj);
		}

		ChangeList.TypeIterator ti = model.EnumerateLocalChanges<T>();
		while (ti.HasMore)
		{
			DatabaseObject obj = ti.GetNextAndMove();
			if (!obj.IsDeleted && keyComparer((T)(object)obj, key1, key2))
				l.Add((T)(object)obj);
		}

		return l;
	}
}

/// <summary>
/// Reader for a 3 property composite key hash index. Use this class to lookup a <see cref="DatabaseObject"/> using hash index.
/// </summary>
/// <typeparam name="T">Type of <see cref="DatabaseObject"/> being looked up.</typeparam>
/// <typeparam name="TKey1">Type of the first key.</typeparam>
/// <typeparam name="TKey2">Type of the second key.</typeparam>
/// <typeparam name="TKey3">Type of the third key.</typeparam>
/// <seealso href="../articles/guide/data_model.html#hash-indexes">VeloxDB The definitive guide: Hash indexes</seealso>
/// <seealso cref="HashIndexAttribute"/>
/// <seealso cref="ObjectModel.GetHashIndex{T, TKey1}(string)"/>
public sealed class HashIndexReader<T, TKey1, TKey2, TKey3> where T : DatabaseObject
{
	ObjectModel model;
	Func<T, TKey1, TKey2, TKey3, bool> keyComparer;
	IHashIndexReader<TKey1, TKey2, TKey3> storageReader;
	ClassData classData;

	internal HashIndexReader(ObjectModel model, short id, ClassData classData, Func<T, TKey1, TKey2, TKey3, bool> keyComparer)
	{
		this.model = model;
		this.keyComparer = keyComparer;
		this.classData = classData;
		storageReader = model.Transaction.Engine.GetHashIndex<TKey1, TKey2, TKey3>(id);
	}

	/// <summary>
	/// Gets an object by composite key.
	/// </summary>
	/// <param name="key1">The first key.</param>
	/// <param name="key2">The second key.</param>
	/// <param name="key3">The third key.</param>
	/// <returns>Requested object if found, otherwise `null`.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="HashIndexReader{T, TKey1}"/> has been disposed.</exception>
	public T GetObject(TKey1 key1, TKey2 key2, TKey3 key3)
	{
		model.ValidateThread();
		if (model.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		ObjectReader[] rs = model.Context.ObjectReaders;
		storageReader.GetObjects(model.Transaction, key1, key2, key3, ref rs, out int count);

		for (int i = 0; i < count; i++)
		{
			DatabaseObject obj = model.GetObjectOrCreate(rs[i], classData);
			if (obj != null && obj.State == DatabaseObjectState.Read)
				return (T)(object)obj;
		}

		ChangeList.TypeIterator ti = model.EnumerateLocalChanges<T>();
		while (ti.HasMore)
		{
			DatabaseObject obj = ti.GetNextAndMove();
			if (!obj.IsDeleted && keyComparer((T)(object)obj, key1, key2, key3))
				return (T)(object)obj;
		}

		return default(T);
	}

	/// <summary>
	/// Gets all objects by key.
	/// </summary>
	/// <param name="key1">The first key.</param>
	/// <param name="key2">The second key.</param>
	/// <param name="key3">The third key.</param>
	/// <returns>A List of objects that match the key. If no object matches, empty list is returned.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="HashIndexReader{T, TKey1}"/> has been disposed.</exception>
	public List<T> GetObjects(TKey1 key1, TKey2 key2, TKey3 key3)
	{
		model.ValidateThread();
		if (model.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		ObjectReader[] rs = model.Context.ObjectReaders;
		storageReader.GetObjects(model.Transaction, key1, key2, key3, ref rs, out int count);

		List<T> l = new List<T>(count + 2);
		for (int i = 0; i < count; i++)
		{
			DatabaseObject obj = model.GetObjectOrCreate(rs[i], classData);
			if (obj != null && obj.State == DatabaseObjectState.Read)
				l.Add((T)(object)obj);
		}

		ChangeList.TypeIterator ti = model.EnumerateLocalChanges<T>();
		while (ti.HasMore)
		{
			DatabaseObject obj = ti.GetNextAndMove();
			if (!obj.IsDeleted && keyComparer((T)(object)obj, key1, key2, key3))
				l.Add((T)(object)obj);
		}

		return l;
	}
}

/// <summary>
/// Reader for a 4 property composite key hash index. Use this class to lookup a <see cref="DatabaseObject"/> using hash index.
/// </summary>
/// <typeparam name="T">Type of <see cref="DatabaseObject"/> being looked up.</typeparam>
/// <typeparam name="TKey1">Type of the first key.</typeparam>
/// <typeparam name="TKey2">Type of the second key.</typeparam>
/// <typeparam name="TKey3">Type of the third key.</typeparam>
/// <typeparam name="TKey4">Type of the fourth key.</typeparam>
/// <seealso href="../articles/guide/data_model.html#hash-indexes">VeloxDB The definitive guide: Hash indexes</seealso>
/// <seealso cref="HashIndexAttribute"/>
/// <seealso cref="ObjectModel.GetHashIndex{T, TKey1}(string)"/>
public sealed class HashIndexReader<T, TKey1, TKey2, TKey3, TKey4> where T : DatabaseObject
{
	ObjectModel model;
	Func<T, TKey1, TKey2, TKey3, TKey4, bool> keyComparer;
	IHashIndexReader<TKey1, TKey2, TKey3, TKey4> storageReader;
	ClassData classData;

	internal HashIndexReader(ObjectModel model, short id, ClassData classData, Func<T, TKey1, TKey2, TKey3, TKey4, bool> keyComparer)
	{
		this.model = model;
		this.keyComparer = keyComparer;
		this.classData = classData;
		storageReader = model.Transaction.Engine.GetHashIndex<TKey1, TKey2, TKey3, TKey4>(id);
	}

	/// <summary>
	/// Gets an object by composite key.
	/// </summary>
	/// <param name="key1">The first key.</param>
	/// <param name="key2">The second key.</param>
	/// <param name="key3">The third key.</param>
	/// <param name="key4">The fourth key.</param>
	/// <returns>Requested object if found, otherwise `null`.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="HashIndexReader{T, TKey1}"/> has been disposed.</exception>
	public T GetObject(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4)
	{
		model.ValidateThread();
		if (model.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		ObjectReader[] rs = model.Context.ObjectReaders;
		storageReader.GetObjects(model.Transaction, key1, key2, key3, key4, ref rs, out int count);

		for (int i = 0; i < count; i++)
		{
			DatabaseObject obj = model.GetObjectOrCreate(rs[i], classData);
			if (obj != null && obj.State == DatabaseObjectState.Read)
				return (T)(object)obj;
		}

		ChangeList.TypeIterator ti = model.EnumerateLocalChanges<T>();
		while (ti.HasMore)
		{
			DatabaseObject obj = ti.GetNextAndMove();
			if (!obj.IsDeleted && keyComparer((T)(object)obj, key1, key2, key3, key4))
				return (T)(object)obj;
		}

		return default(T);
	}

	/// <summary>
	/// Gets all objects by key.
	/// </summary>
	/// <param name="key1">The first key.</param>
	/// <param name="key2">The second key.</param>
	/// <param name="key3">The third key.</param>
	/// <param name="key4">The fourth key.</param>
	/// <returns>A List of objects that match the key. If no object matches, empty list is returned.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="HashIndexReader{T, TKey1}"/> has been disposed.</exception>
	public List<T> GetObjects(TKey1 key1, TKey2 key2, TKey3 key3, TKey4 key4)
	{
		model.ValidateThread();
		if (model.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		ObjectReader[] rs = model.Context.ObjectReaders;
		storageReader.GetObjects(model.Transaction, key1, key2, key3, key4, ref rs, out int count);

		List<T> l = new List<T>(count + 2);
		for (int i = 0; i < count; i++)
		{
			DatabaseObject obj = model.GetObjectOrCreate(rs[i], classData);
			if (obj != null && obj.State == DatabaseObjectState.Read)
				l.Add((T)(object)obj);
		}

		ChangeList.TypeIterator ti = model.EnumerateLocalChanges<T>();
		while (ti.HasMore)
		{
			DatabaseObject obj = ti.GetNextAndMove();
			if (!obj.IsDeleted && keyComparer((T)(object)obj, key1, key2, key3, key4))
				l.Add((T)(object)obj);
		}

		return l;
	}
}
