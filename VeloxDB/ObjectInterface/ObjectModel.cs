﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.Arm;
using System.Threading;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage;
namespace VeloxDB.ObjectInterface;

/// <summary>
/// <see cref="ObjectModel"/> provides methods for querying the database and creating new objects.
/// </summary>
public unsafe sealed partial class ObjectModel
{
	static Action<ObjectModel, long> refresher;

	StorageEngine engine;
	MemoryManager memoryManager;
	ObjectModelContextPool contextPool;
	ObjectModelContext context;

	int ownerThreadId;

	StringStorage engineStringStorage;
	BlobStorage engineBlobStorage;
	Transaction transaction;

	ObjectModelData modelData;

	LongDictionary<DatabaseObject> objectMap;
	ChangeList changeList;

	int stringCount;
	string[] strings;

	long[] inverseReferenceIds;
	InverseReferenceChanges invRefChanges;
	LongDictionary<int> deletedInvRefs;

	DeletedSet deletedSet;
	long[] toDelete1, toDelete2;

	CriticalDatabaseException storedException;

	bool disposed;

	static ObjectModel()
	{
		refresher = (o, id) =>
		{
			if (o.objectMap.TryGetValue(id, out var obj))
				o.RefreshObjectFromDatabase(obj);
		};
	}

	internal ObjectModel(ObjectModelData modelData, ObjectModelContextPool contextPool, TransactionType tranType)
	{
		this.ownerThreadId = Thread.CurrentThread.ManagedThreadId;
		this.contextPool = contextPool;
		this.engine = contextPool.Engine;
		this.memoryManager = this.engine.MemoryManager;
		this.engineStringStorage = engine.StringStorage;
		this.engineBlobStorage = engine.BlobStorage;

		this.modelData = modelData;

		context = contextPool.GetContext();
		context.RefreshModelIfNeeded(modelData.ModelDesc);

		objectMap = context.ObjectMap;
		changeList = context.ChangeList;
		inverseReferenceIds = context.InverseReferenceIds;
		invRefChanges = context.InverseReferenceChanges;
		deletedInvRefs = context.DeletedInvRefs;
		strings = context.Strings;
		deletedSet = context.DeletedSet;
		toDelete1 = context.ToDelete1;
		toDelete2 = context.ToDelete2;

		try
		{
			transaction = engine.CreateTransaction(tranType);
		}
		catch
		{
			contextPool.PutContext(context);
			throw;
		}
	}

	internal bool Disposed => disposed;
	internal int ChangeCount => changeList.Count;
	internal Transaction Transaction => transaction;
	internal ObjectModelContext Context => context;
	internal DeletedSet DeletedSet => deletedSet;
	internal DataModelDescriptor ModelDesc => modelData.ModelDesc;
	internal CriticalDatabaseException StoredException => storedException;
	internal LongDictionary<DatabaseObject> ObjectMap => objectMap;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal DatabaseObject GetCommitObject(int index)
	{
		return changeList[index];
	}

	/// <summary>
	/// Get object by id.
	/// </summary>
	/// <typeparam name="T">The type of object to get.</typeparam>
	/// <param name="id">Object's Id.</param>
	/// <returns>Returns the queried object if found, otherwise returns null.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	/// <exception cref="ArgumentException">
	/// If type `T` is not marked with the <see cref="DatabaseClassAttribute"/>.
	/// </exception>
	[return: MaybeNull]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public T GetObject<T>(long id) where T : DatabaseObject
	{
		ValidateThread();

		return (T)GetObject(id);
	}

	/// <summary>
	/// Get object by id. This is strict version of <see cref="GetObject"/>,
	/// the difference being that strict version throws an exception if object is not found.
	/// </summary>
	/// <typeparam name="T">The type of object to get.</typeparam>
	/// <param name="id">Object's Id.</param>
	/// <returns>
	/// Returns the queried object if found.
	/// </returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	/// <exception cref="ArgumentException">
	/// If object with given `id` is not found in the database.
	/// <br/>
	/// or
	/// <br/>
	/// If type `T` is not marked with the <see cref="DatabaseClassAttribute"/>.
	/// </exception>
	[return: NotNull]
	public T GetObjectStrict<T>(long id) where T : DatabaseObject
	{
		T result = GetObject<T>(id);

		if (result == null)
			throw new ArgumentException($"Object of type {typeof(T).Name} with id {id} could not be found.");

		return result;
	}

	private DatabaseObject GetLocalObject(long id)
	{
		if (objectMap.TryGetValue(id, out DatabaseObject obj))
			return obj.IsDeleted ? null : obj;

		return null;
	}

	internal bool IsObjectDeleted(long id)
	{
		bool b = deletedSet.Contains(id);
#if TEST_BUILD
		if (objectMap.TryGetValue(id, out DatabaseObject obj))
			Checker.AssertTrue(obj.IsDeleted == b);
#endif

		return b;
	}

	internal DatabaseObject GetObject(long id)
	{
		if (disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		TTTrace.Write(transaction.Id, id);

		if (id == 0)
			return null;

		if (objectMap.TryGetValue(id, out DatabaseObject obj))
			return obj.IsDeleted ? null : obj;

		if (!modelData.TryGetClassByTypeId(IdHelper.GetClassId(id), out ClassData classData))
			return null;

		if (deletedSet.Contains(id))
			return null;

		byte* objPtr = null;
		if (transaction.Type == TransactionType.Read)
		{
			objPtr = ProvideObjectPointer(id, false);
			if ((ulong)objPtr == (ulong)sizeof(long))
				return null;
		}
		else
		{
			try
			{
				if (!engine.ObjectExists(transaction, classData.ClassDesc, id))
					return null;
			}
			catch (Exception e)
			{
				ProcessException(e);
				throw;
			}
		}

		obj = classData.Creator(this, classData, objPtr, id, DatabaseObjectState.None, null);

		return obj;
	}

	internal byte* ProvideObjectPointer(long id, bool forceNoReadLock)
	{
		ObjectReader or;
		try
		{
			or = engine.GetObject(transaction, id, forceNoReadLock);
		}
		catch (Exception e)
		{
			ProcessException(e);
			throw;
		}

		byte* objPtr = or.Object;
		TTTrace.Write(transaction.Id, id, (ulong)objPtr);
		return objPtr + sizeof(long); // Version is skipped
	}

	internal DatabaseObject GetObjectOrCreate(ObjectReader objReader, ClassData classData)
	{
		if (disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		long id = objReader.GetIdOptimized();

		TTTrace.Write(transaction.Id, id);

		if (objectMap.TryGetValue(id, out DatabaseObject obj))
			return obj.IsDeleted ? null : obj;

		if (deletedSet.Contains(id))
			return null;

		if (classData.ClassDesc.Id != IdHelper.GetClassId(id))
			classData = modelData.GetClassByClassId(IdHelper.GetClassId(id));

		TTTrace.Write(transaction.Id, id);

		byte* objPtr = objReader.Object;
		objPtr += sizeof(long); // Version is skipped
		obj = classData.Creator(this, classData, objPtr, id, DatabaseObjectState.None, null);

		return obj;
	}

	/// <summary>
	/// Create a new object.
	/// </summary>
	/// <typeparam name="T">Type of object to create.</typeparam>
	/// <returns>A new instance of requested type.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	/// <exception cref="ArgumentException">
	/// If requested type is abstract (<see cref="DatabaseClassAttribute.IsAbstract"/> is `true`)
	/// <br/>
	/// or
	/// <br/>
	/// If type `T` is not marked with the <see cref="DatabaseClassAttribute"/>.
	/// </exception>
	/// <remarks>
	/// The method creates a new object, which will be committed to the database after the database operation completes.
	/// The object is created with all properties set to default values. References are set to `null`, while simple types are set to
	/// <see cref="DatabasePropertyAttribute.DefaultValue"/> if specified, and 0 if not. The `Id` property is initialized and set
	/// to object's Id. This `Id` can be used to fetch the object from the database.
	/// </remarks>
	public T CreateObject<T>() where T : DatabaseObject
	{
		ValidateThread();

		if (disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		if (!modelData.TryGetClassByUserType(typeof(T), out ClassData classData) || classData.ClassDesc.IsAbstract)
			throw new ArgumentException("Invalid object type.");

		long id = classData.ClassDesc.MakeId(context.GetId());
		TTTrace.Write(transaction.Id, id);

		byte* buffer = AllocObjectBuffer(classData, true);
		*(long*)buffer = id;
		DatabaseObject obj = classData.Creator(this, classData, buffer, id, DatabaseObjectState.Inserted, changeList);

		return (T)obj;
	}

	internal T CreateObject<T>(long id) where T : DatabaseObject
	{
		ValidateThread();

		if (disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		if (!modelData.TryGetClassByUserType(typeof(T), out ClassData classData) || classData.ClassDesc.IsAbstract)
			throw new ArgumentException("Invalid object type.");

		TTTrace.Write(transaction.Id, id);

		byte* buffer = AllocObjectBuffer(classData, true);
		*(long*)buffer = id;
		DatabaseObject obj = classData.Creator(this, classData, buffer, id, DatabaseObjectState.Inserted, changeList);

		return (T)obj;
	}

	/// <summary>
	/// Retrieves all the objects of a given type (or any derived type) from the database.
	/// </summary>
	/// <typeparam name="T">Type of objects to get.</typeparam>
	/// <returns>`IEnumerable` of all objects of a given type in the database.</returns>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	/// <exception cref="ArgumentException">If type `T` is not marked with the<see cref="DatabaseClassAttribute"/></exception>
	/// <remarks>
	/// When used with a base type, `GetAllObjects` will return all subtypes as well.
	/// There is no guarantee for the order of returned objects.
	/// </remarks>
	public IEnumerable<T> GetAllObjects<T>() where T : DatabaseObject
	{
		ValidateThread();

		if (disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		if (!modelData.TryGetClassByUserType(typeof(T), out ClassData classData))
			throw new ArgumentException("Invalid object type.");

		TTTrace.Write(transaction.Id, classData.ClassDesc.Id);

		return new ClassScanEnumerable<T>(this, classData);
	}

	private void ForEachObject(ClassData classData, Action<ObjectReader> dbAction, Action<DatabaseObject> localAction)
	{
		ObjectReader[] rs = context.GetObjectReaders();

		try
		{
			ClassScan scan = engine.BeginClassScan(transaction, classData.ClassDesc, true);

			ChangeList.TypeIterator changeIterator = new ChangeList.TypeIterator();

			int objectCount = 0;
			int readObjectCount = 0;

			while (true)
			{
				if (scan != null)
				{
					if (readObjectCount >= objectCount)
					{
						readObjectCount = 0;
						if (!scan.Next(rs, out objectCount))
						{
							scan.Dispose();
							scan = null;
							changeIterator = changeList.IterateType(classData);
						}
					}
				}

				if (scan != null)
				{
					ObjectReader r = rs[readObjectCount++];
					long id = r.GetIdOptimized();
					if (objectMap.TryGetValue(id, out DatabaseObject obj))
					{
						if (obj.State == DatabaseObjectState.Read)
							localAction(obj);
					}
					else if (!deletedSet.Contains(id))
					{
						dbAction(r);
					}
				}
				else
				{
					DatabaseObject obj;
					do
					{
						if (!changeIterator.HasMore)
							return;

						obj = changeIterator.GetNextAndMove();
					}
					while (obj.IsDeleted);

					localAction(obj);
				}
			}
		}
		catch (Exception e)
		{
			ProcessException(e);
			throw;
		}
		finally
		{
			context.PutObjectReaders(rs);
		}
	}

	internal ChangeList.TypeIterator EnumerateLocalChanges(ClassData classData)
	{
		return changeList.IterateType(classData);
	}

	internal int GetTypeChangeCount(ClassData classData)
	{
		return changeList.GetTypeChangeCount(classData);
	}

	internal bool HasLocalChanges(ClassData classData)
	{
		return changeList.HasLocalChange(classData);
	}

	/// <summary>
	/// Get a hash index by name.
	/// </summary>
	/// <typeparam name="T">Type for which the hash index is requested.</typeparam>
	/// <typeparam name="TKey1">Type of hash index's key.</typeparam>
	/// <param name="name">The name of the hash index.</param>
	/// <returns>Requested hash index</returns>
	/// <exception cref="ArgumentException">
	/// 	If type `T` is not marked with the <see cref="DatabaseClassAttribute"/>
	/// 	<br/>
	/// 	or
	/// 	<br/>
	/// 	if requested hash index is not found.
	/// </exception>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	/// <seealso cref="HashIndexAttribute"/>
	/// <seealso cref="HashIndexReader{T, TKey1}"/>
	public HashIndexReader<T, TKey1> GetHashIndex<T, TKey1>(string name) where T : DatabaseObject
	{
		ValidateThread();
		IndexData indexData = modelData.GetIndexData(typeof(T).Namespace, name);
		return new HashIndexReader<T, TKey1>(this, indexData);
	}

	/// <summary>
	/// Get hash index with composite key by name.
	/// </summary>
	/// <typeparam name="T">Type for which the hash index is requested.</typeparam>
	/// <typeparam name="TKey1">Type of hash index's first key.</typeparam>
	/// <typeparam name="TKey2">Type of hash index's second key.</typeparam>
	/// <param name="name">The name of the hash index.</param>
	/// <returns>Requested hash index</returns>
	/// <exception cref="ArgumentException">
	/// 	If type `T` is not marked with the <see cref="DatabaseClassAttribute"/>
	/// 	<br/>
	/// 	or
	/// 	<br/>
	/// 	if requested hash index is not found.
	/// </exception>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	/// <seealso cref="HashIndexAttribute"/>
	/// <seealso cref="HashIndexReader{T, TKey1, TKey2}"/>
	public HashIndexReader<T, TKey1, TKey2> GetHashIndex<T, TKey1, TKey2>(string name) where T : DatabaseObject
	{
		ValidateThread();
		IndexData indexData = modelData.GetIndexData(typeof(T).Namespace, name);
		return new HashIndexReader<T, TKey1, TKey2>(this, indexData);
	}

	/// <summary>
	/// Get hash index with composite key by name.
	/// </summary>
	/// <typeparam name="T">Type for which the hash index is requested.</typeparam>
	/// <typeparam name="TKey1">Type of hash index's first key.</typeparam>
	/// <typeparam name="TKey2">Type of hash index's second key.</typeparam>
	/// <typeparam name="TKey3">Type of hash index's third key.</typeparam>
	/// <param name="name">The name of the hash index.</param>
	/// <returns>Requested hash index</returns>
	/// <exception cref="ArgumentException">
	/// 	If type `T` is not marked with the <see cref="DatabaseClassAttribute"/>
	/// 	<br/>
	/// 	or
	/// 	<br/>
	/// 	if requested hash index is not found.
	/// </exception>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	/// <seealso cref="HashIndexAttribute"/>
	/// <seealso cref="HashIndexReader{T, TKey1, TKey2, TKey3}"/>
	public HashIndexReader<T, TKey1, TKey2, TKey3> GetHashIndex<T, TKey1, TKey2, TKey3>(string name) where T : DatabaseObject
	{
		ValidateThread();
		IndexData indexData = modelData.GetIndexData(typeof(T).Namespace, name);
		return new HashIndexReader<T, TKey1, TKey2, TKey3>(this, indexData);
	}

	/// <summary>
	/// Get hash index with composite key by name.
	/// </summary>
	/// <typeparam name="T">Type for which the hash index is requested.</typeparam>
	/// <typeparam name="TKey1">Type of hash index's first key.</typeparam>
	/// <typeparam name="TKey2">Type of hash index's second key.</typeparam>
	/// <typeparam name="TKey3">Type of hash index's third key.</typeparam>
	/// <typeparam name="TKey4">Type of hash index's fourth key.</typeparam>
	/// <param name="name">The name of the hash index.</param>
	/// <returns>Requested hash index</returns>
	/// <exception cref="ArgumentException">
	/// 	If type `T` is not marked with the <see cref="DatabaseClassAttribute"/>
	/// 	<br/>
	/// 	or
	/// 	<br/>
	/// 	if requested hash index is not found.
	/// </exception>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	/// <seealso cref="HashIndexAttribute"/>
	/// <seealso cref="HashIndexReader{T, TKey1, TKey2, TKey3, TKey4}"/>
	public HashIndexReader<T, TKey1, TKey2, TKey3, TKey4> GetHashIndex<T, TKey1, TKey2, TKey3, TKey4>(string name) where T : DatabaseObject
	{
		ValidateThread();
		IndexData indexData = modelData.GetIndexData(typeof(T).Namespace, name);
		return new HashIndexReader<T, TKey1, TKey2, TKey3, TKey4>(this, indexData);
	}

	/// <summary>
	/// Get a sorted index by name.
	/// </summary>
	/// <typeparam name="T">Type for which the sorted index is requested.</typeparam>
	/// <typeparam name="TKey1">Type of sorted index's key.</typeparam>
	/// <param name="name">The name of the sorted index.</param>
	/// <returns>Requested sorted index</returns>
	/// <exception cref="ArgumentException">
	/// 	If type `T` is not marked with the <see cref="DatabaseClassAttribute"/>
	/// 	<br/>
	/// 	or
	/// 	<br/>
	/// 	if requested sorted index is not found.
	/// </exception>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	/// <seealso cref="SortedIndexAttribute"/>
	/// <seealso cref="SortedIndexReader{T, TKey1}"/>
	public SortedIndexReader<T, TKey1> GetSortedIndex<T, TKey1>(string name) where T : DatabaseObject
	{
		ValidateThread();
		IndexData indexData = modelData.GetIndexData(typeof(T).Namespace, name);
		return new SortedIndexReader<T, TKey1>(this, indexData);
	}

	/// <summary>
	/// Get sorted index with composite key by name.
	/// </summary>
	/// <typeparam name="T">Type for which the sorted index is requested.</typeparam>
	/// <typeparam name="TKey1">Type of sorted index's first key.</typeparam>
	/// <typeparam name="TKey2">Type of sorted index's second key.</typeparam>
	/// <param name="name">The name of the sorted index.</param>
	/// <returns>Requested sorted index</returns>
	/// <exception cref="ArgumentException">
	/// 	If type `T` is not marked with the <see cref="DatabaseClassAttribute"/>
	/// 	<br/>
	/// 	or
	/// 	<br/>
	/// 	if requested sorted index is not found.
	/// </exception>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	/// <seealso cref="SortedIndexAttribute"/>
	/// <seealso cref="SortedIndexReader{T, TKey1, TKey2}"/>
	public SortedIndexReader<T, TKey1, TKey2> GetSortedIndex<T, TKey1, TKey2>(string name) where T : DatabaseObject
	{
		ValidateThread();
		IndexData indexData = modelData.GetIndexData(typeof(T).Namespace, name);
		return new SortedIndexReader<T, TKey1, TKey2>(this, indexData);
	}

	/// <summary>
	/// Get sorted index with composite key by name.
	/// </summary>
	/// <typeparam name="T">Type for which the sorted index is requested.</typeparam>
	/// <typeparam name="TKey1">Type of sorted index's first key.</typeparam>
	/// <typeparam name="TKey2">Type of sorted index's second key.</typeparam>
	/// <typeparam name="TKey3">Type of sorted index's third key.</typeparam>
	/// <param name="name">The name of the sorted index.</param>
	/// <returns>Requested sorted index</returns>
	/// <exception cref="ArgumentException">
	/// 	If type `T` is not marked with the <see cref="DatabaseClassAttribute"/>
	/// 	<br/>
	/// 	or
	/// 	<br/>
	/// 	if requested sorted index is not found.
	/// </exception>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	/// <seealso cref="SortedIndexAttribute"/>
	/// <seealso cref="SortedIndexReader{T, TKey1, TKey2, TKey3}"/>
	public SortedIndexReader<T, TKey1, TKey2, TKey3> GetSortedIndex<T, TKey1, TKey2, TKey3>(string name) where T : DatabaseObject
	{
		ValidateThread();
		IndexData indexData = modelData.GetIndexData(typeof(T).Namespace, name);
		return new SortedIndexReader<T, TKey1, TKey2, TKey3>(this, indexData);
	}

	/// <summary>
	/// Get sorted index with composite key by name.
	/// </summary>
	/// <typeparam name="T">Type for which the sorted index is requested.</typeparam>
	/// <typeparam name="TKey1">Type of sorted index's first key.</typeparam>
	/// <typeparam name="TKey2">Type of sorted index's second key.</typeparam>
	/// <typeparam name="TKey3">Type of sorted index's third key.</typeparam>
	/// <typeparam name="TKey4">Type of sorted index's fourth key.</typeparam>
	/// <param name="name">The name of the sorted index.</param>
	/// <returns>Requested sorted index</returns>
	/// <exception cref="ArgumentException">
	/// 	If type `T` is not marked with the <see cref="DatabaseClassAttribute"/>
	/// 	<br/>
	/// 	or
	/// 	<br/>
	/// 	if requested sorted index is not found.
	/// </exception>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	/// <seealso cref="SortedIndexAttribute"/>
	/// <seealso cref="SortedIndexReader{T, TKey1, TKey2, TKey3, TKey4}"/>
	public SortedIndexReader<T, TKey1, TKey2, TKey3, TKey4> GetSortedIndex<T, TKey1, TKey2, TKey3, TKey4>(string name) where T : DatabaseObject
	{
		ValidateThread();
		IndexData indexData = modelData.GetIndexData(typeof(T).Namespace, name);
		return new SortedIndexReader<T, TKey1, TKey2, TKey3, TKey4>(this, indexData);
	}

	internal void AbandonObject(DatabaseObject obj)
	{
		if (disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		TTTrace.Write(transaction.Id, obj.Id);
		obj.State = DatabaseObjectState.Abandoned;
		objectMap.Remove(obj.Id);
	}

	internal void DeleteObject(DatabaseObject obj, bool performCascadeDelete)
	{
		if (disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		TTTrace.Write(transaction.Id, obj.Id, performCascadeDelete);

		obj.InvalidateInverseReferences();

		if (obj.State == DatabaseObjectState.Read)
			changeList.Add(obj);

		obj.State |= DatabaseObjectState.Deleted;

		if (performCascadeDelete)
		{
			Checker.AssertTrue(!deletedSet.Contains(obj.Id));

			int prevDeletedCount = deletedSet.Count;

			int count = 1;
			toDelete1[0] = obj.Id;
			while (count > 0)
			{
				TTTrace.Write(transaction.Id, count);
				SingleIterationDelete(ref count);
				Utils.Exchange(ref toDelete1, ref toDelete2);
			}

			TTTrace.Write(transaction.Id, deletedSet.Count, prevDeletedCount);
			if (deletedSet.Count > prevDeletedCount)
				deletedSet.IncVersion();
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal long GetSetToNullReference(long id)
	{
		TTTrace.Write(transaction.Id, deletedSet.Contains(id), id);

		if (deletedSet.Contains(id))
		{
			return 0;
		}
		else
		{
			return id;
		}
	}

	private void SingleIterationDelete(ref int count)
	{
		int newCount = 0;
		ScanClassesSet scanClasses = context.ScanClasses;

		try
		{
			for (int i = 0; i < count; i++)
			{
				long id = toDelete1[i];

				TTTrace.Write(transaction.Id, id);

				DatabaseObject obj = GetObject(id);
				if (obj != null)
				{
					TTTrace.Write(transaction.Id, id);
					DeleteObject(obj, false);
				}

				deletedSet.Add(id);

				ClassDescriptor classDesc = modelData.ModelDesc.GetClass(IdHelper.GetClassId(id));
				ReadOnlyArray<ReferencePropertyDescriptor> invRefs = classDesc.CascadeDeletePreventInverseReferences;
				for (int j = 0; j < invRefs.Length; j++)
				{
					ReferencePropertyDescriptor p = invRefs[j];
					if (p.TrackInverseReferences)
					{
						PrepareInverseReferences(id, p.Id, out int cascCount);
						if (p.DeleteTargetAction == DeleteTargetAction.PreventDelete && cascCount > 0)
							DisposeAndThrowDueToPreventDelete(p, id, inverseReferenceIds[0]);

						for (int k = 0; k < cascCount; k++)
						{
							TTTrace.Write(transaction.Id, id, inverseReferenceIds[k]);
							toDelete2[newCount++] = inverseReferenceIds[k];
						}
					}
					else
					{
						TTTrace.Write(transaction.Id, id, p.Id);
						scanClasses.AddInvReferenceProperty(p, modelData);
					}
				}
			}

			if (scanClasses.Count > 0)
				ScanClassesAndCascadeDelete(scanClasses, ref newCount);

			TTTrace.Write(transaction.Id, newCount);
			count = newCount;
		}
		finally
		{
			scanClasses.Clear();
		}
	}

	private void DisposeAndThrowDueToPreventDelete(ReferencePropertyDescriptor propDesc, long id, long referencingId)
	{
		TTTrace.Write(transaction.Id, referencingId);
		Dispose();
		ClassDescriptor classDesc = IdHelper.GetClass(propDesc.OwnerClass.Model, referencingId);
		throw new DatabaseException(DatabaseErrorDetail.CreateReferencedDelete(id, referencingId, classDesc.FullName, propDesc.Name));
	}

	private void ScanClassesAndCascadeDelete(ScanClassesSet scanClasses, ref int newCount)
	{
		TTTrace.Write(transaction.Id, scanClasses.Count, newCount);

		int nc = newCount;
		scanClasses.ForEeach((key, value) =>
		{
			TTTrace.Write(transaction.Id, key, value.Count);

			ClassData classData = modelData.GetClassByClassId(key);
			List<ScanClassesSet.ScanedProperty> l = value;
			ForEachObject(classData, r =>
			{
				TTTrace.Write(transaction.Id, r.GetIdOptimized(), value.Count);

				for (int i = 0; i < value.Count; i++)
				{
					ReferencePropertyDescriptor p = l[i].Property;
					TTTrace.Write(transaction.Id, r.GetIdOptimized(), p.Id, (int)p.Multiplicity);

					if (p.Multiplicity == Multiplicity.Many)
					{
						if (r.LongArrayContainsValue(p.Id, deletedSet.Ids, out long value))
						{
							TTTrace.Write(transaction.Id, r.GetIdOptimized(), (int)p.DeleteTargetAction, nc);

							if (p.DeleteTargetAction == DeleteTargetAction.PreventDelete)
								DisposeAndThrowDueToPreventDelete(p, value, r.GetIdOptimized());

							toDelete2[nc++] = r.GetIdOptimized();
						}
					}
					else
					{
						long id = r.GetLongByIdOptimized(p.Id);
						TTTrace.Write(transaction.Id, r.GetIdOptimized(), id, deletedSet.Count);

						if (deletedSet.Contains(id))
						{
							TTTrace.Write(transaction.Id, r.GetIdOptimized(), id, (int)p.DeleteTargetAction, nc);

							if (p.DeleteTargetAction == DeleteTargetAction.PreventDelete)
								DisposeAndThrowDueToPreventDelete(p, id, r.GetIdOptimized());

							toDelete2[nc++] = r.GetIdOptimized();
						}
					}
				}
			}, obj =>
			{
				TTTrace.Write(transaction.Id, obj.Id, value.Count, deletedSet.Count);
				for (int i = 0; i < value.Count; i++)
				{
					ClassDescriptor classDesc = obj.ClassData.ClassDesc;
					// Field offset is provided here as an argument and it can't be built into the checker code since
					// checker might be generated for an abstract class (owner of the reference property).
					// Offset is calculated starting from the id (version is skipped).
					int fieldOffset = classDesc.PropertyByteOffsets[classDesc.GetPropertyIndex(l[i].Property.Id)] - sizeof(long);
					if (l[i].ReferenceChecker(obj, deletedSet.Ids, fieldOffset))
					{
						TTTrace.Write(transaction.Id, obj.Id, nc);
						toDelete2[nc++] = obj.Id;
					}
				}
			});
		});

		newCount = nc;
	}

	internal void InvalidateInverseReference(long refId, int propertyId)
	{
		TTTrace.Write(transaction.Id, refId, propertyId);

		if (refId == 0)
			return;

		DatabaseObject obj = GetLocalObject(refId);
		if (obj == null)
			return;

		InverseReferenceInvalidator invalidator = obj.ClassData.GetInverseReferenceInvalidator(propertyId);
		invalidator?.Invoke(obj);
	}

	internal void InvalidateInverseReferences(ReferenceArray refIds, int propertyId)
	{
		if (refIds == null)
			return;

		TTTrace.Write(transaction.Id, propertyId, refIds.Count);

		int count = refIds.Count;
		for (int i = 0; i < count; i++)
		{
			long refId = refIds.GetId(i);
			TTTrace.Write(transaction.Id, propertyId, refId);

			if (refId == 0)
				continue;

			DatabaseObject obj = GetLocalObject(refId);
			if (obj == null)
				continue;

			InverseReferenceInvalidator invalidator = obj.ClassData.GetInverseReferenceInvalidator(propertyId);
			invalidator?.Invoke(obj);
		}
	}

	internal void InvalidateInverseReferencesFromHandle(ulong handle, int propertyId)
	{
		if (handle == 0)
			return;

		long* lp = PropertyTypesHelper.DBUnpackLongArray(engineBlobStorage.RetrieveBlob(handle), out int count);

		TTTrace.Write(transaction.Id, propertyId, (ulong)lp, count);

		for (int i = 0; i < count; i++)
		{
			long refId = lp[i];
			TTTrace.Write(transaction.Id, propertyId, refId);

			if (refId == 0)
				continue;

			DatabaseObject obj = GetLocalObject(refId);
			if (obj == null)
				continue;

			InverseReferenceInvalidator invalidator = obj.ClassData.GetInverseReferenceInvalidator(propertyId);
			invalidator?.Invoke(obj);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void ReferenceModified(long inverseRefId, long oldDirectRefId, long newDirectRefId, int propertyId)
	{
		TTTrace.Write(transaction.Id, inverseRefId, oldDirectRefId, newDirectRefId, propertyId);

		if (oldDirectRefId != 0)
		{
			InvalidateInverseReference(oldDirectRefId, propertyId);
			invRefChanges.Add(oldDirectRefId, inverseRefId, propertyId, false);
		}

		if (newDirectRefId != 0)
		{
			InvalidateInverseReference(newDirectRefId, propertyId);
			invRefChanges.Add(newDirectRefId, inverseRefId, propertyId, true);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void ReferenceArrayModified(long inverseRefId, ReferenceArray oldDirectRefIds, ReferenceArray newDirectRefIds, int propertyId)
	{
		if (oldDirectRefIds != null)
		{
			TTTrace.Write(transaction.Id, inverseRefId, oldDirectRefIds.Count, propertyId);
			for (int i = 0; i < oldDirectRefIds.Count; i++)
			{
				long id = oldDirectRefIds.GetId(i);
				TTTrace.Write(transaction.Id, inverseRefId, id, propertyId);
				if (id != 0)
				{
					InvalidateInverseReference(id, propertyId);
					invRefChanges.Add(id, inverseRefId, propertyId, false);
				}
			}
		}

		if (newDirectRefIds != null)
		{
			TTTrace.Write(transaction.Id, inverseRefId, newDirectRefIds.Count, propertyId);
			for (int i = 0; i < newDirectRefIds.Count; i++)
			{
				long id = newDirectRefIds.GetId(i);
				TTTrace.Write(transaction.Id, inverseRefId, id, propertyId);
				if (id != 0)
				{
					InvalidateInverseReference(id, propertyId);
					invRefChanges.Add(id, inverseRefId, propertyId, true);
				}
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void ObjectModified(DatabaseObject obj)
	{
		obj.VerifyModifyAccess();

		if (obj.State == DatabaseObjectState.Read)
		{
			ObjectModifiedFirstTime(obj);
			return;
		}
	}

	private void ObjectModifiedFirstTime(DatabaseObject obj)
	{
		if (transaction.Type == TransactionType.Read)
			throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.ReadTranWriteAttempt));

		byte* buffer = AllocObjectBuffer(obj.ClassData, false);
		obj.Modified(buffer, obj.ClassData.ObjectSize);
		changeList.Add(obj);

		TTTrace.Write(transaction.Id, obj.Id, (ulong)buffer);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private byte* AllocObjectBuffer(ClassData classData, bool zeroedOut)
	{
		try
		{
			byte* buffer = classData.MemoryManager.Allocate(memoryManager);

			for (int i = 0; i < classData.BitFieldByteSize; i++)
			{
				buffer[i] = 0;  // Reset bit fields
			}

			buffer += classData.BitFieldByteSize;
			if (zeroedOut)
				Utils.ZeroMemory(buffer + sizeof(long), classData.ObjectSize - sizeof(long));

			return buffer;
		}
		catch (Exception e)
		{
			ProcessException(e);
			throw;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void FreeObjectBuffer(ClassData classData, byte* buffer)
	{
		try
		{
			TTTrace.Write(transaction.Id, classData.ClassDesc.Id, (ulong)buffer);
			buffer -= classData.BitFieldByteSize;
			classData.MemoryManager.Free(memoryManager, buffer);
		}
		catch (Exception e)
		{
			ProcessException(e);
			throw;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal string GetString(ulong handle, bool modified)
	{
		try
		{
			TTTrace.Write(transaction.Id, handle, modified);

			if (handle == 0)
				return null;

			if (modified)
			{
				handle--;
				return strings[handle];
			}
			else
			{
				return engineStringStorage.GetString(handle);
			}
		}
		catch (Exception e)
		{
			ProcessException(e);
			throw;
		}
	}

	private void PrepareInverseReferences(long id, int propertyId, out int count)
	{
		TTTrace.Write(transaction.Id, id, propertyId);

		try
		{
			engine.GetInverseReferences(transaction, id, propertyId, ref inverseReferenceIds, out count);
		}
		catch (Exception e)
		{
			ProcessException(e);
			throw;
		}

		if (invRefChanges.TryCollectChanges(id, propertyId, deletedInvRefs, ref inverseReferenceIds, ref count) && deletedInvRefs.Count > 0)
		{
			TTTrace.Write(transaction.Id, id, propertyId, deletedInvRefs.Count, count);

			int rem = 0;
			for (int i = 0; i < count; i++)
			{
				if (deletedInvRefs.TryGetValue(inverseReferenceIds[i], out int c))
				{
					TTTrace.Write(transaction.Id, id, propertyId, inverseReferenceIds[i], c, rem);

					rem++;
					c--;
					if (c == 0)
					{
						deletedInvRefs.Remove(inverseReferenceIds[i]);
					}
					else
					{
						deletedInvRefs[inverseReferenceIds[i]] = c;
					}
				}
				else if (rem > 0)
				{
					TTTrace.Write(transaction.Id, id, propertyId, inverseReferenceIds[i], rem);
					inverseReferenceIds[i - rem] = inverseReferenceIds[i];
				}
			}

			TTTrace.Write(transaction.Id, id, propertyId, count, rem);
			count -= rem;
			Checker.AssertTrue(deletedInvRefs.Count == 0);
		}

		if (deletedSet.HasDeleted)
		{
			TTTrace.Write(transaction.Id, id, propertyId, count);

			int rem = 0;
			for (int i = 0; i < count; i++)
			{
				if (deletedSet.Contains(inverseReferenceIds[i]))
				{
					TTTrace.Write(transaction.Id, id, propertyId, inverseReferenceIds[i], rem);
					rem++;
				}
				else if (rem > 0)
				{
					TTTrace.Write(transaction.Id, id, propertyId, inverseReferenceIds[i], rem);
					inverseReferenceIds[i - rem] = inverseReferenceIds[i];
				}
			}

			TTTrace.Write(transaction.Id, id, propertyId, count, rem);
			count -= rem;
		}
	}

	internal void GetInverseReferences(long id, int propertyId, ref long[] invRefIds, out int count)
	{
		PrepareInverseReferences(id, propertyId, out count);

		TTTrace.Write(transaction.Id, id, propertyId, count);

		if (invRefIds.Length < count)
			Array.Resize(ref invRefIds, Math.Max(count, invRefIds.Length * 2));

		for (int i = 0; i < count; i++)
		{
			TTTrace.Write(transaction.Id, id, propertyId, inverseReferenceIds[i]);
			invRefIds[i] = inverseReferenceIds[i];
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal byte* GetArray(ulong handle)
	{
		try
		{
			return engineBlobStorage.RetrieveBlob(handle);
		}
		catch (Exception e)
		{
			ProcessException(e);
			throw;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal uint StoreString(string s)
	{
		if (strings.Length == stringCount)
			Array.Resize<string>(ref strings, strings.Length * 2);

		TTTrace.Write(transaction.Id, stringCount);
		strings[stringCount++] = s;
		return (uint)stringCount;
	}

	/// <summary>
	/// Applies changes accumulated in <see cref="ObjectModel"/> to the database.
	/// </summary>
	/// <exception cref="ObjectDisposedException">If <see cref="ObjectModel"/> has been disposed.</exception>
	/// <remarks>
	/// Changes made during the transaction are kept in <see cref="ObjectModel"/> until the operation ends.
	/// These changes as they accumulate can negatively impact <see cref="ObjectModel"/> performance. <see cref="ObjectModel.ApplyChanges()"/>
	/// allows you to apply these changes early. This will clear <see cref="ObjectModel"/>'s caches and can improve performance.
	/// <note>
	/// 	Database must be in consistent state when <see cref="ObjectModel.ApplyChanges()"/> is called.
	/// 	If there are any non nullable references that are set to null <see cref="ObjectModel.ApplyChanges()"/> will
	/// 	throw an exception and transaction will be rolled back.
	/// </note>
	/// </remarks>
	public void ApplyChanges()
	{
		ValidateThread();

		if (disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		ApplyChanges(false);
	}

	private void ApplyChanges(bool isCommit)
	{
		TTTrace.Write(transaction.Id, isCommit, changeList.Count, deletedSet.HasDeleted);

		if (changeList.Count == 0 && !deletedSet.HasDeleted)
			return;

		ChangesetWriter writer = engine.ChangesetWriterPool.Get();

		try
		{
			if (deletedSet.HasDeleted)
			{
				deletedSet.ForEach(id =>
				{
					TTTrace.Write(transaction.Id, id);
					if (!objectMap.TryGetValue(id, out DatabaseObject obj) || !obj.IsCreated)
					{
						TTTrace.Write(transaction.Id, id, obj != null, obj == null ? false : obj.IsCreated);
						ClassDescriptor classDesc = IdHelper.GetClass(modelData.ModelDesc, id);
						writer.StartDeleteBlock(classDesc);
						writer.AddDelete(id);
					}
				});
			}

			for (int i = 0; i < changeList.Count; i++)
			{
				DatabaseObject obj = changeList[i];
				TTTrace.Write(transaction.Id, obj.Id, obj.IsDeleted, obj.IsCreated);

				if (!obj.IsDeleted)
				{
					if (obj.IsCreated)
					{
						obj.CreateInsertBlock(writer);
					}
					else
					{
						obj.CreateUpdateBlock(writer);
					}
				}

				if (isCommit)
					obj.Cleanup();
			}

			try
			{
				using (Changeset ch = writer.FinishWriting())
				{
					engine.ApplyChangeset(transaction, ch, true);
				}
			}
			finally
			{
				if (!isCommit)
				{


					for (int i = 0; i < changeList.Count; i++)
					{
						DatabaseObject obj = changeList[i];
						long id = obj.Id;
						obj.Cleanup();

						if (!obj.IsDeleted)
						{
							RefreshObjectFromDatabase(obj);
						}
					}

					transaction.ProcessSetToNullAffected(refresher, this);

					deletedSet.Clear();
					invRefChanges.Clear();
					deletedInvRefs.Clear();
					stringCount = 0;
				}

				changeList.Clear();
			}
		}
		catch (Exception e)
		{
			ProcessException(e);
			throw;
		}
		finally
		{
			engine.ChangesetWriterPool.Put(writer);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void ValidateThread()
	{
		if (ownerThreadId != Thread.CurrentThread.ManagedThreadId)
			throw new InvalidOperationException("It is not allowed to access object model from a thread different than the one that created it.");
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void RefreshObjectFromDatabase(DatabaseObject obj)
	{
		try
		{
			ObjectReader or = engine.GetObject(transaction, obj.Id);
			Checker.AssertFalse(or.IsEmpty());
			byte* objPtr = or.Object + sizeof(long);    // Version is skipped
			obj.Refresh(objPtr);
		}
		catch (Exception e)
		{
			ProcessException(e);
			throw;
		}
	}

	/// <summary>
	/// Rollbacks the transaction associated with this instance of ObjectModel class. ObjectModel instance is no longer usable after this.
	/// </summary>
	public void Rollback()
	{
		TTTrace.Write(transaction.Id);

		ValidateThread();
		engine.RollbackTransaction(transaction);
		Dispose();
	}

	internal void CommitAndDispose()
	{
		TTTrace.Write(transaction.Id);

		ValidateThread();

		if (disposed)
			return;

		ApplyChanges(true);

		try
		{
			engine.CommitTransaction(transaction);
		}
		catch (Exception e)
		{
			ProcessException(e);
			throw;
		}

		DisposeInternal(false);
	}

	internal void CommitAsyncAndDispose(Action<object, DatabaseException> callback, object state)
	{
		TTTrace.Write(transaction.Id);

		ValidateThread();

		if (disposed)
			return;

		ApplyChanges(true);

		DisposeInternal(false);

		try
		{
			engine.CommitTransactionAsync(transaction, callback, state);
		}
		catch (Exception e)
		{
			ProcessException(e);
			throw;
		}
	}

	internal void Dispose()
	{
		TTTrace.Write(transaction.Id);
		DisposeInternal(true);
	}

	internal void ProcessException(Exception e)
	{
		if (e is not DatabaseException)
		{
			TTTrace.Write(transaction.Id, e.GetType().Name);
			if (e is CriticalDatabaseException)
			{
				storedException = (CriticalDatabaseException)e;
			}
			else
			{
				storedException = new CriticalDatabaseException("Unexpected error occured.", e);
			}

			Dispose();
		}
		else
		{
			TTTrace.Write(transaction.Id, (int)(e as DatabaseException).Detail.ErrorType);
			if (transaction.Closed)
				Dispose();
		}
	}

	private void DisposeInternal(bool disposeTransaction)
	{
		ValidateThread();

		if (disposed)
			return;

		disposed = true;

		TTTrace.Write(transaction.Id, disposeTransaction);

		for (int i = 0; i < changeList.Count; i++)
		{
			DatabaseObject obj = changeList[i];
			obj.Cleanup();
		}

		if (disposeTransaction)
			transaction?.Dispose();

		context.Reset();
		contextPool.PutContext(context);
	}
}
