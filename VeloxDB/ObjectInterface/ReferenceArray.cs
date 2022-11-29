using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Velox.Common;
using Velox.Descriptor;
using Velox.Storage;

namespace Velox.ObjectInterface;

/// <summary>
/// Base Reference Array class, you should always use generic version <see cref="ReferenceArray{T}"/>
/// </summary>
/// <seealso cref="ReferenceArray{T}"/>
public abstract class ReferenceArray
{
	DatabaseObject owner;
	ReferencePropertyDescriptor propDesc;

	Action<DatabaseObject> changeNotifier;

	internal ReferenceArray(DatabaseObject owner, Action<DatabaseObject> changeNotifier, ReferencePropertyDescriptor propDesc)
	{
		this.owner = owner;
		this.propDesc = propDesc;
		this.changeNotifier = changeNotifier;
	}

	/// <summary>
	/// Gets the number of elements contained in the <see cref="ReferenceArray"/>.
	/// </summary>
	public abstract int Count { get; }
	internal DatabaseObject Owner => owner;
	internal Action<DatabaseObject> ChangeNotifier => changeNotifier;
	internal ReferencePropertyDescriptor PropertyDescritpor => propDesc;
	internal int PropertyId => propDesc.Id;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void SetOwner(DatabaseObject owner, ReferencePropertyDescriptor propDesc, Action<DatabaseObject> changeNotifier)
	{
		this.owner = owner;
		this.propDesc = propDesc;
		this.changeNotifier = changeNotifier;
		InitFilterDeleted();
		ValidateOwnershipChange();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void WriteToChangesetWriter(ChangesetWriter writer, ReferenceArray array)
	{
		if (array == null)
		{
			writer.WriteNullArray();
			return;
		}

		array.WriteToChangesetWriter(writer);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal unsafe static void Refresh(ReferenceArray ra, byte* buffer)
	{
		if (ra != null)
			ra.Refresh(buffer);
	}

	internal abstract long GetId(int index);
	private protected abstract void InitFilterDeleted();
	internal unsafe abstract void Refresh(byte* buffer);
	internal abstract void WriteToChangesetWriter(ChangesetWriter writer);
	internal abstract void ValidateOwnershipChange();
}

/// <summary>
/// Represents an array of references to database objects.
/// </summary>
/// <typeparam name="T">The type of items in the array.</typeparam>
/// <seealso cref="DatabaseArray{T}"/>
/// <seealso cref="DatabaseReferenceAttribute"/>
public unsafe sealed class ReferenceArray<T> : ReferenceArray, IList<T> where T : DatabaseObject
{
	int count;
	byte* buffer;
	long[] ids;
	int lastDeletedVersion;
	int version;

	/// <summary>
	/// Creates a new empty instance of the <see cref="ReferenceArray{T}"/>.
	/// </summary>
	public ReferenceArray() : this(4)
	{
	}

	/// <summary>
	/// Creates a new empty instance of the <see cref="ReferenceArray{T}"/>. With specified capacity.
	/// </summary>
	/// <param name="capacity">Initial capacity. The initial size of the backing array.</param>
	public ReferenceArray(int capacity = 4) :
		base(null, null, null)
	{
		ids = new long[Math.Max(1, capacity)];
	}

	/// <summary>
	/// Creates a new instance of the <see cref="ReferenceArray{T}"/> that contains elements copied from the supplied collection.
	/// </summary>
	/// <param name="collection">The collection whose elements are to be copied.</param>
	/// <exception cref="ArgumentNullException">`collection` is `null` or an item in the collection is `null`.</exception>
	/// <exception cref="ArgumentException">If any item in `collection` is a deleted item.</exception>
	public ReferenceArray(IEnumerable<T> collection) :
		base(null, null, null)
	{
		Checker.NotNull(collection, nameof(collection));
		int capacity = 8;
		ICollection<T> c = collection as ICollection<T>;
		if (collection != null)
			capacity = c.Count;

		ids = new long[Math.Max(4, capacity)];
		foreach (T item in collection)
		{
			if (item.IsDeleted)
				throw new ArgumentException("Object has been deleted.");

			Add(item);
		}

		lastDeletedVersion = -1;
	}

	internal ReferenceArray(DatabaseObject owner, Action<DatabaseObject> changeNotifier, byte* buffer, int propId) :
		base(owner, changeNotifier, (ReferencePropertyDescriptor)owner.ClassData.ClassDesc.GetProperty(propId))
	{
		owner.VerifyAccess();

		this.count = *(int*)buffer;
		this.buffer = buffer + sizeof(int);
		this.lastDeletedVersion = -1;
	}

	/// <summary>
	/// Gets the number of items contained in the <see cref="ReferenceArray{T}"/>.
	/// </summary>
	/// <exception cref="InvalidOperationException">If the parent object of the <see cref="DatabaseArray{T}"/> has been deleted or abandoned.</exception>
	/// <exception cref="ObjectDisposedException">If the parent object of the <see cref="DatabaseArray{T}"/> has been disposed.</exception>
	public override int Count
	{
		get
		{
			Owner?.VerifyAccess();
			CheckAndFilterDeleted();
			return count;
		}
	}

	/// <summary>
	/// Gets if the <see cref="ReferenceArray{T}"/> is Readonly. Always returns false.
	/// </summary>
	public bool IsReadOnly => false;

	/// <summary>
	/// Index accessor.
	/// </summary>
	/// <param name="index">Index of the item to get.</param>
	/// <returns>Requested item.</returns>
	/// <exception cref="IndexOutOfRangeException">If `index` is less than 0 or greater than <see cref="Count"/></exception>
	/// <exception cref="InvalidOperationException">If the parent object of the <see cref="DatabaseArray{T}"/> has been deleted or abandoned.</exception>
	/// <exception cref="ObjectDisposedException">If the parent object of the <see cref="DatabaseArray{T}"/> has been disposed.</exception>
	public T this[int index]
	{
		get
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			CheckAndFilterDeleted();

			long id = ids != null ? ids[index] : ((long*)buffer)[index];
			return (T)(object)Owner.Owner.GetObject(id);
		}

		set
		{
			if (value == null)
				throw new ArgumentNullException(nameof(value));

			Owner?.VerifyAccessAndModel((DatabaseObject)(object)value);

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			CheckAndFilterDeleted();

			if (ids == null)
				CreateModifiedCopy();

			if (Owner != null && PropertyDescritpor.TrackInverseReferences)
				Owner.Owner.ReferenceModified(Owner.Id, ids[index], value.Id, PropertyId);

			version++;
			ids[index] = value.Id;
		}
	}

	internal override long GetId(int index)
	{
		CheckAndFilterDeleted();
		long id = ids != null ? ids[index] : ((long*)buffer)[index];
		return id;
	}

	private protected override void InitFilterDeleted()
	{
		CheckAndFilterDeleted();
	}

	internal override void Refresh(byte* buffer)
	{
		ids = null;
		this.count = *(int*)buffer;
		this.buffer = buffer + sizeof(int);
		lastDeletedVersion = Owner.Owner.DeletedSet.Version;
	}

	internal void AddOrRemove(T item, bool isAdd)
	{
		if (isAdd)
			Add(item);
		else
			Remove(item);
	}

	/// <summary>
	/// Add an item to the end of the <see cref="ReferenceArray{T}"/>.
	/// </summary>
	/// <param name="item">The item to be added to the end of the <see cref="ReferenceArray{T}"/>.</param>
	/// <exception cref="ArgumentNullException">`item` is `null`</exception>
	/// <exception cref="InvalidOperationException">
	/// If the added item would cause <see cref="Count"/> to exceed `int.MaxValue`
	/// <br/>
	/// or
	/// <br/>
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been disposed.
	/// </exception>
	public void Add(T item)
	{
		if (item == null)
			throw new ArgumentNullException(nameof(item));

		Owner?.VerifyAccessAndModel((DatabaseObject)(object)item);
		CheckAndFilterDeleted();

		if (ids == null)
			CreateModifiedCopy();

		if (count == ids.Length)
			Resize();

		if (Owner != null && PropertyDescritpor.TrackInverseReferences)
			Owner.Owner.ReferenceModified(Owner.Id, 0, item.Id, PropertyId);

		version++;
		ids[count++] = item.Id;
	}

	/// <summary>
	/// Adds the elements of the specified collection to the end of the <see cref="DatabaseArray{T}"/>.
	/// </summary>
	/// <param name="collection">Collection whose elements will be added.</param>
	/// <exception cref="ArgumentNullException">`collection` is `null`.</exception>
	public void AddRange(IEnumerable<T> collection)
	{
		Checker.NotNull(collection, nameof(collection));
		Owner?.VerifyAccess();

		ICollection<T> c = collection as ICollection<T>;
		if (collection != null)
		{
			long desiredSize = (long)Count + c.Count;

			if(desiredSize > int.MaxValue)
				throw MaximumSizeExceeded();

			if(desiredSize > ids.Length)
				Resize((int)desiredSize);
		}

		foreach(T item in collection)
		{
			Add(item);
		}
	}

	/// <summary>
	/// Clears the <see cref="ReferenceArray{T}"/>.
	/// </summary>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been disposed.
	/// </exception>
	public void Clear()
	{
		Owner?.VerifyAccess();
		CheckAndFilterDeleted();

		if (ids == null)
			CreateModifiedCopy();

		if (Owner != null && PropertyDescritpor.TrackInverseReferences)
			Owner.Owner.ReferenceArrayModified(Owner.Id, this, null, PropertyId);

		version++;
		count = 0;
	}

	/// <summary>
	/// Determines if an item is in the <see cref="ReferenceArray{T}"/>.
	/// </summary>
	/// <param name="item">The item to locate in the <see cref="ReferenceArray{T}"/>.</param>
	/// <returns>`true` if item is present in the <see cref="ReferenceArray{T}"/>, false if not.</returns>
	/// <exception cref="ArgumentNullException">`item` is `null`.</exception>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been disposed.
	/// </exception>
	public bool Contains(T item)
	{
		if (item == null)
			throw new ArgumentNullException(nameof(item));

		Owner?.VerifyAccessAndModel((DatabaseObject)(object)item);
		CheckAndFilterDeleted();

		IEqualityComparer<T> comp = EqualityComparer<T>.Default;
		for (int i = 0; i < count; i++)
		{
			if (comp.Equals(this[i], item))
				return true;
		}

		return false;
	}

	internal bool ContainsAnyId(LongHashSet ids)
	{
		if (this.ids != null)
		{
			for (int i = 0; i < count; i++)
			{
				if (ids.Contains(this.ids[i]))
					return true;
			}
		}
		else
		{
			for (int i = 0; i < count; i++)
			{
				long id = ((long*)buffer)[i];
				if (ids.Contains(id))
					return true;
			}
		}

		return false;
	}

	/// <summary>
	/// Copies all items from the <see cref="ReferenceArray{T}"/> to the given array, starting at the specified index of the target array.
	/// </summary>
	/// <param name="array">Destination array.</param>
	/// <param name="arrayIndex">Zero based index in `array` at which copying begins.</param>
	/// <exception cref="ArgumentNullException">`array` is `null`.</exception>
	/// <exception cref="ArgumentException">There is not enough space in `array` to accommodate all the items.</exception>
	/// <exception cref="ArgumentOutOfRangeException">`arrayIndex` is less than 0.</exception>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been disposed.
	/// </exception>
	public void CopyTo(T[] array, int arrayIndex)
	{
		Owner?.VerifyAccess();
		CheckAndFilterDeleted();

		for (int i = 0; i < count; i++)
		{
			array[arrayIndex + i] = this[i];
		}
	}

	/// <summary>
	/// Returns an enumerator that iterates through <see cref="ReferenceArray{T}"/>.
	/// </summary>
	/// <returns>An enumerator.</returns>
	/// <remarks>
	/// If <see cref="ReferenceArray{T}"/> changes, enumerator is invalidated.
	/// Any attempts to use it after that will throw an <see cref="InvalidOperationException"/>.
	/// </remarks>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been disposed.
	/// </exception>
	public IEnumerator<T> GetEnumerator()
	{
		Owner?.VerifyAccess();
		CheckAndFilterDeleted();

		int currVersion = version;
		for (int i = 0; i < count; i++)
		{
			if (currVersion != version)
				throw new InvalidOperationException("The collection has been modified. Enumeration can't continue.");

			yield return this[i];
		}
	}

	/// <summary>
	/// Finds an item in the <see cref="ReferenceArray{T}"/> and returns its position.
	/// </summary>
	/// <param name="item">Item to find in <see cref="ReferenceArray{T}"/>.</param>
	/// <returns>If the element is found, it return the zero based index in <see cref="ReferenceArray{T}"/>, otherwise it returns -1.</returns>
	/// <exception cref="ArgumentNullException">`item` is `null`.</exception>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been disposed.
	/// </exception>
	public int IndexOf(T item)
	{
		if (item == null)
			throw new ArgumentNullException(nameof(item));

		Owner?.VerifyAccessAndModel((DatabaseObject)(object)item);
		CheckAndFilterDeleted();

		IEqualityComparer<T> comp = EqualityComparer<T>.Default;
		for (int i = 0; i < count; i++)
		{
			if (comp.Equals(this[i], item))
				return i;
		}

		return -1;
	}

	/// <summary>
	/// Inserts an item into <see cref="ReferenceArray{T}"/> at the specified position.
	/// </summary>
	/// <param name="index">Zero based position at which to insert `item`.</param>
	/// <param name="item">The item to insert.</param>
	/// <exception cref="ArgumentNullException">`item` is `null`.</exception>
	/// <exception cref="ArgumentOutOfRangeException">If `index` is less than 0 or greater than <see cref="Count"/></exception>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been disposed.
	/// </exception>
	public void Insert(int index, T item)
	{
		if (item == null)
			throw new ArgumentNullException(nameof(item));

		Owner?.VerifyAccessAndModel((DatabaseObject)(object)item);
		CheckAndFilterDeleted();

		if (ids == null)
			CreateModifiedCopy();

		if (count == ids.Length)
			Resize();

		for (int i = count; i > index; i--)
		{
			ids[i] = ids[i - 1];
		}

		if (Owner != null && PropertyDescritpor.TrackInverseReferences)
			Owner.Owner.ReferenceModified(Owner.Id, 0, item.Id, PropertyId);

		version++;
		ids[index] = item.Id;
		count++;
	}

	/// <summary>
	/// Remove the first occurrence of the item from the <see cref="ReferenceArray{T}"/>.
	/// </summary>
	/// <param name="item">Item to be removed.</param>
	/// <returns>`true` if `item` was removed, otherwise `false`.</returns>
	/// <exception cref="ArgumentNullException">`item` is `null`.</exception>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="ReferenceArray{T}"/> has been disposed.
	/// </exception>
	public bool Remove(T item)
	{
		if (item == null)
			throw new ArgumentNullException(nameof(item));

		Owner?.VerifyAccessAndModel((DatabaseObject)(object)item);

		int index = IndexOf(item);
		if (index == -1)
			return false;

		RemoveAt(index);
		return true;
	}

	/// <summary>
	/// Remove an item at the given position.
	/// </summary>
	/// <param name="index">Zero based index of an item to remove.</param>
	/// <exception cref="ArgumentOutOfRangeException">If `index` is less than 0 or greater than <see cref="Count"/></exception>
	/// <exception cref="InvalidOperationException">If the parent object of the <see cref="DatabaseArray{T}"/> has been deleted or abandoned.</exception>
	/// <exception cref="ObjectDisposedException">If the parent object of the <see cref="DatabaseArray{T}"/> has been disposed.</exception>
	public void RemoveAt(int index)
	{
		Owner?.VerifyAccess();
		CheckAndFilterDeleted();

		if (ids == null)
			CreateModifiedCopy();

		if (Owner != null)
			Owner.Owner.ReferenceModified(Owner.Id, ids[index], 0, PropertyId);

		for (int i = index; i < count - 1; i++)
		{
			ids[i] = ids[i + 1];
		}

		version++;
		count--;
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		return GetEnumerator();
	}

	internal void CreateModifiedCopy()
	{
		ChangeNotifier?.Invoke(Owner);

		ids = new long[Math.Max(4, count)];
		for (int i = 0; i < count; i++)
		{
			ids[i] = ((long*)buffer)[i];
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void CheckAndFilterDeleted()
	{
		if (Owner != null && PropertyDescritpor.DeleteTargetAction == DeleteTargetAction.SetToNull)
		{
			DeletedSet deletedSet = Owner.Owner.DeletedSet;
			if (lastDeletedVersion == deletedSet.Version || deletedSet.Count == 0)
				return;

			FilterDeleted(deletedSet);
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void FilterDeleted(DeletedSet deletedSet)
	{
		if (ids == null)
		{
			for (int i = 0; i < count; i++)
			{
				long id = ((long*)buffer)[i];
				if (deletedSet.Contains(id))
				{
					CreateModifiedCopy();
					break;
				}
			}
		}

		if (ids == null)
			return;

		int rem = 0;
		for (int i = 0; i < count; i++)
		{
			if (deletedSet.Contains(ids[i]))
			{
				rem++;
			}
			else
			{
				ids[i - rem] = ids[i];
			}
		}

		if (rem > 0)
		{
			version++;
			count -= rem;
		}

		lastDeletedVersion = deletedSet.Version;
	}

	private void Resize()
	{
		Resize(count + 1);
	}

	private void Resize(int count)
	{
		if (ids.Length == int.MaxValue)
		{
			throw MaximumSizeExceeded();
		}

		long desiredSize = Math.Max((long)ids.Length * 2, count);
		int newCapacity = (int)Math.Min(int.MaxValue, desiredSize);

		long[] newItems = new long[newCapacity];
		Array.Copy(ids, newItems, ids.Length);
		ids = newItems;
	}

	private static InvalidOperationException MaximumSizeExceeded()
	{
		return new InvalidOperationException("Maximum size of the database array has been exceeded.");
	}

	internal override void WriteToChangesetWriter(ChangesetWriter writer)
	{
		CheckAndFilterDeleted();
		writer.WriteLongSubArray(ids, count);
	}

	internal override void ValidateOwnershipChange()
	{
		if (count == 0)
			return;

		Owner.VerifyAccessAndModel((DatabaseObject)(object)this[0]);
	}
}
