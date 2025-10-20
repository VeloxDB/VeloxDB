using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.ObjectInterface;

internal delegate void ModifyDirectReferenceDelegate(DatabaseObject dirRefObj, DatabaseObject invRefObj, bool isArrayInsert);

/// <summary>
/// Base inverse reference set class, you should always use generic version <see cref="InverseReferenceSet{T}"/>.
/// </summary>
/// <seealso cref="InverseReferenceSet{T}"/>
public abstract class InverseReferenceSet
{
	DatabaseObject owner;
	int propertyId;

	internal InverseReferenceSet(DatabaseObject owner, int propertyId)
	{
		this.owner = owner;
		this.propertyId = propertyId;
	}

	/// <summary>
	/// Gets the number of items in <see cref="InverseReferenceSet"/>.
	/// </summary>
	public abstract int Count { get; }
	internal DatabaseObject Owner => owner;
	internal int PropertyId => propertyId;

	internal abstract void Invalidate();
}

/// <summary>
/// Represents a set of inverse references.
/// </summary>
/// <typeparam name="T">The type of items in the array.</typeparam>
public unsafe sealed class InverseReferenceSet<T> : InverseReferenceSet, ICollection<T>, IReadOnlyList<T> where T : DatabaseObject
{
	int count;
	long[] ids;
	int version;
	ModifyDirectReferenceDelegate modifyDirectReferenceAction;

	internal InverseReferenceSet(DatabaseObject owner, int propertyId, ModifyDirectReferenceDelegate modifyDirectReferenceAction) :
		base(owner, propertyId)
	{
		count = -1;
		ids = EmptyArray<long>.Instance;
		this.modifyDirectReferenceAction = modifyDirectReferenceAction;
	}

	/// <summary>
	/// Gets the number of items in <see cref="InverseReferenceSet{T}"/>
	/// </summary>
	public override int Count
	{
		get
		{
			VerifyAndMaterialize();
			return count;
		}
	}

	/// <summary>
	/// Gets if the <see cref="InverseReferenceSet{T}"/> is Readonly. Always returns false.
	/// </summary>
	public bool IsReadOnly => false;

	/// <summary>
	/// Index accessor.
	/// </summary>
	/// <param name="index">Index of the item to get.</param>
	/// <returns>Requested item.</returns>
	/// <exception cref="IndexOutOfRangeException">If `index` is less than 0 or greater than <see cref="Count"/></exception>
	/// <exception cref="InvalidOperationException">
	/// 	If the parent object of the <see cref="DatabaseArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">If the parent object of the <see cref="DatabaseArray{T}"/> has been disposed.</exception>
	public T this[int index]
	{
		get
		{
			VerifyAndMaterialize();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			return Owner.Owner.GetObject<T>(ids[index]);
		}
	}

#if TEST_BUILD
	internal IEnumerable<long> GetIds()
	{
		VerifyAndMaterialize();
		for (int i = 0; i < count; i++)
		{
			yield return ids[i];
		}
	}
#endif

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void VerifyAndMaterialize()
	{
		Owner.VerifyAccessInverseReferences();
		if (count != -1)
			return;

		Owner.Owner.GetInverseReferences(Owner.Id, PropertyId, ref ids, out count);
	}

	internal override void Invalidate()
	{
		version++;
		count = -1;
	}

	/// <summary>
	/// Releases the memory held by <see cref="InverseReferenceSet{T}"/>.
	/// </summary>
	/// <remarks>
	/// <see cref="InverseReferenceSet{T}"/> is lazy, it will not fetch inverse references until it needs to. Once you access inverse references,
	/// it will allocate an array to hold them. You can release this array by calling <see cref="ReleaseMemory"/>. Once the array is released,
	/// if you once again access the <see cref="InverseReferenceSet{T}"/> it will allocate memory again.
	/// </remarks>
	public void ReleaseMemory()
	{
		Invalidate();
		ids = EmptyArray<long>.Instance;
	}

	/// <summary>
	/// Add an item to <see cref="InverseReferenceSet{T}"/>.
	/// </summary>
	/// <param name="item">The item to be added.</param>
	/// <exception cref="ArgumentNullException">If `item` is `null`.</exception>
	/// <exception cref="InvalidOperationException">If the parent object of the <see cref="InverseReferenceSet{T}"/> has been deleted or abandoned.</exception>
	/// <exception cref="ObjectDisposedException">If the parent object of the <see cref="InverseReferenceSet{T}"/> has been disposed.</exception>
	/// <remarks>
	///	This is the same as creating a direct reference.
	///	<note>
	///		Order in <see cref="InverseReferenceSet{T}"/> is not guaranteed. Added object can appear anywhere in <see cref="InverseReferenceSet{T}"/>.
	///	</note>
	/// </remarks>
	public void Add(T item)
	{
		if (item == null)
			throw new ArgumentNullException(nameof(item));

		Owner.VerifyAccess();
		DatabaseObject invRefObj = Owner.Owner.GetObject(item.Id);
		modifyDirectReferenceAction(Owner, invRefObj, true);
	}

	/// <summary>
	/// Remove an item from <see cref="InverseReferenceSet{T}"/>.
	/// </summary>
	/// <param name="item">Item to remove.</param>
	/// <remarks>
	/// This is the same as removing the direct reference.
	/// </remarks>
	/// <returns>`true` if `item` was removed, otherwise if item is not found, it returns `false`.</returns>
	/// <exception cref="ArgumentNullException">`item` is `null`.</exception>
	/// <exception cref="InvalidOperationException">If the parent object of the <see cref="InverseReferenceSet{T}"/> has been deleted or abandoned.</exception>
	/// <exception cref="ObjectDisposedException">If the parent object of the <see cref="InverseReferenceSet{T}"/> has been disposed.</exception>
	public bool Remove(T item)
	{
		if (item == null)
			throw new ArgumentNullException(nameof(item));

		if (!Contains(item))
			return false;

		DatabaseObject invRefObj = Owner.Owner.GetObject(item.Id);
		modifyDirectReferenceAction(null, invRefObj, true);

		return true;
	}

	/// <summary>
	/// Clears all references.
	/// </summary>
 	/// <exception cref="InvalidOperationException">If the parent object of the <see cref="InverseReferenceSet{T}"/> has been deleted or abandoned.</exception>
	/// <exception cref="ObjectDisposedException">If the parent object of the <see cref="InverseReferenceSet{T}"/> has been disposed.</exception>
	public void Clear()
	{
		VerifyAndMaterialize();
		for (int i = 0; i < count; i++)
		{
			DatabaseObject dirRefObj = Owner.Owner.GetObject(ids[i]);
			modifyDirectReferenceAction(Owner, dirRefObj, false);
		}
	}

	/// <summary>
	/// Determines if an item is in the <see cref="InverseReferenceSet{T}"/>.
	/// </summary>
	/// <param name="item"></param>
	/// <returns>`true` if item is present in the <see cref="InverseReferenceSet{T}"/>, false if not.</returns>
	/// <exception cref="ArgumentNullException">If `item` is `null`.</exception>
 	/// <exception cref="InvalidOperationException">If the parent object of the <see cref="InverseReferenceSet{T}"/> has been deleted or abandoned.</exception>
	/// <exception cref="ObjectDisposedException">If the parent object of the <see cref="InverseReferenceSet{T}"/> has been disposed.</exception>
	public bool Contains(T item)
	{
		if (item == null)
			throw new ArgumentNullException(nameof(item));

		VerifyAndMaterialize();
		for (int i = 0; i < count; i++)
		{
			if (ids[i] == item.Id)
				return true;
		}

		return false;
	}

	/// <summary>
	/// Copies all items from the <see cref="InverseReferenceSet{T}"/> to the given array, starting at the specified index of the target array.
	/// </summary>
	/// <param name="array">Destination array.</param>
	/// <param name="arrayIndex">Zero based index in `array` at which copying begins.</param>
	/// <exception cref="ArgumentNullException">`array` is `null`.</exception>
	/// <exception cref="ArgumentException">There is not enough space in `array` to accommodate all the items.</exception>
	/// <exception cref="ArgumentOutOfRangeException">`arrayIndex` is less than 0.</exception>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="InverseReferenceSet{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="InverseReferenceSet{T}"/> has been disposed.
	/// </exception>
	public void CopyTo(T[] array, int arrayIndex)
	{
		Checker.NotNull(array, nameof(array));

		if(arrayIndex < 0)
			throw new ArgumentOutOfRangeException($"{nameof(arrayIndex)} is less than 0.");

		VerifyAndMaterialize();

		if((long)arrayIndex + count > array.Length)
			throw new ArgumentException($"Not enough space in {nameof(array)}.");

		for (int i = 0; i < count; i++)
		{
			array[arrayIndex + i] = Owner.Owner.GetObject<T>(ids[i]);
		}
	}

	/// <summary>
	/// Returns an enumerator that iterates through <see cref="InverseReferenceSet{T}"/>.
	/// </summary>
	/// <returns>An enumerator.</returns>
	/// <remarks>
	/// If <see cref="InverseReferenceSet{T}"/> changes, enumerator is invalidated.
	/// Any attempts to use it after that will throw an <see cref="InvalidOperationException"/>.
	/// </remarks>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="InverseReferenceSet{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="InverseReferenceSet{T}"/> has been disposed.
	/// </exception>
	public IEnumerator<T> GetEnumerator()
	{
		VerifyAndMaterialize();

		int currVersion = version;
		for (int i = 0; i < count; i++)
		{
			yield return Owner.Owner.GetObject<T>(ids[i]);

			if (currVersion != version)
			{
				Checker.AssertTrue(count == -1);
				throw new InvalidOperationException("The collection has been modified. Enumeration can't continue.");
			}
		}

	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		return GetEnumerator();
	}
}
