using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage;

namespace VeloxDB.ObjectInterface;

[Flags]
internal enum DatabaseObjectState : byte
{
	None = 0x00,
	Read = 0x00,
	Modified = 0x01,
	Deleted = 0x02,
	Inserted = 0x04,
	Abandoned = 0x08,
	NotConstructedFully = 0x10,
	Selected = 0x20,
	CanBeAbandonedMask = unchecked((byte)~DatabaseObjectState.Selected),
}

/// <summary>
/// The ultimate base class of all VeloxDB database objects. Derive this class to implement a database class.
/// </summary>
public unsafe abstract class DatabaseObject
{
	private protected ObjectModel owner;
	long id;
	internal byte* buffer;
	DatabaseObjectState state;
	internal ClassData classData;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void BeginInit(ObjectModel owner, ClassData classData, byte* buffer, DatabaseObjectState state, ChangeList changeList)
	{
		Checker.AssertTrue(buffer != null);
		this.owner = owner;
		this.classData = classData;
		this.buffer = buffer;
		this.id = *(long*)buffer;
		this.state = state | DatabaseObjectState.NotConstructedFully;

		owner.ObjectMap.Add(this.Id, this);
		changeList?.Add(this);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void EndInit()
	{
		this.state = state & ~DatabaseObjectState.NotConstructedFully;
	}

	internal static string BufferFieldName => nameof(buffer);
	internal static string OwnerFieldName => nameof(owner);
	internal static string ClassDataFieldName => nameof(classData);
	internal static string VerifyAccessMethodName => nameof(VerifyAccess);
	internal static string VerifyReferencingMethodName => nameof(VerifyReferencing);
	internal static string CreateInsertBlockName => nameof(CreateInsertBlock);
	internal static string CreateUpdateBlockName => nameof(CreateUpdateBlock);
	internal static string OnRefreshName => nameof(OnRefresh);

	internal ClassData ClassData => classData;
	internal DatabaseObjectState State { get => state; set => state = value; }

	/// <summary>
	/// Gets if the object is deleted.
	/// </summary>
	public bool IsDeleted => (state & DatabaseObjectState.Deleted) != DatabaseObjectState.Read;

	/// <summary>
	/// Gets if the object is created.
	/// </summary>
	public bool IsCreated => (state & DatabaseObjectState.Inserted) != DatabaseObjectState.Read;

	/// <summary>
	/// Gets if the object is abandoned. Objects are abandoned with <see cref="Abandon"/>.
	/// </summary>
	public bool IsAbandoned => (state & DatabaseObjectState.Abandoned) != DatabaseObjectState.Read;

	/// <summary>
	/// Gets if the object can be abandoned. Inserted, updated and deleted objects can't be abandoned. See <see cref="Abandon"/>.
	/// </summary>
	public bool CanBeAbandoned => (state & DatabaseObjectState.CanBeAbandonedMask) == DatabaseObjectState.Read;

	/// <summary>
	/// Gets or sets if the object is selected for conversion to DTO.
	/// </summary>
	public bool IsSelected
	{
		get => (state & DatabaseObjectState.Selected) == DatabaseObjectState.Selected;
		set => state = (DatabaseObjectState)((-*(byte*)&value ^ (byte)state) & (byte)DatabaseObjectState.Selected);
	}

	/// <summary>
	/// Get's the <see cref="DatabaseObject"/>'s object model.
	/// </summary>
	public ObjectModel Owner => owner;

	/// <summary>
	/// Gets <see cref="DatabaseObject"/>'s Id.
	/// </summary>
	/// <remarks>
	/// <see cref="DatabaseObject"/> has an `Id` that is auto assigned by the database. The id represents primary key, it is unique
	/// for all database objects, across all types. The id is never reused even if the object is deleted.
	/// If you need additional keys use <see cref="HashIndexAttribute"/> with <see cref="HashIndexAttribute.IsUnique"/> set to `true`.
	/// </remarks>
	public long Id => id;

	internal bool IsInsertedOrModified => (state & (DatabaseObjectState.Inserted | DatabaseObjectState.Modified)) != DatabaseObjectState.None;

	internal abstract void CreateInsertBlock(ChangesetWriter writer);
	internal abstract void CreateUpdateBlock(ChangesetWriter writer);
	internal abstract void InvalidateInverseReferences();

	/// <summary>
	/// Select the object for conversion to DTO.
	/// </summary>
	public void Select()
	{
		state |= DatabaseObjectState.Selected;
	}

	/// <summary>
	/// Marks the object as deleted.
	/// </summary>
	public void Delete()
	{
		owner.ValidateThread();

		if (IsDeleted)
			return;

		owner.DeleteObject(this, true);
	}

	/// <summary>
	/// Tells <see cref="ObjectModel"/> that it doesn't have to keep the reference to the object any more.
	/// </summary>
	/// <remarks>
	/// VeloxDB keeps data in internal unmanaged structures. When accessing objects through <see cref="ObjectModel"/>
	/// VeloxDB creates object oriented wrappers to access these structures. Besides providing access, these objects also store changes
	/// made in the transaction (updates, deletion). These objects are kept for the duration of the transaction and
	/// discarded once the transaction is done. If you only read an object and know that you wont be reading it again,
	/// you can call <see cref="Abandon"/> to signal the database that it doesn't have to keep the object around.
	/// </remarks>
	/// <exception cref="InvalidOperationException"><see cref="DatabaseObject"/> is created, updated or deleted.</exception>
	public void Abandon()
	{
		owner.ValidateThread();

		if (state == DatabaseObjectState.Abandoned)
			return;

		if (!CanBeAbandoned)
			throw new InvalidOperationException("Modified objects can't be abandoned.");

		owner.AbandonObject(this);
	}

	internal virtual void OnRefresh()
	{
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void Refresh(byte* buffer)
	{
		this.buffer = buffer;
		state = (state & DatabaseObjectState.Selected);
		OnRefresh();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void Modified(byte* buffer, int size)
	{
		Common.Utils.CopyMemory(this.buffer, buffer, size);
		this.buffer = buffer;
		state |= DatabaseObjectState.Modified;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void Cleanup()
	{
		if ((state & (DatabaseObjectState.Modified | DatabaseObjectState.Inserted)) != DatabaseObjectState.Read)
			owner.FreeObjectBuffer(classData, buffer);

		buffer = null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void VerifyAccessAndModel(DatabaseObject obj)
	{
		owner.ValidateThread();

		if (obj != null && !object.ReferenceEquals(obj.owner, this.owner))
			throw new InvalidOperationException("Given object does not belong to the same object model.");

		if (owner.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		if ((state & (DatabaseObjectState.Deleted | DatabaseObjectState.Abandoned)) != DatabaseObjectState.None)
		{
			if ((state & DatabaseObjectState.Deleted) != DatabaseObjectState.None)
			{
				throw new InvalidOperationException($"Object of type {classData.ClassDesc.FullName} has been deleted.");
			}
			else
			{
				throw new InvalidOperationException(
					$"Object of type {classData.ClassDesc.FullName} has been abandoned. It can no longer be used.");
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void VerifyAccess()
	{
		owner.ValidateThread();

		if (owner.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		if ((state & (DatabaseObjectState.Deleted | DatabaseObjectState.Abandoned)) != DatabaseObjectState.None)
			ThrowAccessError(false);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void VerifyModifyAccess()
	{
		owner.ValidateThread();

		if (owner.Disposed)
			throw new ObjectDisposedException(nameof(ObjectModel));

		if ((state & (DatabaseObjectState.Deleted | DatabaseObjectState.Abandoned | DatabaseObjectState.NotConstructedFully)) != DatabaseObjectState.None)
			ThrowAccessError(true);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void ThrowAccessError(bool isModify)
	{
		if (isModify && (state & DatabaseObjectState.NotConstructedFully) != DatabaseObjectState.None)
			throw new InvalidOperationException($"Constructor of class {classData.ClassDesc.FullName} is not allowed to access database property setters.");

		if ((state & DatabaseObjectState.Deleted) != DatabaseObjectState.None)
			throw new InvalidOperationException($"Object of type {classData.ClassDesc.FullName} has been deleted.");

		if ((state & DatabaseObjectState.Abandoned) != DatabaseObjectState.None)
		{
			throw new InvalidOperationException(
				$"Object of type {classData.ClassDesc.FullName} has been abandoned. It can no longer be used.");
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void VerifyReferencing(DatabaseObject obj, DatabaseObject referencingObject)
	{
		if (obj == null)
			return;

		if (obj != null && !object.ReferenceEquals(obj.owner, referencingObject.owner))
			throw new InvalidOperationException("Referenced object does not belong to the same object model.");

		if ((obj.state & (DatabaseObjectState.Deleted | DatabaseObjectState.Abandoned)) != DatabaseObjectState.None)
		{
			if ((obj.state & DatabaseObjectState.Deleted) != DatabaseObjectState.None)
			{
				throw new InvalidOperationException($"Object of type {obj.classData.ClassDesc.FullName} has been deleted.");
			}
			else
			{
				throw new InvalidOperationException(
					$"Object of type {obj.classData.ClassDesc.FullName} has been abandoned. It can no longer be used.");
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void TakeArrayOwnersip(DatabaseArray array, Action<DatabaseObject> changeNotifier)
	{
		if (array != null && array.Owner != null)
		{
			throw new InvalidOperationException("Given array could not be assigned to an array " +
				"property because it is already used as the value of a property.");
		}

		array.SetOwner(this, changeNotifier);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void TakeReferenceArrayOwnersip(int propId, ReferenceArray refArray, Action<DatabaseObject> changeNotifier)
	{
		if (refArray == null)
			return;

		if (refArray.Owner != null)
		{
			throw new InvalidOperationException("Given reference array could not be assigned to an array property" +
				" because it is already used as the value of another property.");
		}

		ReferencePropertyDescriptor propDesc = (ReferencePropertyDescriptor)classData.ClassDesc.GetProperty(propId);
		refArray.SetOwner(this, propDesc, changeNotifier);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static long GetId(DatabaseObject obj)
	{
		return obj == null ? 0 : obj.Id;
	}
}
