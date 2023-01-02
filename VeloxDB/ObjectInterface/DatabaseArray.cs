using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage;

namespace VeloxDB.ObjectInterface;

/// <summary>
/// Base database array class, you should always use generic version <see cref="DatabaseArray{T}"/>
/// </summary>
/// <seealso cref="DatabaseArray{T}"/>
public unsafe abstract class DatabaseArray
{
	DatabaseObject owner;
	Action<DatabaseObject> changeNotifier;

	internal DatabaseArray(DatabaseObject owner, Action<DatabaseObject> changeNotifier)
	{
		this.owner = owner;
		this.changeNotifier = changeNotifier;
	}

	internal DatabaseObject Owner => owner;
	internal Action<DatabaseObject> ChangeNotifier => changeNotifier;

	internal static PropertyType ManagedToPropertyType(Type type)
	{
		if (typeof(DatabaseArray<byte>).IsAssignableFrom(type))
			return PropertyType.ByteArray;
		else if (typeof(DatabaseArray<short>).IsAssignableFrom(type))
			return PropertyType.ShortArray;
		else if (typeof(DatabaseArray<int>).IsAssignableFrom(type))
			return PropertyType.IntArray;
		else if (typeof(DatabaseArray<long>).IsAssignableFrom(type))
			return PropertyType.LongArray;
		else if (typeof(DatabaseArray<float>).IsAssignableFrom(type))
			return PropertyType.FloatArray;
		else if (typeof(DatabaseArray<double>).IsAssignableFrom(type))
			return PropertyType.DoubleArray;
		else if (typeof(DatabaseArray<bool>).IsAssignableFrom(type))
			return PropertyType.BoolArray;
		else if (typeof(DatabaseArray<DateTime>).IsAssignableFrom(type))
			return PropertyType.DateTimeArray;
		else if (typeof(DatabaseArray<string>).IsAssignableFrom(type))
			return PropertyType.StringArray;
		else
			return PropertyType.None;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void SetOwner(DatabaseObject owner, Action<DatabaseObject> changeNotifier)
	{
		this.owner = owner;
		this.changeNotifier = changeNotifier;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void WriteToChangesetWriter(ChangesetWriter writer, DatabaseArray array)
	{
		if (array == null)
		{
			writer.WriteNullArray();
			return;
		}

		array.WriteToChangesetWriter(writer);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal unsafe static void Refresh(DatabaseArray ra, byte* buffer)
	{
		if (ra != null)
			ra.Refresh(buffer);
	}

	internal abstract void Refresh(byte* buffer);
	internal abstract void WriteToChangesetWriter(ChangesetWriter writer);
}

/// <summary>
/// Represents a strongly typed array of simple types.
/// </summary>
/// <typeparam name="T">The type of elements in the array.</typeparam>
/// <seealso cref="ReferenceArray{T}"/>
public unsafe abstract class DatabaseArray<T> : DatabaseArray, IList<T>
{
	private const int initialCapacity = 4;
	static readonly Func<int, DatabaseArray<T>> creator;

	private protected byte* buffer;
	private protected int count;
	private protected T[] items;
	private protected int version;

	static DatabaseArray()
	{
		PropertyType pt = PropertyTypesHelper.ManagedTypeToPropertyType(typeof(T));
		if (pt == Descriptor.PropertyType.None || PropertyTypesHelper.IsArray(pt))
			throw new ArgumentException("Generic argument of the database array is invalid.");

		switch (pt)
		{
			case PropertyType.Byte:
				creator = (Func<int, DatabaseArray<T>>)(object)(Func<int, DatabaseArray<byte>>)(x => new ByteDatabaseArray(x));
				break;

			case PropertyType.Short:
				creator = (Func<int, DatabaseArray<T>>)(object)(Func<int, DatabaseArray<short>>)(x => new ShortDatabaseArray(x));
				break;

			case PropertyType.Int:
				creator = (Func<int, DatabaseArray<T>>)(object)(Func<int, DatabaseArray<int>>)(x => new IntDatabaseArray(x));
				break;

			case PropertyType.Long:
				creator = (Func<int, DatabaseArray<T>>)(object)(Func<int, DatabaseArray<long>>)(x => new LongDatabaseArray(x));
				break;

			case PropertyType.Float:
				creator = (Func<int, DatabaseArray<T>>)(object)(Func<int, DatabaseArray<float>>)(x => new FloatDatabaseArray(x));
				break;

			case PropertyType.Double:
				creator = (Func<int, DatabaseArray<T>>)(object)(Func<int, DatabaseArray<double>>)(x => new DoubleDatabaseArray(x));
				break;

			case PropertyType.Bool:
				creator = (Func<int, DatabaseArray<T>>)(object)(Func<int, DatabaseArray<bool>>)(x => new BoolDatabaseArray(x));
				break;

			case PropertyType.DateTime:
				creator = (Func<int, DatabaseArray<T>>)(object)(Func<int, DatabaseArray<DateTime>>)(x => new DateTimeDatabaseArray(x));
				break;

			case PropertyType.String:
				creator = (Func<int, DatabaseArray<T>>)(object)(Func<int, DatabaseArray<string>>)(x => new StringDatabaseArray(x));
				break;

			default:
				throw new ArgumentException("Invalid database array type.");
		}
	}

	internal DatabaseArray(int capacity) :
		base(null, null)
	{
		items = new T[capacity];
	}

	internal DatabaseArray(DatabaseObject owner, Action<DatabaseObject> changeNotifier, byte* buffer) :
		base(owner, changeNotifier)
	{
		owner.VerifyAccess();

		this.count = *(int*)buffer;
		this.buffer = buffer + sizeof(int);
	}

	/// <summary>
	/// Creates a new empty instance of the <see cref="DatabaseArray{T}"/>.
	/// </summary>
	/// <returns>Created <see cref="DatabaseArray{T}"/></returns>
	public static DatabaseArray<T> Create()
	{
		return Create(initialCapacity);
	}

	/// <summary>
	/// Creates a new empty instance of the <see cref="DatabaseArray{T}"/>. With specified capacity.
	/// </summary>
	/// <param name="capacity">Initial capacity. The initial size of the backing array.</param>
	/// <returns>Created <see cref="DatabaseArray{T}"/></returns>
	public static DatabaseArray<T> Create(int capacity)
	{
		return creator(capacity);
	}

	/// <summary>
	/// Creates a new instance of the <see cref="DatabaseArray{T}"/> that contains elements copied from the supplied collection.
	/// </summary>
	/// <param name="collection">The collection whose elements are to be copied.</param>
	/// <returns>Created <see cref="DatabaseArray{T}"/></returns>
	/// <exception cref="ArgumentNullException">`collection` is `null`.</exception>
	public static DatabaseArray<T> Create(IEnumerable<T> collection)
	{
		Checker.NotNull(collection, nameof(collection));

		int capacity = 8;
		ICollection<T> c = collection as ICollection<T>;
		if (collection != null)
			capacity = c.Count;

		DatabaseArray<T> da = Create(capacity);
		foreach (T item in collection)
		{
			da.Add(item);
		}

		return da;
	}

	/// <summary>
	/// Gets the number of items contained in the <see cref="DatabaseArray{T}"/>.
	/// </summary>
	public int Count => count;

	/// <summary>
	/// Gets if the <see cref="DatabaseArray{T}"/> is Readonly. Always returns false.
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
	public abstract T this[int index] { get; set; }

	internal override unsafe void Refresh(byte* buffer)
	{
		items = null;
		this.count = *(int*)buffer;
		this.buffer = buffer + sizeof(int);
	}

	/// <summary>
	/// Add an item to the end of the <see cref="DatabaseArray{T}"/>.
	/// </summary>
	/// <param name="item">The item to be added to the end of the <see cref="DatabaseArray{T}"/>.</param>
	/// <exception cref="InvalidOperationException">
	/// If the added item would cause <see cref="Count"/> to exceed `int.MaxValue`
	/// <br/>
	/// or
	/// <br/>
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been disposed.
	/// </exception>
	public void Add(T item)
	{
		Owner?.VerifyAccess();

		if (items == null)
			CreateModifiedCopy();

		if (count == items.Length)
			Resize();

		version++;
		items[count++] = item;
	}

	/// <summary>
	/// Adds the elements of the specified collection to the end of the <see cref="DatabaseArray{T}"/>.
	/// </summary>
	/// <param name="collection">Collection whose elements will be added.</param>
	/// <exception cref="ArgumentNullException">`collection` is `null`.</exception>
	/// <exception cref="InvalidOperationException">
	/// If the adding `collection` items would cause <see cref="Count"/> to exceed `int.MaxValue`
	/// <br/>
	/// or
	/// <br/>
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been disposed.
	/// </exception>
	public void AddRange(IEnumerable<T> collection)
	{
		Owner?.VerifyAccess();

		Checker.NotNull(collection, nameof(collection));

		ICollection<T> c = collection as ICollection<T>;
		if (collection != null)
		{
			long desiredSize = (long)Count + c.Count;

			if(desiredSize > int.MaxValue)
				throw MaximumSizeExceeded();

			if(desiredSize > items.Length)
				Resize((int)desiredSize);
		}

		foreach(T item in collection)
		{
			version++;
			items[count++] = item;
		}
	}

	/// <summary>
	/// Clears the <see cref="DatabaseArray{T}"/>.
	/// </summary>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been disposed.
	/// </exception>
	public void Clear()
	{
		Owner?.VerifyAccess();

		if (items == null)
			CreateModifiedCopy();

		for (int i = 0; i < count; i++)
		{
			items[i] = default(T);
		}

		version++;
		count = 0;
	}

	/// <summary>
	/// Determines if an item is in the <see cref="DatabaseArray{T}"/>.
	/// </summary>
	/// <param name="item">The item to locate in the <see cref="DatabaseArray{T}"/>.</param>
	/// <returns>`true` if item is present in the <see cref="DatabaseArray{T}"/>, false if not.</returns>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been disposed.
	/// </exception>
	public bool Contains(T item)
	{
		Owner?.VerifyAccess();

		IEqualityComparer<T> comp = EqualityComparer<T>.Default;
		for (int i = 0; i < count; i++)
		{
			if (comp.Equals(this[i], item))
				return true;
		}

		return false;
	}

	/// <summary>
	/// Copies all items from the <see cref="DatabaseArray{T}"/> to the given array, starting at the specified index of the target array.
	/// </summary>
	/// <param name="array">Destination array.</param>
	/// <param name="arrayIndex">Zero based index in `array` at which copying begins.</param>
	/// <exception cref="ArgumentNullException">`array` is `null`.</exception>
	/// <exception cref="ArgumentException">There is not enough space in `array` to accommodate all the items.</exception>
	/// <exception cref="ArgumentOutOfRangeException">`arrayIndex` is less than 0.</exception>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been disposed.
	/// </exception>
	public void CopyTo(T[] array, int arrayIndex)
	{
		Checker.NotNull(array, nameof(array));

		if(arrayIndex < 0)
			throw new ArgumentOutOfRangeException($"{nameof(arrayIndex)} is less than 0.");

		if((long)arrayIndex + Count > array.Length)
			throw new ArgumentException($"Not enough space in {nameof(array)}.");

		Owner?.VerifyAccess();

		for (int i = 0; i < count; i++)
		{
			array[arrayIndex + i] = this[i];
		}
	}

	/// <summary>
	/// Returns an enumerator that iterates through <see cref="DatabaseArray{T}"/>.
	/// </summary>
	/// <returns>An enumerator.</returns>
	/// <remarks>
	/// If <see cref="DatabaseArray{T}"/> changes, enumerator is invalidated.
	/// Any attempts to use it after that will throw an <see cref="InvalidOperationException"/>.
	/// </remarks>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been disposed.
	/// </exception>
	public IEnumerator<T> GetEnumerator()
	{
		Owner?.VerifyAccess();

		int currVersion = version;
		for (int i = 0; i < count; i++)
		{
			if (currVersion != version)
				throw new InvalidOperationException("The collection has been modified. Enumeration can't continue.");

			yield return this[i];
		}
	}

	/// <summary>
	/// Finds an item in the <see cref="DatabaseArray{T}"/> and returns its position.
	/// </summary>
	/// <param name="item">Item to find in <see cref="DatabaseArray{T}"/>.</param>
	/// <returns>If the element is found, it return the zero based index in <see cref="DatabaseArray{T}"/>, otherwise it returns -1.</returns>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been disposed.
	/// </exception>
	public int IndexOf(T item)
	{
		Owner?.VerifyAccess();

		IEqualityComparer<T> comp = EqualityComparer<T>.Default;
		for (int i = 0; i < count; i++)
		{
			if (comp.Equals(this[i], item))
				return i;
		}

		return -1;
	}

	/// <summary>
	/// Inserts an item into <see cref="DatabaseArray{T}"/> at the specified position.
	/// </summary>
	/// <param name="index">Zero based position at which to insert `item`.</param>
	/// <param name="item">The item to insert.</param>
	/// <exception cref="ArgumentOutOfRangeException">If `index` is less than 0 or greater than <see cref="Count"/></exception>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been disposed.
	/// </exception>
	public void Insert(int index, T item)
	{
		Owner?.VerifyAccess();
		Checker.CheckRange(index, 0, Count - 1, nameof(index));

		if (items == null)
			CreateModifiedCopy();

		if (count == items.Length)
			Resize();

		for (int i = count; i > index; i--)
		{
			items[i] = items[i - 1];
		}

		version++;
		items[index] = item;
		count++;
	}

	/// <summary>
	/// Remove the first occurrence of the item from the <see cref="DatabaseArray{T}"/>.
	/// </summary>
	/// <param name="item">Item to be removed.</param>
	/// <returns>`true` if `item` was removed, otherwise `false`.</returns>
	/// <exception cref="InvalidOperationException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been deleted or abandoned.
	/// </exception>
	/// <exception cref="ObjectDisposedException">
	/// If the parent object of the <see cref="DatabaseArray{T}"/> has been disposed.
	/// </exception>
	public bool Remove(T item)
	{
		Owner?.VerifyAccess();

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
		Checker.CheckRange(index, 0, Count - 1, nameof(index));

		if (items == null)
			CreateModifiedCopy();

		for (int i = index; i < count - 1; i++)
		{
			items[i] = items[i + 1];
		}

		version++;
		items[count - 1] = default(T);
		count--;
	}

	/// <summary>
	/// Returns an enumerator that iterates through <see cref="DatabaseArray{T}"/>.
	/// </summary>
	/// <returns>An enumerator.</returns>
	/// <remarks>
	/// If <see cref="DatabaseArray{T}"/> changes, enumerator is invalidated.
	/// Any attempts to use it after that will throw an <see cref="InvalidOperationException"/>.
	/// </remarks>
	IEnumerator IEnumerable.GetEnumerator()
	{
		return GetEnumerator();
	}

	private protected abstract void CreateItems();

	private protected void CreateModifiedCopy()
	{
		ChangeNotifier?.Invoke(Owner);
		CreateItems();
	}

	private void Resize()
	{
		Resize(count + 1);
	}

	private void Resize(int count)
	{
		if (items.Length == int.MaxValue)
		{
			throw MaximumSizeExceeded();
		}

		long desiredSize = Math.Max((long)items.Length * 2, count);
		int newCapacity = (int)Math.Min(int.MaxValue, desiredSize);

		T[] newItems = new T[newCapacity];
		Array.Copy(items, newItems, items.Length);
		items = newItems;
	}

	private static InvalidOperationException MaximumSizeExceeded()
	{
		return new InvalidOperationException("Maximum size of the database array has been exceeded.");
	}
}

internal unsafe sealed class ByteDatabaseArray : DatabaseArray<byte>
{
	public ByteDatabaseArray(int capacity) :
		base(capacity)
	{
	}

	public ByteDatabaseArray(DatabaseObject owner, Action<DatabaseObject> changeNotifier, byte* buffer) :
		base(owner, changeNotifier, buffer)
	{
	}

	public override byte this[int index]
	{
		get
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items != null)
				return items[index];

			return ((byte*)buffer)[index];
		}

		set
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items == null)
				CreateModifiedCopy();

			version++;
			items[index] = value;
		}
	}

	private protected override void CreateItems()
	{
		items = new byte[Math.Max(4, count)];
		for (int i = 0; i < count; i++)
		{
			items[i] = ((byte*)buffer)[i];
		}
	}

	internal override void WriteToChangesetWriter(ChangesetWriter writer)
	{
		writer.WriteByteSubArray(items, count);
	}
}

internal unsafe sealed class ShortDatabaseArray : DatabaseArray<short>
{
	public ShortDatabaseArray(int capacity) :
		base(capacity)
	{
	}

	public ShortDatabaseArray(DatabaseObject owner, Action<DatabaseObject> changeNotifier, byte* buffer) :
		base(owner, changeNotifier, buffer)
	{
	}

	public override short this[int index]
	{
		get
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items != null)
				return items[index];

			return ((short*)buffer)[index];
		}

		set
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items == null)
				CreateModifiedCopy();

			version++;
			items[index] = value;
		}
	}

	private protected override void CreateItems()
	{
		items = new short[Math.Max(4, count)];
		for (int i = 0; i < count; i++)
		{
			items[i] = ((short*)buffer)[i];
		}
	}

	internal override void WriteToChangesetWriter(ChangesetWriter writer)
	{
		writer.WriteShortSubArray(items, count);
	}
}

internal unsafe sealed class IntDatabaseArray : DatabaseArray<int>
{
	public IntDatabaseArray(int capacity) :
		base(capacity)
	{
	}

	public IntDatabaseArray(DatabaseObject owner, Action<DatabaseObject> changeNotifier, byte* buffer) :
		base(owner, changeNotifier, buffer)
	{
	}

	public override int this[int index]
	{
		get
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items != null)
				return items[index];

			return ((int*)buffer)[index];
		}

		set
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items == null)
				CreateModifiedCopy();

			version++;
			items[index] = value;
		}
	}

	private protected override void CreateItems()
	{
		items = new int[Math.Max(4, count)];
		for (int i = 0; i < count; i++)
		{
			items[i] = ((int*)buffer)[i];
		}
	}

	internal override void WriteToChangesetWriter(ChangesetWriter writer)
	{
		writer.WriteIntSubArray(items, count);
	}
}

internal unsafe sealed class LongDatabaseArray : DatabaseArray<long>
{
	public LongDatabaseArray(int capacity) :
		base(capacity)
	{
	}

	public LongDatabaseArray(DatabaseObject owner, Action<DatabaseObject> changeNotifier, byte* buffer) :
		base(owner, changeNotifier, buffer)
	{
	}

	public override long this[int index]
	{
		get
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items != null)
				return items[index];

			return ((long*)buffer)[index];
		}

		set
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items == null)
				CreateModifiedCopy();

			version++;
			items[index] = value;
		}
	}

	private protected override void CreateItems()
	{
		items = new long[Math.Max(4, count)];
		for (int i = 0; i < count; i++)
		{
			items[i] = ((long*)buffer)[i];
		}
	}

	internal override void WriteToChangesetWriter(ChangesetWriter writer)
	{
		writer.WriteLongSubArray(items, count);
	}
}

internal unsafe sealed class FloatDatabaseArray : DatabaseArray<float>
{
	public FloatDatabaseArray(int capacity) :
		base(capacity)
	{
	}

	public FloatDatabaseArray(DatabaseObject owner, Action<DatabaseObject> changeNotifier, byte* buffer) :
		base(owner, changeNotifier, buffer)
	{
	}

	public override float this[int index]
	{
		get
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items != null)
				return items[index];

			return ((float*)buffer)[index];
		}

		set
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items == null)
				CreateModifiedCopy();

			version++;
			items[index] = value;
		}
	}

	private protected override void CreateItems()
	{
		items = new float[Math.Max(4, count)];
		for (int i = 0; i < count; i++)
		{
			items[i] = ((float*)buffer)[i];
		}
	}

	internal override void WriteToChangesetWriter(ChangesetWriter writer)
	{
		writer.WriteFloatSubArray(items, count);
	}
}

internal unsafe sealed class DoubleDatabaseArray : DatabaseArray<double>
{
	public DoubleDatabaseArray(int capacity) :
		base(capacity)
	{
	}

	public DoubleDatabaseArray(DatabaseObject owner, Action<DatabaseObject> changeNotifier, byte* buffer) :
		base(owner, changeNotifier, buffer)
	{
	}

	public override double this[int index]
	{
		get
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items != null)
				return items[index];

			return ((double*)buffer)[index];
		}

		set
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items == null)
				CreateModifiedCopy();

			version++;
			items[index] = value;
		}
	}

	private protected override void CreateItems()
	{
		items = new double[Math.Max(4, count)];
		for (int i = 0; i < count; i++)
		{
			items[i] = ((double*)buffer)[i];
		}
	}

	internal override void WriteToChangesetWriter(ChangesetWriter writer)
	{
		writer.WriteDoubleSubArray(items, count);
	}
}

internal unsafe sealed class BoolDatabaseArray : DatabaseArray<bool>
{
	public BoolDatabaseArray(int capacity) :
		base(capacity)
	{
	}

	public BoolDatabaseArray(DatabaseObject owner, Action<DatabaseObject> changeNotifier, byte* buffer) :
		base(owner, changeNotifier, buffer)
	{
	}

	public override bool this[int index]
	{
		get
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items != null)
				return items[index];

			return ((byte*)buffer)[index] == 1;
		}

		set
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items == null)
				CreateModifiedCopy();

			version++;
			items[index] = value;
		}
	}

	private protected override void CreateItems()
	{
		items = new bool[Math.Max(4, count)];
		for (int i = 0; i < count; i++)
		{
			items[i] = ((byte*)buffer)[i] == 1;
		}
	}

	internal override void WriteToChangesetWriter(ChangesetWriter writer)
	{
		writer.WriteBoolSubArray(items, count);
	}
}

internal unsafe sealed class DateTimeDatabaseArray : DatabaseArray<DateTime>
{
	public DateTimeDatabaseArray(int capacity) :
		base(capacity)
	{
	}

	public DateTimeDatabaseArray(DatabaseObject owner, Action<DatabaseObject> changeNotifier, byte* buffer) :
		base(owner, changeNotifier, buffer)
	{
	}

	public override DateTime this[int index]
	{
		get
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items != null)
				return items[index];

			return DateTime.FromBinary(((long*)buffer)[index]);
		}

		set
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items == null)
				CreateModifiedCopy();

			version++;
			items[index] = value;
		}
	}

	private protected override void CreateItems()
	{
		items = new DateTime[Math.Max(4, count)];
		for (int i = 0; i < count; i++)
		{
			items[i] = DateTime.FromBinary(((long*)buffer)[i]);
		}
	}

	internal override void WriteToChangesetWriter(ChangesetWriter writer)
	{
		writer.WriteDateTimeSubArray(items, count);
	}
}

internal unsafe sealed class StringDatabaseArray : DatabaseArray<string>
{
	string[] unpackedArray;

	public StringDatabaseArray(int capacity) :
		base(capacity)
	{
	}

	public StringDatabaseArray(DatabaseObject owner, Action<DatabaseObject> changeNotifier, byte* buffer) :
		base(owner, changeNotifier, buffer)
	{
		unpackedArray = PropertyTypesHelper.DBUnpackStringArray(buffer);
	}

	public override string this[int index]
	{
		get
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items != null)
				return items[index];

			return unpackedArray[index];
		}

		set
		{
			Owner?.VerifyAccess();

			if (index >= count || index < 0)
				throw new IndexOutOfRangeException();

			if (items == null)
				CreateModifiedCopy();

			version++;
			items[index] = value;
		}
	}

	private protected override void CreateItems()
	{
		items = new string[Math.Max(4, count)];
		for (int i = 0; i < count; i++)
		{
			items[i] = unpackedArray[i];
		}
	}

	internal override void WriteToChangesetWriter(ChangesetWriter writer)
	{
		writer.WriteStringSubArray(items, count);
	}
}
