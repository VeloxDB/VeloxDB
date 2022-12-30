using System;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal sealed unsafe class ChangesetBlock
{
	OperationType operationType;
	ClassDescriptor classDescriptor;
	int propertyCount;
	ChangesetBlockProperty[] properties;
	int operationCount;
	PropertySet affectedStringBlobs;
	ulong rewindVersion;

	public OperationType OperationType => operationType;
	public ClassDescriptor ClassDescriptor => classDescriptor;
	public int PropertyCount => propertyCount;
	public int OperationCount => operationCount;
	public ChangesetBlockProperty[] Properties => properties;

	public ChangesetBlock()
	{
		properties = new ChangesetBlockProperty[ClassDescriptor.MaxPropertyCount];
		affectedStringBlobs = new PropertySet();

		// First property is always Id
		properties[0] = new ChangesetBlockProperty(PropertyDescriptor.IdIndex, PropertyType.Long);
	}

	public ulong RewindVersion => rewindVersion;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal ChangesetBlockProperty GetProperty(int index)
	{
		Checker.AssertTrue(index < propertyCount);
		return properties[index];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool StringBlobNotAffected(int index)
	{
		return affectedStringBlobs.IsReset(index);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Init(OperationType operationType, ClassDescriptor classDescriptor, int propertyCount, int operationCount)
	{
		Checker.AssertTrue(properties[0].Index == PropertyDescriptor.IdIndex);    // Make sure first property has not been messed with

		this.operationType = operationType;
		this.classDescriptor = classDescriptor;
		this.operationCount = operationCount;
		this.propertyCount = 1; // Skip Id property
		this.affectedStringBlobs.Clear();
	}

	public void InitRewind(ulong rewindVersion)
	{
		this.operationType = OperationType.Rewind;
		this.rewindVersion = rewindVersion;
		classDescriptor = null;
		propertyCount = 0;
		operationCount = 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddProperty(int index, PropertyType type)
	{
		if (index != -1 && (type == PropertyType.String || type >= PropertyType.ByteArray))
			affectedStringBlobs.Set(index);

		properties[propertyCount++] = new ChangesetBlockProperty(index, type);
	}

	public int GetStorageSize()
	{
		int s = 0;
		for (int i = 1; i < propertyCount; i++)
		{
			s += sizeof(int) + PropertyTypesHelper.GetItemSize(properties[i].Type);
		}

		return s;
	}

	private unsafe struct PropertySet
	{
		const int capacity = 512;
		const int dwordCapacity = capacity / 64;

		static PropertySet()
		{
			if (capacity != ClassDescriptor.MaxPropertyCount)
				throw new NotImplementedException("Property set not updated with maximum supported number of class properties.");
		}

		fixed ulong bitset[dwordCapacity];
		int maxAffected;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Clear()
		{
			for (int i = 0; i <= maxAffected; i++)
			{
				bitset[i] = 0;
			}

#if DEBUG
			for (int i = 0; i < dwordCapacity; i++)
			{
				if (bitset[i] != 0)
					throw new CriticalDatabaseException();
			}
#endif

			maxAffected = -1;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Set(int n)
		{
			int index = n >> 6;
			ulong mask = (ulong)1 << (n & 63);
			Checker.AssertTrue((bitset[index] & mask) == 0);
			bitset[index] |= mask;
			if (index > maxAffected)
				maxAffected = index;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool IsSet(int n)
		{
			int index = n >> 6;
			ulong mask = (ulong)1 << (n & 63);
			return (bitset[index] & (ulong)mask) != 0;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool IsReset(int n)
		{
			int index = n >> 6;
			ulong mask = (ulong)1 << (n & 63);
			return (bitset[index] & (ulong)mask) == 0;
		}
	}
}

internal struct ChangesetBlockProperty
{
	public int Index { get; private set; }
	public PropertyType Type { get; private set; }

	public ChangesetBlockProperty(int index, PropertyType type)
	{
		this.Index = index;
		this.Type = type;
	}
}
