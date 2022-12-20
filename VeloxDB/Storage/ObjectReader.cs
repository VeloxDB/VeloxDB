using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage;
using static System.Math;

namespace VeloxDB.Storage;

internal unsafe struct ObjectReader
{
	const int versioneIndex = 0;   // Guarantee by the meta model
	const int idIndex = 1;          // Guarantee by the meta model

	byte* obj;
	Class @class;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal ObjectReader(byte* obj, Class @class)
	{
		this.obj = obj;
		this.@class = @class;
	}

	internal byte* Object => (byte*)((ulong)obj & 0x7FFFFFFFFFFFFFFF);
	internal ClassObject* ClassObject => (ClassObject*)(((ulong)obj & 0x7FFFFFFFFFFFFFFF) - Storage.ClassObject.DataOffset);
	internal Class Class => @class;
	internal IntPtr ClassObjectPtr => (IntPtr)ClassObject;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool IsEmpty()
	{
		return obj == null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal unsafe ClassObject* GetObject()
	{
		return (ClassObject*)(obj - Storage.ClassObject.DataOffset);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private PropertyDescriptor GetProperty(int propertyId, PropertyType propertyType, out int byteOffset)
	{
		ClassDescriptor classDesc = @class.ClassDesc;
		int index = classDesc.GetPropertyIndex(propertyId);
		if (index == -1)
			throw new ArgumentException("Invalid property.");

		byteOffset = classDesc.PropertyByteOffsets[index];

		PropertyDescriptor propDesc = classDesc.Properties[index];
		if (propDesc.PropertyType != propertyType)
			throw new InvalidOperationException("Invalid property type.");

		return propDesc;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private PropertyDescriptor GetPropertyByIndex(int propertyIndex, PropertyType propertyType, out int byteOffset)
	{
		byteOffset = @class.ClassDesc.PropertyByteOffsets[propertyIndex];

		PropertyDescriptor propDesc = @class.ClassDesc.Properties[propertyIndex];
		if (propDesc.PropertyType != propertyType)
			throw new InvalidOperationException("Invalid property type.");

		return propDesc;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public short GetTypeId(Transaction tran)
	{
		ValidateRead(tran);
		return @class.ClassDesc.Id;
	}

	public byte GetByte(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.Byte, out int byteOffset);
		return (obj + byteOffset)[0];
	}

	public byte GetByteByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.Byte, out int byteOffset);
		return (obj + byteOffset)[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte GetByteOptimized(int offset)
	{
		return (obj + offset)[0];
	}

	public short GetShort(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.Short, out int byteOffset);
		return ((short*)(obj + byteOffset))[0];
	}

	public short GetShortByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.Short, out int byteOffset);
		return ((short*)(obj + byteOffset))[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public short GetShortOptimized(int offset)
	{
		return ((short*)(obj + offset))[0];
	}

	public int GetInt(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.Int, out int byteOffset);
		return ((int*)(obj + byteOffset))[0];
	}

	public int GetIntByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.Int, out int byteOffset);
		return ((int*)(obj + byteOffset))[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int GetIntOptimized(int offset)
	{
		return ((int*)(obj + offset))[0];
	}

	public long GetLong(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.Long, out int byteOffset);
		return ((long*)(Object + byteOffset))[0];
	}

	public long GetLongByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.Long, out int byteOffset);
		return ((long*)(Object + byteOffset))[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long GetLongByIdOptimized(int propertyId)
	{
		ClassDescriptor classDesc = @class.ClassDesc;
		int index = classDesc.GetPropertyIndex(propertyId);
		int byteOffset = classDesc.PropertyByteOffsets[index];
		return ((long*)(Object + byteOffset))[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long GetLongOptimized(int offset)
	{
		return ((long*)(obj + offset))[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal ulong GetVersionOptimized()
	{
		return ((ulong*)Object)[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long GetId(Transaction tran)
	{
		ValidateRead(tran);

		return ((long*)(Object + 8))[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long GetIdOptimized()
	{
		return ((long*)(Object + 8))[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long GetId()
	{
		return ((long*)(Object + 8))[0];
	}

	public float GetFloat(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.Float, out int byteOffset);
		return ((float*)(obj + byteOffset))[0];
	}

	public float GetFloatByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.Float, out int byteOffset);
		return ((float*)(obj + byteOffset))[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public float GetFloatOptimized(int offset)
	{
		return ((float*)(obj + offset))[0];
	}

	public double GetDouble(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.Double, out int byteOffset);
		return ((double*)(obj + byteOffset))[0];
	}

	public double GetDoubleByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.Double, out int byteOffset);
		return ((double*)(obj + byteOffset))[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public double GetDoubleOptimized(int offset)
	{
		return ((double*)(obj + offset))[0];
	}

	public string GetString(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.String, out int byteOffset);
		ulong handle = ((ulong*)(obj + byteOffset))[0];
		return @class.Engine.StringStorage.GetString(handle);
	}

	public string GetStringByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.String, out int byteOffset);
		ulong handle = ((ulong*)(obj + byteOffset))[0];
		return @class.Engine.StringStorage.GetString(handle);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public string GetStringOptimized(int offset)
	{
		ulong handle = ((ulong*)(obj + offset))[0];
		return @class.Engine.StringStorage.GetString(handle);
	}

	public bool GetBool(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.Bool, out int byteOffset);
		return ((bool*)(obj + byteOffset))[0];
	}

	public bool GetBoolByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.Bool, out int byteOffset);
		return ((bool*)(obj + byteOffset))[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool GetBoolOptimized(int offset)
	{
		return ((bool*)(obj + offset))[0];
	}

	public long GetReference(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.Long, out int byteOffset);
		return ((long*)(obj + byteOffset))[0];
	}

	public long GetReferenceByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.Long, out int byteOffset);
		return ((long*)(obj + byteOffset))[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long GetReferenceOptimized(int offset)
	{
		return ((long*)(obj + offset))[0];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal long GetReference(int byteOffset)
	{
		return ((long*)(obj + byteOffset))[0];
	}

	public DateTime GetDateTime(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.DateTime, out int byteOffset);
		long val = ((long*)(obj + byteOffset))[0];
		return DateTime.FromBinary(val);
	}

	public DateTime GetDateTimeByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.DateTime, out int byteOffset);
		long val = ((long*)(obj + byteOffset))[0];
		return DateTime.FromBinary(val);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public DateTime GetDateTimeOptimized(int offset)
	{
		return DateTime.FromBinary(((long*)(obj + offset))[0]);
	}

	public byte[] GetByteArray(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.ByteArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackByteArray(blob);
	}

	public byte[] GetByteArrayByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.ByteArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackByteArray(blob);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte[] GetByteArrayOptimized(int offset)
	{
		ulong handle = ((ulong*)(obj + offset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackByteArray(blob);
	}

	public short[] GetShortArray(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.ShortArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackShortArray(blob);
	}

	public short[] GetShortArrayByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.ShortArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackShortArray(blob);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public short[] GetShortArrayOptimized(int offset)
	{
		ulong handle = ((ulong*)(obj + offset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackShortArray(blob);
	}

	public int[] GetIntArray(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.IntArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackIntArray(blob);
	}

	public int[] GetIntArrayByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.IntArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackIntArray(blob);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int[] GetIntArrayOptimized(int offset)
	{
		ulong handle = ((ulong*)(obj + offset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackIntArray(blob);
	}

	public long[] GetLongArray(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.LongArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackLongArray(blob);
	}

	public long[] GetLongArrayByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.LongArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackLongArray(blob);
	}

	internal bool LongArrayContainsValue(int propertyId, LongHashSet values, out long value)
	{
		ClassDescriptor classDesc = @class.ClassDesc;
		int index = classDesc.GetPropertyIndex(propertyId);
		int byteOffset = classDesc.PropertyByteOffsets[index];

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
		{
			value = 0;
			return false;
		}

		return PropertyTypesHelper.DBCheckLongArrayForValue(blob, values, out value);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal long[] GetLongArrayOptimized(int offset)
	{
		ulong handle = ((ulong*)(obj + offset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackLongArray(blob);
	}

	public float[] GetFloatArray(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.FloatArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackFloatArray(blob);
	}

	public float[] GetFloatArrayByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.FloatArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackFloatArray(blob);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public float[] GetFloatArrayOptimized(int offset)
	{
		ulong handle = ((ulong*)(obj + offset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackFloatArray(blob);
	}

	public double[] GetDoubleArray(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.DoubleArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackDoubleArray(blob);
	}

	public double[] GetDoubleArrayByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.DoubleArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackDoubleArray(blob);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public double[] GetDoubleArrayOptimized(int offset)
	{
		ulong handle = ((ulong*)(obj + offset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackDoubleArray(blob);
	}

	public string[] GetStringArray(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.StringArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackStringArray(blob);
	}

	public string[] GetStringArrayByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.StringArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackStringArray(blob);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public string[] GetStringArrayOptimized(int offset)
	{
		ulong handle = ((ulong*)(obj + offset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackStringArray(blob);
	}

	public bool[] GetBoolArray(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.BoolArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackBoolArray(blob);
	}

	public bool[] GetBoolArrayByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.BoolArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackBoolArray(blob);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool[] GetBoolArrayOptimized(int offset)
	{
		ulong handle = ((ulong*)(obj + offset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackBoolArray(blob);
	}

	public long[] GetReferenceArray(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.LongArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackLongArray(blob);
	}

	public long[] GetReferenceArrayByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.LongArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackLongArray(blob);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long[] GetReferenceArrayOptimized(int offset)
	{
		ulong handle = ((ulong*)(obj + offset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackLongArray(blob);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal void GetReferenceArray(StorageEngine engine, int byteOffset, ref long* refs, ref int refsSize, out int refCount)
	{
		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
		{
			refCount = 0;
			return;
		}

		refCount = ((int*)blob)[0];
		if (refCount > refsSize)
		{
			refsSize = Min(refCount, refsSize * 2);
			NativeAllocator.Free((IntPtr)refs);
			refs = (long*)NativeAllocator.Allocate(refsSize * 8);
		}

		PropertyTypesHelper.DBUnpackLongArray(blob, refs);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal bool ContainsReference(StorageEngine engine, int byteOffset, long reference)
	{
		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return false;

		long* lp = PropertyTypesHelper.DBUnpackLongArray(blob, out int count);
		for (int i = 0; i < count; i++)
		{
			if (lp[i] == reference)
				return true;
		}

		return false;
	}

	public DateTime[] GetDateTimeArray(int propertyId, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetProperty(propertyId, PropertyType.DateTimeArray, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackDateTimeArray(blob);
	}

	public DateTime[] GetDateTimeArrayByIndex(int propertyIndex, Transaction tran)
	{
		ValidateRead(tran);
		PropertyDescriptor propDesc = GetPropertyByIndex(propertyIndex, PropertyType.DateTime, out int byteOffset);

		ulong handle = ((ulong*)(obj + byteOffset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackDateTimeArray(blob);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public DateTime[] GetDateTimeArrayOptimized(int offset)
	{
		ulong handle = ((ulong*)(obj + offset))[0];
		byte* blob = @class.Engine.BlobStorage.RetrieveBlob(handle);
		if (blob == null)
			return null;

		return PropertyTypesHelper.DBUnpackDateTimeArray(blob);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private readonly void ValidateRead(Transaction tran)
	{
		if (tran.Closed)
			throw new InvalidOperationException("Transaction has been closed.");

		if (obj == null)
			throw new InvalidOperationException("Object reader is empty.");
	}
}
