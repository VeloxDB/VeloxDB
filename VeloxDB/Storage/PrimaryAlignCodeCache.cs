using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using Velox.Common;
using Velox.Descriptor;
using Velox.Storage.ModelUpdate;

namespace Velox.Storage;

internal unsafe delegate void GenerateAlignDelegate(ClassObject* obj, StringStorage stringStorage,
	BlobStorage blobStorage, ChangesetWriter writer, ulong commonVersion);

internal unsafe class PrimaryAlignCodeCache
{
	static MethodInfo prevVersionPlaceholderMethod = typeof(ChangesetWriter).
		GetMethod(nameof(ChangesetWriter.CreatePreviousVersionPlaceholder), BindingFlags.Instance | BindingFlags.Public);

	static MethodInfo addIdMethod;
	static MethodInfo[] addMethods;

	readonly object sync = new object();
	Dictionary<short, GenerateAlignDelegate> cache;

	static PrimaryAlignCodeCache()
	{
		addIdMethod = typeof(PrimaryAlignCodeCache).GetMethod("AddId", BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);

		addMethods = new MethodInfo[Utils.MaxEnumValue(typeof(PropertyType)) + 1];
		addMethods[(int)PropertyType.Byte] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddByte), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.Short] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddShort), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.Int] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddInt), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.Long] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddLong), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.Float] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddFloat), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.Double] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddDouble), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.Bool] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddBool), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.DateTime] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddDateTime), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.String] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddString), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.ByteArray] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddByteArray), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.ShortArray] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddShortArray), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.IntArray] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddIntArray), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.LongArray] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddLongArray), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.FloatArray] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddFloatArray), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.DoubleArray] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddDoubleArray), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.BoolArray] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddBoolArray), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.DateTimeArray] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddDateTimeArray), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		addMethods[(int)PropertyType.StringArray] = typeof(PrimaryAlignCodeCache).GetMethod(nameof(AddStringArray), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
	}

	public PrimaryAlignCodeCache()
	{
		cache = new Dictionary<short, GenerateAlignDelegate>(128);
	}

	public void InitCache(DataModelDescriptor modelDesc)
	{
		foreach (ClassDescriptor classDesc in modelDesc.GetAllClasses())
		{
			if (!classDesc.IsAbstract)
				GetAlignDelegate(classDesc);
		}
	}

	public void UpdateCache(DataModelUpdate modelUpdate)
	{
		lock (sync)
		{
			foreach (ClassDescriptor classDesc in modelUpdate.DeletedClasses.Values.Select(x => x.ClassDesc).
				Concat(modelUpdate.UpdatedClasses.Values.Where(x => x.PropertyListModified || x.IsAbstractModified).
				Select(x => x.ClassDesc)))
			{
				cache.Remove(classDesc.Id);
			}

			foreach (ClassDescriptor classDesc in modelUpdate.InsertedClasses.Select(x => x.ClassDesc).
				Concat(modelUpdate.UpdatedClasses.Values.Where(x => x.PropertyListModified || x.IsAbstractModified).
				Select(x => x.ClassDesc)))
			{
				if (!classDesc.IsAbstract)
					GetAlignDelegate(classDesc);
			}
		}
	}

	public GenerateAlignDelegate GetAlignDelegate(ClassDescriptor classDesc)
	{
		lock (sync)
		{
			if (!cache.TryGetValue(classDesc.Id, out GenerateAlignDelegate alignDelegate))
			{
				alignDelegate = CreateAlignDelegate(classDesc);
				cache.Add(classDesc.Id, alignDelegate);
			}

			return alignDelegate;
		}
	}

	private static GenerateAlignDelegate CreateAlignDelegate(ClassDescriptor classDesc)
	{
		DynamicMethod method = new DynamicMethod("__" + Guid.NewGuid().ToString("N"), null,
			new Type[] { typeof(ClassObject*), typeof(StringStorage), typeof(BlobStorage),
				typeof(ChangesetWriter), typeof(ulong) }, typeof(PrimaryAlignCodeCache).Module);

		GenerateBody(classDesc, method);

		return (GenerateAlignDelegate)method.CreateDelegate(typeof(GenerateAlignDelegate));
	}

	private static void GenerateBody(ClassDescriptor classDesc, DynamicMethod method)
	{
		TTTrace.Write(classDesc.Id, classDesc.Properties.Length);

		ILGenerator il = method.GetILGenerator();

		// Create placeholder for operation header
		il.Emit(OpCodes.Ldarg_3);
		il.Emit(OpCodes.Call, prevVersionPlaceholderMethod);

		// Add Id
		il.Emit(OpCodes.Ldarg_3);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, addIdMethod);

		for (int i = 0; i < classDesc.Properties.Length; i++)
		{
			PropertyDescriptor pd = classDesc.Properties[i];
			if (pd.Id == SystemCode.DatabaseObject.IdProp)
				continue;

			TTTrace.Write(classDesc.Id, pd.Name, pd.Id, (int)pd.PropertyType, classDesc.PropertyByteOffsets[i]);

			il.Emit(OpCodes.Ldarg_3);
			if (pd.PropertyType < PropertyType.String)
			{
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Ldc_I4, (int)(ClassObject.DataOffset + classDesc.PropertyByteOffsets[i]));
				il.Emit(OpCodes.Call, addMethods[(int)pd.PropertyType]);
			}
			else
			{
				il.Emit(pd.PropertyType == PropertyType.String ? OpCodes.Ldarg_1 : OpCodes.Ldarg_2);
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Ldc_I4, (int)(ClassObject.DataOffset + classDesc.PropertyByteOffsets[i]));
				il.Emit(OpCodes.Ldarg, 4);
				il.Emit(OpCodes.Call, addMethods[(int)pd.PropertyType]);
			}
		}

		il.Emit(OpCodes.Ret);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddId(ChangesetWriter writer, ClassObject* obj)
	{
		TTTrace.Write(obj->id);
		writer.WriteLong(obj->id);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddByte(ChangesetWriter writer, ClassObject* obj, uint byteOffset)
	{
		TTTrace.Write(*((byte*)obj + byteOffset));
		writer.WriteByte(*((byte*)obj + byteOffset));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddShort(ChangesetWriter writer, ClassObject* obj, uint byteOffset)
	{
		TTTrace.Write(*((short*)((byte*)obj + byteOffset)));
		writer.WriteShort(*((short*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddInt(ChangesetWriter writer, ClassObject* obj, uint byteOffset)
	{
		TTTrace.Write(*((int*)((byte*)obj + byteOffset)));
		writer.WriteInt(*((int*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddLong(ChangesetWriter writer, ClassObject* obj, uint byteOffset)
	{
		TTTrace.Write(*((long*)((byte*)obj + byteOffset)));
		writer.WriteLong(*((long*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddFloat(ChangesetWriter writer, ClassObject* obj, uint byteOffset)
	{
		TTTrace.Write(*((float*)((byte*)obj + byteOffset)));
		writer.WriteFloat(*((float*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddDouble(ChangesetWriter writer, ClassObject* obj, uint byteOffset)
	{
		TTTrace.Write(*((double*)((byte*)obj + byteOffset)));
		writer.WriteDouble(*((double*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddBool(ChangesetWriter writer, ClassObject* obj, uint byteOffset)
	{
		TTTrace.Write(*((bool*)((byte*)obj + byteOffset)));
		writer.WriteBool(*((bool*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddDateTime(ChangesetWriter writer, ClassObject* obj, uint byteOffset)
	{
		TTTrace.Write(*((long*)((byte*)obj + byteOffset)));
		writer.WriteDateTime(*((DateTime*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddString(ChangesetWriter writer, StringStorage stringStorage,
		ClassObject* obj, uint byteOffset, ulong commonVersion)
	{
		ulong handle = *((ulong*)((byte*)obj + byteOffset));
		ulong version = stringStorage.GetStringVersion(handle);
		if (version >= commonVersion)
		{
			TTTrace.Write();
			writer.WriteString(stringStorage.GetString(handle));
		}
		else
		{
			TTTrace.Write();
			writer.WriteStringOptional(null, false);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddByteArray(ChangesetWriter writer, BlobStorage blobStorage,
		ClassObject* obj, uint byteOffset, ulong commonVersion)
	{
		ulong handle = *((ulong*)((byte*)obj + byteOffset));
		ulong version = blobStorage.GetVersion(handle);
		if (version >= commonVersion)
		{
			TTTrace.Write();
			byte* bp = blobStorage.RetrieveBlob(handle);
			writer.WriteByteArray(bp != null ? PropertyTypesHelper.DBUnpackByteArray(bp) : null);
		}
		else
		{
			TTTrace.Write();
			writer.WriteByteArrayOptional(null, false);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddShortArray(ChangesetWriter writer, BlobStorage blobStorage,
		ClassObject* obj, uint byteOffset, ulong commonVersion)
	{
		ulong handle = *((ulong*)((byte*)obj + byteOffset));
		ulong version = blobStorage.GetVersion(handle);
		if (version >= commonVersion)
		{
			TTTrace.Write();
			byte* bp = blobStorage.RetrieveBlob(handle);
			writer.WriteShortArray(bp != null ? PropertyTypesHelper.DBUnpackShortArray(bp) : null);
		}
		else
		{
			TTTrace.Write();
			writer.WriteShortArrayOptional(null, false);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddIntArray(ChangesetWriter writer, BlobStorage blobStorage,
		ClassObject* obj, uint byteOffset, ulong commonVersion)
	{
		ulong handle = *((ulong*)((byte*)obj + byteOffset));
		ulong version = blobStorage.GetVersion(handle);
		if (version >= commonVersion)
		{
			TTTrace.Write();
			byte* bp = blobStorage.RetrieveBlob(handle);
			writer.WriteIntArray(bp != null ? PropertyTypesHelper.DBUnpackIntArray(bp) : null);
		}
		else
		{
			TTTrace.Write();
			writer.WriteIntArrayOptional(null, false);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddLongArray(ChangesetWriter writer, BlobStorage blobStorage,
		ClassObject* obj, uint byteOffset, ulong commonVersion)
	{
		ulong handle = *((ulong*)((byte*)obj + byteOffset));
		ulong version = blobStorage.GetVersion(handle);
		if (version >= commonVersion)
		{
			TTTrace.Write();
			byte* bp = blobStorage.RetrieveBlob(handle);
			writer.WriteLongArray(bp != null ? PropertyTypesHelper.DBUnpackLongArray(bp) : null);
		}
		else
		{
			TTTrace.Write();
			writer.WriteLongArrayOptional(null, false);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddFloatArray(ChangesetWriter writer, BlobStorage blobStorage,
		ClassObject* obj, uint byteOffset, ulong commonVersion)
	{
		ulong handle = *((ulong*)((byte*)obj + byteOffset));
		ulong version = blobStorage.GetVersion(handle);
		if (version >= commonVersion)
		{
			TTTrace.Write();
			byte* bp = blobStorage.RetrieveBlob(handle);
			writer.WriteFloatArray(bp != null ? PropertyTypesHelper.DBUnpackFloatArray(bp) : null);
		}
		else
		{
			TTTrace.Write();
			writer.WriteFloatArrayOptional(null, false);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddDoubleArray(ChangesetWriter writer, BlobStorage blobStorage,
		ClassObject* obj, uint byteOffset, ulong commonVersion)
	{
		ulong handle = *((ulong*)((byte*)obj + byteOffset));
		ulong version = blobStorage.GetVersion(handle);
		if (version >= commonVersion)
		{
			TTTrace.Write();
			byte* bp = blobStorage.RetrieveBlob(handle);
			writer.WriteDoubleArray(bp != null ? PropertyTypesHelper.DBUnpackDoubleArray(bp) : null);
		}
		else
		{
			TTTrace.Write();
			writer.WriteDoubleArrayOptional(null, false);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddBoolArray(ChangesetWriter writer, BlobStorage blobStorage,
		ClassObject* obj, uint byteOffset, ulong commonVersion)
	{
		ulong handle = *((ulong*)((byte*)obj + byteOffset));
		ulong version = blobStorage.GetVersion(handle);
		if (version >= commonVersion)
		{
			TTTrace.Write();
			byte* bp = blobStorage.RetrieveBlob(handle);
			writer.WriteBoolArray(bp != null ? PropertyTypesHelper.DBUnpackBoolArray(bp) : null);
		}
		else
		{
			TTTrace.Write();
			writer.WriteBoolArrayOptional(null, false);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddDateTimeArray(ChangesetWriter writer, BlobStorage blobStorage,
		ClassObject* obj, uint byteOffset, ulong commonVersion)
	{
		ulong handle = *((ulong*)((byte*)obj + byteOffset));
		ulong version = blobStorage.GetVersion(handle);
		if (version >= commonVersion)
		{
			TTTrace.Write();
			byte* bp = blobStorage.RetrieveBlob(handle);
			writer.WriteDateTimeArray(bp != null ? PropertyTypesHelper.DBUnpackDateTimeArray(bp) : null);
		}
		else
		{
			TTTrace.Write();
			writer.WriteDateTimeArrayOptional(null, false);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void AddStringArray(ChangesetWriter writer, BlobStorage blobStorage,
		ClassObject* obj, uint byteOffset, ulong commonVersion)
	{
		ulong handle = *((ulong*)((byte*)obj + byteOffset));
		ulong version = blobStorage.GetVersion(handle);
		if (version >= commonVersion)
		{
			TTTrace.Write();
			byte* bp = blobStorage.RetrieveBlob(handle);
			writer.WriteStringArray(bp != null ? PropertyTypesHelper.DBUnpackStringArray(bp) : null);
		}
		else
		{
			TTTrace.Write();
			writer.WriteStringArrayOptional(null, false);
		}
	}
}
