using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal unsafe delegate void ObjectCopyDelegate(IntPtr src, IntPtr dst, StringStorage stringStorage, BlobStorage blobStorage);

internal sealed class UpdateClassPropertiesJob : ModelUpdateJob
{
	static MethodInfo StringDecRefCountMethod = typeof(StringStorage).GetMethod(nameof(StringStorage.DecRefCount),
		BindingFlags.Public | BindingFlags.Instance);
	static MethodInfo BlobDecRefCountMethod = typeof(BlobStorage).GetMethod(nameof(BlobStorage.DecRefCount),
		BindingFlags.Public | BindingFlags.Instance);
	static MethodInfo CopyHeaderMethod = typeof(UpdateClassPropertiesJob).GetMethod(
		nameof(CopyObjectHeader), BindingFlags.NonPublic | BindingFlags.Static);

	ulong commitVersion;
	ClassScan scan;
	ObjectCopyDelegate copyDelegate;
	HashIndexComparerPair[] hashIndexes;

	public UpdateClassPropertiesJob(ClassScan scan, ObjectCopyDelegate copyDelegate, HashIndexComparerPair[] hashIndexes, ulong commitVersion)
	{
		this.scan = scan;
		this.copyDelegate = copyDelegate;
		this.hashIndexes = hashIndexes;
		this.commitVersion = commitVersion;
	}

	public static IEnumerable<UpdateClassPropertiesJob> Start(Database database, ModelUpdateContext updateContext, ulong commitVersion)
	{
		foreach (ClassUpdate cu in updateContext.ModelUpdate.UpdatedClasses.Values)
		{
			TTTrace.Write(database.TraceId, database.Id, cu.PrevClassDesc.Id);

			bool propertiesModified = cu.DeletedProperties.Length != 0 || cu.InsertedProperties.Length != 0 || cu.IsAbstractModified;
			if (!propertiesModified && !cu.HasDefaultValueChanges)
				continue;

			if (cu.ClassDesc.IsAbstract)
				continue;

			database.Trace.Debug("Updating class {0}.", cu.PrevClassDesc.FullName);
			TTTrace.Write(database.TraceId, database.Id, cu.PrevClassDesc.Id);

			ClassBase @class = database.GetClass(cu.PrevClassDesc.Index);
			@class.StartPropertyUpdate(cu.ClassDesc, propertiesModified);

			if (propertiesModified)
			{
				ObjectCopyDelegate copyDelegate = GenerateObjectCopyMethod(cu);

				List<HashIndexComparerPair> hashIndexes = new List<HashIndexComparerPair>(1);
				for (int i = 0; i < cu.ClassDesc.HashIndexes.Length; i++)
				{
					HashIndexDescriptor hashIndexDesc = cu.ClassDesc.HashIndexes[i];
					HashIndexDescriptor prevHashIndexDesc = database.ModelDesc.GetHashIndex(hashIndexDesc.Id);

					if (!updateContext.TryGetNewHashIndex(hashIndexDesc.Id, out HashIndex hashIndex, out _))
						hashIndex = database.GetHashIndex(prevHashIndexDesc.Id, out _);

					if (!hashIndex.PendingRefill)
					{
						KeyComparerDesc kad = @class.ClassDesc.GetHashAccessDescByPropertyName(hashIndex.HashIndexDesc);
						HashComparer comparer = new HashComparer(kad, null);
						hashIndexes.Add(new HashIndexComparerPair(hashIndex, comparer));
					}
				}

				ClassScan[] classScans = @class.GetDisposingClassScans(null, false, out long tc);
				ulong cv = (cu.RequiresDefaultValueWrite && !updateContext.ModelUpdate.IsAlignment) ? commitVersion : 0;

				for (int k = 0; k < classScans.Length; k++)
				{
					yield return new UpdateClassPropertiesJob(classScans[k], copyDelegate, hashIndexes.ToArray(), cv);
				}
			}
		}
	}

	public static void Finish(Database database, ModelUpdateContext updateContext)
	{
		foreach (ClassUpdate cu in updateContext.ModelUpdate.UpdatedClasses.Values)
		{
			bool propertiesModified = cu.DeletedProperties.Length != 0 || cu.InsertedProperties.Length != 0 || cu.IsAbstractModified;
			if (!propertiesModified && !cu.HasDefaultValueChanges)
				continue;

			if (cu.ClassDesc.IsAbstract)
				continue;

			TTTrace.Write(database.TraceId, database.Id, cu.PrevClassDesc.Id);

			ClassBase @class = database.GetClass(cu.PrevClassDesc.Index);
			@class.FinishPropertyUpdate();
		}
	}

	public override void Execute()
	{
		using (scan)
		{
			ulong[] handles = new ulong[128];
			int count = handles.Length;
			while (scan.NextHandles(handles, 0, ref count))
			{
				for (int i = 0; i < count; i++)
				{
					scan.Class.UpdateModelForObject(handles[i], copyDelegate, hashIndexes, commitVersion);
				}

				count = handles.Length;
			}
		}
	}

	private unsafe static void CopyObjectHeader(ClassObject* src, ClassObject* dst)
	{
		dst->nextCollisionHandle = src->nextCollisionHandle;
		dst->nextVersionHandle = src->nextVersionHandle;
		dst->readerInfo = src->readerInfo;
	}

	private static ObjectCopyDelegate GenerateObjectCopyMethod(ClassUpdate classUpdate)
	{
		DynamicMethod dm = new DynamicMethod("__CopyObject_" + Guid.NewGuid().ToString("N"), typeof(void),
			new Type[] { typeof(IntPtr), typeof(IntPtr), typeof(StringStorage), typeof(BlobStorage) }, false);

		ILGenerator il = dm.GetILGenerator();

		HashSet<int> deletedProps = new HashSet<int>(1);

		// Generate code to free strings and blobs that are being removed
		foreach (PropertyDelete propDelete in classUpdate.DeletedProperties)
		{
			PropertyDescriptor propDesc = propDelete.PropDesc;
			deletedProps.Add(propDesc.Id);
			if (propDesc.Kind == PropertyKind.Array)
			{
				il.Emit(OpCodes.Ldarg, 3);
				il.Emit(OpCodes.Ldarg, 0);
				il.Emit(OpCodes.Ldc_I4, classUpdate.PrevClassDesc.
					PropertyByteOffsets[classUpdate.PrevClassDesc.GetPropertyIndex(propDesc.Id)]);
				il.Emit(OpCodes.Add);
				il.Emit(OpCodes.Ldind_I8);
				il.Emit(OpCodes.Call, BlobDecRefCountMethod);
			}
			else if (propDesc.PropertyType == PropertyType.String)
			{
				il.Emit(OpCodes.Ldarg, 2);
				il.Emit(OpCodes.Ldarg, 0);
				il.Emit(OpCodes.Ldc_I4, classUpdate.PrevClassDesc.
					PropertyByteOffsets[classUpdate.PrevClassDesc.GetPropertyIndex(propDesc.Id)]);
				il.Emit(OpCodes.Add);
				il.Emit(OpCodes.Ldind_I8);
				il.Emit(OpCodes.Call, StringDecRefCountMethod);
			}
		}

		// Generate code to copy retained properties
		for (int i = 0; i < classUpdate.PrevClassDesc.Properties.Length; i++)
		{
			PropertyDescriptor propDesc = classUpdate.PrevClassDesc.Properties[i];
			if (deletedProps.Contains(propDesc.Id))
				continue;

			il.Emit(OpCodes.Ldarg, 1);
			il.Emit(OpCodes.Ldc_I4, classUpdate.ClassDesc.PropertyByteOffsets[classUpdate.ClassDesc.GetPropertyIndex(propDesc.Id)]);
			il.Emit(OpCodes.Add);

			il.Emit(OpCodes.Ldarg, 0);
			il.Emit(OpCodes.Ldc_I4, classUpdate.PrevClassDesc.PropertyByteOffsets[i]);
			il.Emit(OpCodes.Add);
			il.Emit(GetLoadSimpleTypeInstruction(propDesc.PropertyType));

			il.Emit(GetStoreSimpleTypeInstruction(propDesc.PropertyType));
		}

		// Generate code to set default values for new properties
		foreach (PropertyInsert propInsert in classUpdate.InsertedProperties)
		{
			il.Emit(OpCodes.Ldarg, 1);
			il.Emit(OpCodes.Ldc_I4, classUpdate.ClassDesc.
				PropertyByteOffsets[classUpdate.ClassDesc.GetPropertyIndex(propInsert.PropDesc.Id)]);
			il.Emit(OpCodes.Add);
			EmitLoadDefaultValueInstruction(propInsert.PropDesc, il);
			il.Emit(GetStoreSimpleTypeInstruction(propInsert.PropDesc.PropertyType));
		}

		il.Emit(OpCodes.Ret);

		return (ObjectCopyDelegate)dm.CreateDelegate(typeof(ObjectCopyDelegate));
	}

	public static void EmitLoadDefaultValueInstruction(PropertyDescriptor propDesc, ILGenerator il)
	{
		switch (propDesc.PropertyType)
		{
			case PropertyType.Byte:
				il.Emit(OpCodes.Ldc_I4, (int)(byte)propDesc.DefaultValue);
				break;

			case PropertyType.Short:
				il.Emit(OpCodes.Ldc_I4, (int)(short)propDesc.DefaultValue);
				break;

			case PropertyType.Int:
				il.Emit(OpCodes.Ldc_I4, (int)propDesc.DefaultValue);
				break;

			case PropertyType.Long:
				il.Emit(OpCodes.Ldc_I8, (long)propDesc.DefaultValue);
				break;

			case PropertyType.Float:
				il.Emit(OpCodes.Ldc_R4, (float)propDesc.DefaultValue);
				break;

			case PropertyType.Double:
				il.Emit(OpCodes.Ldc_R8, (double)propDesc.DefaultValue);
				break;

			case PropertyType.Bool:
				il.Emit(OpCodes.Ldc_I4, (bool)propDesc.DefaultValue ? 1 : 0);
				break;

			case PropertyType.DateTime:
				il.Emit(OpCodes.Ldc_I8, ((DateTime)propDesc.DefaultValue).ToBinary());
				break;

			default:
				il.Emit(OpCodes.Ldc_I8, (long)0);
				break;
		}
	}

	public static OpCode GetLoadSimpleTypeInstruction(PropertyType type)
	{
		switch (type)
		{
			case PropertyType.Byte:
				return OpCodes.Ldind_U1;

			case PropertyType.Short:
				return OpCodes.Ldind_I2;

			case PropertyType.Int:
				return OpCodes.Ldind_I4;

			case PropertyType.Long:
				return OpCodes.Ldind_I8;

			case PropertyType.Float:
				return OpCodes.Ldind_R4;

			case PropertyType.Double:
				return OpCodes.Ldind_R8;

			case PropertyType.Bool:
				return OpCodes.Ldind_U1;

			case PropertyType.String:
				return OpCodes.Ldind_I8;

			case PropertyType.DateTime:
				return OpCodes.Ldind_I8;

			default:
				return OpCodes.Ldind_I8;    // Arrays
		}
	}

	public static OpCode GetStoreSimpleTypeInstruction(PropertyType type)
	{
		switch (type)
		{
			case PropertyType.Byte:
				return OpCodes.Stind_I1;

			case PropertyType.Short:
				return OpCodes.Stind_I2;

			case PropertyType.Int:
				return OpCodes.Stind_I4;

			case PropertyType.Long:
				return OpCodes.Stind_I8;

			case PropertyType.Float:
				return OpCodes.Stind_R4;

			case PropertyType.Double:
				return OpCodes.Stind_I8;

			case PropertyType.Bool:
				return OpCodes.Stind_I1;

			case PropertyType.DateTime:
				return OpCodes.Stind_I8;

			default:
				return OpCodes.Stind_I8;    // Arrays
		}
	}
}
