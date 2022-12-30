using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage.ModelUpdate;

namespace VeloxDB.Storage;

internal unsafe delegate ulong ApplyAlignDelegate(ClassObject* obj, StringStorage stringStorage,
	BlobStorage blobStorage, TransactionContext tc, ChangesetReader reader, ClassDescriptor classDesc,
	HashIndexDeleteDelegate hashDelegate, ulong objHandle);

internal unsafe delegate void HashIndexDeleteDelegate(ClassObject* obj, ulong objHandle, int hashIndexIndex);

internal unsafe class StandbyAlignCodeCache
{
	static MethodInfo readReferenceMethod;
	static MethodInfo readIndexedReferenceMethod;
	static MethodInfo readStringMethod;
	static MethodInfo readIndexedStringMethod;
	static MethodInfo readArrayMethod;
	static MethodInfo skipArrayMethod;
	static MethodInfo readReferenceArrayMethod;
	static MethodInfo[] readMethods;
	static MethodInfo[] readIndexedMethods;
	static MethodInfo[] skipMethods;
	static MethodInfo getAffectedIndexesMethod;
	static MethodInfo readOnlyArrayGetMethod;

	readonly object sync = new object();
	Dictionary<Key, ApplyAlignDelegate> cache;

	static StandbyAlignCodeCache()
	{
		readReferenceMethod = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadReference), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readIndexedReferenceMethod = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadIndexedReference), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readStringMethod = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadString), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readIndexedStringMethod = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadIndexedString), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readArrayMethod = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadArray), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		skipArrayMethod = typeof(StandbyAlignCodeCache).GetMethod(nameof(SkipArray), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readReferenceArrayMethod = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadReferenceArray), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		getAffectedIndexesMethod = typeof(ClassDescriptor).GetProperty(nameof(ClassDescriptor.PropertyHashIndexIndexes), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance).GetGetMethod();
		readOnlyArrayGetMethod = typeof(ReadOnlyArray<ReadOnlyArray<int>>).GetMethod(nameof(ReadOnlyArray<int>.Get), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);

		readMethods = new MethodInfo[Utils.MaxEnumValue(typeof(PropertyType)) + 1];
		readMethods[(int)PropertyType.Byte] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadByte), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readMethods[(int)PropertyType.Short] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadShort), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readMethods[(int)PropertyType.Int] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadInt), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readMethods[(int)PropertyType.Long] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadLong), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readMethods[(int)PropertyType.Float] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadFloat), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readMethods[(int)PropertyType.Double] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadDouble), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readMethods[(int)PropertyType.Bool] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadBool), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readMethods[(int)PropertyType.DateTime] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadDateTime), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);

		readIndexedMethods = new MethodInfo[Utils.MaxEnumValue(typeof(PropertyType)) + 1];
		readIndexedMethods[(int)PropertyType.Byte] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadIndexedByte), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readIndexedMethods[(int)PropertyType.Short] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadIndexedShort), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readIndexedMethods[(int)PropertyType.Int] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadIndexedInt), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readIndexedMethods[(int)PropertyType.Long] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadIndexedLong), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readIndexedMethods[(int)PropertyType.Float] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadIndexedFloat), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readIndexedMethods[(int)PropertyType.Double] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadIndexedDouble), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readIndexedMethods[(int)PropertyType.Bool] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadIndexedBool), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		readIndexedMethods[(int)PropertyType.DateTime] = typeof(StandbyAlignCodeCache).GetMethod(nameof(ReadIndexedDateTime), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);

		skipMethods = new MethodInfo[Utils.MaxEnumValue(typeof(PropertyType)) + 1];
		skipMethods[(int)PropertyType.Byte] = typeof(StandbyAlignCodeCache).GetMethod(nameof(SkipByte), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		skipMethods[(int)PropertyType.Short] = typeof(StandbyAlignCodeCache).GetMethod(nameof(SkipShort), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		skipMethods[(int)PropertyType.Int] = typeof(StandbyAlignCodeCache).GetMethod(nameof(SkipInt), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		skipMethods[(int)PropertyType.Long] = typeof(StandbyAlignCodeCache).GetMethod(nameof(SkipLong), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		skipMethods[(int)PropertyType.Float] = typeof(StandbyAlignCodeCache).GetMethod(nameof(SkipFloat), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		skipMethods[(int)PropertyType.Double] = typeof(StandbyAlignCodeCache).GetMethod(nameof(SkipDouble), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		skipMethods[(int)PropertyType.Bool] = typeof(StandbyAlignCodeCache).GetMethod(nameof(SkipBool), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		skipMethods[(int)PropertyType.DateTime] = typeof(StandbyAlignCodeCache).GetMethod(nameof(SkipDateTime), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
		skipMethods[(int)PropertyType.String] = typeof(StandbyAlignCodeCache).GetMethod(nameof(SkipString), BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);
	}

	public StandbyAlignCodeCache()
	{
		cache = new Dictionary<Key, ApplyAlignDelegate>(128);
	}

	public ApplyAlignDelegate GetAlignDelegate(ChangesetBlock block)
	{
		Key key = new Key(block);
		lock (sync)
		{
			if (!cache.TryGetValue(key, out ApplyAlignDelegate alignDelegate))
			{
				alignDelegate = CreateAlignDelegate(block);
				cache.Add(key, alignDelegate);
			}

			return alignDelegate;
		}
	}

	public void UpdateCache(DataModelUpdate modelUpdate)
	{
		lock (sync)
		{
			List<Key> keys = new List<Key>();
			foreach (Key key in cache.Keys)
			{
				if ((modelUpdate.UpdatedClasses.TryGetValue(key.ClassId, out ClassUpdate cu) &&
					(cu.PropertyListModified || cu.HashedPropertiesModified || cu.ReferenceTrackingModified)) ||
					modelUpdate.DeletedClasses.ContainsKey(key.ClassId))
				{
					keys.Add(key);
				}
			}

			foreach (Key key in keys)
			{
				cache.Remove(key);
			}
		}
	}

	private static ApplyAlignDelegate CreateAlignDelegate(ChangesetBlock block)
	{
		DynamicMethod method = new DynamicMethod("__" + Guid.NewGuid().ToString("N"), typeof(ulong),
			new Type[] { typeof(ClassObject*), typeof(StringStorage), typeof(BlobStorage),
				typeof(TransactionContext), typeof(ChangesetReader), typeof(ClassDescriptor),
				typeof(HashIndexDeleteDelegate), typeof(ulong) }, typeof(StandbyAlignCodeCache).Module);

		GenerateBody(block, method);

		return (ApplyAlignDelegate)method.CreateDelegate(typeof(ApplyAlignDelegate));
	}

	private static void GenerateBody(ChangesetBlock block, DynamicMethod method)
	{
		TTTrace.Write(block.ClassDescriptor != null ? block.ClassDescriptor.Id : 0);

		ILGenerator il = method.GetILGenerator();

		LocalBuilder indexMaskVar = il.DeclareLocal(typeof(ulong));
		il.Emit(OpCodes.Ldc_I4_0);
		il.Emit(OpCodes.Conv_I8);
		il.Emit(OpCodes.Stloc, indexMaskVar);

		for (int i = 1; i < block.PropertyCount; i++)
		{
			ChangesetBlockProperty csp = block.Properties[i];
			TTTrace.Write(csp.Index, (int)csp.Type);

			if (csp.Index == -1)
			{
				if (csp.Type <= PropertyType.String)
				{
					il.Emit(OpCodes.Ldarg, 4);
					il.Emit(OpCodes.Call, skipMethods[(int)csp.Type]);
				}
				else
				{
					il.Emit(OpCodes.Ldarg, 4);
					il.Emit(OpCodes.Ldc_I4, (int)csp.Type);
					il.Emit(OpCodes.Call, skipArrayMethod);
				}
			}
			else
			{
				PropertyDescriptor pd = block.ClassDescriptor.Properties[csp.Index];
				ReadOnlyArray<int> affectedHashIndexIndexes = block.ClassDescriptor.PropertyHashIndexIndexes[csp.Index];
				int offset = ClassObject.DataOffset + block.ClassDescriptor.PropertyByteOffsets[csp.Index];

				TTTrace.Write(block.ClassDescriptor.Id, pd.Id, offset, affectedHashIndexIndexes != null ?
					affectedHashIndexIndexes.Length : 0);

				il.Emit(OpCodes.Ldarg, 4);
				if (csp.Type < PropertyType.String)
				{
					if (pd.Kind == PropertyKind.Reference)
					{
						ReferencePropertyDescriptor rpd = (ReferencePropertyDescriptor)pd;
						il.Emit(OpCodes.Ldarg_0);
						il.Emit(OpCodes.Ldc_I4, offset);
						il.Emit(OpCodes.Ldarg_3);
						il.Emit(OpCodes.Ldc_I4, block.ClassDescriptor.Index);
						il.Emit(OpCodes.Ldc_I4, rpd.Id);
						il.Emit(OpCodes.Ldc_I4, rpd.TrackInverseReferences ? 1 : 0);

						if (affectedHashIndexIndexes != null && affectedHashIndexIndexes.Length > 0)
						{
							LoadIndexedParameters(il, indexMaskVar, csp);
							il.Emit(OpCodes.Call, readIndexedReferenceMethod);
							il.Emit(OpCodes.Stloc, indexMaskVar);
						}
						else
						{
							il.Emit(OpCodes.Call, readReferenceMethod);
						}
					}
					else
					{
						il.Emit(OpCodes.Ldarg_0);
						il.Emit(OpCodes.Ldc_I4, offset);

						if (affectedHashIndexIndexes != null && affectedHashIndexIndexes.Length > 0)
						{
							Checker.AssertFalse(pd.Id == SystemCode.DatabaseObject.Version);
							LoadIndexedParameters(il, indexMaskVar, csp);
							il.Emit(OpCodes.Call, readIndexedMethods[(int)csp.Type]);
							il.Emit(OpCodes.Stloc, indexMaskVar);
						}
						else
						{
							il.Emit(OpCodes.Call, readMethods[(int)csp.Type]);
						}
					}
				}
				else if (csp.Type == PropertyType.String)
				{
					il.Emit(OpCodes.Ldarg_1);
					il.Emit(OpCodes.Ldarg_0);
					il.Emit(OpCodes.Ldc_I4, offset);

					if (affectedHashIndexIndexes != null && affectedHashIndexIndexes.Length > 0)
					{
						LoadIndexedParameters(il, indexMaskVar, csp);
						il.Emit(OpCodes.Call, readIndexedStringMethod);
						il.Emit(OpCodes.Stloc, indexMaskVar);
					}
					else
					{
						il.Emit(OpCodes.Call, readStringMethod);
					}
				}
				else
				{
					il.Emit(OpCodes.Ldarg_2);
					il.Emit(OpCodes.Ldarg_0);
					il.Emit(OpCodes.Ldc_I4, offset);
					if (pd.Kind == PropertyKind.Reference)
					{
						ReferencePropertyDescriptor rpd = (ReferencePropertyDescriptor)pd;
						il.Emit(OpCodes.Ldarg_3);
						il.Emit(OpCodes.Ldc_I4, block.ClassDescriptor.Index);
						il.Emit(OpCodes.Ldc_I4, rpd.Id);
						il.Emit(OpCodes.Ldc_I4, (int)(rpd.TrackInverseReferences ? 1 : 0));
						il.Emit(OpCodes.Call, readReferenceArrayMethod);
					}
					else
					{
						il.Emit(OpCodes.Ldc_I4, (int)csp.Type);
						il.Emit(OpCodes.Call, readArrayMethod);
					}
				}
			}
		}

		il.Emit(OpCodes.Ldloc, indexMaskVar);
		il.Emit(OpCodes.Ret);
	}

	private static void LoadIndexedParameters(ILGenerator il, LocalBuilder indexMaskVar, ChangesetBlockProperty csp)
	{
		il.Emit(OpCodes.Ldloc, indexMaskVar);
		il.Emit(OpCodes.Ldarg, 5);
		il.Emit(OpCodes.Call, getAffectedIndexesMethod);
		il.Emit(OpCodes.Ldc_I4, csp.Index);
		il.Emit(OpCodes.Call, readOnlyArrayGetMethod);
		il.Emit(OpCodes.Ldarg, 6);
		il.Emit(OpCodes.Ldarg, 7);
	}

	private static ulong UpatedAffectedIndexes(ClassObject* obj, ulong objHandle,
		ulong currentAffectedIndexes, ReadOnlyArray<int> affectedIndexIndexes, HashIndexDeleteDelegate hashDelegate)
	{
		if (hashDelegate == null || affectedIndexIndexes == null)
			return currentAffectedIndexes;

		for (int i = 0; i < affectedIndexIndexes.Length; i++)
		{
			int index = affectedIndexIndexes[i];
			if ((currentAffectedIndexes & (ulong)(1 << index)) == 0)
			{
				TTTrace.Write(currentAffectedIndexes, index);
				currentAffectedIndexes |= ((ulong)1 << index);
				hashDelegate(obj, objHandle, index);
			}
		}

		return currentAffectedIndexes;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void ReadByte(ChangesetReader reader, ClassObject* obj, uint byteOffset)
	{
		*((byte*)obj + byteOffset) = reader.ReadByte();
		TTTrace.Write(*((byte*)obj + byteOffset));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static ulong ReadIndexedByte(ChangesetReader reader, ClassObject* obj, uint byteOffset, ulong currentAffectedIndexes,
		ReadOnlyArray<int> affectedIndexIndexes, HashIndexDeleteDelegate hashDelegate, ulong objHandle)
	{
		byte prev = *((byte*)obj + byteOffset);
		byte curr = reader.ReadByte();
		TTTrace.Write(curr);

		if (prev != curr)
		{
			currentAffectedIndexes = UpatedAffectedIndexes(obj, objHandle, currentAffectedIndexes, affectedIndexIndexes, hashDelegate);
			*((byte*)obj + byteOffset) = curr;
		}

		return currentAffectedIndexes;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void SkipByte(ChangesetReader reader)
	{
		reader.ReadByte();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void ReadShort(ChangesetReader reader, ClassObject* obj, uint byteOffset)
	{
		*((short*)((byte*)obj + byteOffset)) = reader.ReadShort();
		TTTrace.Write(*((short*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static ulong ReadIndexedShort(ChangesetReader reader, ClassObject* obj, uint byteOffset, ulong currentAffectedIndexes,
		ReadOnlyArray<int> affectedIndexIndexes, HashIndexDeleteDelegate hashDelegate, ulong objHandle)
	{
		short prev = *((short*)((byte*)obj + byteOffset));
		short curr = reader.ReadShort();
		TTTrace.Write(curr);

		if (prev != curr)
		{
			currentAffectedIndexes = UpatedAffectedIndexes(obj, objHandle, currentAffectedIndexes, affectedIndexIndexes, hashDelegate);
			*((short*)((byte*)obj + byteOffset)) = curr;
		}

		return currentAffectedIndexes;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void SkipShort(ChangesetReader reader)
	{
		reader.ReadShort();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void ReadInt(ChangesetReader reader, ClassObject* obj, uint byteOffset)
	{
		*((int*)((byte*)obj + byteOffset)) = reader.ReadInt();
		TTTrace.Write(*((int*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static ulong ReadIndexedInt(ChangesetReader reader, ClassObject* obj, uint byteOffset, ulong currentAffectedIndexes,
		ReadOnlyArray<int> affectedIndexIndexes, HashIndexDeleteDelegate hashDelegate, ulong objHandle)
	{
		int prev = *((int*)((byte*)obj + byteOffset));
		int curr = reader.ReadInt();
		TTTrace.Write(curr);

		if (prev != curr)
		{
			currentAffectedIndexes = UpatedAffectedIndexes(obj, objHandle, currentAffectedIndexes, affectedIndexIndexes, hashDelegate);
			*((int*)((byte*)obj + byteOffset)) = curr;
		}

		return currentAffectedIndexes;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void SkipInt(ChangesetReader reader)
	{
		reader.ReadInt();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void ReadLong(ChangesetReader reader, ClassObject* obj, uint byteOffset)
	{
		*((long*)((byte*)obj + byteOffset)) = reader.ReadLong();
		TTTrace.Write(*((long*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static ulong ReadIndexedLong(ChangesetReader reader, ClassObject* obj, uint byteOffset, ulong currentAffectedIndexes,
		ReadOnlyArray<int> affectedIndexIndexes, HashIndexDeleteDelegate hashDelegate, ulong objHandle)
	{
		long prev = *((long*)((byte*)obj + byteOffset));
		long curr = reader.ReadLong();
		TTTrace.Write(curr);

		if (prev != curr)
		{
			currentAffectedIndexes = UpatedAffectedIndexes(obj, objHandle, currentAffectedIndexes, affectedIndexIndexes, hashDelegate);
			*((long*)((byte*)obj + byteOffset)) = curr;
		}

		return currentAffectedIndexes;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void SkipLong(ChangesetReader reader)
	{
		reader.ReadLong();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void ReadReference(ChangesetReader reader, ClassObject* obj, uint byteOffset,
		TransactionContext tc, ushort classIndex, int propId, bool trackRefs)
	{
		long prevRef = *((long*)((byte*)obj + byteOffset));
		long currRef = reader.ReadLong();
		TTTrace.Write(currRef);

		if (currRef != prevRef)
		{
			if (prevRef != 0)
				tc.AddGroupingInvRefChange(classIndex, obj->id, prevRef, propId, trackRefs, (byte)InvRefChangeType.Delete);

			if (currRef != 0)
				tc.AddGroupingInvRefChange(classIndex, obj->id, currRef, propId, trackRefs, (byte)InvRefChangeType.Insert);

			*((long*)((byte*)obj + byteOffset)) = currRef;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static ulong ReadIndexedReference(ChangesetReader reader, ClassObject* obj, uint byteOffset, TransactionContext tc,
		ushort classIndex, int propId, bool trackRefs, ulong currentAffectedIndexes,
		ReadOnlyArray<int> affectedIndexIndexes, HashIndexDeleteDelegate hashDelegate, ulong objHandle)
	{
		long prevRef = *((long*)((byte*)obj + byteOffset));
		long currRef = reader.ReadLong();
		TTTrace.Write(currRef);

		if (currRef != prevRef)
		{
			if (prevRef != 0)
				tc.AddGroupingInvRefChange(classIndex, obj->id, prevRef, propId, trackRefs, (byte)InvRefChangeType.Delete);

			if (currRef != 0)
				tc.AddGroupingInvRefChange(classIndex, obj->id, currRef, propId, trackRefs, (byte)InvRefChangeType.Insert);

			currentAffectedIndexes = UpatedAffectedIndexes(obj, objHandle, currentAffectedIndexes, affectedIndexIndexes, hashDelegate);

			*((long*)((byte*)obj + byteOffset)) = currRef;
		}

		return currentAffectedIndexes;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void ReadFloat(ChangesetReader reader, ClassObject* obj, uint byteOffset)
	{
		*((float*)((byte*)obj + byteOffset)) = reader.ReadFloat();
		TTTrace.Write(*((float*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static ulong ReadIndexedFloat(ChangesetReader reader, ClassObject* obj, uint byteOffset, ulong currentAffectedIndexes,
		ReadOnlyArray<int> affectedIndexIndexes, HashIndexDeleteDelegate hashDelegate, ulong objHandle)
	{
		float prev = *((float*)((byte*)obj + byteOffset));
		float curr = reader.ReadFloat();
		TTTrace.Write(curr);

		if (prev != curr)
		{
			currentAffectedIndexes = UpatedAffectedIndexes(obj, objHandle, currentAffectedIndexes, affectedIndexIndexes, hashDelegate);
			*((float*)((byte*)obj + byteOffset)) = curr;
		}

		return currentAffectedIndexes;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void SkipFloat(ChangesetReader reader)
	{
		reader.ReadFloat();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void ReadDouble(ChangesetReader reader, ClassObject* obj, uint byteOffset)
	{
		*((double*)((byte*)obj + byteOffset)) = reader.ReadDouble();
		TTTrace.Write(*((double*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static ulong ReadIndexedDouble(ChangesetReader reader, ClassObject* obj, uint byteOffset, ulong currentAffectedIndexes,
		ReadOnlyArray<int> affectedIndexIndexes, HashIndexDeleteDelegate hashDelegate, ulong objHandle)
	{
		double prev = *((double*)((byte*)obj + byteOffset));
		double curr = reader.ReadDouble();
		TTTrace.Write(curr);

		if (prev != curr)
		{
			currentAffectedIndexes = UpatedAffectedIndexes(obj, objHandle, currentAffectedIndexes, affectedIndexIndexes, hashDelegate);
			*((double*)((byte*)obj + byteOffset)) = curr;
		}

		return currentAffectedIndexes;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void SkipDouble(ChangesetReader reader)
	{
		reader.ReadDouble();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void ReadBool(ChangesetReader reader, ClassObject* obj, uint byteOffset)
	{
		*((bool*)((byte*)obj + byteOffset)) = reader.ReadBool();
		TTTrace.Write(*((bool*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static ulong ReadIndexedBool(ChangesetReader reader, ClassObject* obj, uint byteOffset, ulong currentAffectedIndexes,
		ReadOnlyArray<int> affectedIndexIndexes, HashIndexDeleteDelegate hashDelegate, ulong objHandle)
	{
		bool prev = *((bool*)((byte*)obj + byteOffset));
		bool curr = reader.ReadBool();
		TTTrace.Write(curr);

		if (prev != curr)
		{
			currentAffectedIndexes = UpatedAffectedIndexes(obj, objHandle, currentAffectedIndexes, affectedIndexIndexes, hashDelegate);
			*((bool*)((byte*)obj + byteOffset)) = curr;
		}

		return currentAffectedIndexes;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void SkipBool(ChangesetReader reader)
	{
		reader.ReadBool();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void ReadDateTime(ChangesetReader reader, ClassObject* obj, uint byteOffset)
	{
		*((DateTime*)((byte*)obj + byteOffset)) = reader.ReadDateTime();
		TTTrace.Write(*((long*)((byte*)obj + byteOffset)));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static ulong ReadIndexedDateTime(ChangesetReader reader, ClassObject* obj, uint byteOffset, ulong currentAffectedIndexes,
		ReadOnlyArray<int> affectedIndexIndexes, HashIndexDeleteDelegate hashDelegate, ulong objHandle)
	{
		DateTime prev = *((DateTime*)((byte*)obj + byteOffset));
		DateTime curr = reader.ReadDateTime();
		TTTrace.Write(curr.ToBinary());

		if (prev != curr)
		{
			currentAffectedIndexes = UpatedAffectedIndexes(obj, objHandle, currentAffectedIndexes, affectedIndexIndexes, hashDelegate);
			*((DateTime*)((byte*)obj + byteOffset)) = curr;
		}

		return currentAffectedIndexes;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void SkipDateTime(ChangesetReader reader)
	{
		reader.ReadDateTime();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void ReadString(ChangesetReader reader, StringStorage stringStorage, ClassObject* obj, uint byteOffset)
	{
		TTTrace.Write();
		string s = reader.ReadString(out bool isDefined);

		if (isDefined)
		{
			stringStorage.DecRefCount(*((ulong*)((byte*)obj + byteOffset)));
			ulong handle = stringStorage.AddString(s);
			*((ulong*)((byte*)obj + byteOffset)) = handle;
			stringStorage.SetStringVersion(handle, obj->version);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static ulong ReadIndexedString(ChangesetReader reader, StringStorage stringStorage, ClassObject* obj,
		uint byteOffset, ulong currentAffectedIndexes,
		ReadOnlyArray<int> affectedIndexIndexes, HashIndexDeleteDelegate hashDelegate, ulong objHandle)
	{
		TTTrace.Write();
		string s = reader.ReadString(out bool isDefined);

		if (isDefined)
		{
			currentAffectedIndexes = UpatedAffectedIndexes(obj, objHandle, currentAffectedIndexes, affectedIndexIndexes, hashDelegate);

			stringStorage.DecRefCount(*((ulong*)((byte*)obj + byteOffset)));
			ulong handle = stringStorage.AddString(s);
			*((ulong*)((byte*)obj + byteOffset)) = handle;
			stringStorage.SetStringVersion(handle, obj->version);
		}

		return currentAffectedIndexes;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void SkipString(ChangesetReader reader)
	{
		reader.ReadString(out bool isDefined);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void ReadArray(ChangesetReader reader, BlobStorage blobStorage, ClassObject* obj,
		uint byteOffset, PropertyType propertyType)
	{
		TTTrace.Write();
		ulong currBlob = ReadBlob(reader, blobStorage, propertyType, out bool isDefined);

		if (isDefined)
		{
			ulong prevBlob = *((ulong*)((byte*)obj + byteOffset));
			blobStorage.DecRefCount(prevBlob);

			*((ulong*)((byte*)obj + byteOffset)) = currBlob;
			blobStorage.SetVersion(currBlob, obj->version);
			*((ulong*)((byte*)obj + byteOffset)) = currBlob;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void SkipArray(ChangesetReader reader, PropertyType propertyType)
	{
		SkipBlob(reader, propertyType);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal static void ReadReferenceArray(ChangesetReader reader, BlobStorage blobStorage, ClassObject* obj,
		uint byteOffset, TransactionContext tc, ushort classIndex, int propId, bool trackRefs)
	{
		TTTrace.Write();
		ulong currBlob = ReadBlob(reader, blobStorage, PropertyType.LongArray, out bool isDefined);

		if (isDefined)
		{
			ulong prevBlob = *((ulong*)((byte*)obj + byteOffset));
			CreateRefArrayChange(tc, obj->id, prevBlob, blobStorage, classIndex, propId, trackRefs, InvRefChangeType.Delete);
			blobStorage.DecRefCount(prevBlob);

			*((ulong*)((byte*)obj + byteOffset)) = currBlob;
			CreateRefArrayChange(tc, obj->id, currBlob, blobStorage, classIndex, propId, trackRefs, InvRefChangeType.Insert);
			blobStorage.SetVersion(currBlob, obj->version);
		}
	}

	private static void CreateRefArrayChange(TransactionContext tc, long id, ulong handle, BlobStorage blobStorage,
		ushort classIndex, int propId, bool trackRefs, InvRefChangeType type)
	{
		byte* blob = blobStorage.RetrieveBlob(handle);
		if (blob != null)
		{
			int refCount = *((int*)blob);
			long* refs = (long*)(blob + 4);
			for (int j = 0; j < refCount; j++)
			{
				tc.AddGroupingInvRefChange(classIndex, id, refs[j], propId, trackRefs, (byte)type);
			}
		}
	}

	private static ulong ReadBlob(ChangesetReader reader, BlobStorage blobStorage, PropertyType propType, out bool isDefined)
	{
		TTTrace.Write();

		bool isNull;
		if (propType == PropertyType.StringArray)
		{
			byte* buffer;
			int size = reader.ReadStringArraySize(out isNull, out isDefined);
			if (!isDefined)
				return 0;

			ulong handle = blobStorage.AllocBlob(isNull, size, out buffer);
			if (size > 0)
				reader.ReadBytes(buffer, size);

			return handle;
		}
		else
		{
			byte* buffer;
			int len = reader.ReadLength(out isNull, out isDefined);
			if (!isDefined)
				return 0;

			int size = len * PropertyTypesHelper.GetElementSize(propType);
			ulong handle = blobStorage.AllocBlob(isNull, size + 4, out buffer);
			if (!isNull)
			{
				*((int*)buffer) = len;
				reader.ReadBytes(buffer + 4, size);
			}

			return handle;
		}
	}

	private static void SkipBlob(ChangesetReader reader, PropertyType propType)
	{
		bool isNull;
		if (propType == PropertyType.StringArray)
		{
			int size = reader.ReadStringArraySize(out isNull, out bool isDefined);
			if (!isDefined)
				return;

			if (size > 0)
				reader.SkipBytes(size);
		}
		else
		{
			int len = reader.ReadLength(out isNull, out bool isDefined);
			if (!isDefined)
				return;

			int size = len * PropertyTypesHelper.GetElementSize(propType);
			if (!isNull)
				reader.SkipBytes(size);
		}
	}

	private struct Key : IEquatable<Key>
	{
		short classId;
		ChangesetBlockProperty[] properties;

		public Key(ChangesetBlock block)
		{
			this.classId = block.ClassDescriptor.Id;

			// Skip the id as it is always present as the first property
			this.properties = new ChangesetBlockProperty[block.PropertyCount - 1];
			for (int i = 0; i < block.PropertyCount - 1; i++)
			{
				this.properties[i] = block.Properties[i + 1];
			}
		}

		public short ClassId => classId;

		public bool Equals(Key other)
		{
			if (classId != other.classId)
				return false;

			if (properties == null && other.properties == null)
				return true;

			if (properties == null || other.properties == null)
				return true;

			if (properties.Length != other.properties.Length)
				return true;

			for (int i = 0; i < properties.Length; i++)
			{
				if (properties[i].Index != other.properties[i].Index || properties[i].Type != other.properties[i].Type)
					return false;
			}

			return true;
		}

		public override int GetHashCode()
		{
			int h = classId;
			for (int i = 0; i < properties.Length; i++)
			{
				h = (h * 31 + properties[i].Index) * 31 + (int)properties[i].Type;
			}

			return h;
		}
	}
}
