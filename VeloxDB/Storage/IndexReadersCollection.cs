using System;
using System.Collections.Generic;
using System.Reflection.Emit;
using System.Reflection;
using System.Linq;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage.ModelUpdate;

namespace VeloxDB.Storage;

internal delegate IndexReaderBase IndexReaderCreator(string cultureName, bool caseSensitive, ReadOnlyArray<SortOrder> sortOrders);

internal unsafe sealed class IndexReadersCollection
{
	const string assemblyName = "__IndexReaders";
	static ModuleBuilder moduleBuilder;

	static readonly Type[] hashReaderBaseTypes = new Type[] { typeof(HashIndexReaderBase<>), typeof(HashIndexReaderBase<,>),
		typeof(HashIndexReaderBase<,,>), typeof(HashIndexReaderBase<,,,>) };

	static readonly Type[] sortedReaderBaseTypes = new Type[] { typeof(SortedIndexReaderBase<>), typeof(SortedIndexReaderBase<,>),
		typeof(SortedIndexReaderBase<,,>), typeof(SortedIndexReaderBase<,,,>) };

	static readonly Type[] ctorTypes = new Type[] { typeof(string), typeof(bool), typeof(ReadOnlyArray<SortOrder>) };

	Dictionary<short, IndexReaderCreator> readerFactories;

	static IndexReadersCollection()
	{
		AssemblyName aName = new AssemblyName(assemblyName);
		AssemblyBuilder ab = AssemblyBuilder.DefineDynamicAssembly(aName, AssemblyBuilderAccess.RunAndCollect);
		moduleBuilder = ab.DefineDynamicModule(aName.Name);
	}

	public IndexReadersCollection(IndexReadersCollection prevReaders, DataModelUpdate modelUpdate)
	{
		readerFactories = new Dictionary<short, IndexReaderCreator>(modelUpdate.ModelDesc.GetAllIndexes().Count());
		foreach (IndexDescriptor indexDesc in modelUpdate.ModelDesc.GetAllIndexes())
		{
			if (modelUpdate.PrevModelDesc.GetIndex(indexDesc.Id) != null && !modelUpdate.IndexModified(indexDesc.Id))
			{
				readerFactories.Add(indexDesc.Id, prevReaders.readerFactories[indexDesc.Id]);
			}
			else
			{
				readerFactories.Add(indexDesc.Id, CreateReaderClass(indexDesc));
			}
		}
	}

	public IndexReadersCollection(DataModelDescriptor model)
	{
		readerFactories = new Dictionary<short, IndexReaderCreator>(model.GetAllIndexes().Count());
		foreach (IndexDescriptor indexDesc in model.GetAllIndexes())
		{
			readerFactories.Add(indexDesc.Id, CreateReaderClass(indexDesc));
		}
	}

	public IndexReaderCreator GetReaderFactory(short indexId)
	{
		return readerFactories[indexId];
	}

	private IndexReaderCreator CreateReaderClass(IndexDescriptor indexDesc)
	{
		Type[] propertyTypes = new Type[indexDesc.Properties.Length];
		for (int i = 0; i < indexDesc.Properties.Length; i++)
		{
			propertyTypes[i] = PropertyTypesHelper.PropertyTypeToManagedType(indexDesc.Properties[i].PropertyType);
		}

		Type baseType;
		if (indexDesc.Type == ModelItemType.HashIndex)
			baseType = hashReaderBaseTypes[propertyTypes.Length - 1].MakeGenericType(propertyTypes);
		else
			baseType = sortedReaderBaseTypes[propertyTypes.Length - 1].MakeGenericType(propertyTypes);

		TypeBuilder readerType = moduleBuilder.DefineType("__" + Guid.NewGuid().ToString("N"),
			TypeAttributes.Sealed | TypeAttributes.Class | TypeAttributes.Public, baseType);

		ConstructorBuilder cb = readerType.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, ctorTypes);
		ILGenerator il = cb.GetILGenerator();

		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Ldarg_2);
		il.Emit(OpCodes.Ldarg_3);
		il.Emit(OpCodes.Call, baseType.GetConstructor(ctorTypes));
		il.Emit(OpCodes.Ret);

		// Override PopulateKeyBuffer
		Type[] argTypes = new Type[propertyTypes.Length + 2];
		Array.Copy(propertyTypes, argTypes, propertyTypes.Length);
		argTypes[^2] = typeof(byte*);
		argTypes[^1] = typeof(string[]);

		MethodBuilder methodb = readerType.DefineMethod(HashIndexReaderBase<int>.PopulateMethodName,
			MethodAttributes.Public | MethodAttributes.ReuseSlot | MethodAttributes.Virtual | MethodAttributes.HideBySig,
			typeof(void), argTypes);

		il = methodb.GetILGenerator();

		int offset = 0;
		int stringIndex = 0;
		for (int i = 0; i < propertyTypes.Length; i++)
		{
			il.Emit(OpCodes.Ldarg, propertyTypes.Length + 1); // key buffer
			il.Emit(OpCodes.Ldc_I4, offset);    // Property offset
			il.Emit(OpCodes.Add);   // Location of the value in the key buffer

			int argIndex = i + 1;
			if (propertyTypes[i] == typeof(DateTime))
			{
				GenerateDateTime(il, argIndex);
			}
			else if (propertyTypes[i] == typeof(string))
			{
				GenerateString(il, stringIndex, argIndex, propertyTypes.Length);
				stringIndex++;
			}
			else
			{
				GenerateSimpleType(il, argIndex, PropertyTypesHelper.ManagedTypeToPropertyType(propertyTypes[i]));
			}

			offset += (int)PropertyTypesHelper.GetItemSize(PropertyTypesHelper.ManagedTypeToPropertyType(propertyTypes[i]));
		}

		il.Emit(OpCodes.Ret);

		Type type = readerType.CreateType();
		return GenerateFactory(readerType);
	}

	private static void GenerateSimpleType(ILGenerator il, int argIndex, PropertyType propertyType)
	{
		il.Emit(OpCodes.Ldarg, argIndex);
		il.Emit(GetStoreSimpleTypeInstruction(propertyType));
	}

	private static void GenerateDateTime(ILGenerator il, int argIndex)
	{
		il.Emit(OpCodes.Ldarga, argIndex);
		il.Emit(OpCodes.Call, typeof(DateTime).GetMethod(nameof(DateTime.ToBinary)));
		il.Emit(OpCodes.Stind_I8);
	}

	private static void GenerateString(ILGenerator il, int stringIndex, int argIndex, int propertyCount)
	{
		il.Emit(OpCodes.Ldc_I8, (long)stringIndex);
		il.Emit(OpCodes.Stind_I8);
		il.Emit(OpCodes.Ldarg, propertyCount + 2);
		il.Emit(OpCodes.Ldc_I4, stringIndex);
		il.Emit(OpCodes.Ldarg, argIndex);
		il.Emit(OpCodes.Stelem_Ref);
	}

	private static OpCode GetStoreSimpleTypeInstruction(PropertyType type)
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

			default:
				throw new ArgumentException();
		}
	}

	private static IndexReaderCreator GenerateFactory(TypeBuilder readerType)
	{
		DynamicMethod factMethod = new DynamicMethod("Factory__" + Guid.NewGuid().ToString("N"),
					typeof(IndexReaderBase), ctorTypes, true);

		ILGenerator il = factMethod.GetILGenerator();
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Ldarg_2);
		il.Emit(OpCodes.Newobj, readerType.GetConstructor(ctorTypes));
		il.Emit(OpCodes.Ret);

		return (IndexReaderCreator)factMethod.CreateDelegate(typeof(IndexReaderCreator));
	}

	private struct ComparerKey : IEquatable<ComparerKey>
	{
		int indexId;
		short classId;

		public ComparerKey(int indexId, short classId)
		{
			this.indexId = indexId;
			this.classId = classId;
		}

		public bool Equals(ComparerKey other)
		{
			return indexId == other.indexId && classId == other.classId;
		}

		public override int GetHashCode()
		{
			return (int)(indexId * HashUtils.PrimeMultiplier32 + classId);
		}
	}
}
