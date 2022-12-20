using System;
using System.Collections.Generic;
using System.Reflection.Emit;
using System.Reflection;
using System.Linq;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage.ModelUpdate;

namespace VeloxDB.Storage;

internal unsafe sealed class HashReadersCollection
{
	const string assemblyName = "__HashReaders";
	static ModuleBuilder moduleBuilder;

	static readonly Type[] readerBaseTypes = new Type[] { typeof(HashIndexReaderBase<>), typeof(HashIndexReaderBase<,>),
		typeof(HashIndexReaderBase<,,>), typeof(HashIndexReaderBase<,,,>) };

	Dictionary<short, Func<HashIndexReaderBase>> readerFactories;

	static HashReadersCollection()
	{
		AssemblyName aName = new AssemblyName(assemblyName);
		AssemblyBuilder ab = AssemblyBuilder.DefineDynamicAssembly(aName, AssemblyBuilderAccess.RunAndCollect);
		moduleBuilder = ab.DefineDynamicModule(aName.Name);
	}

	public HashReadersCollection(HashReadersCollection prevReaders, DataModelUpdate modelUpdate)
	{
		readerFactories = new Dictionary<short, Func<HashIndexReaderBase>>(modelUpdate.ModelDesc.GetAllHashIndexes().Count());
		foreach (HashIndexDescriptor hashIndexDesc in modelUpdate.ModelDesc.GetAllHashIndexes())
		{
			if (modelUpdate.PrevModelDesc.GetHashIndex(hashIndexDesc.Id) != null && !modelUpdate.HashIndexModified(hashIndexDesc.Id))
			{
				readerFactories.Add(hashIndexDesc.Id, prevReaders.readerFactories[hashIndexDesc.Id]);
			}
			else
			{
				readerFactories.Add(hashIndexDesc.Id, CreateReaderClass(hashIndexDesc));
			}
		}
	}

	public HashReadersCollection(DataModelDescriptor model)
	{
		readerFactories = new Dictionary<short, Func<HashIndexReaderBase>>(model.GetAllHashIndexes().Count());
		foreach (HashIndexDescriptor hashIndexDesc in model.GetAllHashIndexes())
		{
			readerFactories.Add(hashIndexDesc.Id, CreateReaderClass(hashIndexDesc));
		}
	}

	public Func<HashIndexReaderBase> GetReaderFactory(short hashIndexId)
	{
		return readerFactories[hashIndexId];
	}

	private Func<HashIndexReaderBase> CreateReaderClass(HashIndexDescriptor hashIndexDesc)
	{
		Type[] propertyTypes = new Type[hashIndexDesc.Properties.Length];
		for (int i = 0; i < hashIndexDesc.Properties.Length; i++)
		{
			propertyTypes[i] = PropertyTypesHelper.PropertyTypeToManagedType(hashIndexDesc.Properties[i].PropertyType);
		}

		Type baseType = readerBaseTypes[propertyTypes.Length - 1].MakeGenericType(propertyTypes);

		TypeBuilder readerType = moduleBuilder.DefineType("__" + Guid.NewGuid().ToString("N"),
			TypeAttributes.Sealed | TypeAttributes.Class | TypeAttributes.Public, baseType);

		ConstructorBuilder cb = readerType.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
		ILGenerator il = cb.GetILGenerator();

		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, baseType.GetConstructor(Type.EmptyTypes));
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

	private static Func<HashIndexReaderBase> GenerateFactory(TypeBuilder readerType)
	{
		DynamicMethod factMethod = new DynamicMethod("Factory__" + Guid.NewGuid().ToString("N"),
					typeof(HashIndexReaderBase), Type.EmptyTypes, true);

		ILGenerator il = factMethod.GetILGenerator();
		il.Emit(OpCodes.Newobj, readerType.GetConstructor(Type.EmptyTypes));
		il.Emit(OpCodes.Ret);

		return (Func<HashIndexReaderBase>)factMethod.CreateDelegate(typeof(Func<HashIndexReaderBase>));
	}

	private struct ComparerKey : IEquatable<ComparerKey>
	{
		int hashIndexId;
		short classId;

		public ComparerKey(int hashIndexId, short classId)
		{
			this.hashIndexId = hashIndexId;
			this.classId = classId;
		}

		public bool Equals(ComparerKey other)
		{
			return hashIndexId == other.hashIndexId && classId == other.classId;
		}

		public override int GetHashCode()
		{
			return (int)(hashIndexId * HashUtils.PrimeMultiplier32 + classId);
		}
	}
}
