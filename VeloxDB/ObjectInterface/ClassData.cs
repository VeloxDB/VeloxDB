using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using System.Reflection.Metadata;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage;

namespace VeloxDB.ObjectInterface;

internal unsafe delegate DatabaseObject DatabaseObjectCreatorDelegate(ObjectModel owner,
	ClassData classData, byte* buffer, DatabaseObjectState state, ChangeList changeList);

internal delegate void InverseReferenceInvalidator(DatabaseObject obj);

internal sealed class ClassData
{
	static MethodInfo dbObjBegInitMethod = typeof(DatabaseObject).GetMethod(nameof(DatabaseObject.BeginInit), BindingFlags.NonPublic | BindingFlags.Instance);
	static MethodInfo dbObjEndInitMethod = typeof(DatabaseObject).GetMethod(nameof(DatabaseObject.EndInit), BindingFlags.NonPublic | BindingFlags.Instance);
	static MethodInfo verifyAccessMethod = typeof(DatabaseObject).GetMethod(DatabaseObject.VerifyAccessMethodName, BindingFlags.Instance | BindingFlags.NonPublic);
	static MethodInfo verifyReferencingMethod = typeof(DatabaseObject).GetMethod(DatabaseObject.VerifyReferencingMethodName, BindingFlags.Static | BindingFlags.NonPublic);
	static MethodInfo objModifiedMethod = typeof(ObjectModel).GetMethod(nameof(ObjectModel.ObjectModified), BindingFlags.Instance | BindingFlags.NonPublic);
	static MethodInfo startInsertBlockMethod = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.StartInsertBlockUnsafe), BindingFlags.Instance | BindingFlags.Public);
	static MethodInfo startUpdateBlockMethod = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.StartUpdateBlockUnsafe), BindingFlags.Instance | BindingFlags.Public);
	static MethodInfo addPropertyMethod = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.AddPropertyUnsafe), BindingFlags.Instance | BindingFlags.Public);
	static MethodInfo propertiesDefinedMethod = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.PropertiesDefined), BindingFlags.Instance | BindingFlags.Public);
	static MethodInfo prevVersionPlaceholderMethod = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.CreatePreviousVersionPlaceholder),
		BindingFlags.Instance | BindingFlags.Public);

	static MethodInfo lastValueWrittenMethod = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.LastValueWritten), BindingFlags.Instance | BindingFlags.Public);
	static MethodInfo writeIdMethod = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteLong), BindingFlags.Instance | BindingFlags.Public);
	static MethodInfo idPropertyMethod = typeof(DatabaseObject).GetProperty(nameof(DatabaseObject.Id), BindingFlags.Instance | BindingFlags.Public).GetGetMethod();
	static MethodInfo classDescMethod = typeof(ClassData).GetProperty(nameof(ClassDesc), BindingFlags.Instance | BindingFlags.Public).GetGetMethod();
	static FieldInfo bufferField = typeof(DatabaseObject).GetField(DatabaseObject.BufferFieldName, BindingFlags.NonPublic | BindingFlags.Instance);

	static FieldInfo ownerField = typeof(DatabaseObject).GetField(DatabaseObject.OwnerFieldName, BindingFlags.NonPublic | BindingFlags.Instance);
	static FieldInfo classDataField = typeof(DatabaseObject).GetField(DatabaseObject.ClassDataFieldName, BindingFlags.NonPublic | BindingFlags.Instance);
	static MethodInfo[] addValueMethods;
	static MethodInfo getStringMethod = typeof(ObjectModel).GetMethod(nameof(ObjectModel.GetString), BindingFlags.Instance | BindingFlags.NonPublic);
	static MethodInfo storeStringMethod = typeof(ObjectModel).GetMethod(nameof(ObjectModel.StoreString), BindingFlags.Instance | BindingFlags.NonPublic);
	static MethodInfo getArrayMethod = typeof(ObjectModel).GetMethod(nameof(ObjectModel.GetArray), BindingFlags.Instance | BindingFlags.NonPublic);
	static ConstructorInfo[] arrayCtors;
	static ConstructorInfo arrayNotifierDelegateCtor = typeof(Action<DatabaseObject>).GetConstructor(new Type[] { typeof(object), typeof(IntPtr) });
	static MethodInfo takeArrayOwnershipMethod = typeof(DatabaseObject).GetMethod(nameof(DatabaseObject.TakeArrayOwnersip), BindingFlags.Instance | BindingFlags.NonPublic);
	static MethodInfo writeArrayMethod = typeof(DatabaseArray).GetMethod(nameof(DatabaseArray.WriteToChangesetWriter), BindingFlags.Static | BindingFlags.NonPublic);
	static MethodInfo refreshArrayMethod = typeof(DatabaseArray).GetMethod(nameof(DatabaseArray.Refresh), BindingFlags.Static | BindingFlags.NonPublic);
	static MethodInfo getObjectMethod = typeof(ObjectModel).GetMethod(nameof(ObjectModel.GetObject), BindingFlags.Instance | BindingFlags.NonPublic);
	static MethodInfo invalidateInvRefMethod = typeof(ObjectModel).GetMethod(nameof(ObjectModel.InvalidateInverseReference), BindingFlags.Instance | BindingFlags.NonPublic);
	static MethodInfo invalidateInvRefsMethod = typeof(ObjectModel).GetMethod(nameof(ObjectModel.InvalidateInverseReferences), BindingFlags.Instance | BindingFlags.NonPublic);
	static MethodInfo invalidateInvRefsHandleMethod = typeof(ObjectModel).GetMethod(nameof(ObjectModel.InvalidateInverseReferencesFromHandle), BindingFlags.Instance | BindingFlags.NonPublic);
	static MethodInfo getObjectIdMethod = typeof(DatabaseObject).GetMethod(nameof(DatabaseObject.GetId), BindingFlags.Static | BindingFlags.NonPublic);
	static MethodInfo getIdMethod = typeof(DatabaseObject).GetProperty(nameof(DatabaseObject.Id), BindingFlags.Instance | BindingFlags.Public).GetGetMethod();
	static MethodInfo isInsertedOrModifiedMethod = typeof(DatabaseObject).GetProperty(nameof(DatabaseObject.IsInsertedOrModified), BindingFlags.Instance | BindingFlags.NonPublic).GetGetMethod(true);
	static MethodInfo takeRefArrayOwnershipMethod = typeof(DatabaseObject).GetMethod(nameof(DatabaseObject.TakeReferenceArrayOwnersip), BindingFlags.Instance | BindingFlags.NonPublic);
	static MethodInfo writeRefArrayMethod = typeof(ReferenceArray).GetMethod(nameof(ReferenceArray.WriteToChangesetWriter), BindingFlags.Static | BindingFlags.NonPublic);
	static MethodInfo refreshRefArrayMethod = typeof(ReferenceArray).GetMethod(nameof(ReferenceArray.Refresh), BindingFlags.Static | BindingFlags.NonPublic);
	static MethodInfo invalidateInvRefArrayMethod = typeof(InverseReferenceSet).GetMethod(nameof(InverseReferenceSet.Invalidate), BindingFlags.Instance | BindingFlags.NonPublic);
	static MethodInfo referenceModifiedMethod = typeof(ObjectModel).GetMethod(nameof(ObjectModel.ReferenceModified), BindingFlags.Instance | BindingFlags.NonPublic);
	static MethodInfo referenceArrayModifiedMethod = typeof(ObjectModel).GetMethod(nameof(ObjectModel.ReferenceArrayModified), BindingFlags.Instance | BindingFlags.NonPublic);
	static MethodInfo stringEqualsMethod = typeof(string).GetMethod(nameof(string.Equals), BindingFlags.Static | BindingFlags.Public, null,
		new Type[] { typeof(string), typeof(string), typeof(StringComparison) }, null);
	static MethodInfo dateTimeEqualsMethod = typeof(DateTime).GetMethod(nameof(DateTime.Equals), BindingFlags.Static | BindingFlags.Public, null,
		new Type[] { typeof(DateTime), typeof(DateTime) }, null);
	static MethodInfo setContainsIdMethod = typeof(LongHashSet).GetMethod(nameof(LongHashSet.Contains), BindingFlags.Instance | BindingFlags.Public);
	static MethodInfo getSetToNullReferenceMethod = typeof(ObjectModel).GetMethod(nameof(ObjectModel.GetSetToNullReference), BindingFlags.Instance | BindingFlags.NonPublic);

	ClassDescriptor classDesc;
	DatabaseObjectCreatorDelegate creator;
	MemoryManager.StaticFixedAccessor memoryManager;
	int bufferSize;
	int bitFieldByteSize;
	Type generatedType;

	Dictionary<int, InverseReferenceInvalidator> inverseRefInvalidators;

	static ClassData()
	{
		addValueMethods = new MethodInfo[Common.Utils.MaxEnumValue(typeof(PropertyType)) + 1];
		addValueMethods[(int)PropertyType.Byte] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteByte), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.Short] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteShort), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.Int] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteInt), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.Long] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteLong), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.Float] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteFloat), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.Double] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteDouble), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.Bool] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteBool), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.DateTime] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteLong), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.String] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteString), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.ByteArray] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteByteArray), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.ShortArray] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteShortArray), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.IntArray] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteIntArray), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.LongArray] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteLongArray), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.FloatArray] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteFloatArray), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.DoubleArray] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteDoubleArray), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.BoolArray] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteBoolArray), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.DateTimeArray] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteDateTimeArray), BindingFlags.Instance | BindingFlags.Public);
		addValueMethods[(int)PropertyType.StringArray] = typeof(ChangesetWriter).GetMethod(nameof(ChangesetWriter.WriteStringArray), BindingFlags.Instance | BindingFlags.Public);

		Type[] ctorTypes = new Type[] { typeof(DatabaseObject), typeof(Action<DatabaseObject>), typeof(byte*) };
		arrayCtors = new ConstructorInfo[Common.Utils.MaxEnumValue(typeof(PropertyType)) + 1];
		arrayCtors[(int)PropertyType.ByteArray] = typeof(ByteDatabaseArray).GetConstructor(ctorTypes);
		arrayCtors[(int)PropertyType.ShortArray] = typeof(ShortDatabaseArray).GetConstructor(ctorTypes);
		arrayCtors[(int)PropertyType.IntArray] = typeof(IntDatabaseArray).GetConstructor(ctorTypes);
		arrayCtors[(int)PropertyType.LongArray] = typeof(LongDatabaseArray).GetConstructor(ctorTypes);
		arrayCtors[(int)PropertyType.FloatArray] = typeof(FloatDatabaseArray).GetConstructor(ctorTypes);
		arrayCtors[(int)PropertyType.DoubleArray] = typeof(DoubleDatabaseArray).GetConstructor(ctorTypes);
		arrayCtors[(int)PropertyType.BoolArray] = typeof(BoolDatabaseArray).GetConstructor(ctorTypes);
		arrayCtors[(int)PropertyType.DateTimeArray] = typeof(DateTimeDatabaseArray).GetConstructor(ctorTypes);
		arrayCtors[(int)PropertyType.StringArray] = typeof(StringDatabaseArray).GetConstructor(ctorTypes);
	}

	private ClassData(ClassDescriptor classDesc)
	{
		this.classDesc = classDesc;
	}

	private ClassData(ClassDescriptor classDesc, DatabaseObjectCreatorDelegate creator, Type generatedType,
		Dictionary<int, InverseReferenceInvalidator> inverseRefInvalidators)
	{
		this.classDesc = classDesc;
		this.creator = creator;
		this.generatedType = generatedType;

		bitFieldByteSize = CalculateBitFiledSize(classDesc);

		bufferSize = bitFieldByteSize + sizeof(long);  // Id
		for (int i = 2; i < classDesc.Properties.Length; i++)   // Skip version
		{
			PropertyDescriptor pd = classDesc.Properties[i];
			bufferSize += (int)PropertyTypesHelper.GetItemSize(pd.PropertyType);
		}

		this.memoryManager = Common.MemoryManager.RegisterStaticFixedConsumer(bufferSize);

		this.inverseRefInvalidators = inverseRefInvalidators;
	}

	public ClassDescriptor ClassDesc => classDesc;
	public DatabaseObjectCreatorDelegate Creator => creator;
	public MemoryManager.StaticFixedAccessor MemoryManager => memoryManager;
	public int BitFieldByteSize => bitFieldByteSize;
	public int ObjectSize => bufferSize - bitFieldByteSize;
	public Type GeneratedType => generatedType;
	public int BufferSize => bufferSize;

	public InverseReferenceInvalidator GetInverseReferenceInvalidator(int propertyId)
	{
		if (inverseRefInvalidators == null)
			return null;

		inverseRefInvalidators.TryGetValue(propertyId, out var invalidator);
		return invalidator;
	}

	public static ReferenceCheckerDelegate CreateReferencePropertyDelegate(ClassData cd, ReferencePropertyDescriptor propDesc)
	{
		DynamicMethod checker = new DynamicMethod("__" + Guid.NewGuid(),
			typeof(bool), new Type[] { typeof(DatabaseObject), typeof(LongHashSet) });
		ILGenerator il = checker.GetILGenerator();

		MethodInfo getter = GetPublicProperty(cd.ClassDesc.ObjectModelClass.ClassType, propDesc.Name).GetGetMethod();

		if (propDesc.Multiplicity == Multiplicity.Many)
		{
			MethodInfo mi = getter.ReturnType.GetMethod(nameof(ReferenceArray<DatabaseObject>.ContainsAnyId),
				BindingFlags.Instance | BindingFlags.NonPublic);

			Label lab = il.DefineLabel();
			var loc = il.DeclareLocal(getter.ReturnType);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Callvirt, getter);
			il.Emit(OpCodes.Stloc, loc);
			il.Emit(OpCodes.Ldloc, loc);
			il.Emit(OpCodes.Ldnull);
			il.Emit(OpCodes.Cgt_Un);
			il.Emit(OpCodes.Brfalse, lab);
			il.Emit(OpCodes.Ldloc, loc);
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Call, mi);
			il.Emit(OpCodes.Ret);
			il.MarkLabel(lab);
			il.Emit(OpCodes.Ldc_I4_0);
			il.Emit(OpCodes.Ret);
		}
		else
		{
			Label lab = il.DefineLabel();
			il.Emit(OpCodes.Ldarg_1);

			// Read object id from the internal buffer and obtain it from ObjectModel
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, bufferField);
			il.Emit(OpCodes.Ldc_I4, GetByteOffset(cd.ClassDesc, propDesc));
			il.Emit(OpCodes.Add);
			il.Emit(OpCodes.Ldind_I8);

			il.Emit(OpCodes.Call, setContainsIdMethod);
			il.Emit(OpCodes.Ret);
		}

		return (ReferenceCheckerDelegate)checker.CreateDelegate(typeof(ReferenceCheckerDelegate));
	}

	public static Delegate CreateHashIndexComparer(HashIndexDescriptor hashIndexDesc)
	{
		Type definingType = hashIndexDesc.DefiningObjectModelClass.ClassType;

		List<Type> args = new List<Type>(5);
		args.Add(definingType);

		for (int i = 0; i < hashIndexDesc.Properties.Length; i++)
		{
			PropertyDescriptor pd = hashIndexDesc.Properties[i];
			args.Add(PropertyTypesHelper.PropertyTypeToManagedType(pd.PropertyType));
		}

		DynamicMethod comparer = new DynamicMethod("__" + Guid.NewGuid(), typeof(bool), args.ToArray());
		ILGenerator il = comparer.GetILGenerator();

		for (int i = 0; i < hashIndexDesc.Properties.Length; i++)
		{
			PropertyDescriptor pd = hashIndexDesc.Properties[i];
			MethodInfo getter = GetPublicProperty(definingType, pd.Name).GetGetMethod();

			Label lab = il.DefineLabel();
			if (pd.PropertyType == PropertyType.String)
			{
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Callvirt, getter);
				il.Emit(OpCodes.Ldarg, i + 1);
				il.Emit(OpCodes.Ldc_I4, (int)StringComparison.Ordinal);
				il.Emit(OpCodes.Call, stringEqualsMethod);
				il.Emit(OpCodes.Brtrue, lab);
				il.Emit(OpCodes.Ldc_I4_0);
				il.Emit(OpCodes.Ret);
				il.MarkLabel(lab);
			}
			else if (pd.PropertyType == PropertyType.DateTime)
			{
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Callvirt, getter);
				il.Emit(OpCodes.Ldarg, i + 1);
				il.Emit(OpCodes.Call, dateTimeEqualsMethod);
				il.Emit(OpCodes.Brtrue, lab);
				il.Emit(OpCodes.Ldc_I4_0);
				il.Emit(OpCodes.Ret);
				il.MarkLabel(lab);
			}
			else
			{
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Callvirt, getter);
				il.Emit(OpCodes.Ldarg, i + 1);
				il.Emit(OpCodes.Ceq);
				il.Emit(OpCodes.Brtrue, lab);
				il.Emit(OpCodes.Ldc_I4_0);
				il.Emit(OpCodes.Ret);
				il.MarkLabel(lab);
			}
		}

		il.Emit(OpCodes.Ldc_I4_1);
		il.Emit(OpCodes.Ret);

		Type delegateType;
		if (hashIndexDesc.Properties.Length == 1)
		{
			delegateType = typeof(Func<,,>).MakeGenericType(args[0], args[1], typeof(bool));
		}
		else if (hashIndexDesc.Properties.Length == 2)
		{
			delegateType = typeof(Func<,,,>).MakeGenericType(args[0], args[1], args[2], typeof(bool));
		}
		else if (hashIndexDesc.Properties.Length == 3)
		{
			delegateType = typeof(Func<,,,,>).MakeGenericType(args[0], args[1], args[2], args[3], typeof(bool));
		}
		else
		{
			delegateType = typeof(Func<,,,,,>).MakeGenericType(args[0], args[1], args[2], args[3], args[4], typeof(bool));
		}

		return comparer.CreateDelegate(delegateType);
	}

	public static ClassData CreateClassData(ModuleBuilder moduleBuilder, ClassDescriptor classDesc)
	{
		if (classDesc.IsAbstract)
			return new ClassData(classDesc);

		string guid = Guid.NewGuid().ToString("N");
		TypeBuilder tb = moduleBuilder.DefineType("__" + guid,
			TypeAttributes.Class | TypeAttributes.NotPublic, classDesc.ObjectModelClass.ClassType);

		MethodBuilder onDeleteMethod = tb.DefineMethod(nameof(DatabaseObject.InvalidateInverseReferences), MethodAttributes.Public |
			MethodAttributes.Virtual | MethodAttributes.HideBySig, typeof(void), new Type[] { });
		ILGenerator onDeleteIl = onDeleteMethod.GetILGenerator();

		Dictionary<int, FieldBuilder> arrayFields = null;
		Dictionary<FieldBuilder, MethodBuilder> arrayModifiedNotifiers = null;
		Dictionary<int, int> arrayOffsets = null;

		if (classDesc.BlobPropertyIndexes.Length > 0)
		{
			arrayFields = new Dictionary<int, FieldBuilder>(2);
			arrayModifiedNotifiers = new Dictionary<FieldBuilder, MethodBuilder>();
			arrayOffsets = new Dictionary<int, int>(classDesc.BlobPropertyIndexes.Length);
		}

		int offset = sizeof(long);
		for (int i = 2; i < classDesc.Properties.Length; i++)   // Skip Id and Version
		{
			PropertyDescriptor pd = classDesc.Properties[i];
			ObjectModelProperty op = FindObjectModelProperty(classDesc, pd.Name);
			CreateClassProperty(classDesc, tb, classDesc.Properties[i], op, offset, i, arrayFields, arrayModifiedNotifiers);

			if (pd.Kind == PropertyKind.Reference)
				CreateINvalidateInverseReferences(tb, onDeleteIl, (ReferencePropertyDescriptor)classDesc.Properties[i], offset, arrayFields);

			if (pd.Kind == PropertyKind.Array ||
				(pd.Kind == PropertyKind.Reference && (pd as ReferencePropertyDescriptor).Multiplicity == Multiplicity.Many))
			{
				arrayOffsets.Add(pd.Id, offset);
			}

			offset += (int)PropertyTypesHelper.GetItemSize(pd.PropertyType);
		}

		onDeleteIl.Emit(OpCodes.Ret);

		List<Tuple<FieldBuilder, ModifyDirectReferenceDelegate>> refModifiers = new List<Tuple<FieldBuilder, ModifyDirectReferenceDelegate>>();
		CreateInverseReferenceProperties(classDesc, tb, refModifiers, out Dictionary<int, MethodInfo> inverseRefInvalidators);

		CreateStaticConstructor(tb, arrayModifiedNotifiers);

		Type[] ctorTypes = new Type[] { typeof(ObjectModel), typeof(ClassData),
			typeof(byte*), typeof(DatabaseObjectState), typeof(ChangeList) };
		ConstructorInfo ci = CreateClassConstructor(tb, classDesc, ctorTypes);

		CreateInsertMethod(classDesc, tb, arrayFields);
		CreateUpdateMethod(classDesc, tb, arrayFields);

		CreateOnRefreshMethod(classDesc, tb, arrayFields, arrayOffsets);

		Type generatedType = tb.CreateType();
		foreach (Tuple<FieldBuilder, ModifyDirectReferenceDelegate> item in refModifiers)
		{
			FieldInfo fi = generatedType.GetField(item.Item1.Name, BindingFlags.Static | BindingFlags.NonPublic);
			fi.SetValue(null, item.Item2);
		}

		DynamicMethod creator = new DynamicMethod("__create_" + guid, typeof(DatabaseObject), ctorTypes, tb.Module);
		ILGenerator il = creator.GetILGenerator();
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Ldarg_2);
		il.Emit(OpCodes.Ldarg_3);
		il.Emit(OpCodes.Ldarg, 4);
		il.Emit(OpCodes.Newobj, generatedType.GetConstructor(ctorTypes));
		il.Emit(OpCodes.Ret);

		DatabaseObjectCreatorDelegate creatorDelegate =
			(DatabaseObjectCreatorDelegate)creator.CreateDelegate(typeof(DatabaseObjectCreatorDelegate));

		return new ClassData(classDesc, creatorDelegate, generatedType, CreateDelegates(generatedType, inverseRefInvalidators));
	}

	private static Dictionary<int, InverseReferenceInvalidator> CreateDelegates(Type generatedType, Dictionary<int, MethodInfo> d)
	{
		if (d == null)
			return null;

		Dictionary<int, InverseReferenceInvalidator> r = new Dictionary<int, InverseReferenceInvalidator>(d.Count);
		foreach (var kv in d)
		{
			MethodInfo mi = generatedType.GetMethod(kv.Value.Name, BindingFlags.Static | BindingFlags.NonPublic);
			r.Add(kv.Key, (InverseReferenceInvalidator)Delegate.CreateDelegate(typeof(InverseReferenceInvalidator), mi));
		}

		return r;
	}

	private static void CreateInverseReferenceProperties(ClassDescriptor classDesc, TypeBuilder typeBuilder,
		List<Tuple<FieldBuilder, ModifyDirectReferenceDelegate>> refModifiers,
		out Dictionary<int, MethodInfo> inverseRefInvalidators)
	{
		inverseRefInvalidators = null;
		ObjectModelClass omc = classDesc.ObjectModelClass;
		for (int i = 0; i < omc.InverseReferences.Count; i++)
		{
			ObjectModelInverseReferenceProperty irp = omc.InverseReferences[i];
			ClassDescriptor cd = classDesc.Model.GetClass(irp.ReferencingClass.FullName);
			if (cd == null)
			{
				Throw.InverseReferencePropertyTargetsUnknownClass(irp.PropertyInfo.DeclaringType.FullName, irp.PropertyInfo.Name,
																  irp.ReferencingClass.FullName);
			}

			ReferencePropertyDescriptor p = cd.GetProperty(irp.TargetPropertyName) as ReferencePropertyDescriptor;
			if (p == null)
			{
				Throw.InverseReferencePropertyTargetsUnknownProperty(irp.PropertyInfo.DeclaringType.FullName, irp.PropertyInfo.Name,
																	 irp.TargetPropertyName);
			}

			if (!p.TrackInverseReferences)
			{
				Throw.InverseReferencePropertyTargetsUntrackedProperty(irp.PropertyInfo.DeclaringType.FullName, irp.PropertyInfo.Name,
																	 irp.TargetPropertyName);
			}

			if (p.OwnerClass.Id != cd.Id)
			{
				Throw.InverseReferencePropertyTargetsInvalidClass(irp.PropertyInfo.DeclaringType.FullName, irp.PropertyInfo.Name,
																  irp.ReferencingClass.FullName);
			}

			CreateInverseReferenceProperty(irp, p, typeBuilder, refModifiers, out MethodInfo invalidator);

			if (invalidator != null)
			{
				inverseRefInvalidators ??= new Dictionary<int, MethodInfo>(omc.InverseReferences.Count);
				inverseRefInvalidators.Add(p.Id, invalidator);
			}
		}
	}

	private static void CreateInverseReferenceProperty(ObjectModelInverseReferenceProperty invRefObjProp,
		ReferencePropertyDescriptor propDesc, TypeBuilder typeBuilder, List<Tuple<FieldBuilder, ModifyDirectReferenceDelegate>> refModifiers,
		out MethodInfo invalidator)
	{
		PropertyBuilder pb = typeBuilder.DefineProperty(invRefObjProp.PropertyInfo.Name,
			PropertyAttributes.None, invRefObjProp.PropertyInfo.PropertyType, null);

		MethodBuilder getter = typeBuilder.DefineMethod(invRefObjProp.PropertyInfo.GetGetMethod().Name,
			MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig, invRefObjProp.PropertyInfo.PropertyType, null);

		pb.SetGetMethod(getter);

		FieldBuilder modifyActionField = CreateReferenceModifyDelegate(typeBuilder, invRefObjProp,
			propDesc, out ModifyDirectReferenceDelegate refModifyDel);
		refModifiers.Add(new Tuple<FieldBuilder, ModifyDirectReferenceDelegate>(modifyActionField, refModifyDel));

		ILGenerator il = getter.GetILGenerator();

		// Define field for holding inverse reference array collection
		FieldBuilder arrayField = typeBuilder.DefineField(Guid.NewGuid().ToString("N"),
			typeof(InverseReferenceSet), FieldAttributes.Private);

		// Verify object is not deleted
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, verifyAccessMethod);

		// Check if there is already a collection inside the local field
		Label lab = il.DefineLabel();
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, arrayField);
		il.Emit(OpCodes.Ldnull);
		il.Emit(OpCodes.Ceq);
		il.Emit(OpCodes.Brtrue, lab);

		// collection != null, just return it
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, arrayField);
		il.Emit(OpCodes.Ret);

		il.MarkLabel(lab);

		// collection == null

		// create new collection

		// Since we will store the created array in the array field we need to load the database object (this) onto the stack
		il.Emit(OpCodes.Ldarg_0);

		// Prepare arguments of the array constructor
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldc_I4, propDesc.Id);
		il.Emit(OpCodes.Ldsfld, modifyActionField);

		ConstructorInfo ctor = invRefObjProp.PropertyInfo.PropertyType.GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null,
			new Type[] { typeof(DatabaseObject), typeof(int), typeof(ModifyDirectReferenceDelegate) }, null);
		il.Emit(OpCodes.Newobj, ctor);

		// Store the array into the field  (remember, we already have the this pointer on the stack)
		il.Emit(OpCodes.Stfld, arrayField);

		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, arrayField);
		il.Emit(OpCodes.Ret);


		// Define inverse reference invalidator
		invalidator = typeBuilder.DefineMethod(Guid.NewGuid().ToString("N"),
			MethodAttributes.Private | MethodAttributes.Static, typeof(void), new Type[] { typeof(DatabaseObject) });

		il = ((MethodBuilder)invalidator).GetILGenerator();
		Label skipInvalidateLabel = il.DefineLabel();

		// If set is null skip invalidation
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, arrayField);
		il.Emit(OpCodes.Ldnull);
		il.Emit(OpCodes.Ceq);
		il.Emit(OpCodes.Brtrue, skipInvalidateLabel);

		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, arrayField);
		il.Emit(OpCodes.Callvirt, invalidateInvRefArrayMethod);

		il.MarkLabel(skipInvalidateLabel);
		il.Emit(OpCodes.Ret);
	}

	private static FieldBuilder CreateReferenceModifyDelegate(TypeBuilder typeBuilder,
		ObjectModelInverseReferenceProperty invRefObjProp, ReferencePropertyDescriptor propDesc,
		out ModifyDirectReferenceDelegate refModifyDel)
	{
		FieldBuilder fi = typeBuilder.DefineField(Guid.NewGuid().ToString("N"),
					typeof(ModifyDirectReferenceDelegate), FieldAttributes.Private | FieldAttributes.Static);

		DynamicMethod m = new DynamicMethod(Guid.NewGuid().ToString("N"),
			typeof(void), new Type[] { typeof(DatabaseObject), typeof(DatabaseObject), typeof(bool) });

		ILGenerator il = m.GetILGenerator();

		ObjectModelProperty objProperty = propDesc.OwnerClass.ObjectModelClass.GetProperty(propDesc.Name);
		if (propDesc.Multiplicity == Multiplicity.Many)
		{
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Callvirt, objProperty.PropertyInfo.GetGetMethod());

			MethodInfo mi = objProperty.PropertyInfo.PropertyType.GetMethod(nameof(ReferenceArray<DatabaseObject>.AddOrRemove),
				BindingFlags.NonPublic | BindingFlags.Instance);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldarg_2);
			il.Emit(OpCodes.Call, mi);
		}
		else
		{
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Callvirt, objProperty.PropertyInfo.GetSetMethod());
		}

		il.Emit(OpCodes.Ret);

		refModifyDel = (ModifyDirectReferenceDelegate)m.CreateDelegate(typeof(ModifyDirectReferenceDelegate));
		return fi;
	}

	private static void CreateOnRefreshMethod(ClassDescriptor classDesc, TypeBuilder typeBuilder,
		Dictionary<int, FieldBuilder> arrayFields, Dictionary<int, int> arrayOffsets)
	{
		if (arrayFields == null || arrayFields.Count == 0)
			return;

		MethodBuilder m = typeBuilder.DefineMethod(DatabaseObject.OnRefreshName, MethodAttributes.Public |
			MethodAttributes.Virtual | MethodAttributes.HideBySig | MethodAttributes.ReuseSlot, typeof(void), null);

		ILGenerator il = m.GetILGenerator();

		foreach (var kv in arrayFields)
		{
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, kv.Value);

			// Load ObjectModel and array index onto the stack
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, ownerField);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, bufferField);
			il.Emit(OpCodes.Ldc_I4, arrayOffsets[kv.Key]);
			il.Emit(OpCodes.Add);
			il.Emit(OpCodes.Ldind_I8);

			// Get the array buffer from the engine
			il.Emit(OpCodes.Call, getArrayMethod);

			PropertyDescriptor pd = classDesc.GetProperty(kv.Key);

			// Reset the array with new buffer
			il.Emit(OpCodes.Call, pd.Kind == PropertyKind.Reference ? refreshRefArrayMethod : refreshArrayMethod);
		}

		il.Emit(OpCodes.Ret);
	}

	private static void CreateUpdateMethod(ClassDescriptor classDesc, TypeBuilder typeBuilder, Dictionary<int, FieldBuilder> arrayFields)
	{
		MethodBuilder mb = typeBuilder.DefineMethod(DatabaseObject.CreateUpdateBlockName, MethodAttributes.Public |
			MethodAttributes.Virtual | MethodAttributes.HideBySig, typeof(void), new Type[] { typeof(ChangesetWriter) });
		ILGenerator il = mb.GetILGenerator();

		// Start update block
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, classDataField);
		il.Emit(OpCodes.Call, classDescMethod);
		il.Emit(OpCodes.Call, startUpdateBlockMethod);

		// Add properties
		for (int i = 2; i < classDesc.Properties.Length; i++)
		{
			// if bit set
			Label lab = il.DefineLabel();
			int fieldIndex = (i - 2) / 8;
			int bitIndex = (i - 2) % 8;
			int mask = (byte)(1 << bitIndex);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, bufferField);
			il.Emit(OpCodes.Ldc_I4, CalculateBitFiledSize(classDesc) - fieldIndex);
			il.Emit(OpCodes.Sub);
			il.Emit(OpCodes.Ldind_U1);
			il.Emit(OpCodes.Ldc_I4, mask);
			il.Emit(OpCodes.And);
			il.Emit(OpCodes.Brfalse, lab);

			// taken if, add property
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Ldc_I4, i);
			il.Emit(OpCodes.Call, addPropertyMethod);

			// skipped if
			il.MarkLabel(lab);
		}

		// Signal that all properties are defined
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Call, propertiesDefinedMethod);

		// Create placeholder for operation header
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Call, prevVersionPlaceholderMethod);

		// Add id value
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, idPropertyMethod);
		il.Emit(OpCodes.Call, addValueMethods[(int)PropertyType.Long]);

		int byteOffset = sizeof(long);

		// Add property values
		for (int i = 2; i < classDesc.Properties.Length; i++)
		{
			PropertyDescriptor pd = classDesc.Properties[i];

			// if bit set
			Label lab = il.DefineLabel();
			int fieldIndex = (i - 2) / 8;
			int bitIndex = (i - 2) % 8;
			int mask = (byte)(1 << bitIndex);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, bufferField);
			il.Emit(OpCodes.Ldc_I4, CalculateBitFiledSize(classDesc) - fieldIndex);
			il.Emit(OpCodes.Sub);
			il.Emit(OpCodes.Ldind_U1);
			il.Emit(OpCodes.Ldc_I4, mask);
			il.Emit(OpCodes.And);
			il.Emit(OpCodes.Brfalse, lab);

			// taken if, add value

			// Load changeset writer into stack
			il.Emit(OpCodes.Ldarg_1);

			if (pd.Kind == PropertyKind.Array ||
				(pd.Kind == PropertyKind.Reference && (pd as ReferencePropertyDescriptor).Multiplicity == Multiplicity.Many))
			{
				// Read array from the field
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Ldfld, arrayFields[pd.Id]);

				// Call static array writer
				il.Emit(OpCodes.Call, pd.Kind == PropertyKind.Reference ? writeRefArrayMethod : writeArrayMethod);
			}
			else
			{
				// If this is string we need to call ObjectModel.GetString so load ObjectModel
				if (pd.PropertyType == PropertyType.String)
				{
					il.Emit(OpCodes.Ldarg_0);
					il.Emit(OpCodes.Ldfld, ownerField);
				}
				else if (pd.Kind == PropertyKind.Reference &&
					(pd as ReferencePropertyDescriptor).DeleteTargetAction == DeleteTargetAction.SetToNull)
				{
					// If SetToNull reference we need to call GetSetToNullReference method on the object
					// model so that the reference gets filtered by the deleted set (load ObjectModel)
					il.Emit(OpCodes.Ldarg_0);
					il.Emit(OpCodes.Ldfld, ownerField);
				}

				// Read field value
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Ldfld, bufferField);
				il.Emit(OpCodes.Ldc_I4, byteOffset);
				il.Emit(OpCodes.Add);
				il.Emit(GetLoadSimpleTypeInstruction(pd.PropertyType));

				if (pd.PropertyType == PropertyType.String)
				{
					il.Emit(OpCodes.Ldc_I4_1);  // Modified=true
					il.Emit(OpCodes.Call, getStringMethod);
				}
				else if (pd.Kind == PropertyKind.Reference && (pd as ReferencePropertyDescriptor).DeleteTargetAction == DeleteTargetAction.SetToNull)
				{
					il.Emit(OpCodes.Call, getSetToNullReferenceMethod);
				}

				// Write value into changeset
				il.Emit(OpCodes.Call, addValueMethods[(int)pd.PropertyType]);
			}

			// skipped if
			il.MarkLabel(lab);

			byteOffset += (int)PropertyTypesHelper.GetItemSize(pd.PropertyType);
		}

		// Signal last value written
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Call, lastValueWrittenMethod);

		il.Emit(OpCodes.Ret);
	}

	private static void CreateINvalidateInverseReferences(TypeBuilder typeBuilder, ILGenerator il, ReferencePropertyDescriptor propDesc, int byteOffset,
		Dictionary<int, FieldBuilder> arrayFields)
	{
		// Load ObjectModel so we can call invalidate of inverse refs on it
		//il.Emit(OpCodes.Ldarg_0);
		//il.Emit(OpCodes.Ldfld, ownerField);

		if (propDesc.Multiplicity != Multiplicity.Many)
		{
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, ownerField);

			// Read reference id from the internal buffer and invalidate inverse refs through object model
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, bufferField);
			il.Emit(OpCodes.Ldc_I4, byteOffset);
			il.Emit(OpCodes.Add);
			il.Emit(OpCodes.Ldind_I8);
			il.Emit(OpCodes.Ldc_I4, propDesc.Id);
			il.Emit(OpCodes.Call, invalidateInvRefMethod);
		}
		else
		{
			// Check if there is already a collection inside the local field
			Label lab1 = il.DefineLabel();
			Label lab2 = il.DefineLabel();
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, arrayFields[propDesc.Id]);
			il.Emit(OpCodes.Ldnull);
			il.Emit(OpCodes.Ceq);
			il.Emit(OpCodes.Brtrue, lab1);

			// collection != null, invalidate references
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, ownerField);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, arrayFields[propDesc.Id]);
			il.Emit(OpCodes.Ldc_I4, propDesc.Id);
			il.Emit(OpCodes.Call, invalidateInvRefsMethod);
			il.Emit(OpCodes.Br, lab2);

			il.MarkLabel(lab1);

			// collection == null, read the handle from the buffer
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, ownerField);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, bufferField);
			il.Emit(OpCodes.Ldc_I4, byteOffset);
			il.Emit(OpCodes.Add);
			il.Emit(OpCodes.Ldind_I8);
			il.Emit(OpCodes.Ldc_I4, propDesc.Id);
			il.Emit(OpCodes.Call, invalidateInvRefsHandleMethod);

			il.MarkLabel(lab2);
		}
	}

	private static void CreateInsertMethod(ClassDescriptor classDesc, TypeBuilder typeBuilder, Dictionary<int, FieldBuilder> arrayFields)
	{
		MethodBuilder mb = typeBuilder.DefineMethod(DatabaseObject.CreateInsertBlockName, MethodAttributes.Public |
			MethodAttributes.Virtual | MethodAttributes.HideBySig, typeof(void), new Type[] { typeof(ChangesetWriter) });
		ILGenerator il = mb.GetILGenerator();

		// Start insert block
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, classDataField);
		il.Emit(OpCodes.Call, classDescMethod);
		il.Emit(OpCodes.Call, startInsertBlockMethod);

		// Add properties
		for (int i = 2; i < classDesc.Properties.Length; i++)
		{
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Ldc_I4, i);
			il.Emit(OpCodes.Call, addPropertyMethod);
		}

		// Signal that all properties are defined
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Call, propertiesDefinedMethod);

		// Create placeholder for operation header
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Call, prevVersionPlaceholderMethod);

		// Add id value
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, idPropertyMethod);
		il.Emit(OpCodes.Call, writeIdMethod);

		int byteOffset = sizeof(long);

		// Add property values
		for (int i = 2; i < classDesc.Properties.Length; i++)
		{
			PropertyDescriptor pd = classDesc.Properties[i];

			// Load changeset writer into stack
			il.Emit(OpCodes.Ldarg_1);

			if (pd.Kind == PropertyKind.Array ||
				(pd.Kind == PropertyKind.Reference && (pd as ReferencePropertyDescriptor).Multiplicity == Multiplicity.Many))
			{
				// Read array from the field
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Ldfld, arrayFields[pd.Id]);

				// Call static array writer
				il.Emit(OpCodes.Call, pd.Kind == PropertyKind.Reference ? writeRefArrayMethod : writeArrayMethod);
			}
			else
			{
				// If this is string we need to call ObjectModel.GetString so load ObjectModel
				if (pd.PropertyType == PropertyType.String)
				{
					il.Emit(OpCodes.Ldarg_0);
					il.Emit(OpCodes.Ldfld, ownerField);
				}
				else if (pd.Kind == PropertyKind.Reference &&
					(pd as ReferencePropertyDescriptor).DeleteTargetAction == DeleteTargetAction.SetToNull)
				{
					// If SetToNull reference we need to call GetSetToNullReference method on the object
					// model so that the reference gets filtered by the deleted set (load ObjectModel)
					il.Emit(OpCodes.Ldarg_0);
					il.Emit(OpCodes.Ldfld, ownerField);
				}

				// Read field value
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Ldfld, bufferField);
				il.Emit(OpCodes.Ldc_I4, byteOffset);
				il.Emit(OpCodes.Add);
				il.Emit(GetLoadSimpleTypeInstruction(pd.PropertyType));

				if (pd.PropertyType == PropertyType.String)
				{
					il.Emit(OpCodes.Ldc_I4_1);  // Modified=true
					il.Emit(OpCodes.Call, getStringMethod);
				}
				else if (pd.Kind == PropertyKind.Reference &&
					(pd as ReferencePropertyDescriptor).DeleteTargetAction == DeleteTargetAction.SetToNull)
				{
					il.Emit(OpCodes.Call, getSetToNullReferenceMethod);
				}

				// Write value into changeset
				il.Emit(OpCodes.Call, addValueMethods[(int)pd.PropertyType]);
			}

			byteOffset += (int)PropertyTypesHelper.GetItemSize(pd.PropertyType);
		}

		// Signal last value written
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Call, lastValueWrittenMethod);

		il.Emit(OpCodes.Ret);
	}

	private static void CreateStaticConstructor(TypeBuilder typeBuilder, Dictionary<FieldBuilder, MethodBuilder> modifiedNotifiers)
	{
		if (modifiedNotifiers != null)
		{
			ConstructorBuilder cb = typeBuilder.DefineTypeInitializer();
			ILGenerator il = cb.GetILGenerator();

			foreach (var kv in modifiedNotifiers)
			{
				il.Emit(OpCodes.Ldnull);
				il.Emit(OpCodes.Ldftn, kv.Value);
				il.Emit(OpCodes.Newobj, arrayNotifierDelegateCtor);
				il.Emit(OpCodes.Stsfld, kv.Key);
			}

			il.Emit(OpCodes.Ret);
		}
	}

	private static ConstructorInfo CreateClassConstructor(TypeBuilder typeBuilder, ClassDescriptor classDesc, Type[] ctorTypes)
	{
		ConstructorBuilder cb = typeBuilder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, ctorTypes);

		// Set aggressive in-lining on the constructor
		CustomAttributeBuilder cab = new CustomAttributeBuilder(
			typeof(MethodImplAttribute).GetConstructor(new Type[] { typeof(MethodImplOptions) }),
			new object[] { MethodImplOptions.AggressiveInlining });

		cb.SetCustomAttribute(cab);

		ConstructorInfo baseCtor = classDesc.ObjectModelClass.ClassType.GetConstructor(
			BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance, null, EmptyArray<Type>.Instance, null);

		ILGenerator il = cb.GetILGenerator();

		// First call DatabaseObject.BegInit so that users base ctor has fully initialized object at its disposal
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Ldarg_2);
		il.Emit(OpCodes.Ldarg_3);
		il.Emit(OpCodes.Ldarg, 4);
		il.Emit(OpCodes.Ldarg, 5);
		il.Emit(OpCodes.Call, dbObjBegInitMethod);

		// Now call base ctor when object is fully initialized
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, baseCtor);

		// Finally call DatabaseObject.EndInit to allow setters to be called
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, dbObjEndInitMethod);

		il.Emit(OpCodes.Ret);
		return cb;
	}

	private static void CreateClassProperty(ClassDescriptor classDesc, TypeBuilder typeBuilder, PropertyDescriptor propDesc,
		ObjectModelProperty objProp, int byteOffset, int propertyIndex, Dictionary<int, FieldBuilder> arrayFields,
		Dictionary<FieldBuilder, MethodBuilder> modifiedNotifiers)
	{
		if (propDesc.PropertyType == PropertyType.String)
			CreateStringClassProperty(classDesc, typeBuilder, propDesc, objProp, byteOffset, propertyIndex);
		else if (propDesc.Kind == PropertyKind.Simple)
			CreateSimpleClassProperty(classDesc, typeBuilder, propDesc, objProp, byteOffset, propertyIndex);
		else if (propDesc.Kind == PropertyKind.Array)
			CreateArrayClassProperty(classDesc, typeBuilder, propDesc, objProp, byteOffset, propertyIndex, arrayFields, modifiedNotifiers);
		else if (((ReferencePropertyDescriptor)propDesc).Multiplicity == Multiplicity.Many)
			CreateArrayClassProperty(classDesc, typeBuilder, propDesc, objProp, byteOffset, propertyIndex, arrayFields, modifiedNotifiers);
		else
			CreateReferenceClassProperty(classDesc, typeBuilder, propDesc, objProp, byteOffset, propertyIndex);
	}

	private static void CreateSimpleClassProperty(ClassDescriptor classDesc, TypeBuilder typeBuilder, PropertyDescriptor propDesc,
		ObjectModelProperty objProp, int byteOffset, int propertyIndex)
	{
		PropertyBuilder pb = typeBuilder.DefineProperty(objProp.PropertyInfo.Name,
			PropertyAttributes.None, objProp.PropertyInfo.PropertyType, null);

		MethodBuilder getter = typeBuilder.DefineMethod(objProp.PropertyInfo.GetGetMethod().Name,
			MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
			objProp.PropertyInfo.PropertyType, null);

		MethodBuilder setter = typeBuilder.DefineMethod(objProp.PropertyInfo.GetSetMethod().Name,
			MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
			typeof(void), new Type[] { objProp.PropertyInfo.PropertyType });

		pb.SetGetMethod(getter);
		pb.SetSetMethod(setter);

		ILGenerator il = getter.GetILGenerator();

		// Verify object is not deleted
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, verifyAccessMethod);

		// Read simple value from the internal buffer (and in case of DateTime transform it)
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, byteOffset);
		il.Emit(OpCodes.Add);
		il.Emit(GetLoadSimpleTypeInstruction(propDesc.PropertyType));
		if (propDesc.PropertyType == PropertyType.DateTime)
			il.Emit(OpCodes.Call, ((Delegate)(Func<long, DateTime>)DateTime.FromBinary).Method);

		il.Emit(OpCodes.Ret);


		il = setter.GetILGenerator();

		// Notify ObjectModel that an object has been modified (this also verifies object access).
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, ownerField);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, objModifiedMethod);

		// Modify the bit field
		int fieldIndex = (propertyIndex - 2) / 8;
		int bitIndex = (propertyIndex - 2) % 8;
		int mask = (byte)(1 << bitIndex);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, CalculateBitFiledSize(classDesc) - fieldIndex);
		il.Emit(OpCodes.Sub);
		il.Emit(OpCodes.Dup);
		il.Emit(OpCodes.Ldind_U1);
		il.Emit(OpCodes.Ldc_I4, mask);
		il.Emit(OpCodes.Or);
		il.Emit(OpCodes.Conv_U1);
		il.Emit(OpCodes.Stind_I1);

		// Write new value into the buffer (and in case of DateTime first extract the long value from DateTime value).
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, byteOffset);
		il.Emit(OpCodes.Add);
		if (propDesc.PropertyType == PropertyType.DateTime)
		{
			il.Emit(OpCodes.Ldarga, 1);
			il.Emit(OpCodes.Call, ((Delegate)(Func<long>)DateTime.Now.ToBinary).Method);
		}
		else
		{
			il.Emit(OpCodes.Ldarg_1);
		}

		il.Emit(GetStoreSimpleTypeInstruction(propDesc.PropertyType));
		il.Emit(OpCodes.Ret);
	}

	private static void CreateStringClassProperty(ClassDescriptor classDesc, TypeBuilder typeBuilder, PropertyDescriptor propDesc,
		ObjectModelProperty objProp, int byteOffset, int propertyIndex)
	{
		PropertyBuilder pb = typeBuilder.DefineProperty(objProp.PropertyInfo.Name,
			PropertyAttributes.None, objProp.PropertyInfo.PropertyType, null);

		MethodBuilder getter = typeBuilder.DefineMethod(objProp.PropertyInfo.GetGetMethod().Name,
			MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
			objProp.PropertyInfo.PropertyType, null);

		MethodBuilder setter = typeBuilder.DefineMethod(objProp.PropertyInfo.GetSetMethod().Name,
			MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
			typeof(void), new Type[] { objProp.PropertyInfo.PropertyType });

		pb.SetGetMethod(getter);
		pb.SetSetMethod(setter);

		ILGenerator il = getter.GetILGenerator();

		// Verify object is not deleted
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, verifyAccessMethod);

		// Determine whether the string is modified
		LocalBuilder isModifiedVar = il.DeclareLocal(typeof(bool));
		il.Emit(OpCodes.Ldc_I4_0);
		il.Emit(OpCodes.Stloc, isModifiedVar);

		// Check if object has been modified
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, isInsertedOrModifiedMethod);

		Label notModifiedLabel = il.DefineLabel();

		il.Emit(OpCodes.Brfalse, notModifiedLabel);

		// Object has been modified, load bool indicating whether the property has been modified
		int fieldIndex = (propertyIndex - 2) / 8;
		int bitIndex = (propertyIndex - 2) % 8;
		int mask = (byte)(1 << bitIndex);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, CalculateBitFiledSize(classDesc) - fieldIndex);
		il.Emit(OpCodes.Sub);
		il.Emit(OpCodes.Ldind_U1);
		il.Emit(OpCodes.Ldc_I4, mask);
		il.Emit(OpCodes.And);
		il.Emit(OpCodes.Stloc, isModifiedVar);
		il.MarkLabel(notModifiedLabel);


		// Load object model onto the stack to later call GetString
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, ownerField);

		// Read string index from the internal buffer
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, byteOffset);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Ldind_I8);

		il.Emit(OpCodes.Ldloc, isModifiedVar);

		// Call GetString
		il.Emit(OpCodes.Call, getStringMethod);

		il.Emit(OpCodes.Ret);


		il = setter.GetILGenerator();

		// Notify ObjectModel that an object has been modified (this also verifies object access).
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, ownerField);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, objModifiedMethod);

		// Modify the bit field
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, CalculateBitFiledSize(classDesc) - fieldIndex);
		il.Emit(OpCodes.Sub);
		il.Emit(OpCodes.Dup);
		il.Emit(OpCodes.Ldind_U1);
		il.Emit(OpCodes.Ldc_I4, mask);
		il.Emit(OpCodes.Or);
		il.Emit(OpCodes.Conv_U1);
		il.Emit(OpCodes.Stind_I1);

		// Write string index into the buffer
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, byteOffset);
		il.Emit(OpCodes.Add);

		// Store the string in the ObjectModel and get the string index which will be written to the buffer
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, ownerField);
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Call, storeStringMethod);
		il.Emit(OpCodes.Stind_I4);

		il.Emit(OpCodes.Ret);
	}

	private static void CreateReferenceClassProperty(ClassDescriptor classDesc, TypeBuilder typeBuilder, PropertyDescriptor propDesc,
		ObjectModelProperty objProp, int byteOffset, int propertyIndex)
	{
		PropertyBuilder pb = typeBuilder.DefineProperty(objProp.PropertyInfo.Name,
			PropertyAttributes.None, objProp.PropertyInfo.PropertyType, null);

		MethodBuilder getter = typeBuilder.DefineMethod(objProp.PropertyInfo.GetGetMethod().Name,
			MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
			objProp.PropertyInfo.PropertyType, null);

		MethodBuilder setter = typeBuilder.DefineMethod(objProp.PropertyInfo.GetSetMethod().Name,
			MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
			typeof(void), new Type[] { objProp.PropertyInfo.PropertyType });

		pb.SetGetMethod(getter);
		pb.SetSetMethod(setter);

		ILGenerator il = getter.GetILGenerator();

		// Verify object is not deleted
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, verifyAccessMethod);

		// Load object model so that we can query for the object
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, ownerField);

		// Read object id from the internal buffer and obtain it from ObjectModel
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, byteOffset);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Ldind_I8);
		il.Emit(OpCodes.Call, getObjectMethod);
		il.Emit(OpCodes.Ret);


		il = setter.GetILGenerator();

		// Verify that the referenced object is not deleted
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, verifyReferencingMethod);

		// Notify ObjectModel that an object has been modified (this also verifies object access).
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, ownerField);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, objModifiedMethod);

		if ((propDesc as ReferencePropertyDescriptor).TrackInverseReferences)
		{
			// Notify ObjectModel that references are being modified
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, ownerField);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Call, getObjectIdMethod);

			// Load old referenced id
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, bufferField);
			il.Emit(OpCodes.Ldc_I4, byteOffset);
			il.Emit(OpCodes.Add);
			il.Emit(OpCodes.Ldind_I8);

			// Load new reference id
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Call, getObjectIdMethod);

			il.Emit(OpCodes.Ldc_I4, propDesc.Id);
			il.Emit(OpCodes.Call, referenceModifiedMethod);
		}

		// Modify the bit field
		int fieldIndex = (propertyIndex - 2) / 8;
		int bitIndex = (propertyIndex - 2) % 8;
		int mask = (byte)(1 << bitIndex);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, CalculateBitFiledSize(classDesc) - fieldIndex);
		il.Emit(OpCodes.Sub);
		il.Emit(OpCodes.Dup);
		il.Emit(OpCodes.Ldind_U1);
		il.Emit(OpCodes.Ldc_I4, mask);
		il.Emit(OpCodes.Or);
		il.Emit(OpCodes.Conv_U1);
		il.Emit(OpCodes.Stind_I1);

		// Write new object id into the buffer
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, byteOffset);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Call, getObjectIdMethod);
		il.Emit(OpCodes.Stind_I8);
		il.Emit(OpCodes.Ret);
	}

	private static void CreateArrayClassProperty(ClassDescriptor classDesc, TypeBuilder typeBuilder, PropertyDescriptor propDesc,
		ObjectModelProperty objProp, int byteOffset, int propertyIndex, Dictionary<int, FieldBuilder> arrayFields,
		Dictionary<FieldBuilder, MethodBuilder> modifiedNotifiers)
	{
		bool isReference = propDesc.Kind == PropertyKind.Reference;

		PropertyBuilder pb = typeBuilder.DefineProperty(objProp.PropertyInfo.Name,
			PropertyAttributes.None, objProp.PropertyInfo.PropertyType, null);

		MethodBuilder getter = typeBuilder.DefineMethod(objProp.PropertyInfo.GetGetMethod().Name,
			MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
			objProp.PropertyInfo.PropertyType, null);

		MethodBuilder setter = typeBuilder.DefineMethod(objProp.PropertyInfo.GetSetMethod().Name,
			MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
			typeof(void), new Type[] { objProp.PropertyInfo.PropertyType });

		pb.SetGetMethod(getter);
		pb.SetSetMethod(setter);

		// Define property for holding database/reference array collection
		FieldBuilder arrayField = typeBuilder.DefineField(Guid.NewGuid().ToString("N"),
			isReference ? typeof(ReferenceArray) : typeof(DatabaseArray), FieldAttributes.Private);

		arrayFields.Add(propDesc.Id, arrayField);

		// Modified bit index of the property
		int fieldIndex = (propertyIndex - 2) / 8;
		int bitIndex = (propertyIndex - 2) % 8;
		int mask = (byte)(1 << bitIndex);

		// Define delegate that will be called by the database/reference array when it is modified
		MethodBuilder modifiedMethod = typeBuilder.DefineMethod(Guid.NewGuid().ToString("N"),
			MethodAttributes.Private | MethodAttributes.Static, typeof(void), new Type[] { typeof(DatabaseObject) });

		// Define static variable that will hold the delegate
		FieldBuilder modifiedDelegateField = typeBuilder.DefineField(Guid.NewGuid().ToString("N"),
			typeof(Action<DatabaseObject>), FieldAttributes.Static | FieldAttributes.Private);
		modifiedNotifiers.Add(modifiedDelegateField, modifiedMethod);


		ILGenerator il = modifiedMethod.GetILGenerator();

		// Notify ObjectModel that an object has been modified (this also verifies object access).
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, ownerField);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, objModifiedMethod);

		// Modify the bit field
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, CalculateBitFiledSize(classDesc) - fieldIndex);
		il.Emit(OpCodes.Sub);
		il.Emit(OpCodes.Dup);
		il.Emit(OpCodes.Ldind_U1);
		il.Emit(OpCodes.Ldc_I4, mask);
		il.Emit(OpCodes.Or);
		il.Emit(OpCodes.Conv_U1);
		il.Emit(OpCodes.Stind_I1);

		il.Emit(OpCodes.Ret);

		// Getter
		il = getter.GetILGenerator();

		// Verify object is not deleted
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, verifyAccessMethod);

		// Check if there is already a collection inside the local field
		Label lab = il.DefineLabel();
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, arrayField);
		il.Emit(OpCodes.Ldnull);
		il.Emit(OpCodes.Ceq);
		il.Emit(OpCodes.Brtrue, lab);

		// collection != null, just return it
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, arrayField);
		il.Emit(OpCodes.Ret);

		il.MarkLabel(lab);

		// collection == null

		// Check if array is null and if it is, return null
		lab = il.DefineLabel();
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, byteOffset);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Ldind_I8);
		il.Emit(OpCodes.Ldc_I4_0);
		il.Emit(OpCodes.Ceq);
		il.Emit(OpCodes.Brfalse, lab);
		il.Emit(OpCodes.Ldnull);
		il.Emit(OpCodes.Ret);
		il.MarkLabel(lab);

		// create new collection

		// Since we will store the created array in the array field we need to load the database object (this) onto the stack
		il.Emit(OpCodes.Ldarg_0);

		// Prepare arguments of the array constructor
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldsfld, modifiedDelegateField);

		// Load ObjectModel and array index onto the stack so that we can extract array buffer
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, ownerField);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, byteOffset);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Ldind_I8);

		// Get the array buffer from the engine
		il.Emit(OpCodes.Call, getArrayMethod);

		if (isReference)
		{
			il.Emit(OpCodes.Ldc_I4, propDesc.Id);
			ConstructorInfo ctor = objProp.PropertyInfo.PropertyType.GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null,
				new Type[] { typeof(DatabaseObject), typeof(Action<DatabaseObject>), typeof(byte*), typeof(int) }, null);
			il.Emit(OpCodes.Newobj, ctor);
		}
		else
		{
			il.Emit(OpCodes.Newobj, arrayCtors[(int)propDesc.PropertyType]);
		}

		// Store the array into the field  (remember, we already have the this pointer on the stack)
		il.Emit(OpCodes.Stfld, arrayField);

		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, arrayField);
		il.Emit(OpCodes.Ret);

		// setter
		il = setter.GetILGenerator();

		// Take array ownership
		il.Emit(OpCodes.Ldarg_0);
		if (isReference)
			il.Emit(OpCodes.Ldc_I4, propDesc.Id);
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Ldsfld, modifiedDelegateField);
		il.Emit(OpCodes.Call, isReference ? takeRefArrayOwnershipMethod : takeArrayOwnershipMethod);

		// Notify ObjectModel that an object has been modified (this also verifies object access).
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, ownerField);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, objModifiedMethod);

		if (isReference && (propDesc as ReferencePropertyDescriptor).TrackInverseReferences)
		{
			// Notify ObjectModel that references are being modified
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, ownerField);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Call, getObjectIdMethod);

			// Load old reference array
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Call, getter);

			// Load new reference array
			il.Emit(OpCodes.Ldarg_1);

			il.Emit(OpCodes.Ldc_I4, propDesc.Id);
			il.Emit(OpCodes.Call, referenceArrayModifiedMethod);
		}

		// Modify the bit field
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, bufferField);
		il.Emit(OpCodes.Ldc_I4, CalculateBitFiledSize(classDesc) - fieldIndex);
		il.Emit(OpCodes.Sub);
		il.Emit(OpCodes.Dup);
		il.Emit(OpCodes.Ldind_U1);
		il.Emit(OpCodes.Ldc_I4, mask);
		il.Emit(OpCodes.Or);
		il.Emit(OpCodes.Conv_U1);
		il.Emit(OpCodes.Stind_I1);

		// Write the array value into the array field
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Stfld, arrayField);

		il.Emit(OpCodes.Ret);
	}

	private static int CalculateBitFiledSize(ClassDescriptor classDesc)
	{
		int n = (classDesc.Properties.Length - 2) / 8;
		if ((classDesc.Properties.Length - 2) % 8 != 0)
			n++;

		return n;
	}

	private static ObjectModelProperty FindObjectModelProperty(ClassDescriptor classDesc, string name)
	{
		ObjectModelProperty op = null;
		while (op == null && classDesc != null)
		{
			op = classDesc.ObjectModelClass.GetProperty(name);
			classDesc = classDesc.BaseClass;
		}

		return op;
	}

	private static PropertyInfo GetPublicProperty(Type type, string name)
	{
		foreach (Type t in type.GetInterfaces().Concat(type))
		{
			PropertyInfo pi = t.GetProperty(name);
			if (pi != null)
				return pi;
		}

		return null;
	}

	private static int GetByteOffset(ClassDescriptor classDesc, PropertyDescriptor propDesc)
	{
		int offset = sizeof(long);
		for (int i = 2; i < classDesc.Properties.Length; i++)   // Skip Id and Version
		{
			PropertyDescriptor pd = classDesc.Properties[i];
			if (pd.Id == propDesc.Id)
				return offset;

			offset += (int)PropertyTypesHelper.GetItemSize(pd.PropertyType);
		}

		throw new ArgumentException("Unexisting property.");
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
				throw new ArgumentException();
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
				throw new ArgumentException();
		}
	}
}
