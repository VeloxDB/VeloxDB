using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using VeloxDB.Common;
using VeloxDB.Networking;

namespace VeloxDB.Protocol;

internal sealed class ProtocolClassDescriptor : ProtocolTypeDescriptor
{
	const int MaxPropertyCount = 512;

	static readonly HashSet<Type> internalsVisibleTypes = new HashSet<Type>() { typeof(DatabaseException), typeof(DatabaseErrorDetail) };

	List<ProtocolPropertyDescriptor> properties;
	bool isAbstract;
	bool isRefType;
	bool isSealed;
	int simplePropertySize;

	public ProtocolClassDescriptor() :
		base()
	{
	}

	public ProtocolClassDescriptor(ushort id, Type type) :
		base(id, GetClassName(type), type)
	{
	}

	public override void Init(Type type, ProtocolDiscoveryContext discoveryContext, bool isInput)
	{
		if (!IsPublic(type))
			throw DbAPIDefinitionException.CreateNonAccessibleType(Name);

		if (type.IsGenericType)
			throw DbAPIDefinitionException.CreateGenericType(Name);

		if (type.IsClass && !type.IsAbstract && GetConstructor(type) == null)
			throw DbAPIDefinitionException.CreateMissingConstructor(Name);

		this.isRefType = type.IsClass;
		this.isSealed = type.IsSealed;
		this.isAbstract = type.IsAbstract;
		this.TargetType = type;

		PropertyInfo[] pis = type.GetProperties(System.Reflection.BindingFlags.Public |
			System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.FlattenHierarchy);

		if (typeof(DbAPIErrorException).IsAssignableFrom(type))
			pis = pis.Where(x => typeof(DbAPIErrorException).IsAssignableFrom(x.DeclaringType)).ToArray();

		simplePropertySize = 0;
		properties = new List<ProtocolPropertyDescriptor>(pis.Length);
		for (int i = 0; i < pis.Length; i++)
		{
			PropertyInfo pi = pis[i];
			MethodInfo getter = pi.GetGetMethod();
			MethodInfo setter = pi.GetSetMethod();
			if (getter == null || !getter.IsPublic || setter == null || !setter.IsPublic)
				continue;

			if (BuiltInTypesHelper.IsBuiltInType(pi.PropertyType))
			{
				BuiltInType bt = BuiltInTypesHelper.To(pi.PropertyType);
				if (BuiltInTypesHelper.IsSimple(bt))
					simplePropertySize += BuiltInTypesHelper.GetSimpleSize(bt);

				properties.Add(new ProtocolPropertyDescriptor(bt, null, pi.Name));
			}
			else
			{
				ProtocolTypeDescriptor typeDesc = discoveryContext.GetTypeDuringDiscovery(pi.PropertyType, isInput);
				properties.Add(new ProtocolPropertyDescriptor(BuiltInType.None, typeDesc, pi.Name));
			}
		}

		if (properties.Count > MaxPropertyCount)
			throw DbAPIDefinitionException.CreateMaxPropertyCountExceeded(Name);

		properties.Sort(new PropertyComparer());

		base.TargetType = type;
	}

	public override void DiscoverTypes(Type type, ProtocolDiscoveryContext discoveryContext, bool isInput)
	{
		for (int i = 0; i < properties.Count; i++)
		{
			if (properties[i].TypeDesc != null)
			{
				Checker.AssertNotNull(properties[i].TypeDesc.TargetType);
				discoveryContext.GetTypeDuringDiscovery(properties[i].TypeDesc.TargetType, isInput);
			}
		}
	}

	private static ConstructorInfo GetConstructor(Type type)
	{
		if (internalsVisibleTypes.Contains(type))
		{
			ConstructorInfo result = type.GetConstructor(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance, EmptyArray<Type>.Instance);

			if (!result.IsAssembly && !result.IsPublic)
				return null;

			return result;
		}
		else
			return type.GetConstructor(EmptyArray<Type>.Instance);

	}

	public List<ProtocolPropertyDescriptor> Properties => properties;
	public bool IsRefType => isRefType;
	public override bool IsValueType => !isRefType;
	public override bool IsAbstract => isAbstract;
	public bool IsSealed => isSealed;
	public bool CanBeInherited => isRefType && !isSealed;

	public override ProtocolTypeType Type => ProtocolTypeType.Class;

	private bool IsPublic(Type type)
	{
		return type.IsPublic || (type.IsNested && type.IsNestedPublic && IsPublic(type.DeclaringType));
	}

	private ProtocolPropertyDescriptor FindProperty(string name)
	{
		for (int i = 0; i < properties.Count; i++)
		{
			if (properties[i].Name.Equals(name))
				return properties[i];
		}

		return null;
	}

	protected override bool IsMatchInternal(ProtocolTypeDescriptor otherDesc, Dictionary<ProtocolTypeDescriptor, bool> checkedTypes)
	{
		ProtocolClassDescriptor classDesc = otherDesc as ProtocolClassDescriptor;
		if (classDesc == null)
			return false;

		if (isRefType != classDesc.isRefType || (!isSealed && classDesc.IsSealed) || isAbstract != classDesc.isAbstract)
			return false;

		for (int i = 0; i < properties.Count; i++)
		{
			ProtocolPropertyDescriptor otherPropDesc = classDesc.FindProperty(properties[i].Name);
			if (otherPropDesc == null)
				continue;

			if (!otherPropDesc.IsMatch(properties[i], true, checkedTypes))
				return false;
		}

		return true;
	}

	protected override void OnSerialize(MessageWriter writer, SerializerContext context)
	{
		writer.WriteBool(isRefType);
		writer.WriteBool(isSealed);
		writer.WriteBool(isAbstract);
		writer.WriteInt(simplePropertySize);

		writer.WriteUShort((ushort)properties.Count);
		for (int i = 0; i < properties.Count; i++)
		{
			ProtocolPropertyDescriptor.Serialize(properties[i], writer, context);
		}
	}

	protected override void OnDeserialize(MessageReader reader, DeserializerContext context)
	{
		isRefType = reader.ReadBool();
		isSealed = reader.ReadBool();
		isAbstract = reader.ReadBool();
		simplePropertySize = reader.ReadInt();

		int c = reader.ReadUShort();
		properties = new List<ProtocolPropertyDescriptor>(c);
		for (int i = 0; i < c; i++)
		{
			properties.Add(ProtocolPropertyDescriptor.Deserialize(reader, context));
		}
	}

	private static PropertyInfo FindProperty(Type type, string name)
	{
		if (type == null)
			return null;

		PropertyInfo pi = type.GetProperty(name, BindingFlags.Public | BindingFlags.FlattenHierarchy | BindingFlags.Instance);
		if (pi == null || pi.GetGetMethod() == null || pi.GetSetMethod() == null)
			return null;

		return pi;
	}

	public override void GenerateDeserializerCode(ILGenerator il, DeserializerManager deserializerManager)
	{
		Checker.AssertFalse(isAbstract);

		LocalBuilder instanceVar = il.DeclareLocal(TargetType ?? typeof(object));

		// Increase depth
		LocalBuilder depthVar = il.DeclareLocal(typeof(int));
		il.Emit(OpCodes.Ldarg_3);   // Prev depth
		il.Emit(OpCodes.Ldc_I4_1);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Stloc, depthVar);

		if (isRefType)
		{
			LocalBuilder deserResultVar = il.DeclareLocal(typeof(int));

			// Check if we have been deserialized before
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldloca, instanceVar);
			il.Emit(OpCodes.Ldloc, depthVar);
			il.Emit(OpCodes.Call, Methods.DeserializerContextTryReadMethod);
			il.Emit(OpCodes.Stloc, deserResultVar);

			// Check for Ready result in which case just return the object we got from the context
			Label readyLabel = il.DefineLabel();
			il.Emit(OpCodes.Ldloc, deserResultVar);
			il.Emit(OpCodes.Ldc_I4, (int)ReadObjectResult.Ready);
			il.Emit(OpCodes.Ceq);
			il.Emit(OpCodes.Brfalse, readyLabel);

			if (TargetType != null)
				il.Emit(OpCodes.Ldloc, instanceVar);

			il.Emit(OpCodes.Ret);

			il.MarkLabel(readyLabel);

			// Check for CreateEmpty result in which case we create a new object and add it to the context
			Label createEmptyLabel = il.DefineLabel();
			il.Emit(OpCodes.Ldloc, deserResultVar);
			il.Emit(OpCodes.Ldc_I4, (int)ReadObjectResult.CreateEmpty);
			il.Emit(OpCodes.Ceq);
			il.Emit(OpCodes.Brfalse, createEmptyLabel);

			if (TargetType == null)
			{
				il.Emit(OpCodes.Ldarg_1);               // Load context
				il.Emit(OpCodes.Ldnull);
				il.Emit(OpCodes.Call, Methods.DeserializerContextAddMethod);
			}
			else
			{
				ConstructorInfo ci = GetConstructor(TargetType);
				il.Emit(OpCodes.Newobj, ci);
				il.Emit(OpCodes.Stloc, instanceVar);
				il.Emit(OpCodes.Ldarg_1);               // Load context
				il.Emit(OpCodes.Ldloc, instanceVar);
				il.Emit(OpCodes.Call, Methods.DeserializerContextAddMethod);
				il.Emit(OpCodes.Ldloc, instanceVar);
			}

			il.Emit(OpCodes.Ret);

			il.MarkLabel(createEmptyLabel);

			// We got Deserialize result, create an instance if we haven't received one from the context
			Label gotNullLabel = il.DefineLabel();
			il.Emit(OpCodes.Ldloc, instanceVar);
			il.Emit(OpCodes.Brtrue, gotNullLabel);

			if (TargetType == null)
			{
				il.Emit(OpCodes.Ldarg_1);               // Load context
				il.Emit(OpCodes.Ldnull);
				il.Emit(OpCodes.Call, Methods.DeserializerContextAddMethod);
			}
			else
			{
				ConstructorInfo ci = GetConstructor(TargetType);
				il.Emit(OpCodes.Newobj, ci);
				il.Emit(OpCodes.Stloc, instanceVar);
				il.Emit(OpCodes.Ldarg_1);               // Load context
				il.Emit(OpCodes.Ldloc, instanceVar);
				il.Emit(OpCodes.Call, Methods.DeserializerContextAddMethod);
			}

			il.MarkLabel(gotNullLabel);
		}
		else
		{
			if (TargetType != null)
			{
				il.Emit(OpCodes.Ldloca, instanceVar);
				il.Emit(OpCodes.Initobj, TargetType);

				// Read byte indicator whether empty struct was sent, if it was just return created struct
				Label skipReturnDefault = il.DefineLabel();
				il.Emit(OpCodes.Ldarg_0);
				il.Emit(OpCodes.Call, BuiltInTypesHelper.GetReadMethod(BuiltInType.Byte));
				il.Emit(OpCodes.Ldc_I4_1);
				il.Emit(OpCodes.Beq, skipReturnDefault);
				il.Emit(OpCodes.Ldloc, instanceVar);
				il.Emit(OpCodes.Ret);
				il.MarkLabel(skipReturnDefault);
			}
			else
			{
				// Just read byte value, which must be 1 (since server always serializes all its properties)
				il.Emit(OpCodes.Ldarg, 0);
				il.Emit(OpCodes.Call, BuiltInTypesHelper.GetReadMethod(BuiltInType.Byte));
				il.Emit(OpCodes.Pop);
			}
		}

		//For simple properties we check buffer space for the entire set, and then inline write values
		if (simplePropertySize > 0)
		{
			GenerateInlinedDeserializerCode(il, deserializerManager, instanceVar, depthVar);
		}
		else
		{
			for (int i = 0; i < properties.Count; i++)
			{
				ProtocolPropertyDescriptor propDesc = properties[i];
				if (propDesc.BuiltInType != BuiltInType.None)
					GenerateBuiltInTypeDeserializer(il, propDesc, instanceVar);
				else
					GenerateProtocolTypeDeserializer(il, propDesc, deserializerManager, instanceVar, depthVar);
			}
		}

		if (TargetType != null)
			il.Emit(OpCodes.Ldloc, instanceVar);

		il.Emit(OpCodes.Ret);
	}

	private void GenerateInlinedDeserializerCode(ILGenerator il, DeserializerManager deserializerManager,
		LocalBuilder instanceVar, LocalBuilder depthVar)
	{
		LocalBuilder offsetVar = il.DeclareLocal(typeof(int));
		LocalBuilder bufferVar = il.DeclareLocal(typeof(byte*));
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, Methods.MessageReaderOffsetFld);
		il.Emit(OpCodes.Stloc, offsetVar);

		Label skipFastCaseLabel = il.DefineLabel();
		Label skipSlowCaseLabel = il.DefineLabel();
		il.Emit(OpCodes.Ldloc, offsetVar);
		il.Emit(OpCodes.Ldc_I4, simplePropertySize);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, Methods.MessageReaderSizeFld);
		il.Emit(OpCodes.Bgt, skipFastCaseLabel);

		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, Methods.MessageReaderBufferFld);
		il.Emit(OpCodes.Ldloc, offsetVar);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Stloc, bufferVar);

		int offset = 0;
		int i = 0;
		while (i < properties.Count && BuiltInTypesHelper.IsSimple(properties[i].BuiltInType))
		{
			ProtocolPropertyDescriptor propDesc = properties[i];
			PropertyInfo pi = FindProperty(TargetType, propDesc.Name);

			if (pi != null)
				il.Emit(isRefType ? OpCodes.Ldloc : OpCodes.Ldloca, instanceVar);

			il.Emit(OpCodes.Ldloc, bufferVar);
			if (offset > 0)
			{
				il.Emit(OpCodes.Ldc_I4, offset);
				il.Emit(OpCodes.Add);
			}

			OpCode loadOpCode = BuiltInTypesHelper.GetIndLoadInstruction(propDesc.BuiltInType);
			if (loadOpCode == OpCodes.Ldobj)
			{
				il.Emit(OpCodes.Ldobj, BuiltInTypesHelper.From(propDesc.BuiltInType));
			}
			else
			{
				il.Emit(loadOpCode);
			}

			if (pi == null)
			{
				il.Emit(OpCodes.Pop);
			}
			else
			{
				il.Emit(OpCodes.Call, pi.GetSetMethod());
			}

			offset += BuiltInTypesHelper.GetSimpleSize(propDesc.BuiltInType);
			i++;
		}

		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldloc, offsetVar);
		il.Emit(OpCodes.Ldc_I4, offset);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Stfld, Methods.MessageReaderOffsetFld);

		while (i < properties.Count)
		{
			ProtocolPropertyDescriptor propDesc = properties[i];
			if (propDesc.BuiltInType != BuiltInType.None)
				GenerateBuiltInTypeDeserializer(il, propDesc, instanceVar);
			else
				GenerateProtocolTypeDeserializer(il, propDesc, deserializerManager, instanceVar, depthVar);

			i++;
		}

		il.Emit(OpCodes.Br, skipSlowCaseLabel);
		il.MarkLabel(skipFastCaseLabel);

		for (i = 0; i < properties.Count; i++)
		{
			ProtocolPropertyDescriptor propDesc = properties[i];
			if (propDesc.BuiltInType != BuiltInType.None)
				GenerateBuiltInTypeDeserializer(il, propDesc, instanceVar);
			else
				GenerateProtocolTypeDeserializer(il, propDesc, deserializerManager, instanceVar, depthVar);
		}

		il.MarkLabel(skipSlowCaseLabel);
	}

	public override void GenerateSerializerCode(ILGenerator il, SerializerManager serializerManager)
	{
		Checker.AssertNotNull(TargetType);

		if (isAbstract)
			throw new InvalidOperationException();

		// Increase depth
		LocalBuilder depthVar = il.DeclareLocal(typeof(int));
		il.Emit(OpCodes.Ldarg_3);   // Prev depth
		il.Emit(OpCodes.Ldc_I4_1);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Stloc, depthVar);

		if (isRefType)
		{
			// Check if we have been serialized before
			il.Emit(OpCodes.Ldarg_2);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Ldloc, depthVar);
			il.Emit(OpCodes.Ldc_I4_1);
			il.Emit(OpCodes.Call, Methods.SerializerContextTryWriteMethod);

			Label earlyExitSerLabel = il.DefineLabel();
			il.Emit(OpCodes.Brfalse, earlyExitSerLabel);
			il.Emit(OpCodes.Ret);

			il.MarkLabel(earlyExitSerLabel);
		}
		else
		{
			// Write byte 1 indicating that the structure content follows
			il.Emit(OpCodes.Ldarg_0);   // Load writer
			il.Emit(OpCodes.Ldc_I4_1);
			il.Emit(OpCodes.Call, BuiltInTypesHelper.GetWriteMethod(BuiltInType.Byte));
		}

		if (simplePropertySize > 0)
		{
			GenerateInlinedSerializerCode(il, serializerManager, depthVar);
		}
		else
		{
			for (int i = 0; i < properties.Count; i++)
			{
				ProtocolPropertyDescriptor propDesc = properties[i];
				if (propDesc.BuiltInType != BuiltInType.None)
					GenerateBuiltInTypeSerializer(il, propDesc);
				else
					GenerateProtocolTypeSerializer(il, propDesc, serializerManager, depthVar);
			}
		}

		il.Emit(OpCodes.Ret);
	}

	private static void GenerateDefaultSerializerCode(ILGenerator il, ProtocolTypeDescriptor typeDesc)
	{
		if (IsBuiltInTypeCollection(typeDesc))
		{
			// Write byte 0 (null collection in writer)
			il.Emit(OpCodes.Ldarg_0);   // Load writer
			il.Emit(OpCodes.Ldc_I4_0);
			il.Emit(OpCodes.Call, BuiltInTypesHelper.GetWriteMethod(BuiltInType.Byte));
		}
		else if (typeDesc.Type == ProtocolTypeType.List || typeDesc.Type == ProtocolTypeType.Array ||
			(typeDesc as ProtocolClassDescriptor).isRefType)
		{
			il.Emit(OpCodes.Ldarg_2);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldnull);
			il.Emit(OpCodes.Ldc_I4_0);
			il.Emit(OpCodes.Ldc_I4_1);
			il.Emit(OpCodes.Call, Methods.SerializerContextTryWriteMethod);
			il.Emit(OpCodes.Pop);
		}
		else
		{
			// Write byte 0 (default struct)
			il.Emit(OpCodes.Ldarg_0);   // Load writer
			il.Emit(OpCodes.Ldc_I4_0);
			il.Emit(OpCodes.Call, BuiltInTypesHelper.GetWriteMethod(BuiltInType.Byte));
		}
	}

	private static bool IsBuiltInTypeCollection(ProtocolTypeDescriptor typeDesc)
	{
		return typeDesc.Type == ProtocolTypeType.List && ((ProtocolListDescriptor)typeDesc).ElementBuiltInType != BuiltInType.None ||
			typeDesc.Type == ProtocolTypeType.Array && ((ProtocolArrayDescriptor)typeDesc).ElementBuiltInType != BuiltInType.None;
	}

	private void GenerateInlinedSerializerCode(ILGenerator il, SerializerManager serializerManager, LocalBuilder depthVar)
	{
		LocalBuilder offsetVar = il.DeclareLocal(typeof(int));
		LocalBuilder bufferVar = il.DeclareLocal(typeof(byte*));
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, Methods.MessageWriterOffsetFld);
		il.Emit(OpCodes.Stloc, offsetVar);

		Label skipFastCaseLabel = il.DefineLabel();
		Label skipSlowCaseLabel = il.DefineLabel();
		il.Emit(OpCodes.Ldloc, offsetVar);
		il.Emit(OpCodes.Ldc_I4, simplePropertySize);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, Methods.MessageWriterSizeFld);
		il.Emit(OpCodes.Bgt, skipFastCaseLabel);

		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, Methods.MessageWriterBufferFld);
		il.Emit(OpCodes.Ldloc, offsetVar);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Stloc, bufferVar);

		int offset = 0;
		int i = 0;
		while (i < properties.Count && BuiltInTypesHelper.IsSimple(properties[i].BuiltInType))
		{
			ProtocolPropertyDescriptor propDesc = properties[i];
			il.Emit(OpCodes.Ldloc, bufferVar);
			if (offset > 0)
			{
				il.Emit(OpCodes.Ldc_I4, offset);
				il.Emit(OpCodes.Add);
			}

			PropertyInfo pi = FindProperty(TargetType, propDesc.Name);

			bool initInPlace = false;
			if (pi != null)
			{
				il.Emit(isRefType ? OpCodes.Ldarg : OpCodes.Ldarga, 1);
				il.Emit(OpCodes.Call, pi.GetGetMethod());
			}
			else
			{
				BuiltInTypesHelper.GenerateLoadDefaultValue(il, propDesc.BuiltInType, out initInPlace);
			}

			if (!initInPlace)
			{
				OpCode storeOpCode = BuiltInTypesHelper.GetIndStoreInstruction(propDesc.BuiltInType);
				if (storeOpCode == OpCodes.Stobj)
				{
					il.Emit(OpCodes.Stobj, BuiltInTypesHelper.From(propDesc.BuiltInType));
				}
				else
				{
					il.Emit(storeOpCode);
				}
			}

			offset += BuiltInTypesHelper.GetSimpleSize(propDesc.BuiltInType);
			i++;
		}

		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldloc, offsetVar);
		il.Emit(OpCodes.Ldc_I4, offset);
		il.Emit(OpCodes.Add);
		il.Emit(OpCodes.Stfld, Methods.MessageWriterOffsetFld);

		while (i < properties.Count)
		{
			ProtocolPropertyDescriptor propDesc = properties[i];
			if (propDesc.BuiltInType != BuiltInType.None)
				GenerateBuiltInTypeSerializer(il, propDesc);
			else
				GenerateProtocolTypeSerializer(il, propDesc, serializerManager, depthVar);

			i++;
		}

		il.Emit(OpCodes.Br, skipSlowCaseLabel);
		il.MarkLabel(skipFastCaseLabel);

		for (i = 0; i < properties.Count; i++)
		{
			ProtocolPropertyDescriptor propDesc = properties[i];
			if (propDesc.BuiltInType != BuiltInType.None)
				GenerateBuiltInTypeSerializer(il, propDesc);
			else
				GenerateProtocolTypeSerializer(il, propDesc, serializerManager, depthVar);
		}

		il.MarkLabel(skipSlowCaseLabel);
	}

	private void GenerateProtocolTypeDeserializer(ILGenerator il, ProtocolPropertyDescriptor propDesc,
		DeserializerManager deserializerManager, LocalBuilder instanceVar, LocalBuilder depthVar)
	{
		PropertyInfo pi = FindProperty(TargetType, propDesc.Name);
		TypeDeserializerEntry deserializer = deserializerManager.GetTypeEntryByName(propDesc.TypeDesc.Name);

		if (deserializer.TypeDesc.Type != ProtocolTypeType.Class || !((ProtocolClassDescriptor)deserializer.TypeDesc).CanBeInherited)
		{
			if (pi != null)
				il.Emit(IsRefType ? OpCodes.Ldloc : OpCodes.Ldloca, instanceVar);       // Load object so we can write property

			il.Emit(OpCodes.Ldarg_0);			// Load reader
			il.Emit(OpCodes.Ldarg_1);			// Load context
			il.Emit(OpCodes.Ldarg_2);			// Load deserializer table
			il.Emit(OpCodes.Ldloc, depthVar);	// Load depth
			il.Emit(OpCodes.Call, deserializer.Method);

			if (pi != null)
			{
				il.Emit(OpCodes.Call, pi.GetSetMethod());   // Store object in the property
			}
			else
			{
				il.Emit(OpCodes.Pop);
			}
		}
		else
		{
			if (pi != null)
				il.Emit(IsRefType ? OpCodes.Ldloc : OpCodes.Ldloca, instanceVar);       // Load object so we can write property

			il.Emit(OpCodes.Ldarg_0);			// Load reader
			il.Emit(OpCodes.Ldarg_1);			// Load context
			il.Emit(OpCodes.Ldarg_2);			// Load deserializer table
			il.Emit(OpCodes.Ldloc, depthVar);   // Load depth
			il.Emit(OpCodes.Call, Methods.DeserializePolymorphMethod);

			if (pi != null)
			{
				il.Emit(OpCodes.Call, pi.GetSetMethod());   // Store object in the property
			}
			else
			{
				il.Emit(OpCodes.Pop);
			}
		}
	}

	private void GenerateBuiltInTypeDeserializer(ILGenerator il, ProtocolPropertyDescriptor propDesc, LocalBuilder instanceVar)
	{
		PropertyInfo pi = FindProperty(TargetType, propDesc.Name);

		if (pi != null)
			il.Emit(isRefType ? OpCodes.Ldloc : OpCodes.Ldloca, instanceVar);

		il.Emit(OpCodes.Ldarg, 0);
		il.Emit(OpCodes.Call, BuiltInTypesHelper.GetReadMethod(propDesc.BuiltInType));

		if (pi != null)
			il.Emit(OpCodes.Call, pi.GetSetMethod());
		else
			il.Emit(OpCodes.Pop);
	}

	private void GenerateBuiltInTypeSerializer(ILGenerator il, ProtocolPropertyDescriptor propDesc)
	{
		PropertyInfo pi = FindProperty(TargetType, propDesc.Name);

		il.Emit(OpCodes.Ldarg_0);   // Load writer

		if (pi != null)
		{
			MethodInfo getter = pi.GetGetMethod();
			il.Emit(IsRefType ? OpCodes.Ldarg : OpCodes.Ldarga, 1);       // Load object so we can read property
			il.Emit(OpCodes.Call, getter);
		}
		else
		{
			if (BuiltInTypesHelper.IsInitInPlace(propDesc.BuiltInType))
			{
				LocalBuilder l = il.DeclareLocal(BuiltInTypesHelper.From(propDesc.BuiltInType));
				il.Emit(OpCodes.Ldloca, l);
				il.Emit(OpCodes.Initobj, BuiltInTypesHelper.From(propDesc.BuiltInType));
				il.Emit(OpCodes.Ldloc, l);
			}
			else
			{
				BuiltInTypesHelper.GenerateLoadDefaultValue(il, propDesc.BuiltInType, out _);
			}
		}

		// Write property value directly to writer
		il.Emit(OpCodes.Call, BuiltInTypesHelper.GetWriteMethod(propDesc.BuiltInType));
	}

	private void GenerateProtocolTypeSerializer(ILGenerator il,
		ProtocolPropertyDescriptor propDesc, SerializerManager serializerManager, LocalBuilder depthVar)
	{
		PropertyInfo pi = FindProperty(TargetType, propDesc.Name);

		if (pi != null)
		{
			TypeSerializerEntry serializer = serializerManager.GetTypeSerializer(pi.PropertyType);

			il.Emit(OpCodes.Ldarg_0);   // Load writer

			MethodInfo getter = pi.GetGetMethod();
			il.Emit(IsRefType ? OpCodes.Ldarg : OpCodes.Ldarga, 1);       // Load object so we can read property
			il.Emit(OpCodes.Call, getter);

			il.Emit(OpCodes.Ldarg_2);			// Load context
			il.Emit(OpCodes.Ldloc, depthVar);   // Load depth

			if (propDesc.TypeDesc.Type != ProtocolTypeType.Class || !((ProtocolClassDescriptor)propDesc.TypeDesc).CanBeInherited)
			{
				il.Emit(OpCodes.Call, serializer.Method);
			}
			else
			{
				il.Emit(OpCodes.Call, Methods.SerializePolymorphMethod);
			}
		}
		else
		{
			if (propDesc.TypeDesc.Type != ProtocolTypeType.Class || !((ProtocolClassDescriptor)propDesc.TypeDesc).CanBeInherited)
			{
				GenerateDefaultSerializerCode(il, propDesc.TypeDesc);
			}
			else
			{
				il.Emit(OpCodes.Ldarg_0);			// Load writer
				il.Emit(OpCodes.Ldnull);
				il.Emit(OpCodes.Ldarg_2);           // Load context
				il.Emit(OpCodes.Ldloc, depthVar);   // Load depth
				il.Emit(OpCodes.Call, Methods.SerializePolymorphMethod);
			}
		}
	}

	public static string GetClassName(Type type)
	{
		DbAPITypeAttribute cta = (DbAPITypeAttribute)type.GetCustomAttribute(typeof(DbAPITypeAttribute));
		if (cta == null || cta.Name == null)
			return type.FullName;

		return cta.Name;
	}

	private sealed class PropertyComparer : IComparer<ProtocolPropertyDescriptor>
	{
		public int Compare(ProtocolPropertyDescriptor x, ProtocolPropertyDescriptor y)
		{
			bool simpleType1 = BuiltInTypesHelper.IsSimple(x.BuiltInType);
			bool simpleType2 = BuiltInTypesHelper.IsSimple(y.BuiltInType);

			if (simpleType1 && !simpleType2)
				return -1;

			if (!simpleType1 && simpleType2)
				return 1;

			return x.Name.CompareTo(y.Name);
		}
	}
}
