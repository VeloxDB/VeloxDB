using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using Velox.Common;
using Velox.Config;
using Velox.Networking;

namespace Velox.Protocol;

internal sealed class ProtocolArrayDescriptor : ProtocolTypeDescriptor
{
	BuiltInType elementBuiltInType;
	ProtocolTypeDescriptor elementTypeDesc;

	public ProtocolArrayDescriptor() :
		base()
	{
	}

	public ProtocolArrayDescriptor(ushort id, Type type) :
		base(id, null, type)
	{
	}

	public override void Init(Type type, ProtocolDiscoveryContext discoveryContext, bool isInput)
	{
		Type elementType = type.GetElementType();
		if (BuiltInTypesHelper.IsBuiltInType(elementType))
		{
			elementBuiltInType = BuiltInTypesHelper.To(elementType);
			base.TargetType = elementType.MakeArrayType();
			base.Name = $"Array<{elementType.Name}>";
		}
		else
		{
			elementTypeDesc = discoveryContext.GetTypeDuringDiscovery(elementType, isInput);
			elementTypeDesc.AddComplexType(this);
			base.Name = $"Array<{elementTypeDesc.Name}>";
		}
	}

	public override void DiscoverTypes(Type type, ProtocolDiscoveryContext discoveryContext, bool isInput)
	{
		Type elementType = type.GetElementType();
		if (!BuiltInTypesHelper.IsBuiltInType(elementType))
			discoveryContext.GetTypeDuringDiscovery(elementType, isInput);
	}

	public override ProtocolTypeType Type => ProtocolTypeType.Array;
	public ProtocolTypeDescriptor ElementTypeDesc => elementTypeDesc;
	public override bool IsValueType => false;
	public override bool IsAbstract => false;
	public BuiltInType ElementBuiltInType => elementBuiltInType;

	protected override void ElementTypeSet()
	{
		base.TargetType = elementTypeDesc.TargetType.MakeArrayType();
	}

	protected override void OnSerialize(MessageWriter writer, SerializerContext context)
	{
		writer.WriteByte((byte)elementBuiltInType);
		ProtocolTypeDescriptor.Serialize(elementTypeDesc, writer, context);
	}

	protected override void OnDeserialize(MessageReader reader, DeserializerContext context)
	{
		elementBuiltInType = (BuiltInType)reader.ReadByte();
		if (!BuiltInTypesHelper.IsValidValue(elementBuiltInType))
			throw new DbAPIProtocolException();

		elementTypeDesc = ProtocolTypeDescriptor.Deserialize(reader, context);

		if (elementBuiltInType != BuiltInType.None)
			base.TargetType = BuiltInTypesHelper.From(elementBuiltInType).MakeArrayType();
	}

	public override void GenerateSerializerCode(ILGenerator il, SerializerManager serializerManager)
	{
		if (elementBuiltInType != BuiltInType.None)
		{
			il.Emit(OpCodes.Ldarg_0);   // Load writer
			il.Emit(OpCodes.Ldarg_1);   // Load array
			il.Emit(OpCodes.Call, BuiltInTypesHelper.GetWriteArrayMethod(elementBuiltInType));
		}
		else
		{
			// Increase depth
			LocalBuilder depthVar = il.DeclareLocal(typeof(int));
			il.Emit(OpCodes.Ldarg_3);   // Prev depth
			il.Emit(OpCodes.Ldc_I4_1);
			il.Emit(OpCodes.Add);
			il.Emit(OpCodes.Stloc, depthVar);

			// Check if we have been serialized before
			il.Emit(OpCodes.Ldarg_2);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Ldloc, depthVar);
			il.Emit(OpCodes.Ldc_I4_0);
			il.Emit(OpCodes.Call, Methods.SerializerContextTryWriteMethod);

			Label earlyExitSerLabel = il.DefineLabel();
			il.Emit(OpCodes.Brfalse, earlyExitSerLabel);
			il.Emit(OpCodes.Ret);

			il.MarkLabel(earlyExitSerLabel);

			Type elementType = TargetType.GetElementType();
			TypeSerializerEntry entry = serializerManager.GetTypeSerializer(elementType);

			LocalBuilder lengthVar = il.DeclareLocal(typeof(int));
			il.Emit(OpCodes.Ldarg_1);   // Load array
			il.Emit(OpCodes.Call, Methods.ArrayLengthMethod);
			il.Emit(OpCodes.Stloc, lengthVar);

			// We need to write array header
			il.Emit(OpCodes.Ldarg_0);   // Load writer
			il.Emit(OpCodes.Ldloc, lengthVar);   // Load array length
			il.Emit(OpCodes.Call, BuiltInTypesHelper.GetWriteMethod(BuiltInType.Int));

			Methods.GenerateForLoop(il, iVar =>
			{
				il.Emit(OpCodes.Ldarg_0);   // Load writer

				// Load array element
				il.Emit(OpCodes.Ldarg_1);
				il.Emit(OpCodes.Ldloc, iVar);

				if (elementTypeDesc.IsValueType)
				{
					il.Emit(OpCodes.Ldelem, elementType);
				}
				else
				{
					il.Emit(OpCodes.Ldelem_Ref);
				}

				il.Emit(OpCodes.Ldarg_2);   // Load context
				il.Emit(OpCodes.Ldloc, depthVar);   // Load depth

				if (elementTypeDesc.Type != ProtocolTypeType.Class || !((ProtocolClassDescriptor)elementTypeDesc).CanBeInherited)
				{
					il.Emit(OpCodes.Call, entry.Method);
				}
				else
				{
					il.Emit(OpCodes.Call, Methods.SerializePolymorphMethod);
				}
			}, lengthVar);
		}

		il.Emit(OpCodes.Ret);
	}

	public override void GenerateDeserializerCode(ILGenerator il, DeserializerManager deserializerManager)
	{
		LocalBuilder instanceVar = il.DeclareLocal(TargetType ?? typeof(object));

		if (elementBuiltInType != BuiltInType.None)
		{
			Checker.AssertNotNull(TargetType != null); // Array of simple types is always available
			il.Emit(OpCodes.Ldarg_0);   // Load reader
			if (TargetType == null || !TargetType.GetElementType().IsEnum)
			{
				il.Emit(OpCodes.Call, BuiltInTypesHelper.GetReadArrayMethod(elementBuiltInType));
			}
			else
			{
				FieldInfo factField = BuildFactory(deserializerManager);
				il.Emit(OpCodes.Ldsfld, factField);
				il.Emit(OpCodes.Call, BuiltInTypesHelper.GetReadEnumArrayMethod(elementBuiltInType));
			}

			il.Emit(OpCodes.Stloc, instanceVar);
		}
		else
		{
			// Increase depth
			LocalBuilder depthVar = il.DeclareLocal(typeof(int));
			il.Emit(OpCodes.Ldarg_3);   // Prev depth
			il.Emit(OpCodes.Ldc_I4_1);
			il.Emit(OpCodes.Add);
			il.Emit(OpCodes.Stloc, depthVar);

			// Check if we have been deserialized before
			il.Emit(OpCodes.Ldarg_1);
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldloca, instanceVar);
			il.Emit(OpCodes.Ldloc, depthVar);
			il.Emit(OpCodes.Call, Methods.DeserializerContextTryReadMethod);

			Label earlyExitDeserLabel = il.DefineLabel();
			il.Emit(OpCodes.Ldc_I4, (int)ReadObjectResult.Deserialize);
			il.Emit(OpCodes.Ceq);
			il.Emit(OpCodes.Brtrue, earlyExitDeserLabel);

			if (TargetType != null)
				il.Emit(OpCodes.Ldloc, instanceVar);

			il.Emit(OpCodes.Ret);

			il.MarkLabel(earlyExitDeserLabel);

			Type elementType = TargetType == null ? null : TargetType.GetElementType();
			TypeDeserializerEntry entry = deserializerManager.GetTypeEntryByName(ElementTypeDesc.Name);

			// We need to read array header
			LocalBuilder lengthVar = il.DeclareLocal(typeof(int));
			il.Emit(OpCodes.Ldarg_0);   // Load reader
			il.Emit(OpCodes.Call, BuiltInTypesHelper.GetReadMethod(BuiltInType.Int));
			il.Emit(OpCodes.Stloc, lengthVar);

			if (TargetType != null)
			{
				il.Emit(OpCodes.Ldloc, lengthVar);
				il.Emit(OpCodes.Newarr, elementType);
				il.Emit(OpCodes.Stloc, instanceVar);

				// Add object to deserialized context
				il.Emit(OpCodes.Ldarg_1);
				il.Emit(OpCodes.Ldloc, instanceVar);
				il.Emit(OpCodes.Call, Methods.DeserializerContextAddMethod);
			}
			else
			{
				// Add null to deserialized context (since we are not keeping the object)
				il.Emit(OpCodes.Ldarg_1);
				il.Emit(OpCodes.Ldnull);
				il.Emit(OpCodes.Call, Methods.DeserializerContextAddMethod);
			}

			Methods.GenerateForLoop(il, iVar =>
			{
				if (TargetType != null)
				{
					il.Emit(OpCodes.Ldloc, instanceVar);
					il.Emit(OpCodes.Ldloc, iVar);
				}

				if (entry.TypeDesc.Type != ProtocolTypeType.Class || !((ProtocolClassDescriptor)entry.TypeDesc).CanBeInherited)
				{
					il.Emit(OpCodes.Ldarg_0);           // Load reader
					il.Emit(OpCodes.Ldarg_1);           // Load context
					il.Emit(OpCodes.Ldarg_2);           // Load deserializer tables
					il.Emit(OpCodes.Ldloc, depthVar);   // Load depth
					il.Emit(OpCodes.Call, entry.Method);
				}
				else
				{
					il.Emit(OpCodes.Ldarg_0);           // Load reader
					il.Emit(OpCodes.Ldarg_1);           // Load context
					il.Emit(OpCodes.Ldarg_2);           // Load deserializer tables
					il.Emit(OpCodes.Ldloc, depthVar);  // Load depth
					il.Emit(OpCodes.Call, Methods.DeserializePolymorphMethod);
				}

				if (TargetType != null)
				{
					// Store object in the array
					if (entry.TypeDesc.IsValueType)
					{
						il.Emit(OpCodes.Stelem, elementType);
					}
					else
					{
						il.Emit(OpCodes.Stelem_Ref);
					}
				}
				else
				{
					il.Emit(OpCodes.Pop); // throw away the value
				}
			}, lengthVar);
		}

		if (TargetType != null)
			il.Emit(OpCodes.Ldloc, instanceVar);

		il.Emit(OpCodes.Ret);
	}

	private FieldInfo BuildFactory(DeserializerManager deserializerManager)
	{
		TypeBuilder tb = deserializerManager.ModuleBuilder.DefineType("__" + Guid.NewGuid().ToString("N"),
				TypeAttributes.Class | TypeAttributes.Public, typeof(object));

		Type elementType = TargetType.GetElementType();
		Type arrayType = BuiltInTypesHelper.From(elementBuiltInType).MakeArrayType();
		Type factType = typeof(Func<,>).MakeGenericType(new Type[] { typeof(int), arrayType });
		FieldBuilder fb = tb.DefineField("fact", factType, FieldAttributes.Static | FieldAttributes.Public);

		MethodBuilder method = tb.DefineMethod("Create", MethodAttributes.Static | MethodAttributes.Public, arrayType, new Type[] { typeof(int) });
		ILGenerator il = method.GetILGenerator();

		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Newarr, elementType);
		il.Emit(OpCodes.Ret);

		Type type = tb.CreateType();
		MethodInfo mi = type.GetMethod("Create", BindingFlags.Static | BindingFlags.DeclaredOnly | BindingFlags.Public);
		FieldInfo fi = type.GetField("fact", BindingFlags.Static | BindingFlags.DeclaredOnly | BindingFlags.Public);

		Delegate fact = Delegate.CreateDelegate(factType, mi);
		fi.SetValue(null, fact);

		return fi;
	}

	protected override bool IsMatchInternal(ProtocolTypeDescriptor otherDesc, Dictionary<ProtocolTypeDescriptor, bool> checkedTypes)
	{
		ProtocolArrayDescriptor arrayDesc = otherDesc as ProtocolArrayDescriptor;
		if (arrayDesc == null)
			return false;

		if (elementBuiltInType != arrayDesc.elementBuiltInType)
			return false;

		if (elementTypeDesc == null && arrayDesc.elementTypeDesc == null)
			return true;

		if (elementTypeDesc == null || arrayDesc.elementTypeDesc == null)
			return false;

		return elementTypeDesc.IsMatch(arrayDesc.elementTypeDesc, checkedTypes);
	}
}
