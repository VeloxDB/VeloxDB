using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using VeloxDB.Common;
using VeloxDB.Networking;

namespace VeloxDB.Protocol;

internal abstract class ProtocolTypeDescriptor
{
	public enum ProtocolTypeType
	{
		MinValue = 1,
		Class = 1,
		Array = 2,
		List = 3,
		MaxValue = 3,
	}

	static Func<ProtocolTypeDescriptor>[] creators;

	ushort id;
	string name;
	ProtocolTypeDescriptor[] complexTypesFromThis;

	Type targetType;

	static ProtocolTypeDescriptor()
	{
		creators = new Func<ProtocolTypeDescriptor>[Utils.MaxEnumValue(typeof(ProtocolTypeType)) + 1];
		creators[(int)ProtocolTypeType.Class] = () => new ProtocolClassDescriptor();
		creators[(int)ProtocolTypeType.Array] = () => new ProtocolArrayDescriptor();
		creators[(int)ProtocolTypeType.List] = () => new ProtocolListDescriptor();
	}

	public ProtocolTypeDescriptor()
	{
	}

	public ProtocolTypeDescriptor(ushort id, string name, Type type)
	{
		this.id = id;
		this.name = name;
		this.TargetType = type;
		this.complexTypesFromThis = EmptyArray<ProtocolTypeDescriptor>.Instance;
	}

	public ushort Id { get => id; protected set => id = value; }
	public abstract ProtocolTypeType Type { get; }
	public abstract bool IsValueType { get; }
	public abstract bool IsAbstract { get; }
	public string Name { get => name; set => name = value; }

	public Type TargetType
	{
		get => targetType;
		set
		{
			targetType = value;
			if (complexTypesFromThis != null)
			{
				for (int i = 0; i < complexTypesFromThis.Length; i++)
				{
					complexTypesFromThis[i].ElementTypeSet();
				}
			}
		}
	}

	public abstract void Init(Type type, ProtocolDiscoveryContext discoveryContext, bool isInput);
	public abstract void DiscoverTypes(Type type, ProtocolDiscoveryContext discoveryContext, bool isInput);
	protected abstract void OnSerialize(MessageWriter writer, SerializerContext context);
	protected abstract void OnDeserialize(MessageReader reader, DeserializerContext context);

	public void AddComplexType(ProtocolTypeDescriptor typeDesc)
	{
		if (complexTypesFromThis == null)
		{
			complexTypesFromThis = new ProtocolTypeDescriptor[] { typeDesc };
		}
		else
		{
			Array.Resize(ref complexTypesFromThis, complexTypesFromThis.Length + 1);
			complexTypesFromThis[complexTypesFromThis.Length - 1] = typeDesc;
		}

		if (targetType != null)
			typeDesc.ElementTypeSet();
	}

	protected virtual void ElementTypeSet()
	{
	}

	public static void Serialize(ProtocolTypeDescriptor desc, MessageWriter writer, SerializerContext context)
	{
		if (context.TryWriteInstance(writer, desc, 0, true))
			return;

		writer.WriteByte((byte)desc.Type);
		writer.WriteUShort(desc.id);
		writer.WriteString(desc.name);

		writer.WriteByte((byte)desc.complexTypesFromThis.Length);
		for (int i = 0; i < desc.complexTypesFromThis.Length; i++)
		{
			Serialize(desc.complexTypesFromThis[i], writer, context);
		}

		desc.OnSerialize(writer, context);
	}

	public static ProtocolTypeDescriptor Deserialize(MessageReader reader, DeserializerContext context)
	{
		if (context.TryReadInstanceTyped(reader, out ProtocolTypeDescriptor typeDesc, 0) == ReadObjectResult.Ready)
			return typeDesc;

		ProtocolTypeType type = (ProtocolTypeType)reader.ReadByte();
		if (type < ProtocolTypeType.MinValue || type > ProtocolTypeType.MaxValue)
			throw new DbAPIProtocolException();

		typeDesc = creators[(int)type]();
		context.AddInstance(typeDesc);

		typeDesc.id = reader.ReadUShort();
		typeDesc.name = reader.ReadString();
		if (string.IsNullOrEmpty(typeDesc.name))
			throw new DbAPIProtocolException();

		int c = reader.ReadByte();
		typeDesc.complexTypesFromThis = EmptyArray<ProtocolTypeDescriptor>.Create(c);
		for (int i = 0; i < c; i++)
		{
			typeDesc.complexTypesFromThis[i] = Deserialize(reader, context);
		}

		typeDesc.OnDeserialize(reader, context);

		return typeDesc;
	}

	public abstract void GenerateSerializerCode(ILGenerator il, SerializerManager serializerManager);
	public abstract void GenerateDeserializerCode(ILGenerator il, DeserializerManager deserializerManager);

	protected abstract bool IsMatchInternal(ProtocolTypeDescriptor otherDesc, Dictionary<ProtocolTypeDescriptor, bool> checkedTypes);

	public bool IsMatch(ProtocolTypeDescriptor otherDesc, Dictionary<ProtocolTypeDescriptor, bool> checkedTypes)
	{
		if (checkedTypes.TryGetValue(this, out bool isMatch))
			return isMatch;

		checkedTypes.Add(this, true);
		isMatch = IsMatchInternal(otherDesc, checkedTypes);
		checkedTypes[this] = isMatch;
		return isMatch;
	}
}
