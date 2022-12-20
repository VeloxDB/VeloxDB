using System;
using System.Collections.Generic;
using System.Reflection;
using VeloxDB.Common;
using VeloxDB.Networking;

namespace VeloxDB.Protocol;

internal sealed class ProtocolPropertyDescriptor
{
	BuiltInType builtInType;
	ProtocolTypeDescriptor typeDesc;
	string name;

	public ProtocolPropertyDescriptor()
	{
	}

	public ProtocolPropertyDescriptor(BuiltInType builtInType, ProtocolTypeDescriptor typeDesc, string name)
	{
		this.builtInType = builtInType;
		this.typeDesc = typeDesc;
		this.name = name;
	}

	public string Name => name;
	public BuiltInType BuiltInType => builtInType;
	public ProtocolTypeDescriptor TypeDesc => typeDesc;

	public static void Serialize(ProtocolPropertyDescriptor desc, MessageWriter writer, SerializerContext context)
	{
		if (context.TryWriteInstance(writer, desc, 0, true))
			return;

		writer.WriteByte((byte)desc.builtInType);
		ProtocolTypeDescriptor.Serialize(desc.typeDesc, writer, context);

		writer.WriteString(desc.name);
	}

	public static ProtocolPropertyDescriptor Deserialize(MessageReader reader, DeserializerContext context)
	{
		if (context.TryReadInstanceTyped(reader, out ProtocolPropertyDescriptor p, 0) == ReadObjectResult.Ready)
			return p;

		p = new ProtocolPropertyDescriptor();
		context.AddInstance(p);

		p.builtInType = (BuiltInType)reader.ReadByte();
		if (p.builtInType < BuiltInType.None || p.builtInType > BuiltInType.MaxValue)
			throw new DbAPIProtocolException();

		p.typeDesc = ProtocolTypeDescriptor.Deserialize(reader, context);

		p.name = reader.ReadString();
		if (string.IsNullOrEmpty(p.name))
			throw new DbAPIProtocolException();

		return p;
	}

	public bool IsMatch(ProtocolPropertyDescriptor propDesc, bool checkName, Dictionary<ProtocolTypeDescriptor, bool> checkedTypes)
	{
		if (checkName)
		{
			if (!name.Equals(propDesc.Name))
				return false;
		}

		if (builtInType != propDesc.builtInType)
			return false;

		if (typeDesc == null && propDesc.typeDesc == null)
			return true;

		if (typeDesc == null || propDesc.typeDesc == null)
			return false;

		return typeDesc.IsMatch(propDesc.typeDesc, checkedTypes);
	}
}
