using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using VeloxDB.Common;
using VeloxDB.Networking;

namespace VeloxDB.Protocol;

internal enum ProtocolInterfaceDirection
{
	Request,
	Response
}

internal sealed class ProtocolInterfaceDescriptor
{
	ushort id;
	string name;

	ProtocolTypeDescriptor[] inTypes;
	ProtocolTypeDescriptor[] outTypes;

	ProtocolOperationDescriptor[] operations;
	Dictionary<string, ProtocolOperationDescriptor> operationsByName;
	ProtocolOperationDescriptor[] operationsById;

	Dictionary<string, ProtocolTypeDescriptor> inTypesByName;
	Dictionary<string, ProtocolTypeDescriptor> outTypesByName;

	Type targetType;

	private ProtocolInterfaceDescriptor()
	{
	}

	public ProtocolInterfaceDescriptor(ushort id, Type type, ProtocolOperationDescriptor[] operations)
	{
		this.id = id;
		this.name = GetAPIName(type);
		this.operations = operations;

		operationsByName = new Dictionary<string, ProtocolOperationDescriptor>(operations.Length);
		operationsById = new ProtocolOperationDescriptor[operations.Select(x => (int)x.Id).DefaultIfEmpty(-1).Max() + 1];
		for (int i = 0; i < operations.Length; i++)
		{
			if (operationsByName.ContainsKey(operations[i].Name))
				throw DbAPIDefinitionException.CreateOperationNameDuplicate(operations[i].Name, this.name);

			operationsByName.Add(operations[i].Name, operations[i]);
			operationsById[operations[i].Id] = operations[i];
		}

		this.targetType = type;
	}

	public string Name => name;
	public ProtocolOperationDescriptor[] Operations => operations;
	public ushort Id => id;
	public Type TargetType { get => targetType; set => targetType = value; }
	internal ProtocolTypeDescriptor[] InTypes => inTypes;
	internal ProtocolTypeDescriptor[] OutTypes => outTypes;

	public ProtocolTypeDescriptor GetType(string name, bool isInput)
	{
		ProtocolTypeDescriptor typeDesc;
		if (isInput)
			inTypesByName.TryGetValue(name, out typeDesc);
		else
			outTypesByName.TryGetValue(name, out typeDesc);

		return typeDesc;
	}

	public void SetTypes(ProtocolTypeDescriptor[] inTypes, ProtocolTypeDescriptor[] outTypes)
	{
		this.inTypes = inTypes;
		this.outTypes = outTypes;
	}

	public ProtocolOperationDescriptor GetOperationByName(string name)
	{
		operationsByName.TryGetValue(name, out ProtocolOperationDescriptor desc);
		return desc;
	}

	public void PrepareTypeMaps()
	{
		inTypesByName = new Dictionary<string, ProtocolTypeDescriptor>(inTypes.Length);
		for (int i = 0; i < inTypes.Length; i++)
		{
			inTypesByName.Add(inTypes[i].Name, inTypes[i]);
		}

		outTypesByName = new Dictionary<string, ProtocolTypeDescriptor>(outTypes.Length);
		for (int i = 0; i < outTypes.Length; i++)
		{
			outTypesByName.Add(outTypes[i].Name, outTypes[i]);
		}
	}

	public ProtocolOperationDescriptor GetOperationById(int id)
	{
		return operationsById[id];
	}

	public void Serialize(MessageWriter writer, SerializerContext context)
	{
		writer.WriteUShort(id);
		writer.WriteString(name);
		writer.WriteInt(operations.Length);
		for (int i = 0; i < operations.Length; i++)
		{
			ProtocolOperationDescriptor.Serialize(operations[i], writer, context);
		}

		writer.WriteInt(inTypes.Length);
		for (int i = 0; i < inTypes.Length; i++)
		{
			ProtocolTypeDescriptor.Serialize(inTypes[i], writer, context);
		}

		writer.WriteInt(outTypes.Length);
		for (int i = 0; i < outTypes.Length; i++)
		{
			ProtocolTypeDescriptor.Serialize(outTypes[i], writer, context);
		}
	}

	public static ProtocolInterfaceDescriptor Deserialize(MessageReader reader, DeserializerContext context)
	{
		ProtocolInterfaceDescriptor d = new ProtocolInterfaceDescriptor();
		d.id = reader.ReadUShort();
		d.name = reader.ReadString();

		int c = reader.ReadInt();
		d.operations = new ProtocolOperationDescriptor[c];
		for (int i = 0; i < c; i++)
		{
			d.operations[i] = ProtocolOperationDescriptor.Deserialize(reader, context);
		}

		d.operationsByName = new Dictionary<string, ProtocolOperationDescriptor>(d.operations.Length);
		d.operationsById = new ProtocolOperationDescriptor[d.operations.Select(x => (int)x.Id).DefaultIfEmpty(-1).Max() + 1];
		for (int i = 0; i < d.operations.Length; i++)
		{
			d.operationsByName.Add(d.operations[i].Name, d.operations[i]);
			d.operationsById[d.operations[i].Id] = d.operations[i];
		}

		c = reader.ReadInt();
		d.inTypes = new ProtocolTypeDescriptor[c];
		for (int i = 0; i < c; i++)
		{
			d.inTypes[i] = ProtocolTypeDescriptor.Deserialize(reader, context);
		}

		c = reader.ReadInt();
		d.outTypes = new ProtocolTypeDescriptor[c];
		for (int i = 0; i < c; i++)
		{
			d.outTypes[i] = ProtocolTypeDescriptor.Deserialize(reader, context);
		}

		return d;
	}

	public static string GetAPIName(Type type)
	{
		DbAPIAttribute cta = (DbAPIAttribute)type.GetCustomAttribute(typeof(DbAPIAttribute));
		if (cta == null || cta.Name == null)
			return type.FullName;

		return cta.Name;
	}
}
