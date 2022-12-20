using System;
using System.Collections.Generic;
using VeloxDB.Networking;

namespace VeloxDB.Protocol;

internal sealed class ProtocolDescriptor
{
	const ushort formatVersion = 1;

	Guid guid;
	ProtocolInterfaceDescriptor[] interfaces;
	Dictionary<string, ProtocolInterfaceDescriptor> interfacesByName;

	public ProtocolDescriptor(Guid guid, ProtocolInterfaceDescriptor[] interfaces)
	{
		this.guid = guid;
		this.interfaces = interfaces;
	}

	public Guid Guid => guid;
	internal ProtocolInterfaceDescriptor[] Interfaces => interfaces;

	public ProtocolInterfaceDescriptor GetInterface(string name)
	{
		interfacesByName.TryGetValue(name, out var interfaceDesc);
		return interfaceDesc;
	}

	public void PrepareMaps()
	{
		interfacesByName = new Dictionary<string, ProtocolInterfaceDescriptor>(interfaces.Length);
		for (int i = 0; i < interfaces.Length; i++)
		{
			interfaces[i].PrepareTypeMaps();
			interfacesByName.Add(interfaces[i].Name, interfaces[i]);
		}
	}

	public void Serialize(MessageWriter writer)
	{
		SerializerContext context = new SerializerContext();

		writer.WriteUShort(formatVersion);
		writer.WriteGuid(guid);

		writer.WriteInt(interfaces.Length);
		for (int i = 0; i < interfaces.Length; i++)
		{
			interfaces[i].Serialize(writer, context);
		}
	}

	public static ProtocolDescriptor Deserialize(MessageReader reader)
	{
		DeserializerContext context = new DeserializerContext();

		if (reader.ReadUShort() > formatVersion)
			throw new NotSupportedException();

		Guid guid = reader.ReadGuid();

		int c = reader.ReadInt();
		ProtocolInterfaceDescriptor[] interfaces = new ProtocolInterfaceDescriptor[c];
		for (int i = 0; i < c; i++)
		{
			interfaces[i] = ProtocolInterfaceDescriptor.Deserialize(reader, context);
		}

		return new ProtocolDescriptor(guid, interfaces);
	}
}
