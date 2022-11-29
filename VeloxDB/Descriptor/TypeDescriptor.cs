using System;
using System.IO;
using System.Xml;

namespace Velox.Descriptor;

internal abstract class TypeDescriptor : ModelItemDescriptor
{
	string name;
	string fullName;

	NamespaceDescriptor @namespace;

	public TypeDescriptor()
	{
	}

	public TypeDescriptor(XmlReader reader, NamespaceDescriptor @namespace)
	{
		this.@namespace = @namespace;

		reader.Read();
		name = reader.GetAttribute("Name");
		fullName = @namespace.Name + "." + name;
	}

	public TypeDescriptor(string name, NamespaceDescriptor @namespace)
	{
		this.name = name;
		this.@namespace = @namespace;
		fullName = @namespace.Name + "." + name;
	}

	public TypeDescriptor(string name)
	{
		this.name = name;
	}

	public string Name => name;
	public string FullName => fullName;
	public DataModelDescriptor Model => @namespace.Model;
	public string NamespaceName => @namespace.Name;

	public NamespaceDescriptor Namespace
	{
		get => @namespace;
		set
		{
			@namespace = value;
			fullName = @namespace.Name + "." + name;
		}
	}

	public override void Serialize(BinaryWriter writer, ModelDescriptorSerializerContext context)
	{
		writer.Write(name);
		context.Serialize(@namespace, writer);
	}

	public override void Deserialize(BinaryReader reader, ModelDescriptorDeserializerContext context)
	{
		name = reader.ReadString();
		@namespace = context.Deserialize<NamespaceDescriptor>(reader);
		fullName = @namespace.Name + "." + name;
	}
}
