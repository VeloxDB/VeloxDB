using System;
using System.Xml;

namespace VeloxDB.Descriptor;

internal sealed class HashIndexDescriptor : IndexDescriptor
{
	public HashIndexDescriptor() :
		base()
	{
	}

	public HashIndexDescriptor(NamespaceDescriptor @namespace, ObjectModelHashIndex objectModelHashIndex) :
		base(@namespace, objectModelHashIndex)
	{
	}

	public HashIndexDescriptor(XmlReader reader, NamespaceDescriptor metaNamespace) :
		base(reader, metaNamespace)
	{
	}

	public HashIndexDescriptor(DataModelDescriptor modelDesc, string name, short id,
		string cultureName, bool caseSensitive, bool isUnique, string[] propertyNames) :
		base(modelDesc, name, id, cultureName, caseSensitive, isUnique, propertyNames)
	{
	}

	public override ModelItemType Type => ModelItemType.HashIndex;

	public override string ToString()
	{
		return $"(HashIndex {Name})";
	}
}
