using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Xml;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal sealed class ArrayPropertyDescriptor : PropertyDescriptor
{
	public ArrayPropertyDescriptor()
	{
	}

	public ArrayPropertyDescriptor(ObjectModelProperty objectModelProperty, ClassDescriptor ownerClass) :
	base(objectModelProperty, ownerClass)
	{
		base.PropertyType = objectModelProperty.PropertyType;
		if (base.PropertyType == PropertyType.None)
			Throw.InvalidPropertyType(ownerClass.FullName, this.Name);
	}

	public ArrayPropertyDescriptor(string name, int id, PropertyType propertyType) :
		base(name, id, propertyType)
	{
	}

	public ArrayPropertyDescriptor(XmlReader reader, ClassDescriptor ownerClass) :
		base(reader, ownerClass)
	{
		string type = reader.GetAttribute("Type");
		base.PropertyType = PropertyTypesHelper.GetPropertyType(type);
		reader.Close();
	}

	public override ModelItemType Type => ModelItemType.ArrayProperty;
	public override PropertyKind Kind => PropertyKind.Array;
	public override object DefaultValue => null;

	public override void Serialize(BinaryWriter writer, ModelDescriptorSerializerContext context)
	{
		base.Serialize(writer, context);
	}

	public override void Deserialize(BinaryReader reader, ModelDescriptorDeserializerContext context)
	{
		base.Deserialize(reader, context);
	}
}
