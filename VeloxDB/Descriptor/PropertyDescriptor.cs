using System;
using System.Xml;
using System.IO;
using System.Reflection;

namespace VeloxDB.Descriptor;

internal enum PropertyType : byte
{
	None = 0,
	Byte = 1,
	Short = 2,
	Int = 3,
	Long = 4,
	Float = 5,
	Double = 6,
	Bool = 7,
	DateTime = 8,
	String = 9,
	ByteArray = 50,
	ShortArray = 51,
	IntArray = 52,
	LongArray = 53,
	FloatArray = 54,
	DoubleArray = 55,
	BoolArray = 56,
	DateTimeArray = 57,
	StringArray = 58,
}

internal enum PropertyKind
{
	Simple = 1,
	Reference = 2,
	Array = 3
}

internal abstract class PropertyDescriptor : ModelItemDescriptor
{
	public const int VersionIndex = 0;
	public const int IdIndex = 1;

	string name;
	PropertyType propertyType;
	int id;
	ClassDescriptor ownerClass;

	public PropertyDescriptor()
	{
	}

	public PropertyDescriptor(XmlReader reader, ClassDescriptor ownerClass)
	{
		this.ownerClass = ownerClass;

		reader.Read();

		name = reader.GetAttribute("Name");
		id = int.Parse(reader.GetAttribute("Id"));
	}

	public PropertyDescriptor(ObjectModelProperty objectModelProperty, ClassDescriptor ownerClass)
	{
		this.ownerClass = ownerClass;
		name = objectModelProperty.PropertyInfo.Name;
		id = objectModelProperty.Id;
	}

	public PropertyDescriptor(string name, int id, PropertyType propertyType)
	{
		this.name = name;
		this.id = id;
		this.propertyType = propertyType;
	}

	public string Name => name;
	public int Id => id;
	public ClassDescriptor OwnerClass { get => ownerClass; set => ownerClass = value; }
	public bool IsBuiltin => id == SystemCode.DatabaseObject.IdProp || id == SystemCode.DatabaseObject.Version;

	public PropertyType PropertyType
	{
		get => propertyType;
		protected set
		{
			if (value <= PropertyType.None || (value > PropertyType.String && value < PropertyType.ByteArray) ||
				value > PropertyType.StringArray)
			{
				Throw.InvalidPropertyType(ownerClass.FullName, this.Name);
			}

			propertyType = value;
		}
	}

	public abstract PropertyKind Kind { get; }
	public abstract object DefaultValue { get; }

	public override string ToString()
	{
		return $"({PropertyType} Property {Name})";
	}

	public override void Serialize(BinaryWriter writer, ModelDescriptorSerializerContext context)
	{
		writer.Write(name);
		writer.Write(id);
		writer.Write((byte)propertyType);

		context.Serialize(ownerClass, writer);
	}

	public override void Deserialize(BinaryReader reader, ModelDescriptorDeserializerContext context)
	{
		name = reader.ReadString();
		id = reader.ReadInt32();
		propertyType = (PropertyType)reader.ReadByte();
		ownerClass = context.Deserialize<ClassDescriptor>(reader);
	}

	/// Maps the property id to an integer in such a way that Version property always ends up
	/// at index 0 and Id property ends up at index 1 when sorted inside the class. Also,
	/// string and blob properties are always placed after all the simple properties.
	public static long RemapPropId(PropertyDescriptor p)
	{
		if (p.Id == SystemCode.DatabaseObject.Version)
			return long.MinValue;
		else if (p.Id == SystemCode.DatabaseObject.IdProp)
			return long.MinValue + 1;
		else if (p.PropertyType == PropertyType.String || p.PropertyType >= PropertyType.ByteArray)
			return (long)int.MaxValue + p.Id;
		else
			return p.Id;
	}

	public static int ComparePropertiesByOrder(PropertyDescriptor p1, PropertyDescriptor p2)
	{
		long c1 = RemapPropId(p1);
		long c2 = RemapPropId(p2);

		if (c1 < c2)
			return -1;
		else if (c1 == c2)
			return 0;
		else
			return 1;
	}
}
