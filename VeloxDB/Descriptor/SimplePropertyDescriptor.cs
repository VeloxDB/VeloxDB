using System;
using System.Globalization;
using System.Xml;
using Velox.Common;
using System.IO;
using System.Reflection;
using System.Collections.Generic;

namespace Velox.Descriptor;

internal sealed class SimplePropertyDescriptor : PropertyDescriptor
{
	static readonly Dictionary<string, PropertyType> stringToType;
	static readonly Dictionary<int, object> builtInDefaultValues;

	static readonly string[] supportedDateTimeFormats = new string[] {
		"yyyy-MM-dd HH:mm:ss.fff", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM-dd" };

	object defaultValue;

	static SimplePropertyDescriptor()
	{
		stringToType = new Dictionary<string, PropertyType>();
		for (PropertyType i = PropertyType.Byte; i <= PropertyType.String; i++)
		{
			stringToType.Add(i.ToString(), i);
		}

		builtInDefaultValues = new Dictionary<int, object>(16);
		builtInDefaultValues.Add((int)PropertyType.Byte, (byte)0);
		builtInDefaultValues.Add((int)PropertyType.Short, (short)0);
		builtInDefaultValues.Add((int)PropertyType.Int, (int)0);
		builtInDefaultValues.Add((int)PropertyType.Long, (long)0);
		builtInDefaultValues.Add((int)PropertyType.Float, 0.0f);
		builtInDefaultValues.Add((int)PropertyType.Double, 0.0);
		builtInDefaultValues.Add((int)PropertyType.Bool, false);
		builtInDefaultValues.Add((int)PropertyType.String, null);
		builtInDefaultValues.Add((int)PropertyType.DateTime, DateTime.FromBinary(0));
	}

	public SimplePropertyDescriptor()
	{
	}

	public SimplePropertyDescriptor(ObjectModelProperty objectModelProperty, ClassDescriptor ownerClass) :
		base(objectModelProperty, ownerClass)
	{
		base.PropertyType = objectModelProperty.PropertyType;

		if (objectModelProperty.DefaultValue != null)
		{
			ValidateAndParseDefaultValue(objectModelProperty.DefaultValue);
		}
		else
		{
			defaultValue = builtInDefaultValues[(int)base.PropertyType];
		}
	}

	public SimplePropertyDescriptor(string name, int id, PropertyType propertyType, object defaultValue) :
		base(name, id, propertyType)
	{
		this.defaultValue = defaultValue;
	}

	public SimplePropertyDescriptor(XmlReader reader, ClassDescriptor ownerClass) :
		base(reader, ownerClass)
	{
		string type = reader.GetAttribute("Type");
		base.PropertyType = stringToType[type];

		string defv = reader.GetAttribute("DefaultVal");
		if (defv != null)
		{
			ValidateAndParseDefaultValue(defv);
		}
		else
		{
			defaultValue = builtInDefaultValues[(int)base.PropertyType];
		}

		reader.Close();
	}

	public override ModelItemType Type => ModelItemType.SimpleProperty;
	public override PropertyKind Kind => PropertyKind.Simple;
	public override object DefaultValue => defaultValue;

	private void ValidateAndParseDefaultValue(string value)
	{
		if (PropertyType == PropertyType.String)
		{
			Throw.StringPropertyCantHaveDefaultValue(OwnerClass.FullName, Name);
		}

		if (!ParseDefaultValue(value, null, out this.defaultValue))
		{
			Throw.InvalidDefaultValue(OwnerClass.FullName, Name);
		}
	}

	private bool ParseDefaultValue(string defaultValueString, Type managedType, out object value)
	{
		try
		{
			if (managedType != null && managedType.IsEnum)
				return Enum.TryParse(managedType, defaultValueString, out value);

			switch (base.PropertyType)
			{
				case PropertyType.Byte:
					value = byte.Parse(defaultValueString, CultureInfo.InvariantCulture);
					break;

				case PropertyType.Short:
					value = short.Parse(defaultValueString, CultureInfo.InvariantCulture);
					break;

				case PropertyType.Int:
					value = int.Parse(defaultValueString, CultureInfo.InvariantCulture);
					break;

				case PropertyType.Long:
					value = long.Parse(defaultValueString, CultureInfo.InvariantCulture);
					break;

				case PropertyType.Float:
					value = float.Parse(defaultValueString, CultureInfo.InvariantCulture);
					break;

				case PropertyType.Double:
					value = double.Parse(defaultValueString, CultureInfo.InvariantCulture);
					break;

				case PropertyType.Bool:
					value = bool.Parse(defaultValueString);
					break;

				case PropertyType.DateTime:
					value = ParseDateTime(defaultValueString);
					break;

				case PropertyType.String:
					value = defaultValueString;
					break;

				default:
					throw new InvalidOperationException();  // Should never end up here
			}
		}
		catch (FormatException)
		{
			value = null;
			return false;
		}

		return true;
	}

	private static DateTime ParseDateTime(string val)
	{
		for (int i = 0; i < supportedDateTimeFormats.Length; i++)
		{
			if (DateTime.TryParseExact(val, supportedDateTimeFormats[i],
				CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out DateTime dt))
			{
				return dt.ToUniversalTime();
			}
		}

		throw new FormatException();
	}

	internal static void SerializePropertyValue(BinaryWriter writer, object value)
	{
		Type type = value.GetType();
		if (type == typeof(byte))
		{
			writer.Write((byte)PropertyType.Byte);
			writer.Write((byte)value);
		}
		else if (type == typeof(short))
		{
			writer.Write((byte)PropertyType.Short);
			writer.Write((short)value);
		}
		else if (type == typeof(int))
		{
			writer.Write((byte)PropertyType.Int);
			writer.Write((int)value);
		}
		else if (type == typeof(long))
		{
			writer.Write((byte)PropertyType.Long);
			writer.Write((long)value);
		}
		else if (type == typeof(float))
		{
			writer.Write((byte)PropertyType.Float);
			writer.Write((float)value);
		}
		else if (type == typeof(double))
		{
			writer.Write((byte)PropertyType.Double);
			writer.Write((double)value);
		}
		else if (type == typeof(bool))
		{
			writer.Write((byte)PropertyType.Bool);
			writer.Write((bool)value);
		}
		else if (type == typeof(string))
		{
			writer.Write((byte)PropertyType.String);
			writer.Write((string)value);
		}
		else if (type == typeof(DateTime))
		{
			writer.Write((byte)PropertyType.DateTime);
			writer.Write(((DateTime)value).ToBinary());
		}
		else
		{
			throw new ArgumentException();
		}
	}

	internal static object DeserializePropertyValue(BinaryReader reader)
	{
		PropertyType type = (PropertyType)reader.ReadByte();
		if (type == PropertyType.Byte)
			return reader.ReadByte();
		else if (type == PropertyType.Short)
			return reader.ReadInt16();
		else if (type == PropertyType.Int)
			return reader.ReadInt32();
		else if (type == PropertyType.Long)
			return reader.ReadInt64();
		else if (type == PropertyType.Float)
			return reader.ReadSingle();
		else if (type == PropertyType.Double)
			return reader.ReadDouble();
		else if (type == PropertyType.Bool)
			return reader.ReadBoolean();
		else if (type == PropertyType.String)
			return reader.ReadString();
		else if (type == PropertyType.DateTime)
			return DateTime.FromBinary(reader.ReadInt64());
		else
			throw new ArgumentException();
	}

	private void SerializeDefaultValue(BinaryWriter writer)
	{
		if (object.ReferenceEquals(defaultValue, builtInDefaultValues[(int)base.PropertyType]))
		{
			writer.Write(false);
		}
		else
		{
			writer.Write(true);
			SerializePropertyValue(writer, defaultValue);
		}
	}

	private void DeserializeDefaultValue(BinaryReader reader)
	{
		if (!reader.ReadBoolean())
		{
			defaultValue = builtInDefaultValues[(int)base.PropertyType];
		}
		else
		{
			defaultValue = DeserializePropertyValue(reader);
		}
	}

	public override void Serialize(BinaryWriter writer, ModelDescriptorSerializerContext context)
	{
		base.Serialize(writer, context);
		SerializeDefaultValue(writer);
	}

	public override void Deserialize(BinaryReader reader, ModelDescriptorDeserializerContext context)
	{
		base.Deserialize(reader, context);
		DeserializeDefaultValue(reader);
	}
}
