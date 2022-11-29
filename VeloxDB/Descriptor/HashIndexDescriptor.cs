using System;
using System.Linq;
using System.Collections.Generic;
using System.Xml;
using Velox.Common;
using System.IO;

namespace Velox.Descriptor;

internal sealed class HashIndexDescriptor : TypeDescriptor
{
	public const int MaxIndexedPropCount = 4;

	short id;
	bool isUnique;
	int index;
	int keySize;
	ReadOnlyArray<ClassDescriptor> classes;      // Contains only non abstract classes
	ReadOnlyArray<PropertyDescriptor> properties;

	public HashIndexDescriptor()
	{
	}

	public HashIndexDescriptor(NamespaceDescriptor @namespace, ObjectModelHashIndex objectModelHashIndex) :
		base(objectModelHashIndex.Name, @namespace)
	{
		this.id = objectModelHashIndex.Id;
		this.isUnique = objectModelHashIndex.IsUnique;

		PreparePhaseData prepareData = new PreparePhaseData();
		prepareData.PropertyNames.AddRange(objectModelHashIndex.Properties);

		Model.LoadingTempData.Add(this, prepareData);
	}

	public HashIndexDescriptor(XmlReader reader, NamespaceDescriptor metaNamespace) :
		base(reader, metaNamespace)
	{
		using (reader)
		{
			string value = reader.GetAttribute("IsUnique");
			if (value != null)
				isUnique = bool.Parse(value);

			id = short.Parse(reader.GetAttribute("Id"));

			PreparePhaseData prepareData = new PreparePhaseData();
			while (reader.Read())
			{
				if (reader.NodeType == XmlNodeType.Element && reader.Name.Equals("Property", StringComparison.Ordinal))
					prepareData.PropertyNames.Add(reader.GetAttribute("Name"));
			}

			Model.LoadingTempData.Add(this, prepareData);
		}
	}

	public HashIndexDescriptor(DataModelDescriptor modelDesc, string name, short id, bool isUnique, string[] propertyNames) :
		base(name)
	{
		this.id = id;
		this.isUnique = isUnique;

		PreparePhaseData prepareData = new PreparePhaseData();
		prepareData.PropertyNames.AddRange(propertyNames);
		modelDesc.LoadingTempData.Add(this, prepareData);
	}

	public override ModelItemType Type => ModelItemType.HashIndex;

	public short Id => id;
	public ReadOnlyArray<ClassDescriptor> Classes => classes;
	public ReadOnlyArray<PropertyDescriptor> Properties => properties;
	public bool IsUnique => isUnique;
	public int KeySize => keySize;
	public int Index { get => index; set => index = value; }

	public ObjectModelClass DefiningObjectModelClass
	{
		get
		{
			for (int i = 0; i < classes.Length; i++)
			{
				ObjectModelClass objClass = classes[i].ObjectModelClass;
				if (objClass != null)
				{
					if (objClass.HashIndexes.Find(x => x.FullName.Equals(this.FullName)) != null)
						return objClass;
				}
			}

			throw new InvalidOperationException();
		}
	}

	public void AddClass(ClassDescriptor @class)
	{
		Checker.AssertFalse(@class.IsAbstract);
		PreparePhaseData prepareData = (PreparePhaseData)Model.LoadingTempData[this];
		prepareData.Classes.Add(@class);
	}

	public void Prepare()
	{
		PreparePhaseData prepareData = (PreparePhaseData)Model.LoadingTempData[this];

		if (prepareData.PropertyNames.Count == 0)
			Throw.HashIndexWithoutProperties(FullName);

		if (prepareData.PropertyNames.Count > MaxIndexedPropCount)
			Throw.MaximumNumberOfPropertiesInHashIndexExceeded(FullName);

		if (prepareData.Classes == null || prepareData.Classes.Count == 0)
			Throw.HashIndexWithoutClasses(FullName);

		ClassDescriptor firstClass = prepareData.Classes.First();

		keySize = 0;
		PropertyDescriptor[] props = new PropertyDescriptor[prepareData.PropertyNames.Count];
		for (int i = 0; i < prepareData.PropertyNames.Count; i++)
		{
			PropertyDescriptor propDesc = firstClass.GetProperty(prepareData.PropertyNames[i]);

			if (propDesc == null)
			{
				Throw.HashIndexIndexesUnknownProperty(firstClass.FullName, FullName, prepareData.PropertyNames[i]);
			}

			if (propDesc.Id == SystemCode.DatabaseObject.Version || propDesc.Kind == PropertyKind.Array ||
				propDesc.Kind == PropertyKind.Reference && (propDesc as ReferencePropertyDescriptor).Multiplicity == Multiplicity.Many)
			{
				Throw.HashIndexIndexesInvalidProperty(FullName, propDesc.Name);
			}

			if (PropertyAlreadyIndexed(props, i, propDesc.Id))
				Throw.HashIndexIndexesPropertyMultipleTimes(FullName, propDesc.Name);

			foreach (ClassDescriptor @class in prepareData.Classes)
			{
				if (!object.ReferenceEquals(firstClass, @class) && @class.GetProperty(propDesc.Id) == null)
				{
					Throw.HashIndexIndexesUnknownProperty(@class.FullName, FullName, propDesc.Name);
				}
			}

			props[i] = propDesc;
			keySize += PropertyTypesHelper.GetItemSize(propDesc.PropertyType);
		}

		properties = new ReadOnlyArray<PropertyDescriptor>(props);
		classes = new ReadOnlyArray<ClassDescriptor>(prepareData.Classes.ToArray());
	}

	private bool PropertyAlreadyIndexed(PropertyDescriptor[] mps, int count, int id)
	{
		for (int i = 0; i < count; i++)
		{
			if (mps[i].Id == id)
				return true;
		}

		return false;
	}

	public override void Serialize(BinaryWriter writer, ModelDescriptorSerializerContext context)
	{
		base.Serialize(writer, context);

		writer.Write(id);
		writer.Write(isUnique);

		writer.Write((byte)properties.Length);
		for (int i = 0; i < properties.Length; i++)
		{
			context.Serialize(properties[i], writer);
		}

		writer.Write((short)classes.Length);
		for (int i = 0; i < classes.Length; i++)
		{
			context.Serialize(classes[i], writer);
		}

		writer.Write(keySize);
	}

	public override void Deserialize(BinaryReader reader, ModelDescriptorDeserializerContext context)
	{
		base.Deserialize(reader, context);

		id = reader.ReadInt16();
		isUnique = reader.ReadBoolean();

		int c = reader.ReadByte();
		PropertyDescriptor[] ps = new PropertyDescriptor[c];
		for (int i = 0; i < c; i++)
		{
			ps[i] = context.Deserialize<PropertyDescriptor>(reader);
		}

		properties = new ReadOnlyArray<PropertyDescriptor>(ps);

		c = reader.ReadInt16();
		ClassDescriptor[] cs = new ClassDescriptor[c];
		for (int i = 0; i < c; i++)
		{
			cs[i] = context.Deserialize<ClassDescriptor>(reader);
		}

		classes = new ReadOnlyArray<ClassDescriptor>(cs);

		keySize = reader.ReadInt32();
	}

	public override string ToString()
	{
		return $"(HashIndex {Name})";
	}

	private sealed class PreparePhaseData
	{
		public HashSet<ClassDescriptor> Classes { get; set; } =
			new HashSet<ClassDescriptor>(4, ReferenceEqualityComparer<ClassDescriptor>.Instance);
		public List<string> PropertyNames { get; set; } = new List<string>();
	}
}

internal struct KeyProperty : IEquatable<KeyProperty>
{
	public PropertyType PropertyType { get; private set; }
	public int ByteOffset { get; private set; }

	public KeyProperty(PropertyType propertyType, int byteOffset)
	{
		this.PropertyType = propertyType;
		this.ByteOffset = byteOffset;
	}

	public bool Equals(KeyProperty other)
	{
		return PropertyType == other.PropertyType && ByteOffset == other.ByteOffset;
	}
}

internal sealed class KeyComparerDesc : IEquatable<KeyComparerDesc>
{
	KeyProperty[] properties;

	public KeyComparerDesc(KeyProperty[] properties)
	{
		this.properties = properties;
	}

	public KeyProperty[] Properties => properties;

	public bool Equals(KeyComparerDesc other)
	{
		if (properties.Length != other.properties.Length)
			return false;

		for (int i = 0; i < properties.Length; i++)
		{
			if (!properties[i].Equals(other.properties[i]))
				return false;
		}

		return true;
	}

	public override int GetHashCode()
	{
		ulong h = HashUtils.StartHash64(1);
		for (int i = 0; i < properties.Length; i++)
		{
			h = HashUtils.AdvanceHash64(h, (uint)properties[i].ByteOffset);
			h = HashUtils.AdvanceHash64(h, (byte)properties[i].PropertyType);
		}

		return (int)HashUtils.FinishHash64(h);
	}
}
