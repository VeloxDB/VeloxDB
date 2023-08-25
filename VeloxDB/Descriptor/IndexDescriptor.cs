using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Xml;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal abstract class IndexDescriptor : TypeDescriptor
{
	public const int MaxIndexedPropCount = 4;

	short id;
	string cultureName;
	bool caseSensitive;
	bool isUnique;
	int index;
	int keySize;
	ReadOnlyArray<ClassDescriptor> classes;      // Contains only non abstract classes
	ReadOnlyArray<PropertyDescriptor> properties;

	public IndexDescriptor()
	{
	}

	public IndexDescriptor(NamespaceDescriptor @namespace, ObjectModelIndex objectModelIndex) :
		base(objectModelIndex.Name, @namespace)
	{
		OnCreated();

		this.id = objectModelIndex.Id;
		this.cultureName = objectModelIndex.CultureName;
		this.caseSensitive = objectModelIndex.CaseSensitive;
		this.isUnique = objectModelIndex.IsUnique;

		PreparePhaseData prepareData = new PreparePhaseData();
		prepareData.PropertyNames.AddRange(objectModelIndex.Properties);

		Model.LoadingTempData.Add(this, prepareData);
	}

	public IndexDescriptor(XmlReader reader, NamespaceDescriptor metaNamespace) :
		base(reader, metaNamespace)
	{
		OnCreated();

		using (reader)
		{
			string value = reader.GetAttribute("IsUnique");
			if (value != null)
				isUnique = bool.Parse(value);

			cultureName = reader.GetAttribute("CultureName");
			value = reader.GetAttribute("CaseSensitive");
			caseSensitive = value != null ? bool.Parse(value) : true;

			id = short.Parse(reader.GetAttribute("Id"));

			PreparePhaseData prepareData = new PreparePhaseData();
			while (reader.Read())
			{
				if (reader.NodeType == XmlNodeType.Element && reader.Name.Equals("Property", StringComparison.Ordinal))
				{
					prepareData.PropertyNames.Add(reader.GetAttribute("Name"));
					OnPropertyLoaded(reader);
				}
			}

			Model.LoadingTempData.Add(this, prepareData);
		}
	}

	public IndexDescriptor(DataModelDescriptor modelDesc, string name, short id,
		string cultureName, bool caseSensitive, bool isUnique, string[] propertyNames) :
		base(name)
	{
		this.id = id;
		this.cultureName = cultureName;
		this.caseSensitive = caseSensitive;
		this.isUnique = isUnique;

		PreparePhaseData prepareData = new PreparePhaseData();
		prepareData.PropertyNames.AddRange(propertyNames);
		modelDesc.LoadingTempData.Add(this, prepareData);
	}

	public short Id => id;
	public ReadOnlyArray<ClassDescriptor> Classes => classes;
	public ReadOnlyArray<PropertyDescriptor> Properties => properties;
	public string CultureName => cultureName;
	public bool CaseSensitive => caseSensitive;
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
					if (objClass.Indexes.Find(x => x.FullName.Equals(this.FullName)) != null)
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
			Throw.IndexWithoutProperties(FullName);

		if (prepareData.PropertyNames.Count > MaxIndexedPropCount)
			Throw.MaximumNumberOfPropertiesInIndexExceeded(FullName);

		if (prepareData.Classes == null || prepareData.Classes.Count == 0)
			Throw.IndexWithoutClasses(FullName);

		ClassDescriptor firstClass = prepareData.Classes.First();

		keySize = 0;
		PropertyDescriptor[] props = new PropertyDescriptor[prepareData.PropertyNames.Count];
		for (int i = 0; i < prepareData.PropertyNames.Count; i++)
		{
			PropertyDescriptor propDesc = firstClass.GetProperty(prepareData.PropertyNames[i]);

			if (propDesc == null)
			{
				Throw.IndexIndexesUnknownProperty(firstClass.FullName, FullName, prepareData.PropertyNames[i]);
			}

			if (propDesc.Id == SystemCode.DatabaseObject.Version || propDesc.Kind == PropertyKind.Array ||
				propDesc.Kind == PropertyKind.Reference && (propDesc as ReferencePropertyDescriptor).Multiplicity == Multiplicity.Many)
			{
				Throw.IndexIndexesInvalidProperty(FullName, propDesc.Name);
			}

			if (PropertyAlreadyIndexed(props, i, propDesc.Id))
				Throw.IndexIndexesPropertyMultipleTimes(FullName, propDesc.Name);

			foreach (ClassDescriptor @class in prepareData.Classes)
			{
				if (!object.ReferenceEquals(firstClass, @class) && @class.GetProperty(propDesc.Id) == null)
				{
					Throw.IndexIndexesUnknownProperty(@class.FullName, FullName, propDesc.Name);
				}
			}

			props[i] = propDesc;
			keySize += PropertyTypesHelper.GetItemSize(propDesc.PropertyType);
		}

		properties = new ReadOnlyArray<PropertyDescriptor>(props);
		classes = new ReadOnlyArray<ClassDescriptor>(prepareData.Classes.ToArray());
	}

	protected virtual void OnCreated()
	{
	}

	protected virtual void OnPropertyLoaded(XmlReader reader)
	{
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

	protected virtual void OnSerialize(BinaryWriter writer, ModelDescriptorSerializerContext context)
	{
	}

	protected virtual void OnDeserialize(BinaryReader reader, ModelDescriptorDeserializerContext context)
	{
	}

	public override void Serialize(BinaryWriter writer, ModelDescriptorSerializerContext context)
	{
		base.Serialize(writer, context);

		writer.Write(id);
		writer.Write(isUnique);
		writer.Write(caseSensitive);

		if (cultureName != null)
		{
			writer.Write(true);
			writer.Write(cultureName);
		}
		else
		{
			writer.Write(false);
		}

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

		OnSerialize(writer, context);
	}

	public override void Deserialize(BinaryReader reader, ModelDescriptorDeserializerContext context)
	{
		base.Deserialize(reader, context);

		id = reader.ReadInt16();
		isUnique = reader.ReadBoolean();
		caseSensitive = reader.ReadBoolean();
		if (reader.ReadBoolean())
			cultureName = reader.ReadString();

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

		OnDeserialize(reader, context);
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
	public SortOrder SortOrder { get; private set; }

	public KeyProperty(PropertyType propertyType, int byteOffset, SortOrder sortOrder)
	{
		this.PropertyType = propertyType;
		this.ByteOffset = byteOffset;
		this.SortOrder = sortOrder;
	}

	public bool Equals(KeyProperty other)
	{
		return PropertyType == other.PropertyType && ByteOffset == other.ByteOffset;
	}
}

internal sealed class KeyComparerDesc : IEquatable<KeyComparerDesc>
{
	string cultureName;
	bool caseSensitive;
	KeyProperty[] properties;

	public KeyComparerDesc(KeyProperty[] properties, string cultureName, bool caseSensitive)
	{
		this.properties = properties;
		this.cultureName = cultureName;
		this.caseSensitive = caseSensitive;
	}

	public KeyProperty[] Properties => properties;
	public string CultureName => cultureName;
	public bool CaseSensitive => caseSensitive;

	public bool Equals(KeyComparerDesc other)
	{
		if (!string.Equals(cultureName, other.cultureName))
			return false;

		if (caseSensitive != other.caseSensitive)
			return false;

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

		if (cultureName != null)
			h = HashUtils.AdvanceHash64(h, (uint)cultureName.GetHashCode(StringComparison.Ordinal));

		for (int i = 0; i < properties.Length; i++)
		{
			h = HashUtils.AdvanceHash64(h, (uint)properties[i].ByteOffset);
			h = HashUtils.AdvanceHash64(h, (byte)properties[i].PropertyType);
		}

		return (int)HashUtils.FinishHash64(h);
	}
}
