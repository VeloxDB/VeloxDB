using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal sealed class NamespaceDescriptor : ModelItemDescriptor
{
	string name;

	ReadOnlyArray<ClassDescriptor> classes;
	ReadOnlyArray<IndexDescriptor> indexes;

	DataModelDescriptor model;

	internal NamespaceDescriptor()
	{
	}

	public NamespaceDescriptor(IEnumerable<ObjectModelClass> objectModelClasses, DataModelDescriptor model)
	{
		this.model = model;

		this.name = objectModelClasses.First().ClassType.Namespace;

		List<ClassDescriptor> cls = new List<ClassDescriptor>(objectModelClasses.Count());
		List<IndexDescriptor> inds = new List<IndexDescriptor>();
		foreach (ObjectModelClass objectModelClass in objectModelClasses)
		{
			cls.Add(new ClassDescriptor(this, objectModelClass));
			inds.AddRange(objectModelClass.Indexes.Select<ObjectModelIndex, IndexDescriptor>(x =>
			{
				if (x is ObjectModelHashIndex)
					return new HashIndexDescriptor(this, (ObjectModelHashIndex)x);
				else
					return new SortedIndexDescriptor(this, (ObjectModelSortedIndex)x);
			}));
		}

		cls.Sort((x, y) => x.Name.CompareTo(y.Name));

		classes = new ReadOnlyArray<ClassDescriptor>(cls.ToArray());
		indexes = new ReadOnlyArray<IndexDescriptor>(inds.ToArray());
	}

	public NamespaceDescriptor(string name, ClassDescriptor[] clss, IndexDescriptor[] inds)
	{
		this.name = name;

		for (int i = 0; i < clss.Length; i++)
		{
			clss[i].Namespace = this;
		}

		for (int i = 0; i < inds.Length; i++)
		{
			inds[i].Namespace = this;
		}

		Array.Sort(clss, (x, y) => x.Name.CompareTo(y.Name));

		classes = new ReadOnlyArray<ClassDescriptor>(clss.ToArray());
		indexes = new ReadOnlyArray<IndexDescriptor>(inds.ToArray());
	}

	public NamespaceDescriptor(XmlReader reader, DataModelDescriptor model)
	{
		this.model = model;

		List<ClassDescriptor> cls = new List<ClassDescriptor>();
		List<IndexDescriptor> inds = new List<IndexDescriptor>();

		reader.Read();
		name = reader.GetAttribute("Name");

		while (reader.Read())
		{
			if (reader.NodeType != XmlNodeType.Element)
				continue;

			if (reader.Name.Equals("Class"))
			{
				cls.Add(new ClassDescriptor(reader.ReadSubtree(), this));
			}
			
			if (reader.Name.Equals("HashIndex"))
			{
				inds.Add(new HashIndexDescriptor(reader.ReadSubtree(), this));
			}

			if (reader.Name.Equals("SortedIndex"))
			{
				inds.Add(new SortedIndexDescriptor(reader.ReadSubtree(), this));
			}
		}

		cls.Sort((x, y) => x.Name.CompareTo(y.Name));

		classes = new ReadOnlyArray<ClassDescriptor>(cls.ToArray());
		indexes = new ReadOnlyArray<IndexDescriptor>(inds.ToArray());

		reader.Dispose();
	}

	public override ModelItemType Type => ModelItemType.Namespace;
	public DataModelDescriptor Model { get => model; set => model = value; }
	public string Name => name;
	public ReadOnlyArray<ClassDescriptor> Classes => classes;
	public ReadOnlyArray<IndexDescriptor> Indexes => indexes;

	public override string ToString()
	{
		return $"(Namespace, {Name})";
	}

	public override void Serialize(BinaryWriter writer, ModelDescriptorSerializerContext context)
	{
		writer.Write(name);

		context.Serialize(model, writer);

		writer.Write(classes.Length);
		for (int i = 0; i < classes.Length; i++)
		{
			context.Serialize(classes[i], writer);
		}

		writer.Write(indexes.Length);
		for (int i = 0; i < indexes.Length; i++)
		{
			context.Serialize(indexes[i], writer);
		}
	}

	public override void Deserialize(BinaryReader reader, ModelDescriptorDeserializerContext context)
	{
		name = reader.ReadString();

		model = context.Deserialize<DataModelDescriptor>(reader);

		int c = reader.ReadInt32();
		ClassDescriptor[] cls = new ClassDescriptor[c];
		for (int i = 0; i < c; i++)
		{
			cls[i] = context.Deserialize<ClassDescriptor>(reader);
		}

		classes = new ReadOnlyArray<ClassDescriptor>(cls);

		c = reader.ReadInt32();
		IndexDescriptor[] inds = new IndexDescriptor[c];
		for (int i = 0; i < c; i++)
		{
			inds[i] = context.Deserialize<IndexDescriptor>(reader);
		}

		indexes = new ReadOnlyArray<IndexDescriptor>(inds);
	}
}
