using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Xml;
using System.Xml.Schema;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal sealed class DataModelDescriptor : ModelItemDescriptor
{
	const short serializerVersion = 1;

	short lastUsedClassId;
	int lastUsedPropertyId;
	short lastUsedHashIndexId;

	ReadOnlyArray<ClassDescriptor> classes;
	ReadOnlyArray<HashIndexDescriptor> hashIndexes;

	Dictionary<string, ClassDescriptor> nameToClass;
	Dictionary<short, ClassDescriptor> idToClass;

	Dictionary<string, NamespaceDescriptor> nameToNamespace;

	Dictionary<string, HashIndexDescriptor> nameToHashIndex;
	Dictionary<short, HashIndexDescriptor> idToHashIndex;

	// While the model is being built different types of descriptors need to store temporary data
	// so this generic map is used for that.
	Dictionary<object, object> loadingTempData;

	public DataModelDescriptor(short lastUsedClassId = 0, int lastUsedPropertyId = 0, short lastUsedHashIndexId = 0)
	{
		this.lastUsedClassId = lastUsedClassId;
		this.lastUsedPropertyId = lastUsedPropertyId;
		this.lastUsedHashIndexId = lastUsedHashIndexId;

		nameToClass = new Dictionary<string, ClassDescriptor>(1024, StringComparer.Ordinal);
		idToClass = new Dictionary<short, ClassDescriptor>(1024);
		nameToHashIndex = new Dictionary<string, HashIndexDescriptor>(4);
		idToHashIndex = new Dictionary<short, HashIndexDescriptor>(4);
		nameToNamespace = new Dictionary<string, NamespaceDescriptor>(64);
		loadingTempData = new Dictionary<object, object>(1024, ReferenceEqualityComparer<object>.Instance);

		CreateSystemNamespace();
	}

	public override ModelItemType Type => ModelItemType.Model;
	public int ClassCount => classes.Length;
	public int HashIndexCount => hashIndexes.Length;
	public int NamespaceCount => nameToNamespace.Count;
	public short LastUsedClassId => lastUsedClassId;
	public int LastUsedPropertyId => lastUsedPropertyId;
	public short LastUsedHashIndexId => lastUsedHashIndexId;
	public Dictionary<object, object> LoadingTempData => loadingTempData;

	public static DataModelDescriptor CreateEmpty(PersistenceDescriptor persistenceDescriptor)
	{
		DataModelDescriptor d = new DataModelDescriptor();
		d.Prepare(null);
		if (persistenceDescriptor != null)
			d.AssignLogIndexes(persistenceDescriptor);

		return d;
	}

	public void Register(Stream stream)
	{
		if (stream != null)
		{
			try
			{
				StoreNamespace(ReadNamespace(stream));
			}
			catch (XmlException)
			{
				throw new ArgumentException();
			}
		}
	}

	public void Register(IEnumerable<ObjectModelClass> objectModelClasses)
	{
		Dictionary<string, List<ObjectModelClass>> nss = new Dictionary<string, List<ObjectModelClass>>();
		foreach (ObjectModelClass objectModelClass in objectModelClasses)
		{
			string namespaceName = objectModelClass.ClassType.Namespace;
			if (namespaceName == null)
				namespaceName = string.Empty;

			if (!nss.TryGetValue(namespaceName, out List<ObjectModelClass> l))
			{
				l = new List<ObjectModelClass>(256);
				nss.Add(namespaceName, l);
			}

			l.Add(objectModelClass);
		}

		foreach (string namespaceName in nss.Keys)
		{
			StoreNamespace(new NamespaceDescriptor(nss[namespaceName], this));
		}
	}

	public void Register(ClassDescriptor[] clss, HashIndexDescriptor[] hinds, string namespaceName)
	{
		NamespaceDescriptor nsd = new NamespaceDescriptor(namespaceName, clss, hinds);
		nsd.Model = this;
		StoreNamespace(nsd);
	}

	public void Prepare(PersistenceSettings persistenceSett = null)
	{
		foreach (ClassDescriptor classDesc in nameToClass.Values)
		{
			classDesc.Prepare();
		}

		foreach (HashIndexDescriptor hashIndex in nameToHashIndex.Values)
		{
			hashIndex.Prepare();
		}

		CreateItemLists();
		ValidatePropertyIds();

		foreach (ClassDescriptor classDesc in nameToClass.Values)
		{
			classDesc.PrepareDescendants();
			classDesc.PreparePropertyToHashIndexMapping();
		}

		if (persistenceSett != null)
			AssignClassLogs(persistenceSett);

		foreach (ClassDescriptor classDesc in nameToClass.Values)
		{
			classDesc.PrepareReferenceProperties();
		}

		SetClassInverseRefs();

		UpdateLastUsedIds();

		loadingTempData = null;
	}

	public void AssignLogIndexes(PersistenceDescriptor persistenceDesc)
	{
		foreach (ClassDescriptor classDesc in GetAllClasses())
		{
			classDesc.AssignLogIndex(persistenceDesc);
		}
	}

	private void UpdateLastUsedIds()
	{
		for (int i = 0; i < classes.Length; i++)
		{
			lastUsedClassId = Math.Max(lastUsedClassId, classes[i].Id);
			for (int j = 0; j < classes[i].Properties.Length; j++)
			{
				lastUsedPropertyId = Math.Max(lastUsedPropertyId, classes[i].Properties[j].Id);
			}
		}

		for (int i = 0; i < hashIndexes.Length; i++)
		{
			lastUsedHashIndexId = Math.Max(lastUsedHashIndexId, hashIndexes[i].Id);
		}
	}

	private void CreateItemLists()
	{
		int c = 0;
		ClassDescriptor[] classes = new ClassDescriptor[nameToClass.Count];
		foreach (ClassDescriptor classDersc in nameToClass.Values)
		{
			classes[c++] = classDersc;
		}

		Comparer<short> comp = Comparer<short>.Default;
		Array.Sort(classes, (x, y) => comp.Compare(x.Id, y.Id));
		for (int i = 0; i < classes.Length; i++)
		{
			classes[i].Index = i;
		}

		this.classes = new ReadOnlyArray<ClassDescriptor>(classes);

		c = 0;
		HashIndexDescriptor[] hashIndexes = new HashIndexDescriptor[nameToHashIndex.Count];
		foreach (HashIndexDescriptor hashIndex in nameToHashIndex.Values)
		{
			hashIndexes[c++] = hashIndex;
		}

		Array.Sort(hashIndexes, (x, y) => comp.Compare(x.Id, y.Id));
		for (int i = 0; i < hashIndexes.Length; i++)
		{
			hashIndexes[i].Index = i;
		}

		this.hashIndexes = new ReadOnlyArray<HashIndexDescriptor>(hashIndexes);
	}

	private void ValidatePropertyIds()
	{
		Dictionary<int, PropertyDescriptor> propIds = new Dictionary<int, PropertyDescriptor>(1024);
		foreach (ClassDescriptor classDesc in nameToClass.Values)
		{
			for (int i = 0; i < classDesc.Properties.Length; i++)
			{
				PropertyDescriptor p = classDesc.Properties[i];
				if (!object.ReferenceEquals(p.OwnerClass, classDesc))
					continue;

				if (propIds.ContainsKey(p.Id))
				{
					Throw.DuplicatePropertyId(classDesc.FullName, p.Name);
				}
			}
		}
	}

	private void AssignClassLogs(PersistenceSettings persistenceSett)
	{
		if (persistenceSett == null)
			return;

		Dictionary<short, Tuple<string, ClassDescriptor>> m = new Dictionary<short, Tuple<string, ClassDescriptor>>(256);

		foreach (LogSettings ls in persistenceSett.SecondaryLogs)
		{
			for (int i = 0; i < ls.Classes.Length; i++)
			{
				ClassDescriptor classDesc = GetClass(ls.Classes[i]);
				foreach (short classId in classDesc.DescendentClassIds.Concat(classDesc.Id))
				{
					ClassDescriptor descendantClass = GetClass(classId);

					if (descendantClass == null)
						Checker.ArgumentException($"Class with id {ls.Classes[i]} assigned to a log group could not be found.");

					if (m.TryGetValue(descendantClass.Id, out Tuple<string, ClassDescriptor> t))
					{
						if (object.ReferenceEquals(t.Item2, classDesc))
							throw new ArgumentException($"multiple log files assigned to class {classDesc.FullName}.");

						if (t.Item2.IsAssignable(classDesc.Id))
							m[descendantClass.Id] = new Tuple<string, ClassDescriptor>(ls.Name, classDesc);
					}
					else
					{
						m[descendantClass.Id] = new Tuple<string, ClassDescriptor>(ls.Name, classDesc);
					}
				}
			}
		}

		foreach (ClassDescriptor classDesc in GetAllClasses())
		{
			if (m.TryGetValue(classDesc.Id, out Tuple<string, ClassDescriptor> t))
			{
				classDesc.AssignLogName(t.Item1);
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ClassDescriptor GetClass(string fullName)
	{
		nameToClass.TryGetValue(fullName, out ClassDescriptor classDesc);
		return classDesc;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ClassDescriptor GetClass(short id)
	{
		idToClass.TryGetValue(id, out ClassDescriptor classDesc);
		return classDesc;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal ClassDescriptor GetClassByIndex(int index)
	{
		return classes[index];
	}

	public HashIndexDescriptor GetHashIndex(short id)
	{
		idToHashIndex.TryGetValue(id, out HashIndexDescriptor hashIndex);
		return hashIndex;
	}

	public HashIndexDescriptor GetHashIndex(string fullName)
	{
		nameToHashIndex.TryGetValue(fullName, out HashIndexDescriptor hashIndex);
		return hashIndex;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	internal HashIndexDescriptor GetHashIndexByIndex(int index)
	{
		return hashIndexes[index];
	}

	public int GetHashIndexCount()
	{
		return nameToHashIndex.Count;
	}

	public IEnumerable<ClassDescriptor> GetAllClasses()
	{
		return classes;
	}

	public IEnumerable<HashIndexDescriptor> GetAllHashIndexes()
	{
		return nameToHashIndex.Values;
	}

	public IEnumerable<NamespaceDescriptor> GetAllNamespaces()
	{
		return nameToNamespace.Values;
	}

	public NamespaceDescriptor GetNamespace(string namespaceName)
	{
		NamespaceDescriptor mns;
		nameToNamespace.TryGetValue(namespaceName, out mns);
		return mns;
	}

	public int GetNamespaceClassCount(string namespaceName)
	{
		NamespaceDescriptor mns;
		nameToNamespace.TryGetValue(namespaceName, out mns);

		if (mns == null)
			throw InvalidNamespaceNameException();

		return mns.Classes.Length;
	}

	public IEnumerable<ClassDescriptor> GetAllNamespaceClasses(string namespaceName)
	{

		NamespaceDescriptor mns;
		nameToNamespace.TryGetValue(namespaceName, out mns);

		if (mns == null)
			throw InvalidNamespaceNameException();

		return mns.Classes;
	}

	private NamespaceDescriptor ReadNamespace(Stream stream)
	{
		var sett = CreateReaderSettings();

		using (XmlReader reader = XmlReader.Create(stream, sett))
		{
			reader.MoveToContent();

			do
			{
				if (reader.NodeType == XmlNodeType.Element && reader.Name.Equals("Namespace", StringComparison.Ordinal))
					return new NamespaceDescriptor(reader.ReadSubtree(), this);
			}
			while (reader.Read());
		}

		throw new ArgumentException();
	}

	private XmlReaderSettings CreateReaderSettings()
	{
		XmlSchema schema;
		using (Stream s = Utils.GetResourceStream(Assembly.GetExecutingAssembly(), "VeloxDB.Descriptor.DataModelSchema.xsd"))
			schema = XmlSchema.Read(s, null);

		XmlReaderSettings sett = new XmlReaderSettings();
		sett.IgnoreComments = true;
		sett.IgnoreProcessingInstructions = true;
		sett.Schemas = new XmlSchemaSet();
		sett.Schemas.Add(schema);
		sett.CloseInput = false;
		sett.IgnoreWhitespace = true;
		sett.ValidationType = ValidationType.Schema;
		sett.ValidationFlags = XmlSchemaValidationFlags.None;
		return sett;
	}

	private void StoreNamespace(NamespaceDescriptor @namespace)
	{
		nameToNamespace[@namespace.Name] = @namespace;
		StoreClasses(@namespace);
		StoreHashIndexes(@namespace);
	}

	private void StoreHashIndexes(NamespaceDescriptor @namespace)
	{
		foreach (HashIndexDescriptor hashIndex in @namespace.HashIndexes)
		{
			if (nameToHashIndex.ContainsKey(hashIndex.FullName))
			{
				Throw.DuplicateHashIndexName(hashIndex.FullName, hashIndex.Id);
			}

			nameToHashIndex[hashIndex.FullName] = hashIndex;
			idToHashIndex[hashIndex.Id] = hashIndex;
		}
	}

	private void StoreClasses(NamespaceDescriptor @namespace)
	{
		foreach (ClassDescriptor classDesc in @namespace.Classes)
		{
			if (idToClass.ContainsKey(classDesc.Id))
			{
				Throw.DuplicateClassId(classDesc.FullName, classDesc.Id);
			}

			if (nameToClass.ContainsKey(classDesc.FullName))
			{
				Throw.DuplicateClassName(classDesc.FullName, classDesc.Id);
			}

			nameToClass[classDesc.FullName] = classDesc;
			idToClass[classDesc.Id] = classDesc;
		}
	}

	private void CreateSystemNamespace()
	{
		using (Stream s = Utils.GetResourceStream(Assembly.GetExecutingAssembly(), "VeloxDB.Descriptor.SystemModel.xml"))
		{
			Register(s);
		}
	}

	public static InvalidOperationException InvalidNamespaceNameException()
	{
		return new InvalidOperationException("Invalid namespace name.");
	}

	public static byte[] Serialize(DataModelDescriptor model)
	{
		using (MemoryStream ms = new MemoryStream())
		using (BinaryWriter w = new BinaryWriter(ms))
		{
			Serialize(model, w);
			return ms.ToArray();
		}
	}

	public static void Serialize(DataModelDescriptor model, BinaryWriter writer)
	{
		ModelDescriptorSerializerContext context = new ModelDescriptorSerializerContext();
		context.Serialize(model, writer);
	}

	public static DataModelDescriptor Deserialize(byte[] binary)
	{
		using (MemoryStream ms = new MemoryStream(binary))
		using (BinaryReader r = new BinaryReader(ms))
		{
			return Deserialize(r);
		}
	}

	public static DataModelDescriptor Deserialize(BinaryReader reader)
	{
		ModelDescriptorDeserializerContext context = new ModelDescriptorDeserializerContext();
		DataModelDescriptor model = context.Deserialize<DataModelDescriptor>(reader);
		return model;
	}

	private void SetClassInverseRefs()
	{
		Dictionary<short, List<ReferencePropertyDescriptor>> groups = ReferencePropertyDescriptor.GroupInverseRefs(this);
		foreach (ClassDescriptor classDesc in GetAllClasses())
		{
			groups.TryGetValue(classDesc.Id, out List<ReferencePropertyDescriptor> l);
			classDesc.SetInverseReferences(l);
		}
	}

	public override void Serialize(BinaryWriter writer, ModelDescriptorSerializerContext context)
	{
		writer.Write(serializerVersion);

		writer.Write(lastUsedClassId);
		writer.Write(lastUsedPropertyId);
		writer.Write(lastUsedHashIndexId);

		writer.Write(nameToClass.Count);
		foreach (ClassDescriptor cd in nameToClass.Values)
		{
			context.Serialize(cd, writer);
		}

		writer.Write(nameToHashIndex.Count);
		foreach (HashIndexDescriptor hi in nameToHashIndex.Values)
		{
			context.Serialize(hi, writer);
		}

		writer.Write(nameToNamespace.Count);
		foreach (NamespaceDescriptor nd in nameToNamespace.Values)
		{
			context.Serialize(nd, writer);
		}
	}

	public override void Deserialize(BinaryReader reader, ModelDescriptorDeserializerContext context)
	{
		Prepare();

		short ver = reader.ReadInt16(); // serializer version
		if (ver > serializerVersion)
			throw new NotSupportedException();

		lastUsedClassId = reader.ReadInt16();
		lastUsedPropertyId = reader.ReadInt32();
		lastUsedHashIndexId = reader.ReadInt16();

		int c = reader.ReadInt32();
		nameToClass = new Dictionary<string, ClassDescriptor>(c);
		idToClass = new Dictionary<short, ClassDescriptor>(c);
		for (int i = 0; i < c; i++)
		{
			ClassDescriptor cd = context.Deserialize<ClassDescriptor>(reader);
			nameToClass.Add(cd.FullName, cd);
			idToClass.Add(cd.Id, cd);
		}

		c = reader.ReadInt32();
		nameToHashIndex = new Dictionary<string, HashIndexDescriptor>(c);
		idToHashIndex = new Dictionary<short, HashIndexDescriptor>(c);
		for (int i = 0; i < c; i++)
		{
			HashIndexDescriptor hi = context.Deserialize<HashIndexDescriptor>(reader);
			nameToHashIndex.Add(hi.FullName, hi);
			idToHashIndex.Add(hi.Id, hi);
		}

		c = reader.ReadInt32();
		nameToNamespace = new Dictionary<string, NamespaceDescriptor>(c);
		for (int i = 0; i < c; i++)
		{
			NamespaceDescriptor mn = context.Deserialize<NamespaceDescriptor>(reader);
			nameToNamespace.Add(mn.Name, mn);
		}

		CreateItemLists();

		Dictionary<ClassDescriptor, List<short>> descendantClasses = new Dictionary<ClassDescriptor, List<short>>(ClassCount);
		foreach (ClassDescriptor classDesc in GetAllClasses())
		{
			classDesc.OnDeserializeModel(descendantClasses);
		}

		foreach (ClassDescriptor classDesc in GetAllClasses())
		{
			classDesc.OnDeserializeModelFinished(descendantClasses);
		}

		SetClassInverseRefs();
	}
}
