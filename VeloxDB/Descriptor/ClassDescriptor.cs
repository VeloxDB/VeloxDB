using System;
using System.Collections.Generic;
using System.Xml;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using System.IO;
using VeloxDB.ObjectInterface;
using System.Linq;

namespace VeloxDB.Descriptor;

internal class ClassDescriptor : TypeDescriptor
{
	public const int MinId = -512;

	// No more than this many user classes can exist during the lifetime of the database
	public const int MaxId = 8192 + MinId - 1;

	public const int MaxPropertyCount = 512;
	public const int MaxBlobPropertyCount = 32;
	public const int MaxIndexesPerClass = 32;
	public const int MaxInverseReferencesPerClass = 128;

	short id;
	bool isAbstract;
	int index;
	string logName;
	int logIndex;
	bool prepared;
	int firstStringBlobOffset;
	int firstStringBlobIndex;
	ClassDescriptor baseClass;
	ReadOnlyHashSet<short> descendentClassIdsSet;
	ReadOnlyArray<short> descendentClassIds;
	Dictionary<string, PropertyDescriptor> nameToProperty;
	IntToIntMap propertyIndexes;
	ReadOnlyArray<PropertyDescriptor> properties;
	ReadOnlyArray<IndexDescriptor> indexes;
	ReadOnlyArray<ReferencePropertyDescriptor> inverseReferences;
	ReadOnlyArray<ReferencePropertyDescriptor> cascadeDeletePreventInverseReferences;
	ReadOnlyArray<ReferencePropertyDescriptor> setToNullInverseReferences;
	ReadOnlyArray<int> propertyByteOffsets;
	ReadOnlyArray<int> stringPropertyIndexes;
	ReadOnlyArray<int> blobPropertyIndexes;
	ReadOnlyArray<int> refeferencePropertyIndexes;
	ReadOnlyArray<int> untrackedRefeferencePropertyIndexes;
	ReadOnlyArray<ReadOnlyArray<int>> propertyIndexIndexes;
	ObjectModelClass objectModelClass;

	public ClassDescriptor()
	{
	}

	public ClassDescriptor(NamespaceDescriptor @namespace, ObjectModelClass objectModelClass) :
		base(objectModelClass.ClassType.Name, @namespace)
	{
		PreparePhaseData prepareData = new PreparePhaseData();

		this.objectModelClass = objectModelClass;
		id = objectModelClass.Id;
		isAbstract = objectModelClass.IsAbstract;
		logName = objectModelClass.LogName;

		prepareData.BaseClass = objectModelClass.ClassType.BaseType.FullName;
		if (objectModelClass.ClassType.BaseType == typeof(DatabaseObject))
			prepareData.BaseClass = "System.DatabaseObject";

		foreach (ObjectModelProperty objectModelProperty in objectModelClass.Properties)
		{
			ReferenceObjectModelProperty refObjProp = objectModelProperty as ReferenceObjectModelProperty;
			if (refObjProp != null)
			{
				ReferencePropertyDescriptor p = new ReferencePropertyDescriptor(refObjProp, this);
				prepareData.Properties.Add(p.Id, p);
			}
			else if (typeof(DatabaseArray).IsAssignableFrom(objectModelProperty.PropertyInfo.PropertyType))
			{
				ArrayPropertyDescriptor p = new ArrayPropertyDescriptor(objectModelProperty, this);
				prepareData.Properties.Add(p.Id, p);
			}
			else
			{
				PropertyDescriptor p = new SimplePropertyDescriptor(objectModelProperty, this);
				prepareData.Properties.Add(p.Id, p);
			}
		}

		prepareData.Indexes.AddRange(objectModelClass.Indexes.Select(x => x.FullName));

		Model.LoadingTempData.Add(this, prepareData);
	}

	public ClassDescriptor(XmlReader reader, NamespaceDescriptor metaNamespace) :
		base(reader, metaNamespace)
	{
		using (reader)
		{
			PreparePhaseData prepareData = new PreparePhaseData();

			isAbstract = false;
			string value = reader.GetAttribute("IsAbstract");
			if (value != null)
				isAbstract = bool.Parse(value.ToLowerInvariant());

			value = reader.GetAttribute("Id");
			id = short.Parse(value);
			if (id < MinId || id > MaxId || id == 0)
				Throw.InvalidClassId(FullName);

			prepareData.BaseClass = reader.GetAttribute("BaseClass");
			if (prepareData.BaseClass == null)
				prepareData.BaseClass = "System.DatabaseObject";

			if (prepareData.BaseClass != null && !prepareData.BaseClass.Contains("."))
				prepareData.BaseClass = Namespace.Name + "." + prepareData.BaseClass;

			logName = reader.GetAttribute("Log");
			logIndex = 0;

			while (reader.Read())
			{
				if (reader.NodeType != XmlNodeType.Element)
					continue;

				if (reader.Name.Equals("ArrayProperty", StringComparison.Ordinal))
				{
					ArrayPropertyDescriptor p = new ArrayPropertyDescriptor(reader.ReadSubtree(), this);
					prepareData.Properties.Add(p.Id, p);
				}

				if (reader.Name.Equals("SimpleProperty", StringComparison.Ordinal))
				{
					PropertyDescriptor p = new SimplePropertyDescriptor(reader.ReadSubtree(), this);
					prepareData.Properties.Add(p.Id, p);
				}

				if (reader.Name.Equals("ReferenceProperty", StringComparison.Ordinal))
				{
					ReferencePropertyDescriptor p = new ReferencePropertyDescriptor(reader.ReadSubtree(), this);
					prepareData.Properties.Add(p.Id, p);
				}

				if (reader.Name.Equals("Index", StringComparison.Ordinal))
				{
					prepareData.Indexes.Add(reader.GetAttribute("Name"));
				}
			}

			Model.LoadingTempData.Add(this, prepareData);
		}
	}

	public ClassDescriptor(DataModelDescriptor modelDesc, string name, short id, bool isAbstract,
		string baseClassName, string logName, PropertyDescriptor[] props, string[] indexes) :
		base(name)
	{
		PreparePhaseData prepareData = new PreparePhaseData();

		this.isAbstract = isAbstract;
		this.id = id;
		this.logName = logName;

		prepareData.BaseClass = baseClassName ?? "System.DatabaseObject";

		for (int i = 0; i < props.Length; i++)
		{
			prepareData.Properties.Add(props[i].Id, props[i]);
			props[i].OwnerClass = this;
		}

		prepareData.Indexes.AddRange(indexes);

		modelDesc.LoadingTempData.Add(this, prepareData);
	}

	public override ModelItemType Type => ModelItemType.Class;
	public int Index { get => index; set => index = value; }
	public short Id => id;
	public bool IsAbstract => isAbstract;
	public ClassDescriptor BaseClass => baseClass;
	public ReadOnlyArray<PropertyDescriptor> Properties => properties;
	public ReadOnlyArray<IndexDescriptor> Indexes => indexes;
	public ReadOnlyArray<short> DescendentClassIds => descendentClassIds;
	public ReadOnlyHashSet<short> DescendentClassIdsSet => descendentClassIdsSet;
	public int LogIndex => logIndex;
	public string LogName => logName;
	public ReadOnlyArray<int> PropertyByteOffsets => propertyByteOffsets;
	public ReadOnlyArray<int> StringPropertyIndexes => stringPropertyIndexes;
	public ReadOnlyArray<int> BlobPropertyIndexes => blobPropertyIndexes;
	public bool HasStringsOrBlobs => stringPropertyIndexes.Length > 0 || blobPropertyIndexes.Length > 0;
	public ReadOnlyArray<int> RefeferencePropertyIndexes => refeferencePropertyIndexes;
	public ReadOnlyArray<int> UntrackedRefeferencePropertyIndexes => untrackedRefeferencePropertyIndexes;
	public ReadOnlyArray<ReferencePropertyDescriptor> InverseReferences => inverseReferences;
	public ReadOnlyArray<ReferencePropertyDescriptor> CascadeDeletePreventInverseReferences => cascadeDeletePreventInverseReferences;
	public ReadOnlyArray<ReferencePropertyDescriptor> SetToNullInverseReferences => setToNullInverseReferences;
	public ReadOnlyArray<ReadOnlyArray<int>> PropertyIndexIndexes => propertyIndexIndexes;
	public int FirstStringBlobOffset => firstStringBlobOffset;
	public int FirstStringBlobIndex => firstStringBlobIndex;

	public ObjectModelClass ObjectModelClass { get => objectModelClass; set => objectModelClass = value; }

	public IEnumerable<ClassDescriptor> DirectDescendentClasses
	{
		get
		{
			foreach (short classId in descendentClassIdsSet)
			{
				ClassDescriptor d = Model.GetClass(classId);
				if (d.BaseClass.Id == this.Id)
					yield return d;
			}
		}
	}

	public IEnumerable<ClassDescriptor> SubtreeClasses
	{
		get
		{
			yield return this;
			foreach (short classId in DescendentClassIds)
			{
				yield return Model.GetClass(classId);
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool IsAssignable(short classId)
	{
		return id == classId || descendentClassIdsSet.ContainsKey(classId);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public PropertyDescriptor GetProperty(int propertyId)
	{
		if (!propertyIndexes.TryGetValue(propertyId, out int index))
			return null;

		return properties[index];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int GetPropertyIndex(int propertyId)
	{
		if (!propertyIndexes.TryGetValue(propertyId, out int index))
			return -1;

		return index;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public PropertyDescriptor GetProperty(string name)
	{
		nameToProperty.TryGetValue(name, out PropertyDescriptor pd);
		return pd;
	}

	public KeyComparerDesc GetIndexAccessDesc(IndexDescriptor indexDesc)
	{
		ReadOnlyArray<PropertyDescriptor> ips = indexDesc.Properties;
		KeyProperty[] props = new KeyProperty[ips.Length];
		for (int i = 0; i < props.Length; i++)
		{
			propertyIndexes.TryGetValue(ips[i].Id, out int index);
			if (indexDesc.Type == ModelItemType.HashIndex)
			{
				props[i] = new KeyProperty(ips[i].PropertyType, propertyByteOffsets[index], SortOrder.Asc);
			}
			else
			{
				props[i] = new KeyProperty(ips[i].PropertyType, propertyByteOffsets[index],
					((SortedIndexDescriptor)indexDesc).PropertySortOrder[i]);
			}
		}

		return new KeyComparerDesc(props, indexDesc.CultureName, indexDesc.CaseSensitive);
	}

	public KeyComparerDesc GetIndexAccessDescByPropertyName(IndexDescriptor indexDesc)
	{
		ReadOnlyArray<PropertyDescriptor> ips = indexDesc.Properties;
		KeyProperty[] props = new KeyProperty[ips.Length];
		for (int i = 0; i < props.Length; i++)
		{
			PropertyDescriptor propDesc = GetProperty(ips[i].Name);
			propertyIndexes.TryGetValue(propDesc.Id, out int index);
			if (indexDesc.Type == ModelItemType.HashIndex)
			{
				props[i] = new KeyProperty(propDesc.PropertyType, propertyByteOffsets[index], SortOrder.Asc);
			}
			else
			{
				int propIndex = GetPropertyIndex(propDesc.Id);
				props[i] = new KeyProperty(propDesc.PropertyType, propertyByteOffsets[index],
					((SortedIndexDescriptor)indexDesc).PropertySortOrder[i]);
			}
		}

		return new KeyComparerDesc(props, indexDesc.CultureName, indexDesc.CaseSensitive);
	}

	public void Prepare()
	{
		if (prepared)
			return;

		prepared = true;

		PrepareBaseClass();
		CreateProperties();
		PrepareProperties();
		PrepareIndexes();
	}

	private void PrepareIndexes()
	{
		PreparePhaseData prepareData = (PreparePhaseData)Model.LoadingTempData[this];
		if (prepareData.Indexes.Count > MaxIndexesPerClass)
			Throw.MaximumNumberOfIndexesPerClassExceeded(FullName);

		if (isAbstract)
		{
			indexes = ReadOnlyArray<IndexDescriptor>.Empty;
			return;
		}

		IndexDescriptor[] indexDescs = new IndexDescriptor[prepareData.Indexes.Count];
		for (int i = 0; i < prepareData.Indexes.Count; i++)
		{
			indexDescs[i] = prepareData.Indexes[i].Contains('.') ? Model.GetIndex(prepareData.Indexes[i])
				: Model.GetIndex($"{base.NamespaceName}.{prepareData.Indexes[i]}");

			if (indexDescs[i] == null)
				Throw.UnknownIndex(FullName, prepareData.Indexes[i]);

			indexDescs[i].AddClass(this);
		}

		indexes = new ReadOnlyArray<IndexDescriptor>(indexDescs);
	}

	public void PreparePropertyToIndexMapping()
	{
		List<int>[] l = new List<int>[Properties.Length];
		for (int i = 0; i < indexes.Length; i++)
		{
			IndexDescriptor hdesc = indexes[i];
			for (int j = 0; j < hdesc.Properties.Length; j++)
			{
				PropertyDescriptor propDesc = hdesc.Properties[j];
				int propIndex = GetPropertyIndex(propDesc.Id);
				Checker.AssertFalse(propIndex == -1);
				if (l[propIndex] == null)
					l[propIndex] = new List<int>(2);

				l[propIndex].Add(i);
			}
		}

		ReadOnlyArray<int>[] t = new ReadOnlyArray<int>[l.Length];
		for (int i = 0; i < l.Length; i++)
		{
			if (l[i] != null)
				t[i] = new ReadOnlyArray<int>(l[i].ToArray());
		}

		propertyIndexIndexes = new ReadOnlyArray<ReadOnlyArray<int>>(t);
	}

	private void CreateProperties()
	{
		PreparePhaseData prepareData = (PreparePhaseData)Model.LoadingTempData[this];
		if (prepareData.Properties.Count > MaxPropertyCount)
			Throw.MaximumNumberOfPropertiesInClassExceeded(FullName);

		List<PropertyDescriptor> l = new List<PropertyDescriptor>(prepareData.Properties.Values);
		l.Sort((x, y) => PropertyDescriptor.ComparePropertiesByOrder(x, y));
		properties = new ReadOnlyArray<PropertyDescriptor>(l.ToArray());
	}

	private void PrepareBaseClass()
	{
		PreparePhaseData prepareData = (PreparePhaseData)Model.LoadingTempData[this];

		if (Id == SystemCode.DatabaseObject.Id)
			return;

		baseClass = Model.GetClass(prepareData.BaseClass);
		if (baseClass == null)
			Throw.UnknownBaseClass(FullName);

		if (isAbstract && !baseClass.IsAbstract)
			Throw.AbstractClassNonAbstractParent(FullName);

		baseClass.Prepare();

		ValidateCircularHierarchy();

		PreparePhaseData basePrepareData = (PreparePhaseData)Model.LoadingTempData[baseClass];
		foreach (PropertyDescriptor prop in basePrepareData.Properties.Values)
		{
			prepareData.Properties.Add(prop.Id, prop);
		}

		prepareData.Merge(null, basePrepareData);
	}

	private void ValidateCircularHierarchy()
	{
		HashSet<string> baseClassNames = new HashSet<string>();
		ClassDescriptor curr = baseClass;
		while (curr != null)
		{
			if (baseClassNames.Contains(curr.FullName))
				Throw.CircularInheritance(FullName);

			PreparePhaseData currPrepareData = (PreparePhaseData)Model.LoadingTempData[curr];
			currPrepareData.DescendentClassIds.Add(Id);

			curr = curr.baseClass;
		}
	}

	public void SetInverseReferences(List<ReferencePropertyDescriptor> inverseReferences)
	{
		if (inverseReferences == null)
		{
			this.inverseReferences = ReadOnlyArray<ReferencePropertyDescriptor>.Empty;
		}
		else
		{
			if (inverseReferences.Count > MaxInverseReferencesPerClass)
				Throw.MaximumNumberOfInverseReferencesPerClass(FullName);

			ReferencePropertyDescriptor[] ra = inverseReferences.ToArray();
			IComparer<int> comp = Comparer<int>.Default;
			Array.Sort(ra, (x, y) => comp.Compare(x.Id, y.Id));
			this.inverseReferences = new ReadOnlyArray<ReferencePropertyDescriptor>(ra);
		}

		cascadeDeletePreventInverseReferences = new ReadOnlyArray<ReferencePropertyDescriptor>(
			this.inverseReferences.Where(x => x.DeleteTargetAction != DeleteTargetAction.SetToNull).ToArray());
		setToNullInverseReferences = new ReadOnlyArray<ReferencePropertyDescriptor>(
			this.inverseReferences.Where(x => x.DeleteTargetAction == DeleteTargetAction.SetToNull).ToArray());
	}

	public ReferencePropertyDescriptor FindInverseReference(int propertyId)
	{
		if (inverseReferences.Length < 8)
		{
			for (int i = 0; i < inverseReferences.Length; i++)
			{
				if (inverseReferences[i].Id == propertyId)
					return inverseReferences[i];
			}

			return null;
		}
		else
		{
			int low = 0;
			int high = inverseReferences.Length - 1;
			while (low <= high)
			{
				int mid = (low + high) / 2;
				if (inverseReferences[mid].Id == propertyId)
					return inverseReferences[mid];

				if (inverseReferences[mid].Id < propertyId)
					low = mid + 1;
				else if (inverseReferences[mid].Id > propertyId)
					high = mid - 1;
			}

			return null;
		}
	}

	public void PrepareDescendants()
	{
		PreparePhaseData prepareData = (PreparePhaseData)Model.LoadingTempData[this];
		descendentClassIdsSet = new ReadOnlyHashSet<short>(prepareData.DescendentClassIds);
		descendentClassIds = new ReadOnlyArray<short>(prepareData.DescendentClassIds.ToArray());
	}

	public void AssignLogName(string logName)
	{
		this.logName = logName;
	}

	public void AssignLogIndex(PersistenceDescriptor persistenceDesc)
	{
		if (logName == null || persistenceDesc == null)
		{
			logIndex = PersistenceDescriptor.FirstUserLogIndex;
			return;
		}

		for (int i = 0; i < persistenceDesc.LogDescriptors.Length; i++)
		{
			if (logName.Equals(persistenceDesc.LogDescriptors[i].Name, StringComparison.OrdinalIgnoreCase))
			{
				logIndex = i;
				return;
			}
		}

		logIndex = PersistenceDescriptor.FirstUserLogIndex;
	}

	public void PrepareReferenceProperties()
	{
		for (int i = 0; i < properties.Length; i++)
		{
			PropertyDescriptor p = properties[i];
			if (p.Kind == PropertyKind.Reference)
				((ReferencePropertyDescriptor)p).Prepare();
		}
	}

	private void PrepareProperties()
	{
		propertyIndexes = new IntToIntMap(properties.Length);
		nameToProperty = new Dictionary<string, PropertyDescriptor>(properties.Length);
		int[] offsets = new int[properties.Length];
		List<int> stringProps = new List<int>(4);
		List<int> blobProps = new List<int>(2);
		List<int> refPropes = new List<int>(2);
		List<int> untrackedRefProps = new List<int>(1);
		int offset = 0;
		firstStringBlobOffset = -1;
		firstStringBlobIndex = -1;

		for (int i = 0; i < properties.Length; i++)
		{
			PropertyDescriptor prop = properties[i];
			propertyIndexes.Add(prop.Id, i);

			if (nameToProperty.ContainsKey(prop.Name))
				Throw.DuplicatePropertyName(FullName, prop.Name);

			if (prop.Kind == PropertyKind.Array || (prop.Kind == PropertyKind.Reference &&
				(prop as ReferencePropertyDescriptor).Multiplicity == Multiplicity.Many))
			{
				blobProps.Add(i);
			}

			if (prop.Kind == PropertyKind.Simple && prop.PropertyType == PropertyType.String)
				stringProps.Add(i);

			if (prop.Kind == PropertyKind.Reference)
			{
				refPropes.Add(i);
				if (!((ReferencePropertyDescriptor)prop).TrackInverseReferences)
					untrackedRefProps.Add(i);
			}

			if ((prop.PropertyType == PropertyType.String || prop.PropertyType >= PropertyType.ByteArray) && firstStringBlobOffset == -1)
			{
				firstStringBlobOffset = offset;
				firstStringBlobIndex = i;
			}

			nameToProperty.Add(prop.Name, prop);
			offsets[i] = offset;
			offset += PropertyTypesHelper.GetItemSize(prop.PropertyType);
		}

		stringPropertyIndexes = new ReadOnlyArray<int>(stringProps.ToArray());
		blobPropertyIndexes = new ReadOnlyArray<int>(blobProps.ToArray());
		refeferencePropertyIndexes = new ReadOnlyArray<int>(refPropes.ToArray());
		untrackedRefeferencePropertyIndexes = new ReadOnlyArray<int>(untrackedRefProps.ToArray());
		propertyByteOffsets = new ReadOnlyArray<int>(offsets);
	}

	public void OnDeserializeModel(Dictionary<ClassDescriptor, List<short>> descendants)
	{
		ClassDescriptor currBase = baseClass;
		while (currBase != null)
		{
			if (!descendants.TryGetValue(currBase, out List<short> l))
			{
				l = new List<short>(4);
				descendants.Add(currBase, l);
			}

			l.Add(Id);
			currBase = currBase.baseClass;
		}
	}

	public void OnDeserializeModelFinished(Dictionary<ClassDescriptor, List<short>> descendants)
	{
		if (descendants.TryGetValue(this, out List<short> l))
		{
			descendentClassIdsSet = new ReadOnlyHashSet<short>(new HashSet<short>(l));
			descendentClassIds = new ReadOnlyArray<short>(l.ToArray());
		}
		else
		{
			descendentClassIdsSet = new ReadOnlyHashSet<short>(new HashSet<short>(0));
			descendentClassIds = new ReadOnlyArray<short>(EmptyArray<short>.Instance);
		}

		PrepareProperties();
		PreparePropertyToIndexMapping();
	}

	public override void Serialize(BinaryWriter writer, ModelDescriptorSerializerContext ctx)
	{
		base.Serialize(writer, ctx);
		writer.Write(id);
		writer.Write(isAbstract);
		writer.Write(firstStringBlobOffset);
		writer.Write(firstStringBlobIndex);

		if (logName == null)
		{
			writer.Write((byte)0);
		}
		else
		{
			writer.Write((byte)1);
			writer.Write(logName);
		}

		ctx.Serialize(baseClass, writer);

		writer.Write((short)properties.Length);
		foreach (PropertyDescriptor pd in properties)
		{
			ctx.Serialize(pd, writer);
		}

		writer.Write((byte)indexes.Length);
		foreach (IndexDescriptor msi in indexes)
		{
			ctx.Serialize(msi, writer);
		}
	}

	public override void Deserialize(BinaryReader reader, ModelDescriptorDeserializerContext ctx)
	{
		base.Deserialize(reader, ctx);

		id = reader.ReadInt16();
		isAbstract = reader.ReadBoolean();
		firstStringBlobOffset = reader.ReadInt32();
		firstStringBlobIndex = reader.ReadInt32();

		logName = null;
		if (reader.ReadByte() != 0)
			logName = reader.ReadString();

		baseClass = ctx.Deserialize<ClassDescriptor>(reader);

		int c = reader.ReadInt16();
		PropertyDescriptor[] ps = new PropertyDescriptor[c];
		for (int i = 0; i < c; i++)
		{
			ps[i] = ctx.Deserialize<PropertyDescriptor>(reader);
		}

		properties = new ReadOnlyArray<PropertyDescriptor>(ps);

		c = reader.ReadByte();
		IndexDescriptor[] msis = new IndexDescriptor[c];
		for (int i = 0; i < c; i++)
		{
			msis[i] = ctx.Deserialize<IndexDescriptor>(reader);
		}

		indexes = new ReadOnlyArray<IndexDescriptor>(msis);
	}

	public override string ToString()
	{
		return $"(Class, {FullName})";
	}

	private sealed class PreparePhaseData
	{
		string baseClass;
		Dictionary<int, PropertyDescriptor> properties;
		List<string> indexes;
		HashSet<short> descendentClassIds;

		public PreparePhaseData()
		{
			properties = new Dictionary<int, PropertyDescriptor>(8);
			indexes = new List<string>(0);
			descendentClassIds = new HashSet<short>(1);
		}

		public string BaseClass { get => baseClass; set => baseClass = value; }
		public Dictionary<int, PropertyDescriptor> Properties => properties;
		public List<string> Indexes => indexes;
		public HashSet<short> DescendentClassIds => descendentClassIds;

		public void Merge(ClassDescriptor owner, PreparePhaseData d)
		{
			indexes.AddRange(d.indexes);
		}
	}
}
