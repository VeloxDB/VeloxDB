using System;
using System.Collections.Generic;
using System.IO;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal sealed class ModelDescriptorDeserializerContext
{
	static readonly Func<ModelItemDescriptor>[] factories;

	Dictionary<int, ModelItemDescriptor> objects = new Dictionary<int, ModelItemDescriptor>(512);

	static ModelDescriptorDeserializerContext()
	{
		factories = new Func<ModelItemDescriptor>[Utils.MaxEnumValue(typeof(ModelItemType)) + 1];
		factories[(int)ModelItemType.Model] = () => new DataModelDescriptor();
		factories[(int)ModelItemType.Namespace] = () => new NamespaceDescriptor();
		factories[(int)ModelItemType.Class] = () => new ClassDescriptor();
		factories[(int)ModelItemType.SimpleProperty] = () => new SimplePropertyDescriptor();
		factories[(int)ModelItemType.ArrayProperty] = () => new ArrayPropertyDescriptor();
		factories[(int)ModelItemType.ReferenceProperty] = () => new ReferencePropertyDescriptor();
		factories[(int)ModelItemType.HashIndex] = () => new HashIndexDescriptor();
	}

	public T Deserialize<T>(BinaryReader reader) where T : ModelItemDescriptor
	{
		int id = reader.ReadInt32();

		if (id == -1)
			return null;

		if (!objects.TryGetValue(id, out ModelItemDescriptor item))
		{
			ModelItemType itemType = (ModelItemType)reader.ReadByte();
			Func<ModelItemDescriptor> factory = factories[(int)itemType];
			item = factory();
			objects.Add(id, item);
			item.Deserialize(reader, this);
		}

		return (T)item;
	}
}
