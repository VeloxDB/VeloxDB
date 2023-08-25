using System;
using System.IO;

namespace VeloxDB.Descriptor;

internal enum ModelItemType : byte
{
	Model = 1,
	Namespace = 2,
	Class = 3,
	SimpleProperty = 4,
	ArrayProperty = 5,
	ReferenceProperty = 6,
	HashIndex = 7,
	SortedIndex = 8,
}

internal abstract class ModelItemDescriptor
{
	public abstract ModelItemType Type { get; }

	public abstract void Serialize(BinaryWriter writer, ModelDescriptorSerializerContext context);
	public abstract void Deserialize(BinaryReader reader, ModelDescriptorDeserializerContext context);
}
