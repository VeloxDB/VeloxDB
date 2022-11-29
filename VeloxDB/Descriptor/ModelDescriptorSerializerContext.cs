using System;
using System.Collections.Generic;
using System.IO;
using Velox.Common;

namespace Velox.Descriptor;

internal sealed class ModelDescriptorSerializerContext
{
	int currId;
	Dictionary<ModelItemDescriptor, int> mapping;

	public ModelDescriptorSerializerContext()
	{
		currId = 0;
		mapping = new Dictionary<ModelItemDescriptor, int>(512, ReferenceEqualityComparer<ModelItemDescriptor>.Instance);
	}

	public void Serialize(ModelItemDescriptor obj, BinaryWriter writer)
	{
		if (obj == null)
		{
			writer.Write(-1);
			return;
		}

		if (mapping.TryGetValue(obj, out int id))
		{
			writer.Write(id);
			return;
		}

		id = currId;
		currId++;

		mapping.Add(obj, id);
		writer.Write(id);
		writer.Write((byte)obj.Type);
		obj.Serialize(writer, this);
	}
}
