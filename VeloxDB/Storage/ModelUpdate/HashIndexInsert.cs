using System;
using Velox.Descriptor;

namespace Velox.Storage.ModelUpdate;

internal sealed class HashIndexInsert
{
	HashIndexDescriptor hashIndexDesc;

	public HashIndexInsert(HashIndexDescriptor hashIndexDesc)
	{
		this.hashIndexDesc = hashIndexDesc;
	}

	public HashIndexDescriptor HashIndexDesc => hashIndexDesc;
}
