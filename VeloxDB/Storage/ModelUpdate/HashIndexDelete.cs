using System;
using Velox.Descriptor;

namespace Velox.Storage.ModelUpdate;

internal sealed class HashIndexDelete
{
	HashIndexDescriptor hashIndexDesc;

	public HashIndexDelete(HashIndexDescriptor hashIndexDesc)
	{
		this.hashIndexDesc = hashIndexDesc;
	}

	public HashIndexDescriptor HashIndexDesc => hashIndexDesc;
}
