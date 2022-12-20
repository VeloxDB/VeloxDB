using System;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class HashIndexDelete
{
	HashIndexDescriptor hashIndexDesc;

	public HashIndexDelete(HashIndexDescriptor hashIndexDesc)
	{
		this.hashIndexDesc = hashIndexDesc;
	}

	public HashIndexDescriptor HashIndexDesc => hashIndexDesc;
}
