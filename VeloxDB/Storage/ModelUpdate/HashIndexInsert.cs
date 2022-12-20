using System;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class HashIndexInsert
{
	HashIndexDescriptor hashIndexDesc;

	public HashIndexInsert(HashIndexDescriptor hashIndexDesc)
	{
		this.hashIndexDesc = hashIndexDesc;
	}

	public HashIndexDescriptor HashIndexDesc => hashIndexDesc;
}
