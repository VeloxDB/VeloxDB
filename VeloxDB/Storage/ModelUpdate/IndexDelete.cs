using System;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class IndexDelete
{
	IndexDescriptor indexDesc;

	public IndexDelete(IndexDescriptor indexDesc)
	{
		this.indexDesc = indexDesc;
	}

	public IndexDescriptor IndexDesc => indexDesc;
}
