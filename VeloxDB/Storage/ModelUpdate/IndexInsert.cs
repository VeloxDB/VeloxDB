using System;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class IndexInsert
{
	IndexDescriptor indexDesc;

	public IndexInsert(IndexDescriptor indexDesc)
	{
		this.indexDesc = indexDesc;
	}

	public IndexDescriptor IndexDesc => indexDesc;
}
