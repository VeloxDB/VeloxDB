using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class IndexUpdate
{
	IndexDescriptor prevIndexDesc;
	IndexDescriptor indexDesc;

	bool hasBecomeUnique;
	ReadOnlyArray<ClassDescriptor> insertedClasses;
	ReadOnlyArray<ClassDescriptor> deletedClasses;

	public IndexUpdate(IndexDescriptor prevIndexDesc, IndexDescriptor indexDesc, bool hasBecomeUnique,
		List<ClassDescriptor> insertedClasses, List<ClassDescriptor> deletedClasses)
	{
		this.prevIndexDesc = prevIndexDesc;
		this.indexDesc = indexDesc;
		this.hasBecomeUnique = hasBecomeUnique;
		this.insertedClasses = ReadOnlyArray<ClassDescriptor>.FromNullable(insertedClasses);
		this.deletedClasses = ReadOnlyArray<ClassDescriptor>.FromNullable(deletedClasses);
	}

	public bool HasBecomeUnique => hasBecomeUnique;
	public IndexDescriptor PrevIndexDesc => prevIndexDesc;
	public IndexDescriptor IndexDesc => indexDesc;
	public ReadOnlyArray<ClassDescriptor> InsertedClasses => insertedClasses;
	public ReadOnlyArray<ClassDescriptor> DeletedClasses => deletedClasses;
}
