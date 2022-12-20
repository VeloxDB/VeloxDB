using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class HashIndexUpdate
{
	HashIndexDescriptor prevHashIndexDesc;
	HashIndexDescriptor hashIndexDesc;

	bool hasBecomeUnique;
	ReadOnlyArray<ClassDescriptor> insertedClasses;
	ReadOnlyArray<ClassDescriptor> deletedClasses;

	public HashIndexUpdate(HashIndexDescriptor prevHashIndexDesc, HashIndexDescriptor hashIndexDesc, bool hasBecomeUnique,
		List<ClassDescriptor> insertedClasses, List<ClassDescriptor> deletedClasses)
	{
		this.prevHashIndexDesc = prevHashIndexDesc;
		this.hashIndexDesc = hashIndexDesc;
		this.hasBecomeUnique = hasBecomeUnique;
		this.insertedClasses = ReadOnlyArray<ClassDescriptor>.FromNullable(insertedClasses);
		this.deletedClasses = ReadOnlyArray<ClassDescriptor>.FromNullable(deletedClasses);
	}

	public bool HasBecomeUnique => hasBecomeUnique;
	public HashIndexDescriptor PrevHashIndexDesc => prevHashIndexDesc;
	public HashIndexDescriptor HashIndexDesc => hashIndexDesc;
	public ReadOnlyArray<ClassDescriptor> InsertedClasses => insertedClasses;
	public ReadOnlyArray<ClassDescriptor> DeletedClasses => deletedClasses;
}
