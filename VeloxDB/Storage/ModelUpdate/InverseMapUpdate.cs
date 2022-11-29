using System;
using System.Collections.Generic;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.Storage.ModelUpdate;

internal sealed class InverseMapUpdate
{
	ClassDescriptor classDesc;
	ReadOnlyArray<PropertyDescriptor> untrackedReferences;
	ReadOnlyArray<PropertyDescriptor> trackedReferences;
	ReadOnlyArray<PropertyDescriptor> partiallyDeletedReferences;
	ReadOnlyArray<PropertyDescriptor> deletedReferences;
	ReadOnlyArray<PropertyDescriptor> insertedReferences;

	public InverseMapUpdate(ClassDescriptor classDesc, List<PropertyDescriptor> untrackedProperties,
		List<PropertyDescriptor> trackedProperties, List<PropertyDescriptor> deletedReferences, List<PropertyDescriptor> insertedReferences,
		List<PropertyDescriptor> partiallyDeletedReferences)
	{
		this.classDesc = classDesc;
		this.untrackedReferences = ReadOnlyArray<PropertyDescriptor>.FromNullable(untrackedProperties);
		this.trackedReferences = ReadOnlyArray<PropertyDescriptor>.FromNullable(trackedProperties);
		this.deletedReferences = ReadOnlyArray<PropertyDescriptor>.FromNullable(deletedReferences);
		this.insertedReferences = ReadOnlyArray<PropertyDescriptor>.FromNullable(insertedReferences);
		this.partiallyDeletedReferences = ReadOnlyArray<PropertyDescriptor>.FromNullable(partiallyDeletedReferences);
	}

	public ClassDescriptor ClassDesc => classDesc;
	public ReadOnlyArray<PropertyDescriptor> UntrackedReferences => untrackedReferences;
	public ReadOnlyArray<PropertyDescriptor> TrackedReferences => trackedReferences;
	public ReadOnlyArray<PropertyDescriptor> DeletedReferences => deletedReferences;
	public ReadOnlyArray<PropertyDescriptor> PartiallyDeletedReferences => partiallyDeletedReferences;
	public ReadOnlyArray<PropertyDescriptor> InsertedReferences => insertedReferences;
}
