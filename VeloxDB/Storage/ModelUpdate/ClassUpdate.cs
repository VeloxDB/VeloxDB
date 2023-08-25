using System;
using System.Collections.Generic;
using System.Linq;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal class ClassUpdate
{
	ClassDescriptor prevClassDesc;
	ClassDescriptor classDesc;

	bool isLogModified;
	bool isAbstractModified;
	bool isHierarchyTypeModified;
	bool indexedPropertiesModified;
	bool isBaseClassModified;
	ReadOnlyArray<PropertyInsert> insertedProperties;
	ReadOnlyArray<PropertyDelete> deletedProperties;
	ReadOnlyArray<PropertyUpdate> updatedProperties;

	public ClassUpdate(ClassDescriptor prevClassDesc, ClassDescriptor classDesc, bool isAbstractModified, bool isLogModified,
		bool isHierarchyTypeModified, bool indexedPropertiesModified, bool isBaseClassModified, List<PropertyInsert> insertedProperties,
		List<PropertyDelete> deletedProperties, List<PropertyUpdate> updatedProperties)
	{
		this.prevClassDesc = prevClassDesc;
		this.classDesc = classDesc;
		this.isAbstractModified = isAbstractModified;
		this.isLogModified = isLogModified;
		this.isHierarchyTypeModified = isHierarchyTypeModified;
		this.indexedPropertiesModified = indexedPropertiesModified;
		this.isBaseClassModified = isBaseClassModified;
		this.insertedProperties = ReadOnlyArray<PropertyInsert>.FromNullable(insertedProperties);
		this.deletedProperties = ReadOnlyArray<PropertyDelete>.FromNullable(deletedProperties);
		this.updatedProperties = ReadOnlyArray<PropertyUpdate>.FromNullable(updatedProperties);
	}

	public ClassDescriptor PrevClassDesc => prevClassDesc;
	public ClassDescriptor ClassDesc => classDesc;
	public ReadOnlyArray<PropertyInsert> InsertedProperties => insertedProperties;
	public ReadOnlyArray<PropertyDelete> DeletedProperties => deletedProperties;
	public ReadOnlyArray<PropertyUpdate> UpdatedProperties => updatedProperties;
	public bool IsAbstractModified => isAbstractModified;
	public bool IsLogModified => isLogModified;
	public bool IsHierarchyTypeModified => isHierarchyTypeModified;
	public bool IsBaseClassModified => isBaseClassModified;
	public bool PropertyListModified => deletedProperties.Length > 0 || insertedProperties.Length > 0;
	public bool IndexedPropertiesModified => indexedPropertiesModified;
	public bool ReferenceTrackingModified => updatedProperties.Any(x => x.InvRefTrackingModified);

	public bool HasDefaultValueChanges
	{
		get
		{
			for (int i = 0; i < updatedProperties.Length; i++)
			{
				if (updatedProperties[i].DefaultValueChanged)
					return true;
			}

			return false;
		}
	}

	public bool RequiresDefaultValueWrite => !classDesc.IsAbstract && insertedProperties.Length > 0;
}
