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
	bool hashedPropertiesModified;
	ReadOnlyArray<PropertyInsert> insertedProperties;
	ReadOnlyArray<PropertyDelete> deletedProperties;
	ReadOnlyArray<PropertyUpdate> updatedProperties;

	public ClassUpdate(ClassDescriptor prevClassDesc, ClassDescriptor classDesc, bool isAbstractModified, bool isLogModified,
		bool isHierarchyTypeModified, bool hashedPropertiesModified, List<PropertyInsert> insertedProperties,
		List<PropertyDelete> deletedProperties, List<PropertyUpdate> updatedProperties)
	{
		this.prevClassDesc = prevClassDesc;
		this.classDesc = classDesc;
		this.isAbstractModified = isAbstractModified;
		this.isLogModified = isLogModified;
		this.isHierarchyTypeModified = isHierarchyTypeModified;
		this.hashedPropertiesModified = hashedPropertiesModified;
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
	public bool PropertyListModified => deletedProperties.Length > 0 || insertedProperties.Length > 0;
	public bool HashedPropertiesModified => hashedPropertiesModified;
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

	public bool RequiresDefaultValueWrite
	{
		get
		{
			if (classDesc.IsAbstract)
				return false;

			for (int i = 0; i < insertedProperties.Length; i++)
			{
				PropertyDescriptor propDesc = insertedProperties[i].PropDesc;
				if (propDesc.Kind == PropertyKind.Simple && propDesc.PropertyType != PropertyType.String)
					return true;
			}

			return false;
		}
	}

	public IEnumerable<PropertyDescriptor> DefaultValueRequireingProperties
	{
		get
		{
			for (int i = 0; i < insertedProperties.Length; i++)
			{
				PropertyDescriptor propDesc = insertedProperties[i].PropDesc;
				if (propDesc.Kind == PropertyKind.Simple && propDesc.PropertyType != PropertyType.String)
					yield return propDesc;
			}
		}
	}
}
