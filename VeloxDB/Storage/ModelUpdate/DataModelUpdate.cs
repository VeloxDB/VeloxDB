using System;
using System.Collections.Generic;
using System.Linq;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class DataModelUpdate
{
	Database database;
	DataModelDescriptor prevModelDesc, modelDesc;

	BuildupState state;

	ReadOnlyArray<ClassInsert> insertedClasses;
	ReadOnlyHashMap<short, ClassDelete> deletedClasses;
	ReadOnlyHashMap<short, ClassUpdate> updatedClasses;
	ReadOnlyArray<InverseMapInsert> insertedInvRefMaps;
	ReadOnlyArray<InverseMapDelete> deletedInvRefMaps;
	ReadOnlyArray<InverseMapUpdate> updatedInvRefMaps;
	ReadOnlyHashMap<short, HashIndexInsert> insertedHashIndexes;
	ReadOnlyHashMap<short, HashIndexDelete> deletedHashIndexes;
	ReadOnlyHashMap<short, HashIndexUpdate> updatedHashIndexes;

	bool isAlignment;

	public DataModelUpdate(Database database, DataModelDescriptor prevModelDesc, DataModelDescriptor modelDesc, bool isAlignment)
	{
		this.database = database;
		this.prevModelDesc = prevModelDesc;
		this.modelDesc = modelDesc;
		this.isAlignment = isAlignment;

		DetectDiff();
	}

	public bool IsEmpty
	{
		get
		{
			return insertedClasses.Length == 0 && deletedClasses.Count == 0 && updatedClasses.Count == 0 &&
				insertedInvRefMaps.Length == 0 && deletedInvRefMaps.Length == 0 && updatedInvRefMaps.Length == 0 &&
				insertedHashIndexes.Count == 0 && deletedHashIndexes.Count == 0 && updatedHashIndexes.Count == 0;
		}
	}

	public ReadOnlyArray<ClassInsert> InsertedClasses => insertedClasses;
	public ReadOnlyHashMap<short, ClassDelete> DeletedClasses => deletedClasses;
	public ReadOnlyHashMap<short, ClassUpdate> UpdatedClasses => updatedClasses;
	public ReadOnlyArray<InverseMapInsert> InsertedInvRefMaps => insertedInvRefMaps;
	public ReadOnlyArray<InverseMapDelete> DeletedInvRefMaps => deletedInvRefMaps;
	public ReadOnlyArray<InverseMapUpdate> UpdatedInvRefMaps => updatedInvRefMaps;
	public ReadOnlyHashMap<short, HashIndexInsert> InsertedHashIndexes => insertedHashIndexes;
	public ReadOnlyHashMap<short, HashIndexDelete> DeletedHashIndexes => deletedHashIndexes;
	public ReadOnlyHashMap<short, HashIndexUpdate> UpdatedHashIndexes => updatedHashIndexes;

	public DataModelDescriptor PrevModelDesc => prevModelDesc;
	public DataModelDescriptor ModelDesc => modelDesc;
	public bool IsAlignment => isAlignment;

	public bool RequiresDefaultValueWrites
	{
		get
		{
			foreach (ClassUpdate cu in updatedClasses.Values)
			{
				if (cu.RequiresDefaultValueWrite)
					return true;
			}

			return false;
		}
	}

	public bool HashIndexModified(short id)
	{
		return updatedHashIndexes.ContainsKey(id) ||
			(deletedHashIndexes.ContainsKey(id) && insertedHashIndexes.ContainsKey(id));
	}

	public bool ReferencePropertyStillExists(short classId, int propId)
	{
		ClassDescriptor classDesc = modelDesc.GetClass(classId);
		if (classDesc == null)
			return false;

		PropertyDescriptor propDesc = classDesc.GetProperty(propId);
		return propDesc != null && propDesc.Kind == PropertyKind.Reference;
	}

	private void DetectDiff()
	{
		state = new BuildupState();

		HashSet<short> modifiedTargets = DetectModifiedTargets(prevModelDesc, modelDesc);

		DetectClassesDiff(modifiedTargets);
		DetectInverseRefMapsDiff(prevModelDesc, modelDesc);
		DetectHashIndexesDiff(prevModelDesc, modelDesc);

		insertedClasses = ReadOnlyArray<ClassInsert>.FromNullable(state.InsertedClasses);
		updatedClasses = new ReadOnlyHashMap<short, ClassUpdate>(state.UpdatedClasses);
		deletedClasses = new ReadOnlyHashMap<short, ClassDelete>(state.DeletedClasses);
		insertedHashIndexes = new ReadOnlyHashMap<short, HashIndexInsert>(state.InsertedHashIndexes);
		updatedHashIndexes = new ReadOnlyHashMap<short, HashIndexUpdate>(state.UpdatedHashIndexes);
		deletedHashIndexes = new ReadOnlyHashMap<short, HashIndexDelete>(state.DeletedHashIndexes);
		insertedInvRefMaps = ReadOnlyArray<InverseMapInsert>.FromNullable(state.InsertedInvRefMaps);
		updatedInvRefMaps = ReadOnlyArray<InverseMapUpdate>.FromNullable(state.UpdatedInvRefMaps);
		deletedInvRefMaps = ReadOnlyArray<InverseMapDelete>.FromNullable(state.DeletedInvRefMaps);
	}

	private void DetectClassesDiff(HashSet<short> modifiedTargets)
	{
		foreach (ClassDescriptor classDesc in modelDesc.GetAllClasses())
		{
			ClassDescriptor prevClassDesc = prevModelDesc.GetClass(classDesc.Id);
			TTTrace.Write(database.TraceId, database.Id, classDesc.Id, prevClassDesc != null);

			if (prevClassDesc == null)
			{
				database.Engine.Trace.Debug("Class {0} inserted.", classDesc.FullName);
				state.InsertedClasses.Add(new ClassInsert(classDesc));
			}
			else
			{
				DetectClassDiff(modifiedTargets, prevClassDesc, classDesc);
			}
		}

		foreach (ClassDescriptor prevClassDesc in prevModelDesc.GetAllClasses())
		{
			if (modelDesc.GetClass(prevClassDesc.Id) == null)
			{
				TTTrace.Write(database.TraceId, database.Id, prevClassDesc.Id);

				database.Engine.Trace.Debug("Class {0} deleted.", prevClassDesc.FullName);
				TTTrace.Write(database.TraceId, database.Id, prevClassDesc.Id);

				state.DeletedClasses.Add(prevClassDesc.Id, new ClassDelete(prevClassDesc));
			}
		}
	}

	private void DetectHashIndexesDiff(DataModelDescriptor prevModelDesc, DataModelDescriptor modelDesc)
	{
		foreach (HashIndexDescriptor hashDesc in modelDesc.GetAllHashIndexes())
		{
			HashIndexDescriptor prevHashDesc = prevModelDesc.GetHashIndex(hashDesc.Id);
			TTTrace.Write(database.TraceId, database.Id, prevHashDesc == null ? -1 : prevHashDesc.Id);

			if (prevHashDesc == null)
			{
				database.Engine.Trace.Debug("Hash index {0} inserted.", hashDesc.FullName);

				state.InsertedHashIndexes.Add(hashDesc.Id, new HashIndexInsert(hashDesc));
				foreach (ClassDescriptor hashClassDesc in hashDesc.Classes)
				{
					ValidateHashIndexPropertiesNotNewlyIntroduced(hashDesc, hashClassDesc);
				}
			}
			else
			{
				DetectHashInedexDiff(prevHashDesc, hashDesc);
			}
		}

		foreach (HashIndexDescriptor prevHashDesc in prevModelDesc.GetAllHashIndexes())
		{
			if (modelDesc.GetHashIndex(prevHashDesc.Id) == null)
			{
				database.Engine.Trace.Debug("Hash index {0} deleted.", prevHashDesc.FullName);
				TTTrace.Write(database.TraceId, database.Id, prevHashDesc.Id);

				state.DeletedHashIndexes.Add(prevHashDesc.Id, new HashIndexDelete(prevHashDesc));
			}
		}
	}

	private void DetectHashInedexDiff(HashIndexDescriptor prevHashDesc, HashIndexDescriptor hashDesc)
	{
		if (HashIndexPropertiesDiffer(prevHashDesc, hashDesc))
		{
			database.Engine.Trace.Debug("Hash index {0} properties modified.", prevHashDesc.FullName);
			TTTrace.Write(database.TraceId, database.Id, prevHashDesc.Id);

			state.DeletedHashIndexes.Add(prevHashDesc.Id, new HashIndexDelete(prevHashDesc));
			state.InsertedHashIndexes.Add(hashDesc.Id, new HashIndexInsert(hashDesc));
			return;
		}

		bool hasBecomeUnique = !prevHashDesc.IsUnique && hashDesc.IsUnique;
		if (hasBecomeUnique)
			database.Engine.Trace.Debug("Hash index {0} has become unique.", hashDesc.FullName);

		List<ClassDescriptor> insertedClasses = null;
		List<ClassDescriptor> deletedClasses = null;

		foreach (ClassDescriptor hashClassDesc in hashDesc.Classes)
		{
			ClassDescriptor prevHashClassDesc = prevHashDesc.Classes.FirstOrDefault(x => x.Id == hashClassDesc.Id);
			if (prevHashClassDesc == null)
			{
				if (insertedClasses == null)
					insertedClasses = new List<ClassDescriptor>();

				if (this.prevModelDesc.GetClass(hashClassDesc.Id) != null)  // Newly inserted classes will not affect the hash index
				{
					database.Engine.Trace.Debug("Class {0} inserted into hash index {1}.", hashClassDesc.FullName, hashDesc.FullName);
					TTTrace.Write(database.TraceId, database.Id, hashDesc.Id, hashClassDesc.Id);

					ValidateHashIndexPropertiesNotNewlyIntroduced(hashDesc, hashClassDesc);
					insertedClasses.Add(hashClassDesc);
				}
			}
		}

		foreach (ClassDescriptor prevHashClassDesc in prevHashDesc.Classes)
		{
			ClassDescriptor hashClassDesc = hashDesc.Classes.FirstOrDefault(x => x.Id == prevHashClassDesc.Id);
			if (hashClassDesc == null)
			{
				if (deletedClasses == null)
					deletedClasses = new List<ClassDescriptor>();

				database.Engine.Trace.Debug("Class {0} deleted from hash index {1}.", prevHashClassDesc.FullName, prevHashDesc.FullName);
				TTTrace.Write(database.TraceId, database.Id, prevHashDesc.Id, prevHashClassDesc.Id);

				deletedClasses.Add(prevHashClassDesc);
			}
		}

		if (hasBecomeUnique || insertedClasses != null || deletedClasses != null)
		{
			TTTrace.Write(database.TraceId, database.Id, prevHashDesc.Id, hasBecomeUnique, insertedClasses != null, deletedClasses != null);

			state.UpdatedHashIndexes.Add(prevHashDesc.Id,
				new HashIndexUpdate(prevHashDesc, hashDesc, hasBecomeUnique, insertedClasses, deletedClasses));
		}
	}

	private void ValidateHashIndexPropertiesNotNewlyIntroduced(HashIndexDescriptor hashDesc, ClassDescriptor hashClassDesc)
	{
		if (isAlignment)
			return;

		ClassDescriptor prevHashClassDesc = prevModelDesc.GetClass(hashClassDesc.Id);
		if (prevHashClassDesc == null)
			return;

		ReadOnlyArray<PropertyDescriptor> hashProperties = hashDesc.Properties;
		for (int i = 0; i < hashProperties.Length; i++)
		{
			// It is invalid to add an existing class to a hash index with a newly introduced property
			if (prevHashClassDesc.GetProperty(hashProperties[i].Id) == null)
			{
				throw new DatabaseException(DatabaseErrorDetail.
					CreateInsertedPropertyClassAddedToHashIndex(hashClassDesc.FullName, hashProperties[i].Name));
			}
		}
	}

	private bool HashIndexPropertiesDiffer(HashIndexDescriptor prevHashDesc, HashIndexDescriptor hashDesc)
	{
		if (prevHashDesc.Properties.Length != hashDesc.Properties.Length)
			return true;

		for (int i = 0; i < prevHashDesc.Properties.Length; i++)
		{
			if (prevHashDesc.Properties[i].Id != hashDesc.Properties[i].Id)
			{
				TTTrace.Write(database.TraceId, database.Id, prevHashDesc.Id, prevHashDesc.Properties[i].Id, hashDesc.Properties[i].Id);
				return true;
			}
		}

		return false;
	}

	private void DetectInverseRefMapsDiff(DataModelDescriptor prevModelDesc, DataModelDescriptor modelDesc)
	{
		Dictionary<int, ReferencePropertyDescriptor> temp = new Dictionary<int, ReferencePropertyDescriptor>(4);
		foreach (ClassDescriptor classDesc in modelDesc.GetAllClasses().Where(x => !x.IsAbstract && x.InverseReferences.Length > 0))
		{
			ClassDescriptor prevClassDesc = prevModelDesc.GetClass(classDesc.Id);
			if (prevClassDesc == null || prevClassDesc.InverseReferences.Length == 0 || prevClassDesc.IsAbstract)
			{
				database.Engine.Trace.Debug("Inverse reference map {0} inserted.", classDesc.FullName);
				TTTrace.Write(database.TraceId, database.Id, classDesc.Id);

				state.InsertedInvRefMaps.Add(new InverseMapInsert(classDesc));
			}
			else
			{
				DetectInverseRefMapUpdate(prevClassDesc, classDesc, state.UpdatedInvRefMaps, temp);
			}
		}

		foreach (ClassDescriptor prevClassDesc in prevModelDesc.GetAllClasses().Where(x => !x.IsAbstract && x.InverseReferences.Length != 0))
		{
			ClassDescriptor classDesc = modelDesc.GetClass(prevClassDesc.Id);
			if (classDesc == null || classDesc.InverseReferences.Length == 0 || classDesc.IsAbstract)
			{
				database.Engine.Trace.Debug("Inverse reference map {0} deleted.", prevClassDesc.FullName);
				TTTrace.Write(database.TraceId, database.Id, prevClassDesc.Id);

				state.DeletedInvRefMaps.Add(new InverseMapDelete(prevClassDesc));
			}
		}
	}

	private void DetectInverseRefMapUpdate(ClassDescriptor prevClassDesc, ClassDescriptor classDesc,
		List<InverseMapUpdate> updatedMaps, Dictionary<int, ReferencePropertyDescriptor> temp)
	{
		if (classDesc.IsAbstract)
			return;

		List<PropertyDescriptor> untrackedReferences = null;
		List<PropertyDescriptor> trackedReferences = null;
		List<PropertyDescriptor> deletedReferences = null;
		List<PropertyDescriptor> insertedReferences = null;
		List<PropertyDescriptor> partiallyDeletedReferences = null;

		foreach (ReferencePropertyDescriptor prevRefPropDesc in prevClassDesc.InverseReferences)
		{
			temp.Add(prevRefPropDesc.Id, prevRefPropDesc);
		}

		foreach (ReferencePropertyDescriptor refPropDesc in classDesc.InverseReferences)
		{
			if (temp.TryGetValue(refPropDesc.Id, out ReferencePropertyDescriptor prevRefPropDesc))
			{
				if (refPropDesc.TrackInverseReferences && !prevRefPropDesc.TrackInverseReferences)
				{
					if (trackedReferences == null)
						trackedReferences = new List<PropertyDescriptor>();

					database.Engine.Trace.Debug("Inverse reference {0} of class {1} tracking started, inverse reference map {2}.",
						refPropDesc.Name, refPropDesc.OwnerClass.FullName, classDesc.Id);
					TTTrace.Write(database.TraceId, database.Id, refPropDesc.Id, refPropDesc.OwnerClass.Id, classDesc.Id);

					trackedReferences.Add(prevRefPropDesc);
				}
				else if (!refPropDesc.TrackInverseReferences && prevRefPropDesc.TrackInverseReferences)
				{
					untrackedReferences  ??= new List<PropertyDescriptor>();

					database.Engine.Trace.Debug("Inverse reference {0} of class {1} tracking stopped, inverse reference map {2}.",
						refPropDesc.Name, refPropDesc.OwnerClass.FullName, classDesc.Id);
					TTTrace.Write(database.TraceId, database.Id, refPropDesc.Id, refPropDesc.OwnerClass.Id, classDesc.Id);

					untrackedReferences.Add(prevRefPropDesc);
				}
			}
			else
			{
				database.Engine.Trace.Debug("Inverse reference {0} of class {1} inserted, inverse reference map {2}.",
					refPropDesc.Name, refPropDesc.OwnerClass.FullName, classDesc.Id);
				insertedReferences ??= new List<PropertyDescriptor>();
			}
		}

		temp.Clear();

		foreach (ReferencePropertyDescriptor prevRefPropDesc in prevClassDesc.InverseReferences)
		{
			ClassDescriptor prevDefiningClassDesc = prevRefPropDesc.OwnerClass;
			ClassDescriptor definingClassDesc = classDesc.Model.GetClass(prevDefiningClassDesc.Id);

			if (definingClassDesc == null || definingClassDesc.GetProperty(prevRefPropDesc.Id) == null)
			{
				// Reference property has been removed completely
				if (deletedReferences == null)
					deletedReferences = new List<PropertyDescriptor>();

				database.Engine.Trace.Debug("Inverse reference {0} of class {1} deleted, inverse reference map {2}.",
					prevRefPropDesc.Name, prevDefiningClassDesc.FullName, classDesc.Id);
				TTTrace.Write(database.TraceId, database.Id, prevRefPropDesc.Id, prevDefiningClassDesc.Id, classDesc.Id);

				deletedReferences.Add(prevRefPropDesc);
			}
			else if (!IsNonAbstractSubset(prevDefiningClassDesc, definingClassDesc))
			{
				// Reference property has been removed partially (from some classes by changing the base class and or abstractness)
				if (partiallyDeletedReferences == null)
					partiallyDeletedReferences = new List<PropertyDescriptor>();

				database.Engine.Trace.Debug("Inverse reference {0} of class {1} partially deleted, inverse reference map {2}.",
					prevRefPropDesc.Name, prevDefiningClassDesc.FullName, classDesc.Id);
				TTTrace.Write(database.TraceId, database.Id, prevRefPropDesc.Id, prevDefiningClassDesc.Id, classDesc.Id);

				partiallyDeletedReferences.Add(prevRefPropDesc);
			}
		}

		if (untrackedReferences != null || trackedReferences != null ||
			deletedReferences != null || partiallyDeletedReferences != null || insertedReferences != null)
		{
			updatedMaps.Add(new InverseMapUpdate(classDesc, untrackedReferences,
				trackedReferences, deletedReferences, insertedReferences, partiallyDeletedReferences));
		}
	}

	private bool IsNonAbstractSubset(ClassDescriptor prevClassDesc, ClassDescriptor classDesc)
	{
		foreach (ClassDescriptor cd1 in prevClassDesc.SubtreeClasses)
		{
			ClassDescriptor cd2 = classDesc.Model.GetClass(cd1.Id);
			if (cd2 == null || (!cd1.IsAbstract && cd2.IsAbstract))
				return false;

			if (!classDesc.IsAssignable(cd2.Id))
				return false;
		}

		return true;
	}

	private HashSet<short> DetectModifiedTargets(DataModelDescriptor prevModelDesc, DataModelDescriptor modelDesc)
	{
		HashSet<short> f = new HashSet<short>(1);
		foreach (ClassDescriptor classDesc in modelDesc.GetAllClasses())
		{
			ClassDescriptor prevClassDesc = prevModelDesc.GetClass(classDesc.Id);
			if (prevClassDesc != null)
			{
				if (!IsSubsetOf(prevClassDesc.DescendentClassIdsSet, classDesc.DescendentClassIdsSet))
				{
					TTTrace.Write(classDesc.Id);
					f.Add(classDesc.Id);
				}

				if (classDesc.IsAbstract && !prevClassDesc.IsAbstract)
				{
					TTTrace.Write(classDesc.Id);
					f.Add(classDesc.Id);
					ClassDescriptor curr = prevClassDesc.BaseClass;
					while (curr.Id != SystemCode.DatabaseObject.Id)
					{
						TTTrace.Write(curr.Id);
						f.Add(curr.Id);
						curr = curr.BaseClass;
					}
				}
			}
		}

		return f;
	}

	private bool IsSubsetOf(ReadOnlyHashSet<short> s1, ReadOnlyHashSet<short> s2)
	{
		if (s1.Count > s2.Count)
			return false;

		foreach (short id in s1)
		{
			if (!s2.ContainsKey(id))
				return false;
		}

		return true;
	}

	private void DetectClassDiff(HashSet<short> modifiedTargets, ClassDescriptor prevClassDesc, ClassDescriptor classDesc)
	{
		List<PropertyInsert> insertedProps = null;
		List<PropertyDelete> deletedProps = null;
		List<PropertyUpdate> updatedProps = null;
		bool isAbstratModified = prevClassDesc.IsAbstract != classDesc.IsAbstract;
		bool isLogModified = !string.Equals(prevClassDesc.LogName, classDesc.LogName, StringComparison.OrdinalIgnoreCase);
		bool hashedPropertiesModified = HashedPropertiesModified(prevClassDesc.PropertyHashIndexIndexes, classDesc.PropertyHashIndexIndexes);

		bool isPrevInherited = prevClassDesc.DescendentClassIds.Length > 0 || prevClassDesc.IsAbstract;
		bool isInherited = classDesc.DescendentClassIds.Length > 0 || classDesc.IsAbstract;
		bool isHierarchyTypeModified = isPrevInherited != isInherited;

		if (isAbstratModified)
			database.Engine.Trace.Debug("Modified abstractness of class {0}.", classDesc.FullName);

		if (isLogModified)
			database.Engine.Trace.Debug("Log of class {0} modified.", classDesc.FullName);

		if (hashedPropertiesModified)
			database.Engine.Trace.Debug("Hashed properties of class {0} modified.", classDesc.FullName);

		if (!classDesc.IsAbstract)
		{
			foreach (PropertyDescriptor propDesc in classDesc.Properties)
			{
				int n = prevClassDesc.GetPropertyIndex(propDesc.Id);
				if (n == -1)
				{
					ReferencePropertyDescriptor refPropDesc = propDesc as ReferencePropertyDescriptor;
					if (refPropDesc != null && refPropDesc.Multiplicity == Multiplicity.One)
					{
						throw new DatabaseException(DatabaseErrorDetail.
							CreateInsertedReferencePropertyMultiplicity(classDesc.FullName, propDesc.Name));
					}

					if (insertedProps == null)
						insertedProps = new List<PropertyInsert>();

					database.Engine.Trace.Debug("Property {0} inserted in class {1}.", propDesc.Name, classDesc.FullName);
					insertedProps.Add(new PropertyInsert(propDesc));
				}
				else
				{
					DetectPropertyDiff(prevClassDesc.Properties[n], propDesc, classDesc, modifiedTargets, ref updatedProps);
				}
			}

			foreach (PropertyDescriptor prevPropDesc in prevClassDesc.Properties)
			{
				int n = classDesc.GetPropertyIndex(prevPropDesc.Id);
				if (n == -1)
				{
					if (deletedProps == null)
						deletedProps = new List<PropertyDelete>();

					database.Engine.Trace.Debug("Property {0} deleted from class {1}.", prevPropDesc.Name, classDesc.FullName);
					deletedProps.Add(new PropertyDelete(prevPropDesc));
				}
			}
		}

		if (isHierarchyTypeModified || isLogModified || isAbstratModified || hashedPropertiesModified ||
			insertedProps != null || deletedProps != null || updatedProps != null)
		{
			TTTrace.Write(database.TraceId, database.Id, classDesc.Id, isLogModified, isAbstratModified,
				insertedProps == null ? 0 : insertedProps.Count, deletedProps == null ? 0 : deletedProps.Count,
				updatedProps == null ? 0 : updatedProps.Count, hashedPropertiesModified);

			ClassUpdate cu = new ClassUpdate(prevClassDesc, classDesc, isAbstratModified,
				isLogModified, isHierarchyTypeModified, hashedPropertiesModified, insertedProps, deletedProps, updatedProps);

			state.UpdatedClasses.Add(prevClassDesc.Id, cu);
		}
	}

	private bool HashedPropertiesModified(ReadOnlyArray<ReadOnlyArray<int>> p1, ReadOnlyArray<ReadOnlyArray<int>> p2)
	{
		if ((p1 == null) != (p2 == null))
			return true;

		if (p1 == null)
			return false;

		if (p1.Length != p2.Length)
			return true;

		for (int i = 0; i < p1.Length; i++)
		{
			ReadOnlyArray<int> a1 = p1[i];
			ReadOnlyArray<int> a2 = p2[i];
			if ((a1 != null) != (a2 != null))
				return true;

			if (a1 == null)
				continue;

			if (a1.Length != a2.Length)
				return true;

			for (int j = 0; j < a1.Length; j++)
			{
				if (a1[j] != a2[j])
					return true;
			}
		}

		return false;
	}

	private void DetectPropertyDiff(PropertyDescriptor prevPropDesc, PropertyDescriptor propDesc, ClassDescriptor classDesc,
		HashSet<short> modifiedTargets, ref List<PropertyUpdate> updatedProps)
	{
		if (prevPropDesc.PropertyType != propDesc.PropertyType || prevPropDesc.Kind != propDesc.Kind)
		{
			throw new DatabaseException(DatabaseErrorDetail.
				CreateInvalidPropertyTypeModification(prevPropDesc.OwnerClass.FullName, prevPropDesc.Name));
		}

		bool isMultiplicityModified = false;
		bool isTargetModified = false;
		bool invRefTrackingModified = false;
		bool defaultValueModified = false;
		bool deleteTargetActionModified = false;

		ReferencePropertyDescriptor refPrevPropDesc = prevPropDesc as ReferencePropertyDescriptor;
		ReferencePropertyDescriptor refPropDesc = propDesc as ReferencePropertyDescriptor;
		if (refPrevPropDesc != null)
		{
			isMultiplicityModified = refPrevPropDesc.Multiplicity != refPropDesc.Multiplicity;

			if (refPrevPropDesc.ReferencedClass.Id != refPropDesc.ReferencedClass.Id)
				isTargetModified = true;

			if (modifiedTargets.Contains(refPropDesc.ReferencedClass.Id))
				isTargetModified = true;

			invRefTrackingModified = refPrevPropDesc.TrackInverseReferences != refPropDesc.TrackInverseReferences;
			deleteTargetActionModified = refPrevPropDesc.DeleteTargetAction != refPropDesc.DeleteTargetAction;
		}

		if (propDesc.Kind == PropertyKind.Simple && !object.Equals(propDesc.DefaultValue, prevPropDesc.DefaultValue))
		{
			TTTrace.Write(database.TraceId, database.Id, classDesc.Id, propDesc.Id, classDesc.LogIndex);
			defaultValueModified = true;
		}

		if (isMultiplicityModified)
		{
			database.Engine.Trace.Debug("Reference property {0} of class {1} multiplicity modified to {2}.",
				propDesc.Name, classDesc.FullName, refPropDesc.Multiplicity);
		}

		if (isTargetModified)
			database.Engine.Trace.Debug("Reference property {0} of class {1} target class modified.", propDesc.Name, classDesc.FullName);

		if (invRefTrackingModified)
			database.Engine.Trace.Debug("Tracking of reference property {0} of class {1} modified.", propDesc.Name, classDesc.FullName);

		if (defaultValueModified)
			database.Engine.Trace.Debug("Default value of property {0} of class {1} modified.", propDesc.Name, classDesc.FullName);

		if (deleteTargetActionModified)
			database.Engine.Trace.Debug("Delete target action of reference property {0} of class {1} modified.", propDesc.Name, classDesc.FullName);

		if (isMultiplicityModified || isTargetModified || invRefTrackingModified || defaultValueModified || deleteTargetActionModified)
		{
			TTTrace.Write(database.TraceId, database.Id, classDesc.Id, propDesc.Id,
				isMultiplicityModified, isTargetModified, invRefTrackingModified, defaultValueModified, deleteTargetActionModified);

			if (updatedProps == null)
				updatedProps = new List<PropertyUpdate>();

			updatedProps.Add(new PropertyUpdate(prevPropDesc, propDesc, isTargetModified,
				isMultiplicityModified, invRefTrackingModified, defaultValueModified, deleteTargetActionModified));
		}
	}

	private sealed class BuildupState
	{
		public List<ClassInsert> InsertedClasses { get; private set; } = new List<ClassInsert>();
		public Dictionary<short, ClassDelete> DeletedClasses { get; private set; } = new Dictionary<short, ClassDelete>(4);
		public Dictionary<short, ClassUpdate> UpdatedClasses { get; private set; } = new Dictionary<short, ClassUpdate>(4);
		public List<InverseMapInsert> InsertedInvRefMaps { get; private set; } = new List<InverseMapInsert>();
		public List<InverseMapDelete> DeletedInvRefMaps { get; private set; } = new List<InverseMapDelete>();
		public List<InverseMapUpdate> UpdatedInvRefMaps { get; private set; } = new List<InverseMapUpdate>();
		public Dictionary<short, HashIndexInsert> InsertedHashIndexes { get; private set; } = new Dictionary<short, HashIndexInsert>(2);
		public Dictionary<short, HashIndexDelete> DeletedHashIndexes { get; private set; } = new Dictionary<short, HashIndexDelete>(2);
		public Dictionary<short, HashIndexUpdate> UpdatedHashIndexes { get; private set; } = new Dictionary<short, HashIndexUpdate>(2);
	}
}
