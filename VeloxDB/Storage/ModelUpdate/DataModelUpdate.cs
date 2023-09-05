using System;
using System.Collections.Generic;
using System.Linq;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class DataModelUpdate
{
	Tracing.Source trace;
	long traceId;
	long databaseId;

	DataModelDescriptor prevModelDesc, modelDesc;

	BuildupState state;

	ReadOnlyArray<ClassInsert> insertedClasses;
	ReadOnlyHashMap<short, ClassDelete> deletedClasses;
	ReadOnlyHashMap<short, ClassUpdate> updatedClasses;
	ReadOnlyArray<InverseMapInsert> insertedInvRefMaps;
	ReadOnlyArray<InverseMapDelete> deletedInvRefMaps;
	ReadOnlyArray<InverseRefMapUpdate> updatedInvRefMaps;
	ReadOnlyHashMap<short, IndexInsert> insertedIndexes;
	ReadOnlyHashMap<short, IndexDelete> deletedIndexes;
	ReadOnlyHashMap<short, IndexUpdate> updatedIndexes;

	bool isAlignment;

	public DataModelUpdate(Database database, DataModelDescriptor prevModelDesc, DataModelDescriptor modelDesc, bool isAlignment)
	{
		this.traceId = database != null ? database.TraceId : 0;
		this.trace = database?.Engine.Trace;
		this.databaseId = database != null ? databaseId : 0;
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
				insertedIndexes.Count == 0 && deletedIndexes.Count == 0 && updatedIndexes.Count == 0;
		}
	}

	public ReadOnlyArray<ClassInsert> InsertedClasses => insertedClasses;
	public ReadOnlyHashMap<short, ClassDelete> DeletedClasses => deletedClasses;
	public ReadOnlyHashMap<short, ClassUpdate> UpdatedClasses => updatedClasses;
	public ReadOnlyArray<InverseMapInsert> InsertedInvRefMaps => insertedInvRefMaps;
	public ReadOnlyArray<InverseMapDelete> DeletedInvRefMaps => deletedInvRefMaps;
	public ReadOnlyArray<InverseRefMapUpdate> UpdatedInvRefMaps => updatedInvRefMaps;
	public ReadOnlyHashMap<short, IndexInsert> InsertedIndexes => insertedIndexes;
	public ReadOnlyHashMap<short, IndexDelete> DeletedIndexes => deletedIndexes;
	public ReadOnlyHashMap<short, IndexUpdate> UpdatedIndexes => updatedIndexes;

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

	public bool HasClassesBecomingAbstract
	{
		get
		{
			foreach (ClassUpdate cu in updatedClasses.Values)
			{
				if (cu.IsAbstractModified && cu.ClassDesc.IsAbstract)
					return true;
			}

			return false;
		}
	}

	public bool IndexModified(short id)
	{
		return updatedIndexes.ContainsKey(id) ||
			(deletedIndexes.ContainsKey(id) && insertedIndexes.ContainsKey(id));
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
		DetectIndexesDiff(prevModelDesc, modelDesc);

		insertedClasses = ReadOnlyArray<ClassInsert>.FromNullable(state.InsertedClasses);
		updatedClasses = new ReadOnlyHashMap<short, ClassUpdate>(state.UpdatedClasses);
		deletedClasses = new ReadOnlyHashMap<short, ClassDelete>(state.DeletedClasses);
		insertedIndexes = new ReadOnlyHashMap<short, IndexInsert>(state.InsertedIndexes);
		updatedIndexes = new ReadOnlyHashMap<short, IndexUpdate>(state.UpdatedIndexes);
		deletedIndexes = new ReadOnlyHashMap<short, IndexDelete>(state.DeletedIndexes);
		insertedInvRefMaps = ReadOnlyArray<InverseMapInsert>.FromNullable(state.InsertedInvRefMaps);
		updatedInvRefMaps = ReadOnlyArray<InverseRefMapUpdate>.FromNullable(state.UpdatedInvRefMaps);
		deletedInvRefMaps = ReadOnlyArray<InverseMapDelete>.FromNullable(state.DeletedInvRefMaps);
	}

	private void DetectClassesDiff(HashSet<short> modifiedTargets)
	{
		foreach (ClassDescriptor classDesc in modelDesc.GetAllClasses())
		{
			ClassDescriptor prevClassDesc = prevModelDesc.GetClass(classDesc.Id);
			TTTrace.Write(traceId, databaseId, classDesc.Id, prevClassDesc != null);

			if (prevClassDesc == null)
			{
				trace?.Debug("Class {0} inserted.", classDesc.FullName);
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
				TTTrace.Write(traceId, databaseId, prevClassDesc.Id);

				trace?.Debug("Class {0} deleted.", prevClassDesc.FullName);
				TTTrace.Write(traceId, databaseId, prevClassDesc.Id);

				state.DeletedClasses.Add(prevClassDesc.Id, new ClassDelete(prevClassDesc));
			}
		}
	}

	private void DetectIndexesDiff(DataModelDescriptor prevModelDesc, DataModelDescriptor modelDesc)
	{
		foreach (IndexDescriptor indDesc in modelDesc.GetAllIndexes())
		{
			IndexDescriptor prevIndDesc = prevModelDesc.GetIndex(indDesc.Id);
			TTTrace.Write(traceId, databaseId, prevIndDesc == null ? -1 : prevIndDesc.Id);

			if (prevIndDesc == null)
			{
				trace?.Debug("Index {0} inserted.", indDesc.FullName);

				state.InsertedIndexes.Add(indDesc.Id, new IndexInsert(indDesc));
				foreach (ClassDescriptor indexedClassDesc in indDesc.Classes)
				{
					ValidateIndexPropertiesNotNewlyIntroduced(indDesc, indexedClassDesc);
				}
			}
			else
			{
				DetectIndexDiff(prevIndDesc, indDesc);
			}
		}

		foreach (IndexDescriptor prevIndDesc in prevModelDesc.GetAllIndexes())
		{
			if (modelDesc.GetIndex(prevIndDesc.Id) == null)
			{
				trace?.Debug("Index {0} deleted.", prevIndDesc.FullName);
				TTTrace.Write(traceId, databaseId, prevIndDesc.Id);

				state.DeletedIndexes.Add(prevIndDesc.Id, new IndexDelete(prevIndDesc));
			}
		}
	}

	private void DetectIndexDiff(IndexDescriptor prevIndDesc, IndexDescriptor indDesc)
	{
		if (prevIndDesc.Type != indDesc.Type)
		{
			trace?.Debug("Index {0} type modified.", prevIndDesc.FullName);
			TTTrace.Write(traceId, databaseId, prevIndDesc.Id);

			state.DeletedIndexes.Add(prevIndDesc.Id, new IndexDelete(prevIndDesc));
			state.InsertedIndexes.Add(indDesc.Id, new IndexInsert(indDesc));
			return;
		}

		if (IndexPropertiesDiffer(prevIndDesc, indDesc))
		{
			trace?.Debug("Index {0} properties modified.", prevIndDesc.FullName);
			TTTrace.Write(traceId, databaseId, prevIndDesc.Id);

			state.DeletedIndexes.Add(prevIndDesc.Id, new IndexDelete(prevIndDesc));
			state.InsertedIndexes.Add(indDesc.Id, new IndexInsert(indDesc));
			return;
		}

		if (!string.Equals(prevIndDesc.CultureName, indDesc.CultureName) || prevIndDesc.CaseSensitive != indDesc.CaseSensitive)
		{
			trace?.Debug("Index {0} string comparison rules modified.", prevIndDesc.FullName);
			TTTrace.Write(traceId, databaseId, prevIndDesc.Id);

			state.DeletedIndexes.Add(prevIndDesc.Id, new IndexDelete(prevIndDesc));
			state.InsertedIndexes.Add(indDesc.Id, new IndexInsert(indDesc));
			return;
		}

		bool hasBecomeUnique = !prevIndDesc.IsUnique && indDesc.IsUnique;
		if (hasBecomeUnique)
			trace?.Debug("Index {0} has become unique.", indDesc.FullName);

		List<ClassDescriptor> insertedClasses = null;
		List<ClassDescriptor> deletedClasses = null;

		foreach (ClassDescriptor indexedClassDesc in indDesc.Classes)
		{
			ClassDescriptor prevIndexedClassDesc = prevIndDesc.Classes.FirstOrDefault(x => x.Id == indexedClassDesc.Id);
			if (prevIndexedClassDesc == null)
			{
				if (insertedClasses == null)
					insertedClasses = new List<ClassDescriptor>();

				if (this.prevModelDesc.GetClass(indexedClassDesc.Id) != null)  // Newly inserted classes will not affect the index
				{
					trace?.Debug("Class {0} inserted into index {1}.", indexedClassDesc.FullName, indDesc.FullName);
					TTTrace.Write(traceId, databaseId, indDesc.Id, indexedClassDesc.Id);

					ValidateIndexPropertiesNotNewlyIntroduced(indDesc, indexedClassDesc);
					insertedClasses.Add(indexedClassDesc);
				}
			}
		}

		foreach (ClassDescriptor prevIndexedClassDesc in prevIndDesc.Classes)
		{
			ClassDescriptor indexedClassDesc = indDesc.Classes.FirstOrDefault(x => x.Id == prevIndexedClassDesc.Id);
			if (indexedClassDesc == null)
			{
				if (deletedClasses == null)
					deletedClasses = new List<ClassDescriptor>();

				trace?.Debug("Class {0} deleted from index {1}.", prevIndexedClassDesc.FullName, prevIndDesc.FullName);
				TTTrace.Write(traceId, databaseId, prevIndDesc.Id, prevIndexedClassDesc.Id);

				deletedClasses.Add(prevIndexedClassDesc);
			}
		}

		if (hasBecomeUnique || insertedClasses != null || deletedClasses != null)
		{
			TTTrace.Write(traceId, databaseId, prevIndDesc.Id, hasBecomeUnique, insertedClasses != null, deletedClasses != null);

			state.UpdatedIndexes.Add(prevIndDesc.Id,
				new IndexUpdate(prevIndDesc, indDesc, hasBecomeUnique, insertedClasses, deletedClasses));
		}
	}

	private void ValidateIndexPropertiesNotNewlyIntroduced(IndexDescriptor indexDesc, ClassDescriptor indexedClassDesc)
	{
		if (isAlignment)
			return;

		ClassDescriptor prevIndexedClassDesc = prevModelDesc.GetClass(indexedClassDesc.Id);
		if (prevIndexedClassDesc == null)
			return;

		ReadOnlyArray<PropertyDescriptor> indexedProperties = indexDesc.Properties;
		for (int i = 0; i < indexedProperties.Length; i++)
		{
			// It is invalid to add an existing class to an index with a newly introduced property
			if (prevIndexedClassDesc.GetProperty(indexedProperties[i].Id) == null)
			{
				throw new DatabaseException(DatabaseErrorDetail.
					CreateInsertedPropertyClassAddedToIndex(indexedClassDesc.FullName, indexedProperties[i].Name));
			}
		}
	}

	private bool IndexPropertiesDiffer(IndexDescriptor prevIndexDesc, IndexDescriptor indexDesc)
	{
		if (prevIndexDesc.Properties.Length != indexDesc.Properties.Length)
			return true;

		for (int i = 0; i < prevIndexDesc.Properties.Length; i++)
		{
			if (prevIndexDesc.Properties[i].Id != indexDesc.Properties[i].Id)
			{
				TTTrace.Write(traceId, databaseId, prevIndexDesc.Id, prevIndexDesc.Properties[i].Id, indexDesc.Properties[i].Id);
				return true;
			}

			if (indexDesc.Type == ModelItemType.SortedIndex &&
				((SortedIndexDescriptor)prevIndexDesc).PropertySortOrder[i] != ((SortedIndexDescriptor)indexDesc).PropertySortOrder[i])
			{
				TTTrace.Write(traceId, databaseId, prevIndexDesc.Id, prevIndexDesc.Properties[i].Id, indexDesc.Properties[i].Id);
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
				trace?.Debug("Inverse reference map {0} inserted.", classDesc.FullName);
				TTTrace.Write(traceId, databaseId, classDesc.Id);

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
				trace?.Debug("Inverse reference map {0} deleted.", prevClassDesc.FullName);
				TTTrace.Write(traceId, databaseId, prevClassDesc.Id);

				state.DeletedInvRefMaps.Add(new InverseMapDelete(prevClassDesc));
			}
		}
	}

	private void DetectInverseRefMapUpdate(ClassDescriptor prevClassDesc, ClassDescriptor classDesc,
		List<InverseRefMapUpdate> updatedMaps, Dictionary<int, ReferencePropertyDescriptor> temp)
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

					trace?.Debug("Inverse reference {0} of class {1} tracking started, inverse reference map {2}.",
						refPropDesc.Name, refPropDesc.OwnerClass.FullName, classDesc.Id);
					TTTrace.Write(traceId, databaseId, refPropDesc.Id, refPropDesc.OwnerClass.Id, classDesc.Id);

					trackedReferences.Add(prevRefPropDesc);
				}
				else if (!refPropDesc.TrackInverseReferences && prevRefPropDesc.TrackInverseReferences)
				{
					untrackedReferences  ??= new List<PropertyDescriptor>();

					trace?.Debug("Inverse reference {0} of class {1} tracking stopped, inverse reference map {2}.",
						refPropDesc.Name, refPropDesc.OwnerClass.FullName, classDesc.Id);
					TTTrace.Write(traceId, databaseId, refPropDesc.Id, refPropDesc.OwnerClass.Id, classDesc.Id);

					untrackedReferences.Add(prevRefPropDesc);
				}
			}
			else
			{
				trace?.Debug("Inverse reference {0} of class {1} inserted, inverse reference map {2}.",
					refPropDesc.Name, refPropDesc.OwnerClass.FullName, classDesc.Id);
				insertedReferences ??= new List<PropertyDescriptor>();
			}
		}

		temp.Clear();

		foreach (ReferencePropertyDescriptor prevRefPropDesc in prevClassDesc.InverseReferences)
		{
			ClassDescriptor prevDefiningClassDesc = prevRefPropDesc.OwnerClass;
			ClassDescriptor definingClassDesc = classDesc.Model.GetClass(prevDefiningClassDesc.Id);

			ReferencePropertyDescriptor rpropDesc = (ReferencePropertyDescriptor)definingClassDesc?.GetProperty(prevRefPropDesc.Id);
			if (definingClassDesc == null || rpropDesc == null)
			{
				// Reference property has been removed completely
				Checker.AssertFalse(classDesc.InverseReferences.Any(x => x.Id == prevRefPropDesc.Id));

				if (deletedReferences == null)
					deletedReferences = new List<PropertyDescriptor>();

				trace?.Debug("Inverse reference {0} of class {1} deleted, inverse reference map {2}.",
					prevRefPropDesc.Name, prevDefiningClassDesc.FullName, classDesc.Id);
				TTTrace.Write(traceId, databaseId, prevRefPropDesc.Id, prevDefiningClassDesc.Id, classDesc.Id);

				deletedReferences.Add(prevRefPropDesc);
			}
			else if (!IsNonAbstractSubset(prevDefiningClassDesc, definingClassDesc))
			{
				// Reference property has been removed partially (from some classes by changing the base class and or abstractness)
				if (partiallyDeletedReferences == null)
					partiallyDeletedReferences = new List<PropertyDescriptor>();

				trace?.Debug("Inverse reference {0} of class {1} partially deleted, inverse reference map {2}.",
					prevRefPropDesc.Name, prevDefiningClassDesc.FullName, classDesc.Id);
				TTTrace.Write(traceId, databaseId, prevRefPropDesc.Id, prevDefiningClassDesc.Id, classDesc.Id);

				partiallyDeletedReferences.Add(prevRefPropDesc);
			}
		}

		if (untrackedReferences != null || trackedReferences != null ||
			deletedReferences != null || partiallyDeletedReferences != null || insertedReferences != null)
		{
			updatedMaps.Add(new InverseRefMapUpdate(classDesc, untrackedReferences,
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
		bool indexedPropertiesModified = IndexedPropertiesModified(prevClassDesc, classDesc);

		bool isPrevInherited = prevClassDesc.DescendentClassIds.Length > 0 || prevClassDesc.IsAbstract;
		bool isInherited = classDesc.DescendentClassIds.Length > 0 || classDesc.IsAbstract;
		bool isHierarchyTypeModified = isPrevInherited != isInherited;
		bool isBaseClaseModified = IsBaseClassModified(prevClassDesc, classDesc);

		if (isAbstratModified)
			trace?.Debug("Modified abstractness of class {0}.", classDesc.FullName);

		if (isLogModified)
			trace?.Debug("Log of class {0} modified.", classDesc.FullName);

		if (indexedPropertiesModified)
			trace?.Debug("Indexed properties of class {0} modified.", classDesc.FullName);

		if (isBaseClaseModified)
			trace?.Debug("Base class of class {0} modified.", classDesc.FullName);

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

					trace?.Debug("Property {0} inserted in class {1}.", propDesc.Name, classDesc.FullName);
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

					trace?.Debug("Property {0} deleted from class {1}.", prevPropDesc.Name, classDesc.FullName);
					deletedProps.Add(new PropertyDelete(prevPropDesc));
				}
			}
		}

		if (isHierarchyTypeModified || isLogModified || isAbstratModified || isBaseClaseModified || indexedPropertiesModified ||
			insertedProps != null || deletedProps != null || updatedProps != null)
		{
			TTTrace.Write(traceId, databaseId, classDesc.Id, isLogModified, isAbstratModified, isBaseClaseModified,
				insertedProps == null ? 0 : insertedProps.Count, deletedProps == null ? 0 : deletedProps.Count,
				updatedProps == null ? 0 : updatedProps.Count, indexedPropertiesModified);

			ClassUpdate cu = new ClassUpdate(prevClassDesc, classDesc, isAbstratModified,
				isLogModified, isHierarchyTypeModified, indexedPropertiesModified, isBaseClaseModified, insertedProps, deletedProps, updatedProps);

			state.UpdatedClasses.Add(prevClassDesc.Id, cu);
		}
	}

	private bool IsBaseClassModified(ClassDescriptor prevClassDesc, ClassDescriptor classDesc)
	{
		if (prevClassDesc.BaseClass == null)    // DatabaseObject
			return false;

		return prevClassDesc.BaseClass.Id != classDesc.BaseClass.Id;
	}

	private bool IndexedPropertiesModified(ClassDescriptor prevClassDesc, ClassDescriptor classDesc)
	{
		ReadOnlyArray<ReadOnlyArray<int>> p1 = prevClassDesc.PropertyIndexIndexes;
		ReadOnlyArray<ReadOnlyArray<int>> p2 = classDesc.PropertyIndexIndexes;

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

			if (a1.Length == 0)
				return false;

			if (a1.Length == 1)
				return prevClassDesc.Indexes[a1[0]].Id != classDesc.Indexes[a2[0]].Id;

			HashSet<short> h1 = new HashSet<short>(a1.Length);
			for (int j = 0; j < a1.Length; j++)
			{
				short indexId = prevClassDesc.Indexes[a1[j]].Id;
				Checker.AssertFalse(h1.Contains(indexId));
				h1.Add(indexId);
			}

			for (int j = 0; j < a1.Length; j++)
			{
				if (!h1.Contains(classDesc.Indexes[a2[j]].Id))
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
			TTTrace.Write(traceId, databaseId, classDesc.Id, propDesc.Id, classDesc.LogIndex);
			defaultValueModified = true;
		}

		if (isMultiplicityModified)
		{
			trace?.Debug("Reference property {0} of class {1} multiplicity modified to {2}.",
				propDesc.Name, classDesc.FullName, refPropDesc.Multiplicity);
		}

		if (isTargetModified)
			trace?.Debug("Reference property {0} of class {1} target class modified.", propDesc.Name, classDesc.FullName);

		if (invRefTrackingModified)
			trace?.Debug("Tracking of reference property {0} of class {1} modified.", propDesc.Name, classDesc.FullName);

		if (defaultValueModified)
			trace?.Debug("Default value of property {0} of class {1} modified.", propDesc.Name, classDesc.FullName);

		if (deleteTargetActionModified)
			trace?.Debug("Delete target action of reference property {0} of class {1} modified.", propDesc.Name, classDesc.FullName);

		if (isMultiplicityModified || isTargetModified || invRefTrackingModified || defaultValueModified || deleteTargetActionModified)
		{
			TTTrace.Write(traceId, databaseId, classDesc.Id, propDesc.Id,
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
		public List<InverseRefMapUpdate> UpdatedInvRefMaps { get; private set; } = new List<InverseRefMapUpdate>();
		public Dictionary<short, IndexInsert> InsertedIndexes { get; private set; } = new Dictionary<short, IndexInsert>(2);
		public Dictionary<short, IndexDelete> DeletedIndexes { get; private set; } = new Dictionary<short, IndexDelete>(2);
		public Dictionary<short, IndexUpdate> UpdatedIndexes { get; private set; } = new Dictionary<short, IndexUpdate>(2);
	}
}
