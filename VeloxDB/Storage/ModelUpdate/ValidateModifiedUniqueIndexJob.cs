using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class ValidateModifiedUniqueIndexJob : ModelUpdateValidationJob
{
	Dictionary<short, KeyComparer> comparers;
	Index index;
	ClassScan scan;

	public ValidateModifiedUniqueIndexJob(ModelUpdateContext context, ClassScan scan, Index index, Dictionary<short, KeyComparer> comparers) :
		base(context)
	{
		this.scan = scan;
		this.index = index;
		this.comparers = comparers;
	}

	public static bool ShouldValidate(IndexUpdate indexUpdate)
	{
		return (indexUpdate.InsertedClasses.Length > 0 && indexUpdate.IndexDesc.IsUnique) ||
			(indexUpdate.DeletedClasses.Length > 0 && indexUpdate.HasBecomeUnique);
	}

	public static IEnumerable<ValidateModifiedUniqueIndexJob> Create(Database database, ModelUpdateContext updateContext)
	{
		List<IndexDescriptor> indexDescs = new List<IndexDescriptor>();
		foreach (IndexInsert ii in updateContext.ModelUpdate.InsertedIndexes.Values)
		{
			if (ii.IndexDesc.IsUnique)
				indexDescs.Add(ii.IndexDesc);
		}

		foreach (IndexUpdate hu in updateContext.ModelUpdate.UpdatedIndexes.Values)
		{
			if (ShouldValidate(hu))
				indexDescs.Add(hu.IndexDesc);
		}

		for (int i = 0; i < indexDescs.Count; i++)
		{
			IndexDescriptor indexDesc = indexDescs[i];        // This is descriptor from new model
			Dictionary<short, KeyComparer> comparers = new Dictionary<short, KeyComparer>(indexDesc.Classes.Length);

			long capacity = 0;
			for (int j = 0; j < indexDesc.Classes.Length; j++)
			{
				ClassDescriptor prevClassDesc = database.ModelDesc.GetClass(indexDesc.Classes[j].Id);
				if (prevClassDesc != null)
				{
					Class @class = database.GetClass(prevClassDesc.Index).MainClass;
					if (@class != null)
					{
						IndexDescriptor prevIndexDesc = @class.Database.ModelDesc.GetIndex(indexDesc.Id);
						if (prevIndexDesc == null || updateContext.ModelUpdate.DeletedIndexes.ContainsKey(indexDesc.Id))
						{
							// If the index is introduced in new the model, we do not have an appropriate comparer for it because the class might also be modified
							// but we need a comparer for the new index with the old class.
							KeyComparerDesc kad = @class.ClassDesc.GetIndexAccessDescByPropertyName(indexDesc);
							comparers.Add(@class.ClassDesc.Id, new KeyComparer(kad));
						}
						else
						{
							KeyComparer comparer = @class.GetKeyComparer(prevIndexDesc.Id, true);
							comparers.Add(@class.ClassDesc.Id, comparer);
						}

						capacity += @class.EstimatedObjectCount;
					}
				}
			}

			capacity = (long)(capacity * 1.2f);
			Index index;
			KeyReadLocker locker = null;
			if (indexDesc.Type == ModelItemType.HashIndex)
			{
				index = new HashIndex(database, (HashIndexDescriptor)indexDesc, capacity);
				locker = updateContext.ModelUpdate.UpdatedIndexes.ContainsKey(indexDesc.Id) ? null : new KeyReadLocker(indexDesc, database);
			}
			else
			{
				index = new SortedIndex(database, (SortedIndexDescriptor)indexDesc);
			}

			updateContext.AddNewIndex(index, locker);

			for (int j = 0; j < indexDesc.Classes.Length; j++)
			{
				ClassDescriptor prevClassDesc = database.ModelDesc.GetClass(indexDesc.Classes[j].Id);
				if (prevClassDesc != null)
				{
					Class @class = database.GetClass(prevClassDesc.Index).MainClass;
					if (@class != null)
					{
						database.Trace.Debug("Validating modified/inserted unique index {0} with class {1}.",
							indexDesc.FullName, @class.ClassDesc.FullName);
						TTTrace.Write(database.TraceId, database.Id, indexDesc.Id, @class.ClassDesc.Id);

						ClassScan[] classScans = @class.GetClassScans(null, false, out long tc);
						for (int k = 0; k < classScans.Length; k++)
						{
							yield return new ValidateModifiedUniqueIndexJob(updateContext, classScans[k], index, comparers);
						}
					}
				}
			}
		}
	}

	public override void Execute()
	{
		using (scan)
		{
			KeyComparer comparer = comparers[scan.Class.ClassDesc.Id];
			Func<short, KeyComparer> comparerFinder = x => comparers[x];

			ulong[] handles = new ulong[128];
			int count = handles.Length;
			while (scan.NextHandles(handles, 0, ref count))
			{
				for (int i = 0; i < count; i++)
				{
					DatabaseErrorDetail error = scan.Class.BuildIndex(handles[i], index, comparer, comparerFinder);
					if (error != null)
					{
						context.SetError(error);
						return;
					}
				}

				count = handles.Length;
			}
		}
	}
}
