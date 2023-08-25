using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class InsertClassIntoIndexJob : ModelUpdateJob
{
	Index index;
	ClassScan scan;
	Dictionary<int, KeyComparer> comparers;

	public InsertClassIntoIndexJob(ClassScan scan, Index index, Dictionary<int, KeyComparer> comparers)
	{
		this.scan = scan;
		this.index = index;
		this.comparers = comparers;
	}

	public static IEnumerable<InsertClassIntoIndexJob> Create(Database database, ModelUpdateContext updateContext)
	{
		Dictionary<int, KeyComparer> comparers = PrepareComparers(database, updateContext);

		foreach (Tuple<IndexDescriptor, ClassDescriptor> p in GenerateInserts(updateContext))
		{
			IndexDescriptor indexDesc = p.Item1;        // This is descriptor from new model
			updateContext.TryGetNewIndex(indexDesc.Id, out Index index, out _);
			if (index == null)
				index = database.GetIndexById(indexDesc.Id, out _);

			if (index.PendingRefill)
				continue;

			ClassDescriptor classDesc = p.Item2;
			ClassDescriptor prevClassDesc = database.ModelDesc.GetClass(classDesc.Id);
			if (prevClassDesc != null)
			{
				database.Engine.Trace.Debug("Inserting class {0} into index {1}.", prevClassDesc.FullName, indexDesc.FullName);
				TTTrace.Write(database.TraceId, database.Id, prevClassDesc.Id, indexDesc.Id);

				Class @class = database.GetClass(prevClassDesc.Index).MainClass;
				ClassScan[] classScans = @class.GetClassScans(null, false, out long tc);
				for (int k = 0; k < classScans.Length; k++)
				{
					yield return new InsertClassIntoIndexJob(classScans[k], index, comparers);
				}
			}
		}
	}

	private static Dictionary<int, KeyComparer> PrepareComparers(Database database, ModelUpdateContext updateContext)
	{
		Dictionary<int, KeyComparer> d = new Dictionary<int, KeyComparer>(16);
		foreach (IndexInsert ii in updateContext.ModelUpdate.InsertedIndexes.Values)
		{
			if (updateContext.ModelUpdate.IsAlignment || !ii.IndexDesc.IsUnique)
			{
				foreach (ClassDescriptor classDesc in ii.IndexDesc.Classes)
				{
					ClassDescriptor prevClassDesc = database.ModelDesc.GetClass(classDesc.Id);
					if (prevClassDesc != null)
					{
						KeyComparer k = new KeyComparer(prevClassDesc.GetIndexAccessDescByPropertyName(ii.IndexDesc));
						int key = (prevClassDesc.Id << 16) | ((int)ii.IndexDesc.Id);
						d.Add(key, k);
					}
				}
			}
		}

		foreach (IndexUpdate iu in updateContext.ModelUpdate.UpdatedIndexes.Values)
		{
			if (updateContext.ModelUpdate.IsAlignment || !ValidateModifiedUniqueIndexJob.ShouldValidate(iu))
			{
				foreach (ClassDescriptor classDesc in iu.IndexDesc.Classes)
				{
					ClassDescriptor prevClassDesc = database.ModelDesc.GetClass(classDesc.Id);
					if (prevClassDesc != null)
					{
						KeyComparer k = new KeyComparer(prevClassDesc.GetIndexAccessDescByPropertyName(iu.IndexDesc));
						int key = (prevClassDesc.Id << 16) | ((int)iu.IndexDesc.Id);
						d.Add(key, k);
					}
				}
			}
		}

		return d;
	}

	private static IEnumerable<Tuple<IndexDescriptor, ClassDescriptor>> GenerateInserts(ModelUpdateContext updateContext)
	{
		foreach (IndexInsert ii in updateContext.ModelUpdate.InsertedIndexes.Values)
		{
			if (updateContext.ModelUpdate.IsAlignment || !ii.IndexDesc.IsUnique)
			{
				foreach (ClassDescriptor classDesc in ii.IndexDesc.Classes)
				{
					yield return new Tuple<IndexDescriptor, ClassDescriptor>(ii.IndexDesc, classDesc);
				}
			}
		}

		foreach (IndexUpdate hu in updateContext.ModelUpdate.UpdatedIndexes.Values)
		{
			if (updateContext.ModelUpdate.IsAlignment || !ValidateModifiedUniqueIndexJob.ShouldValidate(hu))
			{
				foreach (ClassDescriptor classDesc in hu.InsertedClasses)
				{
					yield return new Tuple<IndexDescriptor, ClassDescriptor>(hu.IndexDesc, classDesc);
				}
			}
		}
	}

	public override void Execute()
	{
		using (scan)
		{
			KeyComparerDesc kad = scan.Class.ClassDesc.GetIndexAccessDescByPropertyName(index.IndexDesc);
			int key = (scan.Class.ClassDesc.Id << 16) | ((int)index.IndexDesc.Id);
			KeyComparer comparer = comparers[key];
			Func<short, KeyComparer> comparerFinder = x =>
			{
				int k = (x << 16) | ((int)index.IndexDesc.Id);
				return comparers[k];
			};

			TTTrace.Write(scan.Class.Database.TraceId, scan.Class.Database.Id, scan.Class.ClassDesc.Id, index.IndexDesc.Id);

			ulong[] handles = new ulong[128];
			int count = handles.Length;
			while (scan.NextHandles(handles, 0, ref count))
			{
				for (int i = 0; i < count; i++)
				{
					// Unique indexes were created in validation phase
					scan.Class.BuildIndex(handles[i], index, comparer, comparerFinder);
				}

				count = handles.Length;
			}
		}
	}
}
