using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class ValidateModifiedUniqueHashIndexJob : ModelUpdateValidationJob
{
	Dictionary<short, HashComparer> comparers;
	HashIndex hashIndex;
	ClassScan scan;

	public ValidateModifiedUniqueHashIndexJob(ModelUpdateContext context, ClassScan scan, HashIndex hashIndex, Dictionary<short, HashComparer> comparers) :
		base(context)
	{
		this.scan = scan;
		this.hashIndex = hashIndex;
		this.comparers = comparers;
	}

	public static bool ShouldValidate(HashIndexUpdate hindUpdate)
	{
		return (hindUpdate.InsertedClasses.Length > 0 && hindUpdate.HashIndexDesc.IsUnique) ||
			(hindUpdate.DeletedClasses.Length > 0 && hindUpdate.HasBecomeUnique);
	}

	public static IEnumerable<ValidateModifiedUniqueHashIndexJob> Create(Database database, ModelUpdateContext updateContext)
	{
		List<HashIndexDescriptor> hindDescs = new List<HashIndexDescriptor>();
		foreach (HashIndexInsert hi in updateContext.ModelUpdate.InsertedHashIndexes.Values)
		{
			if (hi.HashIndexDesc.IsUnique)
				hindDescs.Add(hi.HashIndexDesc);
		}

		foreach (HashIndexUpdate hu in updateContext.ModelUpdate.UpdatedHashIndexes.Values)
		{
			if (ShouldValidate(hu))
				hindDescs.Add(hu.HashIndexDesc);
		}

		for (int i = 0; i < hindDescs.Count; i++)
		{
			HashIndexDescriptor hindDesc = hindDescs[i];        // This is descriptor from new model
			Dictionary<short, HashComparer> comparers = new Dictionary<short, HashComparer>(hindDesc.Classes.Length);

			long capacity = 0;
			for (int j = 0; j < hindDesc.Classes.Length; j++)
			{
				ClassDescriptor prevClassDesc = database.ModelDesc.GetClass(hindDesc.Classes[j].Id);
				if (prevClassDesc != null)
				{
					Class @class = database.GetClass(prevClassDesc.Index).MainClass;
					if (@class != null)
					{
						HashIndexDescriptor prevHindDesc = @class.Database.ModelDesc.GetHashIndex(hindDesc.Id);
						if (prevHindDesc == null || updateContext.ModelUpdate.DeletedHashIndexes.ContainsKey(hindDesc.Id))
						{
							// If the index is introduced in new the model, we do not have an appropriate comparer for it because the class might also be modified
							// but we need a comparer for the new index with the old class.
							KeyComparerDesc kad = @class.ClassDesc.GetHashAccessDescByPropertyName(hindDesc);
							comparers.Add(@class.ClassDesc.Id, new HashComparer(kad, null));
						}
						else
						{
							HashComparer comparer = @class.GetHashedComparer(prevHindDesc.Id, true);
							comparers.Add(@class.ClassDesc.Id, comparer);
						}

						capacity += @class.EstimatedObjectCount;
					}
				}
			}

			capacity = (long)(capacity * 1.2f);
			HashIndex hashIndex = new HashIndex(database, hindDesc, capacity);
			HashKeyReadLocker locker = updateContext.ModelUpdate.UpdatedHashIndexes.ContainsKey(hindDesc.Id) ? null : new HashKeyReadLocker(hindDesc, database);
			updateContext.AddNewHashIndex(hashIndex, locker);

			for (int j = 0; j < hindDesc.Classes.Length; j++)
			{
				ClassDescriptor prevClassDesc = database.ModelDesc.GetClass(hindDesc.Classes[j].Id);
				if (prevClassDesc != null)
				{
					Class @class = database.GetClass(prevClassDesc.Index).MainClass;
					if (@class != null)
					{
						database.Trace.Debug("Validating modified/inserted unique hash index {0} with class {1}.",
							hindDesc.FullName, @class.ClassDesc.FullName);
						TTTrace.Write(database.TraceId, database.Id, hindDesc.Id, @class.ClassDesc.Id);

						ClassScan[] classScans = @class.GetClassScans(null, false, out long tc);
						for (int k = 0; k < classScans.Length; k++)
						{
							yield return new ValidateModifiedUniqueHashIndexJob(updateContext, classScans[k], hashIndex, comparers);
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
			HashComparer comparer = comparers[scan.Class.ClassDesc.Id];
			Func<short, HashComparer> comparerFinder = x => comparers[x];

			ulong[] handles = new ulong[128];
			int count = handles.Length;
			while (scan.NextHandles(handles, 0, ref count))
			{
				for (int i = 0; i < count; i++)
				{
					DatabaseErrorDetail error = scan.Class.BuildHashIndex(handles[i], hashIndex, comparer, true, comparerFinder);
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
