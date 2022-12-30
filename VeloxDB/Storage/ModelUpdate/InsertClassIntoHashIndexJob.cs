using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class InsertClassIntoHashIndexJob : ModelUpdateJob
{
	HashIndex hashIndex;
	ClassScan scan;

	public InsertClassIntoHashIndexJob(ClassScan scan, HashIndex hashIndex)
	{
		this.scan = scan;
		this.hashIndex = hashIndex;
	}

	public static IEnumerable<InsertClassIntoHashIndexJob> Create(Database database, ModelUpdateContext updateContext)
	{
		foreach (Tuple<HashIndexDescriptor, ClassDescriptor> p in GenerateInserts(updateContext))
		{
			HashIndexDescriptor hindDesc = p.Item1;        // This is descriptor from new model
			updateContext.TryGetNewHashIndex(hindDesc.Id, out HashIndex hashIndex, out _);
			if (hashIndex == null)
				hashIndex = database.GetHashIndex(hindDesc.Id, out _);

			if (hashIndex.PendingRefill)
				continue;

			ClassDescriptor classDesc = p.Item2;
			ClassDescriptor prevClassDesc = database.ModelDesc.GetClass(classDesc.Id);
			if (prevClassDesc != null)
			{
				database.Engine.Trace.Debug("Inserting class {0} into hash index {1}.", prevClassDesc.FullName, hindDesc.FullName);
				TTTrace.Write(database.TraceId, database.Id, prevClassDesc.Id, hindDesc.Id);

				Class @class = database.GetClass(prevClassDesc.Index).MainClass;
				ClassScan[] classScans = @class.GetClassScans(null, false, out long tc);
				for (int k = 0; k < classScans.Length; k++)
				{
					yield return new InsertClassIntoHashIndexJob(classScans[k], hashIndex);
				}
			}
		}
	}

	private static IEnumerable<Tuple<HashIndexDescriptor, ClassDescriptor>> GenerateInserts(ModelUpdateContext updateContext)
	{
		foreach (HashIndexInsert hi in updateContext.ModelUpdate.InsertedHashIndexes.Values)
		{
			if (updateContext.ModelUpdate.IsAlignment || !hi.HashIndexDesc.IsUnique)
			{
				foreach (ClassDescriptor classDesc in hi.HashIndexDesc.Classes)
				{
					yield return new Tuple<HashIndexDescriptor, ClassDescriptor>(hi.HashIndexDesc, classDesc);
				}
			}
		}

		foreach (HashIndexUpdate hu in updateContext.ModelUpdate.UpdatedHashIndexes.Values)
		{
			if (updateContext.ModelUpdate.IsAlignment || !ValidateModifiedUniqueHashIndexJob.ShouldValidate(hu))
			{
				foreach (ClassDescriptor classDesc in hu.InsertedClasses)
				{
					yield return new Tuple<HashIndexDescriptor, ClassDescriptor>(hu.HashIndexDesc, classDesc);
				}
			}
		}
	}

	public override void Execute()
	{
		using (scan)
		{
			KeyComparerDesc kad = scan.Class.ClassDesc.GetHashAccessDescByPropertyName(hashIndex.HashIndexDesc);
			HashComparer comparer = new HashComparer(kad, null);

			TTTrace.Write(scan.Class.Database.TraceId, scan.Class.Database.Id, scan.Class.ClassDesc.Id, hashIndex.HashIndexDesc.Id);

			ulong[] handles = new ulong[128];
			int count = handles.Length;
			while (scan.NextHandles(handles, 0, ref count))
			{
				for (int i = 0; i < count; i++)
				{
					// Unique hash indexes were created in validation phase
					scan.Class.BuildHashIndex(handles[i], hashIndex, comparer, false, null);
				}

				count = handles.Length;
			}
		}
	}
}
