using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class DeleteClassFromHashIndexJob : ModelUpdateJob
{
	HashIndex hashIndex;
	ClassScan scan;

	public DeleteClassFromHashIndexJob(HashIndex hashIndex, ClassScan scan)
	{
		this.hashIndex = hashIndex;
		this.scan = scan;
	}

	public static IEnumerable<DeleteClassFromHashIndexJob> Create(Database database, ModelUpdateContext updateContext)
	{
		foreach (HashIndexUpdate hu in updateContext.ModelUpdate.UpdatedHashIndexes.Values)
		{
			if (updateContext.ModelUpdate.IsAlignment || !ValidateModifiedUniqueHashIndexJob.ShouldValidate(hu))
			{
				HashIndex hashIndex = database.GetHashIndex(hu.PrevHashIndexDesc.Id, out HashKeyReadLocker locker);
				if (hashIndex.PendingRefill)
					continue;

				foreach (ClassDescriptor classDesc in hu.DeletedClasses)
				{
					ClassScan[] scans = database.GetClass(classDesc.Index).GetClassScans(null, false, out long tc);
					database.Engine.Trace.Debug("Deleting class {0} from hash index {1}.", classDesc.FullName, hashIndex.HashIndexDesc.Name);
					TTTrace.Write(database.TraceId, database.Id, classDesc.Id, hashIndex.HashIndexDesc.Id);

					foreach (ClassScan scan in scans)
					{
						yield return new DeleteClassFromHashIndexJob(hashIndex, scan);
					}
				}
			}
		}
	}

	public override void Execute()
	{
		using (scan)
		{
			HashIndexDescriptor prevHindDesc = scan.Class.Database.ModelDesc.GetHashIndex(hashIndex.HashIndexDesc.Id);
			HashComparer comparer = scan.Class.GetHashedComparer(prevHindDesc.Id, true);

			TTTrace.Write(scan.Class.Database.TraceId, scan.Class.Database.Id, hashIndex.HashIndexDesc.Id);

			ulong[] handles = new ulong[128];
			int count = handles.Length;
			while (scan.NextHandles(handles, 0, ref count))
			{
				for (int i = 0; i < count; i++)
				{
					scan.Class.DeleteFromHashIndex(handles[i], hashIndex, comparer);
				}

				count = handles.Length;
			}
		}
	}
}
