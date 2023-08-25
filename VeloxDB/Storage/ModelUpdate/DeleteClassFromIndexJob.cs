using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class DeleteClassFromIndexJob : ModelUpdateJob
{
	Index index;
	ClassScan scan;

	public DeleteClassFromIndexJob(Index index, ClassScan scan)
	{
		this.index = index;
		this.scan = scan;
	}

	public static IEnumerable<DeleteClassFromIndexJob> Create(Database database, ModelUpdateContext updateContext)
	{
		foreach (IndexUpdate hu in updateContext.ModelUpdate.UpdatedIndexes.Values)
		{
			if (updateContext.ModelUpdate.IsAlignment || !ValidateModifiedUniqueIndexJob.ShouldValidate(hu))
			{
				Index index = database.GetIndexById(hu.PrevIndexDesc.Id, out _);
				if (index.PendingRefill)
					continue;

				foreach (ClassDescriptor classDesc in hu.DeletedClasses)
				{
					ClassScan[] scans = database.GetClass(classDesc.Index).GetClassScans(null, false, out long tc);
					database.Engine.Trace.Debug("Deleting class {0} from index {1}.", classDesc.FullName, index.IndexDesc.Name);
					TTTrace.Write(database.TraceId, database.Id, classDesc.Id, index.IndexDesc.Id);

					foreach (ClassScan scan in scans)
					{
						yield return new DeleteClassFromIndexJob(index, scan);
					}
				}
			}
		}
	}

	public override void Execute()
	{
		using (scan)
		{
			IndexDescriptor prevIndexDesc = scan.Class.Database.ModelDesc.GetIndex(index.IndexDesc.Id);
			KeyComparer comparer = scan.Class.GetKeyComparer(prevIndexDesc.Id, true);

			TTTrace.Write(scan.Class.Database.TraceId, scan.Class.Database.Id, index.IndexDesc.Id);

			ulong[] handles = new ulong[128];
			int count = handles.Length;
			while (scan.NextHandles(handles, 0, ref count))
			{
				for (int i = 0; i < count; i++)
				{
					scan.Class.DeleteFromIndex(handles[i], index, comparer);
				}

				count = handles.Length;
			}
		}
	}
}
