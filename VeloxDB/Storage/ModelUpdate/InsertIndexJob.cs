using System;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class InsertIndexJob : ModelUpdateJob
{
	public static void Execute(Database database, ModelUpdateContext updateContext)
	{
		foreach (IndexInsert ii in updateContext.ModelUpdate.InsertedIndexes.Values)
		{
			if (updateContext.TryGetNewIndex(ii.IndexDesc.Id, out _, out _))
				continue;

			database.Engine.Trace.Debug("Inserting index {0}.", ii.IndexDesc.FullName);
			TTTrace.Write(database.TraceId, database.Id, ii.IndexDesc.Id);

			IndexDescriptor indexDesc = ii.IndexDesc;
			long capacity = 0;
			for (int j = 0; j < indexDesc.Classes.Length; j++)
			{
				ClassDescriptor prevClassDesc = database.ModelDesc.GetClass(indexDesc.Classes[j].Id);
				if (prevClassDesc != null)
				{
					TTTrace.Write(database.TraceId, database.Id, ii.IndexDesc.Id, prevClassDesc.Id);

					Class @class = database.GetClass(prevClassDesc.Index).MainClass;
					if (@class != null)
						capacity += @class.EstimatedObjectCount;
				}
			}

			capacity = (long)(capacity * 1.2f);

			Index index;
			KeyReadLocker locker = null;

			if (indexDesc.Type == ModelItemType.HashIndex)
			{
				index = new HashIndex(database, (HashIndexDescriptor)indexDesc, capacity);
				locker = new KeyReadLocker(indexDesc, database);
			}
			else
			{
				index = new SortedIndex(database, (SortedIndexDescriptor)indexDesc);
			}

			updateContext.AddNewIndex(index, locker);
		}
	}

	public override void Execute()
	{
		throw new NotSupportedException();
	}
}
