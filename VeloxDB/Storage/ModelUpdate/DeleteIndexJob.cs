using System;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class DeleteIndexJob : ModelUpdateJob
{
	public static void Execute(Database database, ModelUpdateContext updateContext)
	{
		foreach (IndexDescriptor indexDesc in updateContext.ModelUpdate.PrevModelDesc.GetAllIndexes())
		{
			bool deleted = updateContext.ModelUpdate.DeletedIndexes.ContainsKey(indexDesc.Id);
			if (deleted || updateContext.TryGetNewIndex(indexDesc.Id, out _, out _))
			{
				database.Engine.Trace.Debug("Deleting index {0}, isPermanentlyDeleted = {1}.", indexDesc.FullName, deleted);
				TTTrace.Write(database.TraceId, database.Id, indexDesc.Id, deleted);

				Index index = database.GetIndexById(indexDesc.Id, out KeyReadLocker locker);
				index.Dispose(updateContext.Workers);
				if (deleted)
					locker?.Dispose();
			}
		}
	}

	public override void Execute()
	{
		throw new NotSupportedException();
	}
}
