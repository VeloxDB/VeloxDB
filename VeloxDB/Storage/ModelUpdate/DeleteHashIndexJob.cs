using System;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class DeleteHashIndexJob : ModelUpdateJob
{
	public static void Execute(Database database, ModelUpdateContext updateContext)
	{
		foreach (HashIndexDescriptor hindDesc in updateContext.ModelUpdate.PrevModelDesc.GetAllHashIndexes())
		{
			bool deleted = updateContext.ModelUpdate.DeletedHashIndexes.ContainsKey(hindDesc.Id);
			if (deleted || updateContext.TryGetNewHashIndex(hindDesc.Id, out HashIndex hi, out HashKeyReadLocker l))
			{
				database.Engine.Trace.Debug("Deleting hash index {0}, isPermanentlyDeleted = {1}.", hindDesc.FullName, deleted);
				TTTrace.Write(database.TraceId, database.Id, hindDesc.Id, deleted);

				HashIndex hashIndex = database.GetHashIndex(hindDesc.Id, out HashKeyReadLocker locker);
				hashIndex.Dispose(updateContext.Workers);
				if (deleted)
					locker.Dispose();
			}
		}
	}

	public override void Execute()
	{
		throw new NotSupportedException();
	}
}
