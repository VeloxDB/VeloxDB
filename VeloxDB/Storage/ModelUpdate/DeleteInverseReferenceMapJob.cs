using System;
using VeloxDB.Common;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class DeleteInverseReferenceMapJob : ModelUpdateJob
{
	public static void Execute(Database database, ModelUpdateContext updateContext)
	{
		foreach (InverseMapDelete inverseMapDelete in updateContext.ModelUpdate.DeletedInvRefMaps)
		{
			database.Engine.Trace.Debug("Deleting inverse reference map {0}.", inverseMapDelete.ClassDesc.FullName);
			TTTrace.Write(database.TraceId, database.Id, inverseMapDelete.ClassDesc.Id);

			InverseReferenceMap invRefMap = database.GetInvRefs(inverseMapDelete.ClassDesc.Index);
			invRefMap.Dispose(updateContext.Workers);
		}
	}

	public override void Execute()
	{
		throw new NotSupportedException();
	}
}
