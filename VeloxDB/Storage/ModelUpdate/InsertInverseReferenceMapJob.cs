using System;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.Storage.ModelUpdate;

internal sealed class InsertInverseReferenceMapJob : ModelUpdateJob
{
	public static void Execute(Database database, ModelUpdateContext updateContext)
	{
		foreach (InverseMapInsert hi in updateContext.ModelUpdate.InsertedInvRefMaps)
		{
			database.Engine.Trace.Debug("Inserting inverse reference map {0}.", hi.ClassDesc.FullName);
			TTTrace.Write(database.TraceId, database.Id, hi.ClassDesc.Id);

			InverseReferenceMap invRefMap = new InverseReferenceMap(database, hi.ClassDesc);
			updateContext.AddNewInverseReferenceMap(invRefMap);
		}
	}

	public override void Execute()
	{
		throw new NotSupportedException();
	}
}
