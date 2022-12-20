using System;
using System.Linq;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class DeleteClassJob : ModelUpdateJob
{
	public static void Execute(Database database, ModelUpdateContext updateContext)
	{
		foreach (ClassDescriptor classDesc in updateContext.ModelUpdate.DeletedClasses.Values.Select(x => x.ClassDesc).
			Concat(updateContext.ModelUpdate.UpdatedClasses.Values.Where(x => x.IsAbstractModified).Select(x => x.PrevClassDesc)))
		{
			database.Engine.Trace.Debug("Deleting class {0}.", classDesc.FullName);
			TTTrace.Write(database.TraceId, database.Id, classDesc.Id);

			ClassBase @class = database.GetClass(classDesc.Index, out ClassLocker locker);
			@class.Dispose(updateContext.Workers);
			locker?.Dispose();
		}
	}

	public override void Execute()
	{
		throw new NotSupportedException();
	}
}
