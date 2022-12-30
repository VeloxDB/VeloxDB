using System;
using System.Linq;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class InsertClassJob : ModelUpdateJob
{
	public static void Execute(Database database, ModelUpdateContext updateContext)
	{
		foreach (ClassDescriptor classDesc in updateContext.ModelUpdate.InsertedClasses.Select(x => x.ClassDesc).
			Concat(updateContext.ModelUpdate.UpdatedClasses.Values.Where(x => x.IsAbstractModified).Select(x => x.ClassDesc)))
		{
			database.Engine.Trace.Debug("Inserting class {0}.", classDesc.FullName);
			TTTrace.Write(database.TraceId, database.Id, classDesc.Id);

			ClassBase @class = ClassBase.CreateEmptyClass(database, classDesc, database.Engine.Settings.InitClassSize);
			ClassLocker locker = classDesc.IsAbstract ? null : new ClassLocker(database.Engine, (ushort)classDesc.Index);
			updateContext.AddNewClass(@class, locker);
		}
	}

	public override void Execute()
	{
		throw new NotSupportedException();
	}
}
