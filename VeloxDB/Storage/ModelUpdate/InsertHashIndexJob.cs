using System;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.Storage.ModelUpdate;

internal sealed class InsertHashIndexJob : ModelUpdateJob
{
	public static void Execute(Database database, ModelUpdateContext updateContext)
	{
		foreach (HashIndexInsert hi in updateContext.ModelUpdate.InsertedHashIndexes.Values)
		{
			if (updateContext.TryGetNewHashIndex(hi.HashIndexDesc.Id, out _, out _))
				continue;

			database.Engine.Trace.Debug("Inserting hash index {0}.", hi.HashIndexDesc.FullName);
			TTTrace.Write(database.TraceId, database.Id, hi.HashIndexDesc.Id);

			HashIndexDescriptor hindDesc = hi.HashIndexDesc;
			long capacity = 0;
			for (int j = 0; j < hindDesc.Classes.Length; j++)
			{
				ClassDescriptor prevClassDesc = database.ModelDesc.GetClass(hindDesc.Classes[j].Id);
				if (prevClassDesc != null)
				{
					TTTrace.Write(database.TraceId, database.Id, hi.HashIndexDesc.Id, prevClassDesc.Id);

					Class @class = database.GetClass(prevClassDesc.Index).MainClass;
					capacity += @class.EstimatedObjectCount;
				}
			}

			capacity = (long)(capacity * 1.2f);
			HashIndex hashIndex = new HashIndex(database, hindDesc, capacity);
			HashKeyReadLocker locker = new HashKeyReadLocker(hindDesc, database);
			updateContext.AddNewHashIndex(hashIndex, locker);
		}
	}

	public override void Execute()
	{
		throw new NotSupportedException();
	}
}
