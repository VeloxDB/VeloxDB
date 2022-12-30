using System;
using System.Collections.Generic;
using System.Linq;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class PrepareHashIndexForPendingRefillJob : ModelUpdateJob
{
	public static void Execute(Database database, ModelUpdateContext updateContext)
	{
		foreach (HashIndexDescriptor hindDesc in GetAffected(updateContext.ModelUpdate))
		{
			updateContext.TryGetNewHashIndex(hindDesc.Id, out HashIndex index, out _);
			if (index == null)
				index = database.GetHashIndex(hindDesc.Id, out _);

			index.PrepareForPendingRefill(updateContext.Workers);
		}
	}

	private static IEnumerable<HashIndexDescriptor> GetAffected(DataModelUpdate modelUpdate)
	{
		return modelUpdate.UpdatedHashIndexes.Values.Where(x => IsIndexAffected(modelUpdate, x)).Select(x => x.HashIndexDesc).
			Concat(modelUpdate.InsertedHashIndexes.Values.Select(x => x.HashIndexDesc));
	}

	private static bool IsIndexAffected(DataModelUpdate modelUpdate, HashIndexUpdate hu)
	{
		bool affected = false;
		foreach (ClassDescriptor classDesc in hu.InsertedClasses)
		{
			if (modelUpdate.UpdatedClasses.TryGetValue(classDesc.Id, out ClassUpdate cu))
			{
				if (hu.HashIndexDesc.Properties.Any(p => cu.InsertedProperties.Any(x => x.PropDesc.Id == p.Id)))
				{
					affected = true;
					break;
				}
			}
		}

		return affected;
	}

	public override void Execute()
	{
		throw new NotSupportedException();
	}
}
