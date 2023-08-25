using System;
using System.Collections.Generic;
using System.Linq;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class PrepareIndexForPendingRefillJob : ModelUpdateJob
{
	public static void Execute(Database database, ModelUpdateContext updateContext)
	{
		foreach (IndexDescriptor indexDesc in GetAffected(updateContext.ModelUpdate))
		{
			updateContext.TryGetNewIndex(indexDesc.Id, out Index index, out _);
			if (index == null)
				index = database.GetIndexById(indexDesc.Id, out _);

			index.PrepareForPendingRefill(updateContext.Workers);
		}
	}

	private static IEnumerable<IndexDescriptor> GetAffected(DataModelUpdate modelUpdate)
	{
		return modelUpdate.UpdatedIndexes.Values.Where(x => IsIndexAffected(modelUpdate, x)).Select(x => x.IndexDesc).
			Concat(modelUpdate.InsertedIndexes.Values.Select(x => x.IndexDesc));
	}

	private static bool IsIndexAffected(DataModelUpdate modelUpdate, IndexUpdate hu)
	{
		bool affected = false;
		foreach (ClassDescriptor classDesc in hu.InsertedClasses)
		{
			if (modelUpdate.UpdatedClasses.TryGetValue(classDesc.Id, out ClassUpdate cu))
			{
				if (hu.IndexDesc.Properties.Any(p => cu.InsertedProperties.Any(x => x.PropDesc.Id == p.Id)))
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
