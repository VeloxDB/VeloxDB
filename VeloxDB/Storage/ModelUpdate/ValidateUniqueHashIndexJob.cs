using System;
using System.Collections.Generic;
using VeloxDB.Common;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class ValidateUniqueHashIndexJob : ModelUpdateValidationJob
{
	HashIndex existingHashIndex;
	Utils.Range range;

	public ValidateUniqueHashIndexJob(ModelUpdateContext context, Utils.Range range, HashIndex hashIndex) :
		base(context)
	{
		this.range = range;
		this.existingHashIndex = hashIndex;
	}

	public static IEnumerable<ValidateUniqueHashIndexJob> Create(Database database, ModelUpdateContext updateContext)
	{
		foreach (HashIndexUpdate hu in updateContext.ModelUpdate.UpdatedHashIndexes.Values)
		{
			if (hu.HasBecomeUnique && hu.InsertedClasses.Length == 0 && hu.DeletedClasses.Length == 0)
			{
				database.Trace.Debug("Validating hash index {0} for uniqueness.", hu.PrevHashIndexDesc.FullName);
				TTTrace.Write(database.TraceId, database.Id, hu.PrevHashIndexDesc.Id);

				HashIndex existingHashIndex = database.GetHashIndex(hu.PrevHashIndexDesc.Id, out HashKeyReadLocker locker);
				Utils.Range[] ranges = existingHashIndex.SplitScanRange();
				for (int i = 0; i < ranges.Length; i++)
				{
					yield return new ValidateUniqueHashIndexJob(updateContext, ranges[i], existingHashIndex);
				}
			}
		}
	}

	public override void Execute()
	{
		DatabaseErrorDetail error = existingHashIndex.CheckUniqueness(range);
		if (error != null)
			context.SetError(error);
	}
}
