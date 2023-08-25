using System;
using System.Collections.Generic;
using VeloxDB.Common;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class ValidateUniqueIndexJob : ModelUpdateValidationJob
{
	Index existingIndex;
	IndexScanRange range;

	public ValidateUniqueIndexJob(ModelUpdateContext context, IndexScanRange range, Index index) :
		base(context)
	{
		this.range = range;
		this.existingIndex = index;
	}

	public static IEnumerable<ValidateUniqueIndexJob> Create(Database database, ModelUpdateContext updateContext)
	{
		foreach (IndexUpdate hu in updateContext.ModelUpdate.UpdatedIndexes.Values)
		{
			if (hu.HasBecomeUnique && hu.InsertedClasses.Length == 0 && hu.DeletedClasses.Length == 0)
			{
				database.Trace.Debug("Validating index {0} for uniqueness.", hu.PrevIndexDesc.FullName);
				TTTrace.Write(database.TraceId, database.Id, hu.PrevIndexDesc.Id);

				Index existingIndex = database.GetIndexById(hu.PrevIndexDesc.Id, out _);
				IndexScanRange[] ranges = existingIndex.SplitScanRange();
				for (int i = 0; i < ranges.Length; i++)
				{
					yield return new ValidateUniqueIndexJob(updateContext, ranges[i], existingIndex);
				}
			}
		}
	}

	public override void Execute()
	{
		DatabaseErrorDetail error = existingIndex.CheckUniqueness(range);
		if (error != null)
			context.SetError(error);
	}
}
