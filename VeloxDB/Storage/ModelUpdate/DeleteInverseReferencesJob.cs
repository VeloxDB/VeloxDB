using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class DeleteInverseReferencesJob : ModelUpdateJob
{
	InverseReferenceMap invRefMap;
	Utils.Range range;
	HashSet<int> propIds;

	public DeleteInverseReferencesJob(Utils.Range range, InverseReferenceMap invRefMap, HashSet<int> propIds)
	{
		this.range = range;
		this.invRefMap = invRefMap;
		this.propIds = propIds;
	}

	public static IEnumerable<DeleteInverseReferencesJob> Create(Database database, ModelUpdateContext updateContext)
	{
		HashSet<int> propIds = new HashSet<int>(2);
		foreach (InverseMapUpdate imu in updateContext.ModelUpdate.UpdatedInvRefMaps)
		{
			propIds.UnionWith(imu.TrackedReferences.Select(x =>
			{
				TTTrace.Write(database.TraceId, database.Id, imu.ClassDesc.Id, x.Id);
				return x.Id;
			}));

			propIds.UnionWith(imu.DeletedReferences.Select(x =>
			{
				TTTrace.Write(database.TraceId, database.Id, imu.ClassDesc.Id, x.Id);
				return x.Id;
			}));

			propIds.UnionWith(imu.PartiallyDeletedReferences.Select(x =>
			{
				TTTrace.Write(database.TraceId, database.Id, imu.ClassDesc.Id, x.Id);
				return x.Id;
			}));
		}

		if (propIds.Count > 0)
		{
			foreach (InverseMapUpdate imu in updateContext.ModelUpdate.UpdatedInvRefMaps)
			{
				database.Engine.Trace.Debug("Deleting inverse reference properties from map {0}.", imu.ClassDesc.FullName);
				TTTrace.Write(database.TraceId, database.Id, imu.ClassDesc.FullName);

				ClassDescriptor prevClassDesc = database.ModelDesc.GetClass(imu.ClassDesc.Id);
				InverseReferenceMap invRefMap = database.GetInvRefs(prevClassDesc.Index);
				Utils.Range[] ranges = invRefMap.GetScanRanges();
				for (int i = 0; i < ranges.Length; i++)
				{
					yield return new DeleteInverseReferencesJob(ranges[i], invRefMap, propIds);
				}
			}
		}
	}

	public override void Execute()
	{
		invRefMap.DeleteProperties(range, propIds);
	}
}
