using System;
using System.Collections.Generic;
using System.Linq;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.Storage.ModelUpdate;

internal sealed class UntrackInverseReferencesJob : ModelUpdateJob
{
	InverseReferenceMap invRefMap;
	Utils.Range range;
	HashSet<int> propIds;

	public UntrackInverseReferencesJob(Utils.Range range, InverseReferenceMap invRefMap, HashSet<int> propIds)
	{
		this.range = range;
		this.invRefMap = invRefMap;
		this.propIds = propIds;
	}

	public static IEnumerable<UntrackInverseReferencesJob> Create(Database database, ModelUpdateContext updateContext)
	{
		HashSet<int> propIds = new HashSet<int>(2);
		foreach (InverseMapUpdate imu in updateContext.ModelUpdate.UpdatedInvRefMaps)
		{
			propIds.UnionWith(imu.UntrackedReferences.Select(x =>
			{
				database.Trace.Debug("Untracking inverse reference {0} of class {1}.", x.Name, imu.ClassDesc.FullName);
				TTTrace.Write(database.TraceId, database.Id, x.Id, imu.ClassDesc.Id);
				return x.Id;
			}));
		}

		if (propIds.Count != 0)
		{
			foreach (InverseMapUpdate imu in updateContext.ModelUpdate.UpdatedInvRefMaps)
			{
				if (imu.UntrackedReferences.Length == 0)
					continue;

				database.Trace.Debug("Untracking inverse reference in class {0}.", imu.ClassDesc.FullName);
				TTTrace.Write(database.TraceId, database.Id, imu.ClassDesc.Id);

				ClassDescriptor prevClassDesc = database.ModelDesc.GetClass(imu.ClassDesc.Id);
				InverseReferenceMap invRefMap = database.GetInvRefs(prevClassDesc.Index);
				Utils.Range[] ranges = invRefMap.GetScanRanges();
				for (int i = 0; i < ranges.Length; i++)
				{
					yield return new UntrackInverseReferencesJob(ranges[i], invRefMap, propIds);
				}
			}
		}
	}

	public override void Execute()
	{
		invRefMap.CompactUntrackedProperties(range, propIds);
	}
}
