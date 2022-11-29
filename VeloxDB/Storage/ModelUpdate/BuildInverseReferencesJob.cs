using System;
using System.Collections.Generic;
using System.Linq;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.Storage.ModelUpdate;

internal sealed class BuildInverseReferencesJob : ModelUpdateJob
{
	public static void Execute(Database database, ModelUpdateContext updateContext)
	{
		var filter = CreateFilter(database, updateContext);
		if (filter.Count == 0)
			return;

		database.Engine.Trace.Debug("Building inverse references in model update.");
		TTTrace.Write(database.TraceId, database.Id);

		InverseReferenceBuilder invRefBuilder = new InverseReferenceBuilder(database);
		invRefBuilder.SetFilter(filter);
		invRefBuilder.Build(updateContext.Workers);

		database.Engine.Trace.Debug("Inverse references built.");
	}

	private static Dictionary<short, HashSet<InverseReferenceBuilder.PropertyFilter>> CreateFilter(
		Database database, ModelUpdateContext updateContext)
	{
		Dictionary<short, HashSet<InverseReferenceBuilder.PropertyFilter>> filter =
			new Dictionary<short, HashSet<InverseReferenceBuilder.PropertyFilter>>();

		DataModelDescriptor modelDesc = updateContext.ModelUpdate.ModelDesc;
		DataModelDescriptor prevModelDesc = updateContext.ModelUpdate.PrevModelDesc;

		foreach (InverseMapUpdate imu in updateContext.ModelUpdate.UpdatedInvRefMaps)
		{
			foreach (PropertyDescriptor prevPropDesc in imu.TrackedReferences.Concat(imu.PartiallyDeletedReferences))
			{
				ClassDescriptor prevOwnerClassDesc = prevPropDesc.OwnerClass;
				ClassDescriptor ownerClassDesc = modelDesc.GetClass(prevOwnerClassDesc.Id);
				ReferencePropertyDescriptor propDesc = (ReferencePropertyDescriptor)ownerClassDesc.GetProperty(prevPropDesc.Id);

				IEnumerable<ClassDescriptor> allPrevClasses = prevOwnerClassDesc.SubtreeClasses.
					Where(x => ownerClassDesc.IsAssignable(x.Id));

				foreach (ClassDescriptor prevClassDesc in allPrevClasses)
				{
					if (prevClassDesc == null || prevClassDesc.IsAbstract)
						continue;

					ClassDescriptor classDesc = modelDesc.GetClass(prevClassDesc.Id);
					if (classDesc == null || classDesc.IsAbstract)
						continue;

					if (!filter.TryGetValue(prevClassDesc.Id, out var classFilter))
					{
						classFilter = new HashSet<InverseReferenceBuilder.PropertyFilter>(2);
						filter.Add(prevClassDesc.Id, classFilter);
					}

					classFilter.Add(new InverseReferenceBuilder.PropertyFilter(propDesc.Id, propDesc.TrackInverseReferences));

					TTTrace.Write(database.TraceId, database.Id, prevClassDesc.Id, propDesc.Id, propDesc.TrackInverseReferences);
					database.Engine.Trace.Debug("Including class/property {0}/{1} in inverse reference build filter.",
						prevClassDesc.FullName, propDesc.Id);
				}
			}
		}

		return filter;
	}

	public override void Execute()
	{
		throw new NotSupportedException();
	}
}
