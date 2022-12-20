using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class ValidateNotNullReferenceJob : ModelUpdateValidationJob
{
	ClassScan scan;
	ReferencePropertyDescriptor propDesc;

	public ValidateNotNullReferenceJob(ModelUpdateContext context, ClassScan scan, ReferencePropertyDescriptor propDesc) :
		base(context)
	{
		this.scan = scan;
		this.propDesc = propDesc;
	}

	public static IEnumerable<ValidateNotNullReferenceJob> Create(Database database, ModelUpdateContext context)
	{
		foreach (ClassUpdate cu in context.ModelUpdate.UpdatedClasses.Values)
		{
			foreach (PropertyUpdate pu in cu.UpdatedProperties)
			{
				if (pu.IsMultiplicityModified && (pu.PropDesc as ReferencePropertyDescriptor).Multiplicity == Multiplicity.One)
				{
					database.Trace.Debug("Validating transition of reference property {0} in class {1} to non nullable.",
						pu.PrevPropDesc.Name, cu.PrevClassDesc.FullName);
					TTTrace.Write(database.TraceId, database.Id, pu.PrevPropDesc.Id, cu.PrevClassDesc.Id);

					Class @class = database.GetClass(cu.PrevClassDesc.Index).MainClass;
					if (@class != null)
					{
						foreach (ClassScan scan in @class.GetClassScans(null, false, out long count))
						{
							yield return new ValidateNotNullReferenceJob(context, scan, (ReferencePropertyDescriptor)pu.PrevPropDesc);
						}
					}
				}
			}
		}
	}

	public override void Execute()
	{
		int offset = -1;
		using (scan)
		{
			foreach (ObjectReader r in scan)
			{
				Class @class = r.Class;
				if (offset == -1)
					offset = @class.ClassDesc.PropertyByteOffsets[@class.ClassDesc.GetPropertyIndex(propDesc.Id)];

				long value = r.GetLongOptimized(offset);
				if (value == 0)
				{
					context.SetError(DatabaseErrorDetail.CreateNullReferenceNotAllowed(r.GetIdOptimized(), @class.ClassDesc.FullName, propDesc.Name));
					return;
				}
			}
		}
	}
}
