using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class ValidateReferenceTargetJob : ModelUpdateValidationJob
{
	ClassScan scan;
	PropertyDescriptor prevPropDesc;
	ReferencePropertyDescriptor propDesc;

	public ValidateReferenceTargetJob(ModelUpdateContext context, ClassScan scan, PropertyDescriptor prevPropDesc, ReferencePropertyDescriptor propDesc) :
		base(context)
	{
		this.scan = scan;
		this.prevPropDesc = prevPropDesc;
		this.propDesc = propDesc;
	}

	public static IEnumerable<ValidateReferenceTargetJob> Create(Database database, ModelUpdateContext context)
	{
		foreach (ClassUpdate cu in context.ModelUpdate.UpdatedClasses.Values)
		{
			foreach (PropertyUpdate pu in cu.UpdatedProperties)
			{
				if (pu.IsTargetModified)
				{
					database.Trace.Debug("Validating target for reference {0} of class {1}.",
						pu.PrevPropDesc.Name, cu.PrevClassDesc.FullName);
					TTTrace.Write(database.TraceId, database.Id, pu.PrevPropDesc.Id, cu.PrevClassDesc.Id);

					Class @class = database.GetClass(cu.PrevClassDesc.Index).MainClass;
					if (@class != null)
					{
						foreach (ClassScan scan in @class.GetClassScans(null, false, out long count))
						{
							yield return new ValidateReferenceTargetJob(context, scan, pu.PrevPropDesc, (ReferencePropertyDescriptor)pu.PropDesc);
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
					offset = @class.ClassDesc.PropertyByteOffsets[@class.ClassDesc.GetPropertyIndex(prevPropDesc.Id)];

				if (propDesc.Multiplicity == Multiplicity.Many)
				{
					long[] value = r.GetLongArrayOptimized(offset);
					if (value != null)
					{
						for (int i = 0; i < value.Length; i++)
						{
							ValidateSingleReference(r, @class, value[i]);
						}
					}
				}
				else
				{
					long value = r.GetLongOptimized(offset);
					if (value == 0)
					{
						if (propDesc.Multiplicity == Multiplicity.One)
						{
							context.SetError(DatabaseErrorDetail.CreateNullReferenceNotAllowed(r.GetIdOptimized(), @class.ClassDesc.FullName, propDesc.Name));
							return;
						}
					}
					else
					{
						ValidateSingleReference(r, @class, value);
					}
				}
			}
		}
	}

	private void ValidateSingleReference(ObjectReader r, Class @class, long value)
	{
		short targetClassId = IdHelper.GetClassId(value);
		ClassDescriptor targetClassDesc = propDesc.OwnerClass.Model.GetClass(targetClassId);	// Extract target class from new model
		if (targetClassDesc == null || targetClassDesc.IsAbstract)
		{
			context.SetError(DatabaseErrorDetail.CreateUnknownReference(r.GetIdOptimized(), @class.ClassDesc.FullName, propDesc.Name, value));
			return;
		}

		if (propDesc.ReferencedClass.Id != targetClassDesc.Id && !propDesc.ReferencedClass.DescendentClassIdsSet.ContainsKey(targetClassDesc.Id))
		{
			context.SetError(DatabaseErrorDetail.CreateInvalidReferencedClass(r.GetIdOptimized(), @class.ClassDesc.FullName, targetClassDesc.FullName, propDesc.Name, value));
			return;
		}

		// Now we need the class descriptor from the previous model so that we can get the class from the database
		targetClassDesc = @class.ClassDesc.Model.GetClass(targetClassId);

		Class targetClass = @class.Database.GetClass(targetClassDesc.Index).MainClass;
		if (!targetClass.ObjectExists(null, value))
		{
			context.SetError(DatabaseErrorDetail.CreateUnknownReference(r.GetIdOptimized(), @class.ClassDesc.FullName, propDesc.Name, value));
			return;
		}
	}
}
