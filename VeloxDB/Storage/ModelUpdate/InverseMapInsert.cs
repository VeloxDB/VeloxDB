using System;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class InverseMapInsert
{
	ClassDescriptor classDesc;

	public InverseMapInsert(ClassDescriptor classDesc)
	{
		this.classDesc = classDesc;
	}

	public ClassDescriptor ClassDesc => classDesc;
}
