using System;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class InverseMapDelete
{
	ClassDescriptor classDesc;

	public InverseMapDelete(ClassDescriptor classDesc)
	{
		this.classDesc = classDesc;
	}

	public ClassDescriptor ClassDesc => classDesc;
}
