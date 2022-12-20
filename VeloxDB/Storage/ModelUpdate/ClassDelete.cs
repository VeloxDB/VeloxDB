using System;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class ClassDelete
{
	ClassDescriptor classDesc;

	public ClassDelete(ClassDescriptor classDesc)
	{
		this.classDesc = classDesc;
	}

	public ClassDescriptor ClassDesc => classDesc;
}
