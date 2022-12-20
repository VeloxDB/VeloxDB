using System;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class ClassInsert
{
	ClassDescriptor classDesc;

	public ClassInsert(ClassDescriptor classDesc)
	{
		this.classDesc = classDesc;
	}

	public ClassDescriptor ClassDesc => classDesc;
}
