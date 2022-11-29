using System;
using Velox.Descriptor;

namespace Velox.Storage.ModelUpdate;

internal sealed class ClassInsert
{
	ClassDescriptor classDesc;

	public ClassInsert(ClassDescriptor classDesc)
	{
		this.classDesc = classDesc;
	}

	public ClassDescriptor ClassDesc => classDesc;
}
