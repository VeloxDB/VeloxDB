using System;
using Velox.Descriptor;

namespace Velox.Storage.ModelUpdate;

internal sealed class ClassDelete
{
	ClassDescriptor classDesc;

	public ClassDelete(ClassDescriptor classDesc)
	{
		this.classDesc = classDesc;
	}

	public ClassDescriptor ClassDesc => classDesc;
}
