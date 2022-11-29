using System;
using Velox.Descriptor;

namespace Velox.Storage.ModelUpdate;

internal sealed class InverseMapDelete
{
	ClassDescriptor classDesc;

	public InverseMapDelete(ClassDescriptor classDesc)
	{
		this.classDesc = classDesc;
	}

	public ClassDescriptor ClassDesc => classDesc;
}
