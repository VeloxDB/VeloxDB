using System;
using Velox.Descriptor;

namespace Velox.Storage.ModelUpdate;

internal sealed class InverseMapInsert
{
	ClassDescriptor classDesc;

	public InverseMapInsert(ClassDescriptor classDesc)
	{
		this.classDesc = classDesc;
	}

	public ClassDescriptor ClassDesc => classDesc;
}
