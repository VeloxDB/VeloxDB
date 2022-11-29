using System;
using Velox.Descriptor;

namespace Velox.Storage.ModelUpdate;

internal sealed class PropertyInsert
{
	PropertyDescriptor propDesc;

	public PropertyInsert(PropertyDescriptor propDesc)
	{
		this.propDesc = propDesc;
	}

	public PropertyDescriptor PropDesc => propDesc;
}
