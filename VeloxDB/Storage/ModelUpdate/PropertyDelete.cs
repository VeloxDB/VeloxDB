using System;
using Velox.Descriptor;

namespace Velox.Storage.ModelUpdate;

class PropertyDelete
{
	PropertyDescriptor propDesc;

	public PropertyDelete(PropertyDescriptor propDesc)
	{
		this.propDesc = propDesc;
	}

	public PropertyDescriptor PropDesc => propDesc;
}
