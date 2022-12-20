using System;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

class PropertyDelete
{
	PropertyDescriptor propDesc;

	public PropertyDelete(PropertyDescriptor propDesc)
	{
		this.propDesc = propDesc;
	}

	public PropertyDescriptor PropDesc => propDesc;
}
