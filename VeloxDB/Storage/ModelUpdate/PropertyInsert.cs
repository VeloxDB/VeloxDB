using System;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class PropertyInsert
{
	PropertyDescriptor propDesc;

	public PropertyInsert(PropertyDescriptor propDesc)
	{
		this.propDesc = propDesc;
	}

	public PropertyDescriptor PropDesc => propDesc;
}
