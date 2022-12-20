using System;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.ModelUpdate;

class PropertyUpdate
{
	PropertyDescriptor prevPropDesc;
	PropertyDescriptor propDesc;

	bool isTargetModified;
	bool isMultiplicityModified;
	bool invRefTrackingModified;
	bool defaultValueModified;
	bool deleteTargetActionModified;

	public PropertyUpdate(PropertyDescriptor prevPropDesc, PropertyDescriptor propDesc,
		bool isTargetModified, bool isMultiplicityModified, bool invRefTrackingModified, bool defaultValueModified,
		bool deleteTargetActionModified)
	{
		this.prevPropDesc = prevPropDesc;
		this.propDesc = propDesc;
		this.isTargetModified = isTargetModified;
		this.isMultiplicityModified = isMultiplicityModified;
		this.invRefTrackingModified = invRefTrackingModified;
		this.defaultValueModified = defaultValueModified;
		this.deleteTargetActionModified = deleteTargetActionModified;
	}

	public PropertyDescriptor PrevPropDesc => prevPropDesc;
	public PropertyDescriptor PropDesc => propDesc;

	public bool IsMultiplicityModified => isMultiplicityModified;
	public bool IsTargetModified => isTargetModified;
	public bool InvRefTrackingModified => invRefTrackingModified;
	public bool DefaultValueChanged => defaultValueModified;
	public bool DeleteTargetActionChanged => deleteTargetActionModified;
}
