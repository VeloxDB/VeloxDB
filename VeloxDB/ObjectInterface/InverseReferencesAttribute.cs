using System;

namespace Velox.ObjectInterface;

/// <summary>
/// Specifies that the property represents an inverse reference.
/// </summary>
/// <seealso cref="DatabaseReferenceAttribute"/>
/// <seealso cref="InverseReferenceSet{T}"/>
[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class InverseReferencesAttribute : Attribute
{
	string propertyName;

	/// <param name="propertyName">Name of the property that represent's the direct reference.</param>
	public InverseReferencesAttribute(string propertyName)
	{
		this.propertyName = propertyName;
	}

	/// <summary>
	/// Get's the name of the direct reference property.
	/// </summary>
	public string PropertyName => propertyName;
}
