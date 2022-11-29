using System;

namespace Velox.ObjectInterface;


/// <summary>
/// Specifies that the property is a database property.
/// </summary>
/// <seealso cref="DatabaseReferenceAttribute"/>
[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class DatabasePropertyAttribute : Attribute
{
	string defaultValue;

	/// <param name="defaultValue">Specifies the default value for the property.</param>
	public DatabasePropertyAttribute(string defaultValue = null)
	{
		this.defaultValue = defaultValue;
	}

	/// <summary>
	/// Gets the default value for the property.
	/// </summary>
	public string DefaultValue => defaultValue;
}
