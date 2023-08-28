using System;

namespace VeloxDB.ObjectInterface;


/// <summary>
/// Specifies that the property is a database property.
/// </summary>
/// <seealso cref="DatabaseReferenceAttribute"/>
[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class DatabasePropertyAttribute : Attribute
{
	object defaultValue;

	/// <param name="defaultValue">Specifies the default value for the property.</param>
	public DatabasePropertyAttribute(object defaultValue = null)
	{
		this.defaultValue = defaultValue;
	}

	/// <summary>
	/// Gets the default value for the property.
	/// </summary>
	public object DefaultValue => defaultValue;
}
