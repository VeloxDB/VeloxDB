using System;

namespace VeloxDB.ObjectInterface;

/// <summary>
/// Specifies that the class is Database Class.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class DatabaseClassAttribute : Attribute
{
	bool isAbstract;

	/// <param name="isAbstract">Specifies whether this class is an abstract class. Abstract classes cannot be instantiated and can only be inherited.</param>
	public DatabaseClassAttribute(bool isAbstract = false)
	{
		this.isAbstract = isAbstract;
	}

	/// <summary>
	/// Get whether the class is abstract.
	/// </summary>
	public bool IsAbstract => isAbstract;
}
