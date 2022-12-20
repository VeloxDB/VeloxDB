using System;

namespace VeloxDB.Protocol;

/// <summary>
/// This attribute marks that a class can be transferred using VeloxDB protocol.
/// Attributes for DTO classes are optional. This attribute can be used to change class's protocol name.
/// Default name is full .NET class name.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
public sealed class DbAPITypeAttribute : Attribute
{
	string name;

	///
	public DbAPITypeAttribute()
	{
	}

	/// <summary>
	/// Protocol name for the class. Default is full .NET class name.
	/// </summary>
	public string Name { get => name; set => name = value; }
}
