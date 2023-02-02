using System;

namespace VeloxDB.ObjectInterface;

/// <summary>
/// Apply this attribute to property, method or class you want automapper to ignore
/// </summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class | AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class AutomapperIgnoreAttribute : Attribute
{
	///
	public AutomapperIgnoreAttribute()
	{

	}
}
