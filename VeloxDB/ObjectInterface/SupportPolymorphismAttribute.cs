using System;

namespace Velox.ObjectInterface;

/// <summary>
/// Apply this attribute to automapper method to specify that it supports polymorphism
/// </summary>
[AttributeUsage(AttributeTargets.Method, Inherited = false, AllowMultiple = false)]
public sealed class SupportPolymorphismAttribute : Attribute
{
	///
	public SupportPolymorphismAttribute()
	{
	}
}
