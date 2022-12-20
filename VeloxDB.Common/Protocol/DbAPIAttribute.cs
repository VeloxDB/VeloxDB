using System;

namespace VeloxDB.Protocol;

/// <summary>
/// This attribute can be applied either to a class or an interface. When used on a class,
/// it tells VeloxDB that the class should be made public through VeloxDB protocol.
/// Proxy interfaces that are used for connecting to VeloxDB database api are also marked with this attribute.
/// </summary>
[AttributeUsage(AttributeTargets.Interface | AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class DbAPIAttribute : Attribute
{
	string name;

	///
	public DbAPIAttribute()
	{
	}

	/// <summary>
	/// Specifies a name of the database API. Default name is full class/interface name.
	/// The name on server side and client side must be equal.
	/// </summary>
	public string Name { get => name; set => name = value; }
}
