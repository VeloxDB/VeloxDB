using System;

namespace VeloxDB.AspNet;

/// <summary>
/// The ForwardAttribute class is used to forward an attribute from a VeloxDB API to an ASP.NET API Controller.
/// </summary>
public class ForwardAttribute : Attribute
{
	/// <summary>
	/// Initializes a new instance of the <see cref="ForwardAttribute"/> class with the specified attribute type.
	/// </summary>
	/// <param name="attribute">The type of the attribute to forward.</param>
	public ForwardAttribute(Type attribute)
	{

	}

	/// <summary>
	/// Initializes a new instance of the <see cref="ForwardAttribute"/> class with the specified attribute type and arguments.
	/// </summary>
	/// <param name="attribute">The type of the attribute to forward.</param>
	/// <param name="arguments">The arguments for the attribute.</param>
	public ForwardAttribute(Type attribute, params object[] arguments)
	{

	}

	/// <summary>
	/// Gets the attribute type that is being forwarded.
	/// </summary>
	public Attribute Attribute { get; private set; }

	/// <summary>
	/// Gets the arguments for the attribute.
	/// </summary>
	public object[] Arguments { get; private set; }

	/// <summary>
	/// The NamedArguments property is used to pass named arguments to the attribute constructor.
	/// The array should be composed of key-value pairs, with the keys being strings and placed in odd positions,
	/// and the corresponding values in even positions. It is important to note that the length of the array must be even,
	/// with an equal number of keys and values. The keys must be the names of properties or fields on the attribute type,
	/// and the values should be of the appropriate type for those properties or fields.
	/// </summary>
	public object[] NamedArguments { get; set; }


}