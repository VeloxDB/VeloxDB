using System;

namespace Velox.Protocol;

/// <summary>
/// Specifies that the method throws an exception.
/// </summary>
/// <remarks>
/// Apply the <see cref="DbAPIOperationErrorAttribute"/> to type to indicate to VeloxDB protocol
/// that method can throw an exception of specified type. These exceptions are then propagated to the client.
/// Exceptions that are not specified using this attribute are propagated as <see cref="Velox.Protocol.DbAPIUnknownErrorException"/>.
/// </remarks>
[AttributeUsage(AttributeTargets.Method, Inherited = false, AllowMultiple = true)]
public sealed class DbAPIOperationErrorAttribute : Attribute
{
	Type type;

	/// <summary>
	/// Specifies that methot can throw an exception.
	/// </summary>
	/// <param name="type">The type of the exception that can be thrown.</param>
	public DbAPIOperationErrorAttribute(Type type)
	{
		this.type = type;
	}

	/// <summary>
	/// The type of the exception that can be thrown.
	/// </summary>
	public Type Type => type;
}
