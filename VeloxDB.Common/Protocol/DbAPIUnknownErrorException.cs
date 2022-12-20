using System;

namespace VeloxDB.Protocol;

/// <summary>
/// Thrown when unexpected error happens during database api operation execution.
/// If you need an exception to propagate from operation to client you should use <see cref="DbAPIOperationErrorAttribute"/>
/// </summary>
public sealed class DbAPIUnknownErrorException : DbAPIErrorException
{
	///
	public DbAPIUnknownErrorException() : base("Unexpected error occurred while executing database operation.")
	{
	}
}
