using System;

namespace VeloxDB.Protocol;

/// <summary>
/// This exception is thrown when there is mismatch in client and server API definitions.
/// </summary>
public sealed class DbAPIMismatchException : DbAPIErrorException
{
	///
	public DbAPIMismatchException() :
		base("The operation could not be executed due to a mismatch between the client and server API definitions.")
	{
	}

	///
	public DbAPIMismatchException(string reason) :
		base($"The operation could not be executed due to a mismatch between the client and server API definitions. {reason}")
	{
	}
}
