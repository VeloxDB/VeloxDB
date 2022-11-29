using System;

namespace Velox.Protocol;

/// <summary>
/// This exception is thrown when there is mismatch in client and server API definitions.
/// </summary>
public sealed class DbAPIMismatchException : DbAPIErrorException
{
	///
	public DbAPIMismatchException() :
		base("Operation could not be executed because of the mismatch in client and server API definitions.")
	{
	}
}
