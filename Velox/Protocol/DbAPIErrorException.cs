using System;

namespace Velox.Protocol;

/// <summary>
/// Base database API exception.
/// </summary>
public abstract class DbAPIErrorException : Exception
{
	///
	public DbAPIErrorException()
	{
	}

	///
	public DbAPIErrorException(string message) :
		base(message)
	{
	}

	internal virtual ResponseType ResponseCode => ResponseType.Error;
}
