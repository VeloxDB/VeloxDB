using System;

namespace VeloxDB.Client;

/// <summary>
/// Thrown when invalid connection string is encountered
/// </summary>
public sealed class InvalidConnectionStringException : Exception
{
	internal InvalidConnectionStringException()
	{
	}

	internal InvalidConnectionStringException(string message) : base(message)
	{
	}

	internal InvalidConnectionStringException(string message, Exception innerException) : base(message, innerException)
	{
	}
}