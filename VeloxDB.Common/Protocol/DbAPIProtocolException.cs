using System;

namespace VeloxDB.Protocol;

/// <summary>
/// Thrown when invalid protocol format is detected.
/// </summary>
public class DbAPIProtocolException : DbAPIErrorException
{
	///
	public DbAPIProtocolException() : base("Received data stream has an invalid format.")
	{
	}

	internal override ResponseType ResponseCode => ResponseType.ProtocolError;
}
