using System;

namespace VeloxDB.Protocol;

/// <summary>
/// This exception is thrown when requested database API is either stopped (trying to access a node in standby state),
/// or if database API with given name does not exist.
/// </summary>
public class DbAPIUnavailableException : DbAPIErrorException
{
	///
    public DbAPIUnavailableException() : base("Operation failed to execute.")
	{
	}

	internal override ResponseType ResponseCode => ResponseType.APIUnavailable;
}
