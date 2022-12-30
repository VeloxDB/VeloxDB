using System;

namespace VeloxDB.Protocol;

/// <summary>
/// Object graph failed to serialize/deserialize due to it exceeding the maximum allowed number of objects.
/// </summary>
public class DbAPIObjectCountLimitExceededException : DbAPIErrorException
{
	///
	public DbAPIObjectCountLimitExceededException() : base("Operation failed to execute.")
	{
	}

	internal override ResponseType ResponseCode => ResponseType.Error;
}
