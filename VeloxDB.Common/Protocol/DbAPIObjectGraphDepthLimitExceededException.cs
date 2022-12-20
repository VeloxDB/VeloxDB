using System;

namespace VeloxDB.Protocol;

/// <summary>
/// Object graph failed to be serialized/deserialized due to it exceeding the maximum allowed graph depth.
/// Consider turning graph serialization support on.
/// </summary>
public class DbAPIObjectGraphDepthLimitExceededException : DbAPIErrorException
{
	///
	public DbAPIObjectGraphDepthLimitExceededException() : base("Operation failed to execute.")
	{
	}

	internal override ResponseType ResponseCode => ResponseType.Error;
}
