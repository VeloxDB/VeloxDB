using System;

namespace VeloxDB.Protocol;

/// <summary>
/// This exception is thrown when requested database API is not found.
/// </summary>
public class DbAPINotFoundException : DbAPIErrorException
{
	///
	public DbAPINotFoundException() : base("Unknown database API requested.") { }
}
