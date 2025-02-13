using System;

namespace VeloxDB.Protocol;

/// <summary>
/// This exception is thrown when requested database API is not found.
/// </summary>
public class DbAPINotFoundException : DbAPIErrorException
{
	///
	public DbAPINotFoundException()
	{
	}

	///
	/// <summary>
	/// Initializes a new instance of the <see cref="DbAPINotFoundException"/> class with a specified error message.
	/// </summary>
	/// <param name="unkownApi">The name of the database API that was not found.</param>
	public DbAPINotFoundException(string unkownApi) : base($"Unknown database API: {unkownApi}")
	{
		this.UnknownAPI = unkownApi;
	}

	/// <summary>
	/// Gets the name of the database API that was not found.
	/// </summary>
	public string UnknownAPI { get; private set; }
}
