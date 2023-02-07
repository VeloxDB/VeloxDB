namespace VeloxDB.AspNet;

/// <summary>
/// Provides connections to a VeloxDB instance.
/// </summary>
public interface IVeloxDBConnectionProvider
{
	/// <summary>
	/// Gets a connection of the specified type.
	/// </summary>
	/// <typeparam name="T">The type of connection to get.</typeparam>
	/// <returns>A connection of the specified type.</returns>
	T Get<T>() where T : class;
}
