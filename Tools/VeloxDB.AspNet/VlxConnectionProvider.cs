using VeloxDB.Client;

namespace VeloxDB.AspNet;

internal class VeloxDBConnectionProvider : IVeloxDBConnectionProvider
{
	string connectionString;

	public VeloxDBConnectionProvider(string connectionString)
	{
		this.connectionString = connectionString;
	}

	public T Get<T>() where T : class
	{
		return ConnectionFactory.Get<T>(connectionString);
	}
}