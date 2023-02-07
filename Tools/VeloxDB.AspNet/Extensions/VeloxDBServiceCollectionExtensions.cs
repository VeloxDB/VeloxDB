using Microsoft.Extensions.DependencyInjection;

namespace VeloxDB.AspNet.Extensions;

/// <summary>
///     Extension methods for setting up VeloxDB services in an Microsoft.Extensions.DependencyInjection.IServiceCollection.
/// </summary>
public static class VeloxDBServiceCollectionExtensions
{
	/// <summary>
	/// Adds the <see cref="IVeloxDBConnectionProvider"/> to the <see cref="Microsoft.Extensions.DependencyInjection.IServiceCollection"/>.
	/// </summary>
	/// <param name="services">The <see cref="Microsoft.Extensions.DependencyInjection.IServiceCollection"/> to add services to.</param>
	/// <param name="connectionString">Connection string for VeloxDB instance to connect to.</param>
	/// <returns>The updated <see cref="Microsoft.Extensions.DependencyInjection.IServiceCollection"/></returns>
	public static IServiceCollection AddVeloxDBConnectionProvider(this IServiceCollection services, string connectionString)
	{
		return services.AddSingleton(typeof(IVeloxDBConnectionProvider), new VeloxDBConnectionProvider(connectionString));
	}
}