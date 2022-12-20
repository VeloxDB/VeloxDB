using VeloxDB.Client;
using VeloxDB.Protocol;

namespace VeloxDB.Server;

[DbAPI(Name = AdminAPIServiceNames.DatabaseAdministration)]
public interface IDatabaseAdministration
{
	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	DatabaseTask<UserAssembliesState> GetAssemblyState(bool hashOnly = true);

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	DatabaseTask<PersistenceDescriptor> GetPersistenceConfiguration();

	DatabaseTask UpdateUserAssemblies(AssemblyUpdate assemblyUpdate, Guid assemblyVersionGuid);
	DatabaseTask UpdatePersistenceConfiguration(PersistenceDescriptor persistenceDescriptor);
}
