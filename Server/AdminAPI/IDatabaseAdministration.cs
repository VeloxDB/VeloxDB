using Velox.Protocol;

namespace Velox.Server;

[DbAPI(Name = AdminAPIServiceNames.DatabaseAdministration)]
public interface IDatabaseAdministration
{
    [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
    Task<UserAssembliesState> GetAssemblyState(bool hashOnly = true);

    [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
    Task<PersistenceDescriptor> GetPersistenceConfiguration();

    Task UpdateUserAssemblies(AssemblyUpdate assemblyUpdate, Guid assemblyVersionGuid);
    Task UpdatePersistenceConfiguration(PersistenceDescriptor persistenceDescriptor);
}
