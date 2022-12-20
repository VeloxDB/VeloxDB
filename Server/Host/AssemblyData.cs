using VeloxDB.Protocol;

namespace VeloxDB.Server;

internal record AssemblyData(LoadedAssemblies Loaded, SerializerManager SerializerManager,
	DeserializerManager DeserializerManager, ProtocolDiscoveryContext DiscoveryContext);
