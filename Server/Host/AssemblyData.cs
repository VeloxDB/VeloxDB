using Velox.Protocol;

namespace Velox.Server;

internal record AssemblyData(LoadedAssemblies Loaded, SerializerManager SerializerManager,
	DeserializerManager DeserializerManager, ProtocolDiscoveryContext DiscoveryContext);
