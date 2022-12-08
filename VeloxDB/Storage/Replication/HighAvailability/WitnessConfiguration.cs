using System;
using Velox.Networking;

namespace Velox.Storage.Replication.HighAvailability;

internal abstract class WitnessConfiguration
{
	internal abstract IRaftNode CreateConnection(string traceName, MessageChunkPool chunkPool, Action<bool> connectedStateChanged);
}
