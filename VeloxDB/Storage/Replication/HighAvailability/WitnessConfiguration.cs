using System;
using VeloxDB.Networking;

namespace VeloxDB.Storage.Replication.HighAvailability;

internal abstract class WitnessConfiguration
{
	internal abstract IRaftNode CreateConnection(string traceName, MessageChunkPool chunkPool, Action<bool> connectedStateChanged);
}
