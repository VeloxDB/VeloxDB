using System;

namespace Velox.Storage.Replication.HighAvailability;

internal abstract class WitnessConfiguration
{
	internal abstract IRaftNode CreateConnection(string traceName, Action<bool> connectedStateChanged);
}
