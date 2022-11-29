using System;

namespace Velox.Storage.Replication;

internal enum ReplicaStateType
{
	NotStarted = 1,
	NotUsed = 2,
	Unreplicated = 3,
	Disconnected = 4,
	ConnectedPendingSync = 5,
	ConnectedAsync = 6,
	ConnectedSync = 7,
}
