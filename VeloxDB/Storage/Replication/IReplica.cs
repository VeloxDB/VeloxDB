using System;
using System.Reflection;

namespace VeloxDB.Storage.Replication;

internal enum ReplicaType
{
	Source = 1,
	LocalRead = 2,
	GlobalRead = 3,
	GlobalWrite = 4,
	LocalWrite = 5
}

internal interface IReplica
{
	public ReplicaType Type { get; }
	public int Index { get; }
	public string Name { get; }
}
