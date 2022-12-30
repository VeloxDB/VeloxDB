using System;

namespace VeloxDB.Descriptor;

internal sealed class ReplicationSettings
{
	public ReplicationSettings(string nodeName)
	{
		this.NodeName = nodeName;
		LocalReadReplicas = Array.Empty<ReplicaSettings>();
		GlobalReadReplicas = Array.Empty<ReplicaSettings>();
	}

	public string NodeName { get; set; }
	public ReplicaSettings LocalWriteReplica { get; set; }
	public GlobalWriteReplicaSettings GlobalWriteReplica { get; set; }
	public ReplicaSettings SourceReplica { get; set; }
	public ReplicaSettings[] LocalReadReplicas { get; set; }
	public ReplicaSettings[] GlobalReadReplicas { get; set; }
}
