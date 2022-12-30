using System;
using System.Collections.Generic;
using System.Linq;
using VeloxDB.Common;
using static System.Math;

namespace VeloxDB.Descriptor;

internal sealed class ReplicationDescriptor
{
	string nodeName;
	ReplicaDescriptor localWriteReplica;
	GlobalWriteReplicaDescriptor globalWriteReplica;
	ReplicaDescriptor sourceReplica;
	ReadOnlyArray<ReplicaDescriptor> localReadReplicas;
	ReadOnlyArray<ReplicaDescriptor> globalReadReplicas;

	public ReplicationDescriptor(ReplicationSettings settings)
	{
		if (settings == null)
			settings = new ReplicationSettings(null);

		nodeName = settings.NodeName;
		localWriteReplica = ReplicaDescriptor.Create(settings.LocalWriteReplica);
		globalWriteReplica = GlobalWriteReplicaDescriptor.Create((GlobalWriteReplicaSettings)settings.GlobalWriteReplica);
		sourceReplica = ReplicaDescriptor.Create(settings.SourceReplica);
		localReadReplicas = ReplicaDescriptor.Create(settings.LocalReadReplicas);
		globalReadReplicas = ReplicaDescriptor.Create(settings.GlobalReadReplicas);

		Validate();
	}

	public ReplicaDescriptor LocalWriteReplica => localWriteReplica;
	public GlobalWriteReplicaDescriptor GlobalWriteReplica => globalWriteReplica;
	public ReplicaDescriptor SourceReplica => sourceReplica;
	public ReadOnlyArray<ReplicaDescriptor> LocalReadReplicas => localReadReplicas;
	public ReadOnlyArray<ReplicaDescriptor> GlobalReadReplicas => globalReadReplicas;
	public string NodeName => nodeName;

	public IEnumerable<ReplicaDescriptor> AllReplicas
	{
		get
		{
			if (localWriteReplica != null)
				yield return localWriteReplica;

			if (globalWriteReplica != null)
				yield return globalWriteReplica;

			if (sourceReplica != null)
				yield return sourceReplica;

			foreach (ReplicaDescriptor replica in localReadReplicas.Concat(globalReadReplicas))
			{
				yield return replica;
			}
		}
	}

	public int GetLocalWriteReplicaIndex() => localWriteReplica == null ? -1 : 0;
	public int GetGlobalWriteReplicaIndex() => globalWriteReplica == null ? -1 : GetLocalWriteReplicaIndex() + 1;
	public int GetSourceReplicaIndex() => sourceReplica == null ? -1 : GetLocalWriteReplicaIndex() + 1;
	public int GetLocalReadReplicaIndex(int index) => Max(Max(GetLocalWriteReplicaIndex(),
		GetGlobalWriteReplicaIndex()), GetSourceReplicaIndex()) + index + 1;
	public int GetGlobalReadReplicaIndex(int index) => GetLocalReadReplicaIndex(localReadReplicas.Length - 1) + index + 1;

	public bool IsLocalWriteClusterAvailable()
	{
		return localWriteReplica != null && localWriteReplica.HostAddress != null;
	}

	public void Validate()
	{
		foreach (ReplicaDescriptor replicaDesc in AllReplicas)
		{
			replicaDesc.Validate();
		}

		if (globalWriteReplica != null && (globalWriteReplica.HostAddress == null || globalWriteReplica.PartnerAddresses == null || globalWriteReplica.PartnerAddresses.Length == 0))
			throw new ArgumentException("Global write replica needs both host and partner addresses.");

		if (globalWriteReplica != null && sourceReplica != null)
			throw new ArgumentException("Node must not spcify both global write replica and source replica.");

		if (globalWriteReplica == null && globalReadReplicas.Length > 0)
			throw new ArgumentException("For a node to have global read replicas it must specify the global write replica.");

		if (sourceReplica != null && sourceReplica.PartnerAddresses != null)
			throw new ArgumentException("Partner address is invalid for a source replica.");

		if (sourceReplica != null && sourceReplica.HostAddress == null)
			throw new ArgumentException("Hosting address must be defined for source replica.");

		foreach (ReplicaDescriptor rd in localReadReplicas.Concat(GlobalReadReplicas))
		{
			if (rd.HostAddress != null)
				throw new ArgumentException("Read replica must not specify host address.");

			if (rd.PartnerAddresses == null || rd.PartnerAddresses.Length == 0)
				throw new ArgumentException("Read replica must specify a set of partner addresses.");
		}
	}
}
