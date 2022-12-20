using System.Diagnostics.CodeAnalysis;
using System.Text.Json.Serialization;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Config;
namespace VeloxDB.Server;

internal sealed class ReplicationConfiguration
{
	ClusterConfiguration? clusterConfig;
	public string? ThisNodeName {get; set;}
	public string? ClusterConfigFile {get; set;}
	public int? PrimaryWorkerCount {get; set;}
	public int? StandbyWorkerCount {get; set;}
	public bool? UseSeparateConnectionPerWorker {get; set;}

	[JsonIgnore]
	public ClusterConfiguration? ClusterConfig
	{
		get
		{
			return clusterConfig;
		}
		internal set
		{
			Checker.AssertNotNull(ThisNodeName);
			Checker.AssertNotNull(value);

			clusterConfig = value;
			ReplicationElement? node;

			clusterConfig.TryGetElementByName(ThisNodeName, out node);
			Checker.AssertNotNull(node);

			ThisNode = (ReplicationNode)node;
		}
	}

	[JsonIgnore]
	public ReplicationNode? ThisNode { get; private set; }

	[JsonIgnore]
	public bool IsStandalone
	{
		get
		{
			Checker.AssertNotNull(ThisNode);
			if(ThisNode.Type != ElementType.Node)
				return false;

			StandaloneNode node = (StandaloneNode)ThisNode;

		 	return ThisNode.Parent == null && (node.Children == null || node.Children.Length == 0);
		}
	}

	public void Override(ReplicationConfiguration newReplication)
	{
		if(newReplication.ThisNodeName != null)
		{
			ThisNodeName = newReplication.ThisNodeName;
		}

		if(newReplication.ClusterConfigFile != null)
		{
			ClusterConfigFile = newReplication.ClusterConfigFile;
		}

		if(newReplication.PrimaryWorkerCount != null)
		{
			PrimaryWorkerCount = newReplication.PrimaryWorkerCount;
		}

		if(newReplication.StandbyWorkerCount != null)
		{
			StandbyWorkerCount = newReplication.StandbyWorkerCount;
		}

		if(newReplication.UseSeparateConnectionPerWorker != null)
		{
			UseSeparateConnectionPerWorker = newReplication.UseSeparateConnectionPerWorker;
		}
	}

	public bool HasGlobalWriteWitness()
	{
		return TryGetGlobalWriteCluster(out _);
	}
	public bool TryGetGlobalWriteCluster([NotNullWhen(true)] out GlobalWriteCluster? cluster)
	{
		Checker.AssertNotNull(ThisNodeName, ClusterConfig, ThisNode);

		int depth = 2;
		ReplicationElement current = ThisNode;

		while(depth > 0)
		{
			if(current.Parent == null || !current.IsMember)
				break;

			if(current.IsMember && current.Parent.Type == ElementType.GlobalWrite)
			{
				cluster = (GlobalWriteCluster)current.Parent;
				return true;
			}

			depth--;
			current = current.Parent;
		}

		cluster = null;
		return false;
	}

	public bool TryGetLocalWriteCluster([NotNullWhen(true)] out LocalWriteCluster? lwCluster)
	{
		Checker.AssertNotNull(ThisNode);

		lwCluster = null;
		if(ThisNode.Parent == null || !ThisNode.IsMember || ThisNode.Parent.Type != ElementType.LocalWrite)
			return false;

		lwCluster = (LocalWriteCluster)ThisNode.Parent;
		return true;
	}

	public ReplicationSettings ToRepplicationSettings()
	{
		Checker.AssertNotNull(ClusterConfig, ThisNode);
		ReplicationSettings result = new ReplicationSettings(ThisNode.Name);

		CreateGWReplica(result);
		CreateLWReplica(result);
		CreateSourceReplica(result);
		CreateGlobalReadReplicas(result);
		CreateLocalReadReplicas(result);

		return result;
	}

	private void SetGlobalProperties(ReplicaSettings replica)
	{
		Checker.AssertNotNull(PrimaryWorkerCount, StandbyWorkerCount, UseSeparateConnectionPerWorker);
		replica.SendWorkerCount = (int)PrimaryWorkerCount;
		replica.RedoWorkerCount = (int)StandbyWorkerCount;
		replica.UseSeparateConnectionPerWorker = (bool)UseSeparateConnectionPerWorker;
	}

	private string GetReplicaName(string name1, string name2, string type)
	{
		return $"{name1}-{name2}{type}";
	}

	private void SetClusterReplicaProperties<T>(ClusterBase<T> cluster, string myName, ReplicaSettings replica, string type) where T : ReplicationElement
	{
		Checker.AssertNotNull(ThisNode);
		T first, second;

		if(cluster.First.NameEqual(myName))
		{
			first = cluster.First;
		 	second = cluster.Second;
		}
		else
		{
			first = cluster.Second;
			second = cluster.First;
		}

		SetGlobalProperties(replica);

		replica.Name = GetReplicaName(first.Name, second.Name, type);
		replica.HostAddress = ThisNode.ReplicationAddress.ToString();
		replica.PartnerAddresses = second.GetPrimaryAdresses();
	}
	private void CreateGWReplica(ReplicationSettings settings)
	{
		Checker.AssertNotNull(ThisNode);
		GlobalWriteCluster? cluster;
		if(!TryGetGlobalWriteCluster(out cluster))
			return;

		GlobalWriteReplicaSettings replica = new GlobalWriteReplicaSettings();

		string myName = (ThisNode.Parent.Type == ElementType.GlobalWrite)?ThisNode.Name:ThisNode.Parent.Name;

		SetClusterReplicaProperties(cluster, myName, replica, "GW");
		replica.IsSyncMode = cluster.SynchronousReplication;

		settings.GlobalWriteReplica = replica;
	}

	private void CreateLWReplica(ReplicationSettings settings)
	{
		Checker.AssertNotNull(ThisNode);
		LocalWriteCluster? cluster;
		if(!TryGetLocalWriteCluster(out cluster))
			return;

		ReplicaSettings replica = new ReplicaSettings();
		SetClusterReplicaProperties(cluster, ThisNode.Name, replica, "LW");
		settings.LocalWriteReplica = replica;
	}

	private void CreateSourceReplica(ReplicationSettings settings)
	{
		Checker.AssertNotNull(ThisNode);

		if(ThisNode.Parent == null)
			return;

		ReplicationElement? parent = null;

		if(!ThisNode.IsMember)
		{
			parent = ThisNode.Parent;
		}
		else
		{
			if(ThisNode.Parent.Type == ElementType.LocalWrite && ThisNode.Parent.Parent != null && !ThisNode.Parent.IsMember)
			{
				parent = ThisNode.Parent.Parent;
			}
		}

		if(parent == null)
			return;

		Checker.AssertNotNull(parent.Name, obj2:ThisNode.Name);

		ReplicaSettings replica = new ReplicaSettings();
		SetGlobalProperties(replica);

		replica.Name = GetReplicaName(parent.Name, ThisNode.Name, "S");
		replica.HostAddress = ThisNode.ReplicationAddress.ToString();
		replica.PartnerAddresses = null;

		settings.SourceReplica = replica;
	}

	private ReplicaSettings[] CreateChildrenReplicas(ReplicationElement[] children, string type)
	{
		Checker.AssertNotNull(ThisNode);
		ReplicaSettings[] replicas = new ReplicaSettings[children.Length];

		for(int i = 0; i < children.Length; i++)
		{
			ReplicationElement child = children[i];
			Checker.AssertNotNull(child.Name);

			ReplicaSettings replica = new ReplicaSettings();
			SetGlobalProperties(replica);

			replica.Name = GetReplicaName(ThisNode.Name, child.Name, type);
			replica.HostAddress = null;
			replica.PartnerAddresses = child.GetPrimaryAdresses();
			replicas[i] = replica;
		}

		return replicas;
	}

	private void CreateGlobalReadReplicas(ReplicationSettings settings)
	{
		GlobalWriteCluster? cluster;
		if(!TryGetGlobalWriteCluster(out cluster) || cluster.Children == null)
			return;

		settings.GlobalReadReplicas = CreateChildrenReplicas(cluster.Children, "GR");
	}

	private void CreateLocalReadReplicas(ReplicationSettings settings)
	{
		Checker.AssertNotNull(ThisNode);
		ReplicationElement[]? children = null;
		LocalWriteCluster? cluster;

		if(ThisNode.Type == ElementType.Node)
		{
			StandaloneNode standalone = (StandaloneNode)ThisNode;
			children = standalone.Children;
		}
		else if(TryGetLocalWriteCluster(out cluster))
		{
			children = cluster.Children;
		}

		if(children == null)
			return;

		settings.LocalReadReplicas = CreateChildrenReplicas(children, "LR");
	}
}
