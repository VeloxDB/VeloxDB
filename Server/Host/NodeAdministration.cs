using System.Text;
using VeloxDB.Common;
using VeloxDB.Config;
using VeloxDB.Protocol;
using VeloxDB.Storage.Replication;

namespace VeloxDB.Server;

[DbAPI(Name = "NodeAdministration")]
public sealed class NodeAdministration
{
	string? nodeName;
	string? clusterConfigFile;
	ClusterConfiguration? clusterConfig;
	NodeState state;
	Tracing.Source? engineTrace;

	internal NodeAdministration(string? nodeName, string? clusterConfigFile, ClusterConfiguration? clusterConfig, Tracing.Source? engineTrace)
	{
		Checker.AssertTrue(clusterConfigFile != null || clusterConfig != null);
		this.nodeName = nodeName;
		this.clusterConfigFile = clusterConfigFile;
		this.clusterConfig = clusterConfig;
		this.engineTrace = engineTrace;
		state = new NodeState(nodeName, new List<ReplicaState>(), false, false);
	}

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	public NodeState GetState()
	{
		return state;
	}

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	public FileData? GetClusterConfigFile()
	{
		if (clusterConfigFile != null)
		{
			return new FileData() { Name = Path.GetFileNameWithoutExtension(clusterConfigFile), Data = File.ReadAllBytes(clusterConfigFile) };
		}
		else
		{
			Checker.AssertNotNull(clusterConfig);
			FileData data = new FileData() { Name = clusterConfig.Cluster.Name };
			data.Data = Encoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(clusterConfig.AsJson()));
			return data;
		}
	}

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	public void SetTraceLevel(TraceLevel level)
	{
		engineTrace?.SetTraceLevel(level);
		Tracing.GlobalSource.SetTraceLevel(level);
	}

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	public void SetUserTraceLevel(TraceLevel level)
	{
		APITrace.SetTraceLevel(level);
	}

	internal void OnStateChanged(DatabaseInfo info)
	{
		List<ReplicaState> replicaStates = new List<ReplicaState>(info.Replicas.Length);

		foreach(ReplicaInfo replicaInfo in info.Replicas)
		{
			if(replicaInfo == null)
				continue;

			replicaStates.Add(Convert(replicaInfo));
		}

		state = new NodeState(nodeName, replicaStates, info.IsWitnessConnected, info.IsElectorConnected);
	}

	private ReplicaState Convert(ReplicaInfo replicaInfo)
	{
		return new ReplicaState(replicaInfo.Name, Convert(replicaInfo.StateType),
			Convert(replicaInfo.ReplicaType), replicaInfo.IsPrimary, replicaInfo.IsAligned);
	}

	private ReplicaType Convert(Storage.Replication.ReplicaType replicaType)
	{
		return (ReplicaType)replicaType;
	}

	private ReplicaStateType Convert(Storage.Replication.ReplicaStateType stateType)
	{
		return stateType switch
		{
			Storage.Replication.ReplicaStateType.ConnectedAsync => ReplicaStateType.ConnectedAsync,
			Storage.Replication.ReplicaStateType.ConnectedPendingSync => ReplicaStateType.ConnectedPendingSync,
			Storage.Replication.ReplicaStateType.ConnectedSync => ReplicaStateType.ConnectedSync,
			Storage.Replication.ReplicaStateType.Disconnected => ReplicaStateType.Disconnected,
			Storage.Replication.ReplicaStateType.Unreplicated => ReplicaStateType.NotUsed,
			Storage.Replication.ReplicaStateType.NotStarted => ReplicaStateType.NotUsed,
			Storage.Replication.ReplicaStateType.NotUsed => ReplicaStateType.NotUsed,
			_ => throw new NotSupportedException($"{stateType} is not supported"),
		};
	}
}
