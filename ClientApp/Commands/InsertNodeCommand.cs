using System;
using Velox.ClientApp.Modes;
using Velox.Common;
using Velox.Config;

namespace Velox.ClientApp.Commands;

[Command("create-node", "Creates a new node (either a write or a read node).")]
internal sealed class InsertNodeCommand : Command
{
	[Param("name", "Node name.", ShortName = "n")]
	public string Name { get; set; }

	[Param("host", "Host name or the address of the node.", ShortName = "h1", IsMandatory = true)]
	public string Host { get; set; }

	[Param("rep-port", "Replication endpoint port. Default is " + ClusterConfiguration.DefaultReplicationPortString + ".")]
	public int ReplicationPort { get; set; } = ClusterConfiguration.DefaultReplicationPort;

	[Param("exec-port", "Execution endpoint port. Default is " + ClusterConfiguration.DefaultExecutionPortString + ".")]
	public int ExecutionPort { get; set; } = ClusterConfiguration.DefaultExecutionPort;

	[Param("admin-port", "Administration endpoint port. Default is " + ClusterConfiguration.DefaultAdministrationPortString + ".")]
	public int AdministrationPort { get; set; } = ClusterConfiguration.DefaultAdministrationPort;

	[Param("source", "Name of the existing node/cluster that will be the source of replication data for this node. Specifying this parameter " +
		"implies creation of Read node.", ShortName = "s")]
	public string SourceName { get; set; }

	[Param("sys-name", "Name to assign to a global cluster if one gets created with this operation. Default is \"system\".")]
	public string SystemName { get; set; } = "system";

	[Param("sync-rep", "Indicates whether to use synchronous replication between sites, which is used if this operation introduces a new site. Default is false.")]
	public bool IsSyncReplication { get; set; } = false;

	public override bool IsModeValid(Mode mode)
	{
		return mode is ClusterConfigMode;
	}

	protected override bool OnExecute(Program program)
	{
		if (!ValidateParams())
			return false;

		ClusterConfiguration clusterConfig = ((ClusterConfigMode)program.Mode).ClusterConfig;

		StandaloneNode node = new StandaloneNode()
		{
			Children = new ReplicationElement[0],
			AdministrationAdress = new Endpoint() { Address = Host, Port = (ushort)AdministrationPort },
			ExecutionAdress = new Endpoint() { Address = Host, Port = (ushort)ExecutionPort },
			ReplicationAddress = new Endpoint() { Address = Host, Port = (ushort)ReplicationPort },
			IsMember = false,
			Name = Name ?? Host
		};

		if (SourceName != null)
		{
			if (!clusterConfig.TryGetElementByName(SourceName, out ReplicationElement source))
			{
				ConsoleHelper.ShowError("Source element could not be found.");
				return false;
			}

			if (source.Type == ElementType.LocalWriteNode)
			{
				source = (source as LocalWriteNode).Parent;
				Checker.AssertTrue(source.Type == ElementType.LocalWrite);
			}

			node.Parent = source;
			if (source.Type == ElementType.Node)
			{
				(source as StandaloneNode).Children = (source as StandaloneNode).Children.Concat(node).ToArray();
			}
			else if (source.Type == ElementType.LocalWrite)
			{
				(source as LocalWriteCluster).Children = (source as LocalWriteCluster).Children.Concat(node).ToArray();
			}
			else
			{
				(source as GlobalWriteCluster).Children = (source as GlobalWriteCluster).Children.Concat(node).ToArray();
			}
		}
		else
		{
			if (clusterConfig.Cluster != null && clusterConfig.Cluster.Type == ElementType.GlobalWrite)
			{
				ConsoleHelper.ShowError("Unable to create write node. Both allowed write clusters are already present.");
				return false;
			}

			if (clusterConfig.Cluster == null)
			{
				clusterConfig.Cluster = node;
			}
			else
			{
				GlobalWriteCluster gwCluster = new GlobalWriteCluster()
				{
					Name = SystemName,
					IsMember = false,
					Parent = null,
					First = clusterConfig.Cluster,
					Second = node,
					SynchronousReplication = IsSyncReplication,
					Children = new ReplicationElement[0],
				};

				gwCluster.First.Parent = gwCluster;
				gwCluster.Second.Parent = gwCluster;
				clusterConfig.Cluster = gwCluster;
			}
		}

		((ClusterConfigMode)program.Mode).ClusterModified();

		return true;
	}

	private bool ValidateParams()
	{
		if (ReplicationPort <= 0 || ReplicationPort > ushort.MaxValue)
		{
			ConsoleHelper.ShowError("Invalid value for --rep-port parameter.");
			return false;
		}

		if (ExecutionPort <= 0 || ExecutionPort > ushort.MaxValue)
		{
			ConsoleHelper.ShowError("Invalid value for --exec-port parameter.");
			return false;
		}

		if (AdministrationPort <= 0 || AdministrationPort > ushort.MaxValue)
		{
			ConsoleHelper.ShowError("Invalid value for --admin-port parameter.");
			return false;
		}

		return true;
	}
}
