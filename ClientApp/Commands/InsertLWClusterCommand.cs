﻿using System;
using VeloxDB.ClientApp.Modes;
using VeloxDB.Common;
using VeloxDB.Config;

namespace VeloxDB.ClientApp.Commands;

[Command("create-ha", "Creates a new HA cluster (either a write or a read cluster), together with two additional " +
	"nodes and a witness. Optionally, an existing node can be used as one of the nodes, in which case a single " +
	"additional node and a witness are created.", Usage = "ha-create [parameters]. For the first node, either --host1 parameter or " +
	"--node-name1 parameter must be provided.")]
internal sealed class InsertLWClusterCommand : Command
{
	[Param("name", "Cluster name.", ShortName = "n", IsMandatory = true)]
	public string Name { get; set; }

	[Param("host1", "Host name or the address of the first node.", ShortName = "h1")]
	public string Host1 { get; set; }

	[Param("node-name1", "Name of the first node. If the node exists, that node is joined to the HA cluster, otherwise a new node is created." +
		"If ommited, host name 1 is used instead.")]
	public string NodeName1 { get; set; }

	[Param("host2", "Host name or the address of the second node.", ShortName = "h2", IsMandatory = true)]
	public string Host2 { get; set; }

	[Param("node-name2", "Name of the second node. If ommited, host name 2 is used instead.")]
	public string NodeName2 { get; set; }

	[Param("elector-port", "Election endpoint port. Default is " + ClusterConfiguration.DefaultElectorPortString + ".")]
	public int ElectorPort { get; set; } = ClusterConfiguration.DefaultElectorPort;

	[Param("rep-port", "Replication endpoint port. Default is " + ClusterConfiguration.DefaultReplicationPortString + ".")]
	public int ReplicationPort { get; set; } = ClusterConfiguration.DefaultReplicationPort;

	[Param("exec-port", "Execution endpoint port. Default is " + ClusterConfiguration.DefaultExecutionPortString + ".")]
	public int ExecutionPort { get; set; } = ClusterConfiguration.DefaultExecutionPort;

	[Param("admin-port", "Administration endpoint port. Default is " + ClusterConfiguration.DefaultAdministrationPortString + ".")]
	public int AdministrationPort { get; set; } = ClusterConfiguration.DefaultAdministrationPort;

	[Param("elector-port2", "If provided, represents election endpoint port of the second node. Otherwise, the same port is ised for both nodes.")]
	public int ElectorPort2 { get; set; } = -1;

	[Param("rep-port2", "If provided, represents replication endpoint port of the second node. Otherwise, the same port is ised for both nodes.")]
	public int ReplicationPort2 { get; set; } = ClusterConfiguration.DefaultReplicationPort;

	[Param("exec-port2", "If provided, represents execution endpoint port of the second node. Otherwise, the same port is ised for both nodes.")]
	public int ExecutionPort2 { get; set; } = -1;

	[Param("admin-port2", "If provided, represents administration endpoint port of the second node. Otherwise, the same port is ised for both nodes.")]
	public int AdministrationPort2 { get; set; } = -1;

	[Param("witness", "Path to a shared network location where witness files will be stored.", ShortName = "w", IsMandatory = true)]
	public string WitnessFolderPath { get; set; }

	[Param("source", "Name of the existing node/HA cluster that will be the source of replication data for this HA cluster. Specifying this parameter " +
		"implies creation of Read HA cluster.", ShortName = "s")]
	public string SourceName { get; set; }

	[Param("election-timeout", "Internal RAFT timeout (in seconds) used to detect when no primary node is available and failover needs to occur. " +
		"Default is " + ClusterConfiguration.DefaultElectionTimeoutString + " sec.")]
	public double ElectionTimeout { get; set; } = ClusterConfiguration.DefaultElectionTimeout;

	[Param("witness-timeout", "Timeout (in seconds) used for accessing the shared network location. " +
		"Default is " + ClusterConfiguration.DefaultRemoteFileTimeoutString + " sec.")]
	public double WitnessFileTimeout { get; set; } = ClusterConfiguration.DefaultRemoteFileTimeout;

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
		ClusterConfiguration clusterConfig = ((ClusterConfigMode)program.Mode).ClusterConfig;
		if (!Validate(clusterConfig, out StandaloneNode existingNode))
			return false;

		LocalWriteNode node1;
		if (existingNode != null)
		{
			node1 = new LocalWriteNode()
			{
				AdministrationAdress = existingNode.AdministrationAdress,
				ElectorAddress = new Endpoint() { Address = existingNode.AdministrationAdress.Address, Port = (ushort)ElectorPort },
				ExecutionAdress = existingNode.ExecutionAdress,
				ReplicationAddress = existingNode.ReplicationAddress,
				IsMember = true,
				Name = NodeName1
			};
		}
		else
		{
			node1 = new LocalWriteNode()
			{
				AdministrationAdress = new Endpoint() { Address = Host1, Port = (ushort)AdministrationPort },
				ElectorAddress = new Endpoint() { Address = Host1, Port = (ushort)ElectorPort },
				ExecutionAdress = new Endpoint() { Address = Host1, Port = (ushort)ExecutionPort },
				ReplicationAddress = new Endpoint() { Address = Host1, Port = (ushort)ReplicationPort },
				IsMember = true,
				Name = NodeName1 ?? Host1
			};
		}

		LocalWriteNode node2 = new LocalWriteNode()
		{
			AdministrationAdress = new Endpoint() { Address = Host2, Port = (ushort)AdministrationPort2 },
			ElectorAddress = new Endpoint() { Address = Host2, Port = (ushort)ElectorPort2 },
			ExecutionAdress = new Endpoint() { Address = Host2, Port = (ushort)ExecutionPort2 },
			ReplicationAddress = new Endpoint() { Address = Host2, Port = (ushort)ReplicationPort2 },
			IsMember = true,
			Name = NodeName2 ?? Host2
		};

		LocalWriteCluster lwCluster = new LocalWriteCluster()
		{
			Children = new ReplicationElement[0],
			ElectionTimeout = (float)ElectionTimeout,
			First = node1,
			Second = node2,
			Witness = new SharedFolderWitness() { Path = WitnessFolderPath, RemoteFileTimeout = (float)WitnessFileTimeout },
			Name = Name,
		};

		node1.Parent = lwCluster;
		node2.Parent = lwCluster;

		if (SourceName != null)
		{
			clusterConfig.TryGetElementByName(SourceName, out ReplicationElement source);
			if (source.Type == ElementType.LocalWriteNode)
			{
				source = (source as LocalWriteNode).Parent;
				Checker.AssertTrue(source.Type == ElementType.LocalWrite);
			}

			lwCluster.Parent = source;
			if (source.Type == ElementType.Node)
			{
				(source as StandaloneNode).Children = (source as StandaloneNode).Children.Concat(lwCluster).ToArray();
			}
			else if (source.Type == ElementType.LocalWrite)
			{
				(source as LocalWriteCluster).Children = (source as LocalWriteCluster).Children.Concat(lwCluster).ToArray();
			}
			else
			{
				(source as GlobalWriteCluster).Children = (source as GlobalWriteCluster).Children.Concat(lwCluster).ToArray();
			}
		}
		else
		{
			if (clusterConfig.Cluster == null)
			{
				clusterConfig.Cluster = lwCluster;
			}
			else
			{
				if (existingNode != null)
				{
					lwCluster.Children = existingNode.Children;
					if (existingNode.Children != null)
					{
						for (int i = 0; i < existingNode.Children.Length; i++)
						{
							existingNode.Children[i].Parent = lwCluster;
						}
					}

					if (object.ReferenceEquals(existingNode, clusterConfig.Cluster))
					{
						clusterConfig.Cluster = lwCluster;
					}
					else if (existingNode.Parent.Type == ElementType.GlobalWrite)
					{
						GlobalWriteCluster gwCluster = (GlobalWriteCluster)existingNode.Parent;
						int index = Array.IndexOf(gwCluster.Children, existingNode);
						if (index != -1)
						{
							gwCluster.Children[index] = lwCluster;
						}
						else
						{
							if (object.ReferenceEquals(gwCluster.First, existingNode))
								gwCluster.First = lwCluster;
							else
								gwCluster.Second = lwCluster;
						}

						lwCluster.Parent = gwCluster;
					}
					else if (existingNode.Parent.Type == ElementType.LocalWrite)
					{
						LocalWriteCluster lwParentCluster = (LocalWriteCluster)existingNode.Parent;
						int index = Array.IndexOf(lwParentCluster.Children, existingNode);
						lwParentCluster.Children[index] = lwCluster;
						lwCluster.Parent = lwParentCluster;
					}
					else
					{
						StandaloneNode parentNode = (StandaloneNode)existingNode.Parent;
						int index = Array.IndexOf(parentNode.Children, existingNode);
						parentNode.Children[index] = lwCluster;
						lwCluster.Parent = parentNode;
					}
				}
				else
				{
					GlobalWriteCluster gwCluster = new GlobalWriteCluster()
					{
						Name = SystemName,
						IsMember = false,
						Parent = null,
						First = clusterConfig.Cluster,
						Second = lwCluster,
						SynchronousReplication = IsSyncReplication,
						Children = new ReplicationElement[0],
					};

					gwCluster.First.Parent = gwCluster;
					gwCluster.Second.Parent = gwCluster;
					clusterConfig.Cluster = gwCluster;
				}
			}
		}

		((ClusterConfigMode)program.Mode).ClusterModified();
		return true;
	}

	private bool Validate(ClusterConfiguration clusterConfig, out StandaloneNode existingNode)
	{
		existingNode = null;
		if (!ValidateParams())
			return false;

		if (NodeName1 != null && clusterConfig.TryGetElementByName(NodeName1, out ReplicationElement elem))
		{
			if (elem.Type == ElementType.LocalWriteNode)
			{
				ConsoleHelper.ShowError("Existing node is already a member of an HA cluster.");
				return false;
			}
			else if (elem.Type != ElementType.Node)
			{
				ConsoleHelper.ShowError("Cluster element with a given name already exists.");
				return false;
			}

			existingNode = (StandaloneNode)elem;
		}

		if (existingNode == null && Host1 == null)
		{
			ConsoleHelper.ShowError("host1 parameter is required when a new node is being created HA cluster.");
			return false;
		}

		if (SourceName != null)
		{
			if (existingNode != null)
			{
				ConsoleHelper.ShowError("Providing source parameter is invalid when joining existing node to an HA cluster.");
				return false;
			}

			if (!clusterConfig.TryGetElementByName(SourceName, out ReplicationElement source))
			{
				ConsoleHelper.ShowError("Source element could not be found.");
				return false;
			}
		}
		else
		{
			if (clusterConfig.Cluster != null && clusterConfig.Cluster.Type == ElementType.GlobalWrite)
			{
				GlobalWriteCluster gwCluster = (GlobalWriteCluster)clusterConfig.Cluster;
				if (existingNode == null)
				{
					ConsoleHelper.ShowError("Unable to create a new site. Both allowed sites are already present.");
					return false;
				}
			}
		}

		return true;
	}

	private bool ValidateParams()
	{
		if (Host1 == null && NodeName1 == null)
		{
			ConsoleHelper.ShowError("Either host1 or node-name1 must be provided.");
			return false;
		}

		if (ElectorPort <= 0 || ElectorPort > ushort.MaxValue)
		{
			ConsoleHelper.ShowError("Invalid value for --elector-port parameter.");
			return false;
		}

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

		if (ElectionTimeout <= 0)
		{
			ConsoleHelper.ShowError("Invalid value for --election-timeout parameter.");
			return false;
		}

		if (WitnessFileTimeout <= 0)
		{
			ConsoleHelper.ShowError("Invalid value for --witness-timeout parameter.");
			return false;
		}

		if (ElectorPort2 == -1)
			ElectorPort2 = ElectorPort;

		if (ReplicationPort2 == -1)
			ReplicationPort2 = ReplicationPort;

		if (ExecutionPort2 == -1)
			ExecutionPort2 = ExecutionPort;

		if (AdministrationPort2 == -1)
			AdministrationPort2 = AdministrationPort;

		if (ElectorPort2 <= 0 || ElectorPort2 > ushort.MaxValue)
		{
			ConsoleHelper.ShowError("Invalid value for --elector-port2 parameter.");
			return false;
		}

		if (ReplicationPort2 <= 0 || ReplicationPort2 > ushort.MaxValue)
		{
			ConsoleHelper.ShowError("Invalid value for --rep-port2 parameter.");
			return false;
		}

		if (ExecutionPort2 <= 0 || ExecutionPort2 > ushort.MaxValue)
		{
			ConsoleHelper.ShowError("Invalid value for --exec-port2 parameter.");
			return false;
		}

		if (AdministrationPort2 <= 0 || AdministrationPort2 > ushort.MaxValue)
		{
			ConsoleHelper.ShowError("Invalid value for --admin-port2 parameter.");
			return false;
		}

		return true;
	}
}