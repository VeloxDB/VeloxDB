using System;
using VeloxDB.ClientApp.Modes;
using VeloxDB.Config;

namespace VeloxDB.ClientApp.Commands;

[Command("delete", "Deletes an HA cluster or a node.")]
internal sealed class DeleteReplicationElementCommand : Command
{
	[Param("name", "Name of the node or HA cluster to be deleted.", ShortName = "n", IsMandatory = true)]
	public string Name { get; set; }

	public override bool IsModeValid(Mode mode)
	{
		return mode is ClusterConfigMode;
	}

	protected override bool OnExecute(Program program)
	{
		ClusterConfiguration clusterConfig = ((ClusterConfigMode)program.Mode).ClusterConfig;
		if (!clusterConfig.TryGetElementByName(Name, out var elem))
		{
			Console.WriteLine("Node or HA cluster with the given name could not be found.");
			return false;
		}

		if (elem.Type == ElementType.GlobalWrite)
		{
			Console.WriteLine("Entire database cluster cannot be deleted directly. Delete all the nodes and HA clusters instead.");
			return false;
		}

		if (elem.Type == ElementType.Node || elem.Type == ElementType.LocalWrite)
		{
			ReplicationElement[] children = GetChildren(elem);
			if (children.Length > 0)
			{
				Console.WriteLine("Given node or HA cluster is the replication source for one or more read nodes or HA clusters.");
				return false;
			}

			if (elem.Parent == null)
			{
				clusterConfig.Cluster = null;
			}
			else if (elem.Parent.Type == ElementType.GlobalWrite)
			{
				GlobalWriteCluster gwCluster = (GlobalWriteCluster)elem;
				if (gwCluster.Children.Length > 0)
				{
					Console.WriteLine("Given operation whould result in deletion of Global cluster, but there are read.");
					return false;
				}

				ReplicationElement other = object.ReferenceEquals(gwCluster.First, elem) ? gwCluster.Second : gwCluster.First;
				other.Parent = null;
				clusterConfig.Cluster = other;
			}
			else
			{
				children = GetChildren(elem.Parent);
				children = children.Where(x => !object.ReferenceEquals(elem, x)).ToArray();
				SetChildren(elem.Parent, children);
			}
		}
		else if (elem.Type == ElementType.LocalWriteNode)
		{
			LocalWriteNode node = (LocalWriteNode)elem;
			LocalWriteCluster lwCluster = (LocalWriteCluster)node.Parent;
			LocalWriteNode otherNode = object.ReferenceEquals(lwCluster.First, node) ? lwCluster.Second : lwCluster.First;
			StandaloneNode newNode = new StandaloneNode()
			{
				AdministrationAdress = otherNode.AdministrationAdress,
				ExecutionAdress = otherNode.ExecutionAdress,
				ReplicationAddress = otherNode.ReplicationAddress,
				Children = lwCluster.Children,
				IsMember = false,
				Name = otherNode.Name,
				Parent = lwCluster.Parent,
			};

			if (lwCluster.Parent == null)
			{
				clusterConfig.Cluster = newNode;
			}
			else if (lwCluster.IsMember)
			{
				GlobalWriteCluster gwCluster = (GlobalWriteCluster)lwCluster.Parent;
				if (object.ReferenceEquals(gwCluster.First, lwCluster))
					gwCluster.First = newNode;
				else
					gwCluster.Second = newNode;
			}
			else
			{
				ReplicationElement[] children = GetChildren(lwCluster.Parent);
				children = children.Select(x => object.ReferenceEquals(lwCluster, x) ? newNode : x).ToArray();
				SetChildren(lwCluster.Parent, children);
			}
		}

		((ClusterConfigMode)program.Mode).ClusterModified();
		return true;
	}

	private ReplicationElement[] GetChildren(ReplicationElement elem)
	{
		if (elem.Type == ElementType.Node)
			return (elem as StandaloneNode).Children ?? new ReplicationElement[0];
		else
			return (elem as LocalWriteCluster).Children ?? new ReplicationElement[0];
	}

	private void SetChildren(ReplicationElement elem, ReplicationElement[] children)
	{
		if (elem.Type == ElementType.Node)
			(elem as StandaloneNode).Children = children;
		else
			(elem as LocalWriteCluster).Children = children;
	}
}
