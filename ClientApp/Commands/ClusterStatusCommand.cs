using System;
using System.Xml.Linq;
using Velox.Client;
using Velox.ClientApp.Modes;
using Velox.Common;
using Velox.Config;
using Velox.Networking;
using Velox.Protocol;
using Velox.Server;

namespace Velox.ClientApp.Commands;

[Command("status", "Displayes the status of the cluster.", ProgramMode = ProgramMode.Both)]
internal sealed class ClusterStatusCommand : BindableCommand
{
	public override bool IsModeValid(Mode mode)
	{
		return mode is InitialMode;
	}

	protected override bool OnExecute(Program program)
	{
		ClusterConfiguration clusterConfig = ((InitialMode)program.Mode).ClusterConfig;
		if (clusterConfig.Cluster == null)
		{
			Console.WriteLine("Cluster is empty.");
			return true;
		}

		List<ReplicationNode> nodes = MonitorClusterStatusCommand.CollectNodes(clusterConfig);
		Dictionary<string, NodeState> nodeStates = new Dictionary<string, NodeState>(nodes.Count);
		Task[] tasks = new Task[nodes.Count];
		for (int i = 0; i < nodes.Count; i++)
		{
			tasks[i] = DownloadNodeState(nodes[i], nodeStates);
		}

		try
		{
			Task.WaitAll(tasks);
		}
		catch (Exception e)
		{
			ConsoleHelper.ShowError(null, e);
			return false;
		}

		TreeItem root = GenerateTree(clusterConfig.Cluster, nodeStates);
		Tree tree = new Tree(new Table.ColumnDesc[] { new Table.ColumnDesc() }, root);
		tree.Show();
		return true;
	}

	private TreeItem GenerateTree(ReplicationElement element, Dictionary<string, NodeState> nodeStates)
	{
		TreeItem item;
		if (element.Type == ElementType.GlobalWrite)
		{
			GlobalWriteCluster gwCluster = (GlobalWriteCluster)element;
			item = new TreeItem(CreateGWText(gwCluster));

			item.AddChild(GenerateTree(gwCluster.First, nodeStates));
			item.AddChild(GenerateTree(gwCluster.Second, nodeStates));
			if (gwCluster.Children.Length > 0)
				item.AddChild(new TreeItem(new RichTextItem("")));

			for (int i = 0; i < gwCluster.Children.Length; i++)
			{
				item.AddChild(GenerateTree(gwCluster.Children[i], nodeStates));
			}
		}
		else if (element.Type == ElementType.LocalWrite)
		{
			LocalWriteCluster lwCluster = (LocalWriteCluster)element;
			item = new TreeItem(CreateLWText(lwCluster, nodeStates));

			item.AddChild(GenerateTree(lwCluster.First, nodeStates));
			item.AddChild(GenerateTree(lwCluster.Second, nodeStates));
			if (lwCluster.Children.Length > 0)
				item.AddChild(new TreeItem(new RichTextItem("")));

			for (int i = 0; i < lwCluster.Children.Length; i++)
			{
				item.AddChild(GenerateTree(lwCluster.Children[i], nodeStates));
			}
		}
		else
		{
			if (element.Type == ElementType.LocalWriteNode)
			{
				item = new TreeItem(CreateLWNodeText((LocalWriteNode)element, nodeStates));
				CreateNodeConnections(item, (ReplicationNode)element, nodeStates);
			}
			else
			{
				StandaloneNode node = (StandaloneNode)element;
				item = new TreeItem(CreateStandaloneNodeText(node, nodeStates));
				CreateNodeConnections(item, node, nodeStates);
				if (node != null && node.Children != null)
				{
					for (int i = 0; i < node.Children.Length; i++)
					{
						item.AddChild(GenerateTree(node.Children[i], nodeStates));
					}
				}
			}
		}

		return item;
	}

	private void CreateNodeConnections(TreeItem item, ReplicationNode node, Dictionary<string, NodeState> nodeStates)
	{
		if (!nodeStates.TryGetValue(node.Name, out NodeState state))
			return;

		if (node.Type == ElementType.LocalWriteNode)
		{
			item.AddChild(new TreeItem(CreateWitnessConnectionText(state)));
			item.AddChild(new TreeItem(CreateElectorConnectionText(state)));

			LocalWriteNode otherNode = (node.Parent as LocalWriteCluster).GetOther((LocalWriteNode)node);
			string otherName = otherNode == null ? "HA: " : otherNode.Name;
			item.AddChild(new TreeItem(CreateConnectionText(otherName, ClusterStatusLive.GetReplicaState(state, ReplicaType.LocalWrite))));
		}

		ReplicaState gwState = ClusterStatusLive.GetReplicaState(state, ReplicaType.GlobalWrite);
		if (gwState != null && gwState.StateType != ReplicaStateType.NotUsed)
		{
			ReplicationElement elem = node;
			if (elem.Type == ElementType.LocalWriteNode)
				elem = elem.Parent;

			GlobalWriteCluster gwCluster = (elem.Parent as GlobalWriteCluster);
			if (gwCluster != null)
			{
				ReplicationElement other = gwCluster.GetOther(elem);
				string otherName = other == null ? "Site: " : other.Name;
				item.AddChild(new TreeItem(CreateConnectionText(otherName, gwState)));
			}
		}

		ReplicaState sourceState = ClusterStatusLive.GetReplicaState(state, ReplicaType.Source);
		if (sourceState != null && sourceState.StateType != ReplicaStateType.NotUsed)
			item.AddChild(new TreeItem(CreateConnectionText(node.Parent.Name, sourceState)));
	}

	private RichTextItem CreateConnectionText(string targetName, ReplicaState state)
	{
		Checker.AssertFalse(state.StateType == ReplicaStateType.NotUsed);

		List<TextItem> items = new List<TextItem>(4);
		items.Add(new TextItem() { Text = "──", Color = Colors.TreeStructureColor });
		items.Add(targetName + ": ");
		if (state.StateType == ReplicaStateType.Disconnected)
		{
			items.Add(new TextItem() { Text = "Disconnected", BackgroundColor = Colors.StatusBadColor });
			return new RichTextItem(items.ToArray());
		}

		items.Add(new TextItem() { Text = "Connected", BackgroundColor = Colors.StatusGoodColor });

		if (state.StateType == ReplicaStateType.ConnectedSync || state.StateType == ReplicaStateType.ConnectedAsync)
		{
			items.Add("/");
			items.Add(new TextItem() { Text = state.StateType == ReplicaStateType.ConnectedSync ? "Sync" : "Async", BackgroundColor = Colors.StatusGoodColor });
		}
		else if (state.StateType == ReplicaStateType.ConnectedPendingSync)
		{
			items.Add("/");
			items.Add(new TextItem() { Text = "Async", BackgroundColor = Colors.StatusWarnColor });
		}

		if (!state.IsPrimary)
		{
			items.Add("/");
			items.Add(new TextItem()
			{
				Text = state.IsAligned ? "Aligned" : "Aligning...",
				BackgroundColor = state.IsAligned ? Colors.StatusGoodColor : Colors.StatusWarnColor
			});
		}

		return new RichTextItem(items.ToArray());
	}

	private RichTextItem CreateWitnessConnectionText(NodeState state)
	{
		TextItem[] items = new TextItem[3];
		items[0] = new TextItem() { Text = "──", Color = Colors.TreeStructureColor };
		items[1] = "Witness: ";
		items[2] = new TextItem()
		{
			Text = state.IsWitnessConnected ? "Connected" : "Disconnected",
			BackgroundColor = state.IsWitnessConnected ? Colors.StatusGoodColor : Colors.StatusBadColor
		};

		return new RichTextItem(items);
	}

	private RichTextItem CreateElectorConnectionText(NodeState state)
	{
		TextItem[] items = new TextItem[3];
		items[0] = new TextItem() { Text = "──", Color = Colors.TreeStructureColor };
		items[1] = "Elector: ";
		items[2] = new TextItem()
		{
			Text = state.IsElectorConnected ? "Connected" : "Disconnected",
			BackgroundColor = state.IsElectorConnected ? Colors.StatusGoodColor : Colors.StatusBadColor
		};

		return new RichTextItem(items);
	}

	private RichTextItem CreateGWText(GlobalWriteCluster gwCluster)
	{
		return new RichTextItem(new TextItem() { Text = gwCluster.Name + " (" + (gwCluster.SynchronousReplication ? "Sync)" : "Async)") });
	}

	private RichTextItem CreateLWText(LocalWriteCluster lwCluster, Dictionary<string, NodeState> nodeStates)
	{
		return new RichTextItem(new TextItem()
		{ Text = lwCluster.Name, BackgroundColor = IsPrimaryInGW(lwCluster, nodeStates) ? Colors.GlobalWritePrimaryColor : null });
	}

	private RichTextItem CreateLWNodeText(LocalWriteNode lwNode, Dictionary<string, NodeState> nodeStates)
	{
		return new RichTextItem(new TextItem()
		{
			Text = lwNode.Name + GetNodeAvailabilityText(lwNode, nodeStates),
			BackgroundColor = IsPrimaryInLW(lwNode, nodeStates) ? Colors.GlobalWritePrimaryColor : null
		});
	}

	private RichTextItem CreateStandaloneNodeText(StandaloneNode node, Dictionary<string, NodeState> nodeStates)
	{
		return new RichTextItem(node.Name + GetNodeAvailabilityText(node, nodeStates));
	}

	private string GetNodeAvailabilityText(ReplicationNode node, Dictionary<string, NodeState> nodeStates)
	{
		if (!nodeStates.TryGetValue(node.Name, out NodeState state))
			return " (Unavailable)";
		else
			return " (Running)";
	}

	private bool IsPrimaryInGW(ReplicationElement element, Dictionary<string, NodeState> nodeStates)
	{
		if (element.Parent.Type != ElementType.GlobalWrite)
			return false;

		if (element.Type == ElementType.Node)
		{
			return IsPrimaryNodeInGW((StandaloneNode)element, nodeStates);
		}
		else
		{
			LocalWriteCluster lwCluster = (LocalWriteCluster)element;
			return IsPrimaryNodeInGW(lwCluster.First, nodeStates) || IsPrimaryNodeInGW(lwCluster.Second, nodeStates);
		}
	}

	private bool IsPrimaryInLW(LocalWriteNode node, Dictionary<string, NodeState> nodeStates)
	{
		if (node == null)
			return false;

		if (!nodeStates.TryGetValue(node.Name, out NodeState state))
			return false;

		ReplicaState lwState = ClusterStatusLive.GetReplicaState(state, ReplicaType.LocalWrite);
		return lwState != null && lwState.IsPrimary;
	}

	private bool IsPrimaryNodeInGW(ReplicationNode node, Dictionary<string, NodeState> nodeStates)
	{
		if (node == null)
			return false;

		if (!nodeStates.TryGetValue(node.Name, out NodeState state))
			return false;

		ReplicaState gwState = ClusterStatusLive.GetReplicaState(state, ReplicaType.GlobalWrite);
		return gwState != null && gwState.IsPrimary;
	}

	private bool IsWriteNode(ReplicationElement element)
	{
		if (element.Type == ElementType.LocalWriteNode)
			element = element.Parent;

		return element.Parent == null || (element.Parent.Type == ElementType.GlobalWrite && element.IsMember);
	}

	private async Task DownloadNodeState(ReplicationNode node, Dictionary<string, NodeState> nodeStates)
	{
		ConnectionStringParams cp = new ConnectionStringParams();
		cp.AddAddress(node.AdministrationAdress.ToString());
		cp.ServiceName = AdminAPIServiceNames.NodeAdministration;
		cp.RetryTimeout = Program.ConnectionRetryTimeout;
		cp.OpenTimeout = Program.ConnectionOpenTimeout;
		cp.PoolSize = 1;

		INodeAdministration nodeAdministration = ConnectionFactory.Get<INodeAdministration>(cp.GenerateConnectionString());

		try
		{
			NodeState nodeState = await nodeAdministration.GetState();
			lock (nodeStates)
			{
				nodeStates[node.Name] = nodeState;
			}
		}
		catch (Exception e)
		{
			if (e is CommunicationException || e is ObjectDisposedException || e is ArgumentException ||
				e is TimeoutException || e is DbAPIErrorException)
			{
			}
			else
			{
				throw;
			}
		}
	}
}
