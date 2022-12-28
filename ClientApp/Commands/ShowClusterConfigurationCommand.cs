using System;
using System.Text;
using VeloxDB.ClientApp.Modes;
using VeloxDB.Config;

namespace VeloxDB.ClientApp.Commands;

[Command("show", "Shows the currently edited cluster configuration or details of a specific node configuration.", ProgramMode = ProgramMode.Both)]
internal sealed class ShowClusterConfigurationCommand : BindableCommand
{
	[Param("node", "Name of the node whose configuration should be shown. If ommited, entire cluster configuration is shown.")]
	public string NodeName { get; set; }

	public override bool IsModeValid(Mode mode)
	{
		return mode is ClusterConfigMode || mode is InitialMode;
	}

	protected override bool OnExecute(Program program)
	{
		ClusterConfiguration clusterConfig;
		if (program.Mode is InitialMode)
		{
			if (!CheckClusterBinding(program))
				return false;

			clusterConfig = ((InitialMode)program.Mode).ClusterConfig;
		}
		else
		{
			clusterConfig = ((ClusterConfigMode)program.Mode).ClusterConfig;
		}

		if (clusterConfig.Cluster == null)
		{
			Console.WriteLine("Cluster is empty.");
			return false;
		}

		if (NodeName != null)
		{
			clusterConfig.TryGetElementByName(NodeName, out ReplicationElement replicationElement);
			ReplicationNode node = (ReplicationNode)replicationElement;
			if (node == null)
			{
				Console.WriteLine("Node with a given name could not be found.");
				return false;
			}

			ShowNodeDetails(node);
			return true;
		}

		TreeItem root = GenerateTree(clusterConfig.Cluster);
		Tree tree = new Tree(new Table.ColumnDesc[]
		{
			new Table.ColumnDesc() { WidthPriority = 10 },
			new Table.ColumnDesc() { WidthPriority = 5 },
		}, root);

		tree.Show();
		return true;
	}

	private TreeItem GenerateTree(ReplicationElement element)
	{
		TreeItem item;
		if (element.Type == ElementType.GlobalWrite)
		{
			GlobalWriteCluster gwCluster = (GlobalWriteCluster)element;
			item = new TreeItem(new RichTextItem[]
			{
				new RichTextItem(new TextItem() { Text = element.Name, Color = Colors.TreeClusterNameColor}),
				CreateGWContent(gwCluster)
			});

			item.AddChild(GenerateTree(gwCluster.First));
			item.AddChild(GenerateTree(gwCluster.Second));
			if (gwCluster.Children.Length > 0)
				item.AddChild(new TreeItem(new RichTextItem[] { new RichTextItem(""), new RichTextItem("") }));

			for (int i = 0; i < gwCluster.Children.Length; i++)
			{
				item.AddChild(GenerateTree(gwCluster.Children[i]));
			}
		}
		else if (element.Type == ElementType.LocalWrite)
		{
			LocalWriteCluster lwCluster = (LocalWriteCluster)element;
			item = new TreeItem(new RichTextItem[]
			{
				new RichTextItem(new TextItem() { Text = element.Name + " (HA)", Color = Colors.TreeClusterNameColor }),
				CreateLWContent(lwCluster)
			});

			item.AddChild(GenerateTree(lwCluster.Witness));
			item.AddChild(GenerateTree(lwCluster.First));
			item.AddChild(GenerateTree(lwCluster.Second));
			if (lwCluster.Children.Length > 0)
				item.AddChild(new TreeItem(new RichTextItem[] { new RichTextItem(""), new RichTextItem("") }));

			for (int i = 0; i < lwCluster.Children.Length; i++)
			{
				item.AddChild(GenerateTree(lwCluster.Children[i]));
			}
		}
		else
		{
			item = new TreeItem(new RichTextItem[]
			{
				new RichTextItem(new TextItem() { Text = element.Name + GetNodeMarker(element), Color = Colors.TreeNodeNameColor }),
				new RichTextItem(new TextItem[0])
			});

			StandaloneNode snode = element as StandaloneNode;
			if (snode != null && snode.Children != null)
			{
				for (int i = 0; i < snode.Children.Length; i++)
				{
					item.AddChild(GenerateTree(snode.Children[i]));
				}
			}
		}

		return item;
	}

	private string GetNodeMarker(ReplicationElement element)
	{
		if (IsWriteNode(element))
			return " (Write)";
		else
			return " (Read)";
	}

	private bool IsWriteNode(ReplicationElement element)
	{
		if (element.Type == ElementType.LocalWriteNode)
			element = element.Parent;

		return element.Parent == null || (element.Parent.Type == ElementType.GlobalWrite && element.IsMember);
	}

	private TreeItem GenerateTree(Witness witness)
	{
		SharedFolderWitness sw = (SharedFolderWitness)witness;
		return new TreeItem(new RichTextItem[]
		{
			new RichTextItem(new TextItem() { Text = "Shared Folder Witness", Color = Colors.TreeWitnessNameColor}),
				CreateWitnessContent(witness)
		});
	}

	private static void ShowNodeDetails(ReplicationNode node)
	{
		Console.WriteLine("Node: {0}", node.Name);

		if (node.AdministrationAdress != null)
			Console.WriteLine("Administration endpoint: {0}", node.AdministrationAdress);

		if (node.ExecutionAdress != null)
			Console.WriteLine("Execution endpoint: {0}", node.ExecutionAdress);

		if (node.ReplicationAddress != null)
			Console.WriteLine("Replication endpoint: {0}", node.ReplicationAddress);

		LocalWriteNode lwNode = node as LocalWriteNode;
		if (lwNode != null && lwNode.ElectorAddress != null)
			Console.WriteLine("Election endpoint: {0}", lwNode.ElectorAddress);
	}

	private static RichTextItem CreateGWContent(GlobalWriteCluster gw)
	{
		return new RichTextItem(new TextItem[]
		{
			(gw.SynchronousReplication ? "Sync" : "Async")
		});
	}

	private static RichTextItem CreateLWContent(LocalWriteCluster lw)
	{
		float timeout = lw.ElectionTimeout.HasValue ? lw.ElectionTimeout.Value : ClusterConfiguration.DefaultElectionTimeout;
		return new RichTextItem(new TextItem[]
		{
			new TextItem() { Text = "Election timeout:", Color = Colors.ParamNameColor },
			" " + timeout.ToString("0.00") + " s"
		});
	}

	private static RichTextItem CreateWitnessContent(Witness w)
	{
		SharedFolderWitness sw = (SharedFolderWitness)w;
		return new RichTextItem(new TextItem[]
		{
			new TextItem() { Text = "Path:", Color = Colors.ParamNameColor },
			" " + sw.Path + ", File timeout: " + sw.RemoteFileTimeout + " s."
		});
	}
}
