using System;
using System.Diagnostics;
using VeloxDB.Client;
using VeloxDB.ClientApp.Modes;
using VeloxDB.Common;
using VeloxDB.Config;
using VeloxDB.Networking;
using VeloxDB.Protocol;
using VeloxDB.Server;

namespace VeloxDB.ClientApp.Commands;

[Command("monitor", "Continuously monitors the state of the cluster until Q is pressed.")]
internal sealed class MonitorClusterStatusCommand : Command
{
	[Param("interval", "Refresh interval in milliseconds. Default is 5000.", ShortName = "i")]
	public int Interval { get; set; } = 5000;

	public override bool IsModeValid(Mode mode)
	{
		return mode.GetType() == typeof(InitialMode);
	}

#if TEST_BUILD
	public static bool WaitAllNodes = false;
#endif

	protected override bool OnExecute(Program program)
	{
		if (!CheckClusterBinding(program))
			return false;

		InitialMode mode = (InitialMode)program.Mode;

		if (Interval < 0)
		{
			ConsoleHelper.ShowError("Invalid interval value.");
			return false;
		}

		List<ReplicationNode> nodes = CollectNodes(mode.ClusterConfig);
		Dictionary<string, NodeState> nodeStates = new Dictionary<string, NodeState>(nodes.Count);
		Dictionary<string, NodeState> displayedNodeStates = null;

		CancellationTokenSource cts = new CancellationTokenSource();
		for (int i = 0; i < nodes.Count; i++)
		{
			RefreshNodeState(program.CreateConnectionStringParams(), nodes[i], nodeStates, cts.Token);
		}

		int top = ReadLine.IsRedirectedOrAlternate ? 0 : Console.CursorTop;
		ScreenBuffer screenBuffer = new ScreenBuffer();
		int clearHeight = 0;

		int width = ConsoleHelper.WindowWidth;
		int height = ConsoleHelper.WindowHeight;

#if TEST_BUILD
		if (WaitAllNodes)
			SpinWait.SpinUntil(() => { lock (nodeStates) return nodeStates.Count == nodes.Count; });
#endif

		Stopwatch s = Stopwatch.StartNew();
		while (true)
		{
			Thread.Sleep(100);
			if (!ReadLine.IsRedirectedOrAlternate && Console.KeyAvailable)
			{
				if (Console.ReadKey(true).Key == ConsoleKey.Q)
				{
					cts.Cancel();
					return true;
				}
			}

			bool b = ConsoleHelper.WindowWidth != width || ConsoleHelper.WindowHeight != height;

			if (s.Elapsed.TotalMilliseconds > Interval || b || displayedNodeStates == null)
			{
				s.Restart();

				Dictionary<string, NodeState> tempNodeStates;
				lock (nodeStates)
				{
					tempNodeStates = new Dictionary<string, NodeState>(nodeStates);
				}

				if (tempNodeStates.Count == nodes.Count)
				{
					if (b || displayedNodeStates == null || StatesDiffer(displayedNodeStates, tempNodeStates))
					{
						if (b)
						{
							Console.Clear();
							Console.CursorTop = 0;
							Console.CursorLeft = 0;
							clearHeight = 0;
							top = 0;
						}

						width = ConsoleHelper.WindowWidth;
						height = ConsoleHelper.WindowHeight;
						displayedNodeStates = tempNodeStates;
						ClusterStatusLive clusterStatus = new ClusterStatusLive(mode.ClusterConfig, displayedNodeStates);
						clusterStatus.Show(screenBuffer, top, ref clearHeight);
					}
				}
			}

			if (ReadLine.IsRedirectedOrAlternate)
				return true;
		}
	}

	private bool StatesDiffer(Dictionary<string, NodeState> s1, Dictionary<string, NodeState> s2)
	{
		if (s1.Count != s2.Count)
			return true;

		foreach (var kv1 in s1)
		{
			if (!s2.TryGetValue(kv1.Key, out NodeState state2))
				return true;

			if (!kv1.Value.Equals(state2))
				return true;
		}

		return false;
	}

	public static List<ReplicationNode> CollectNodes(ClusterConfiguration clusterConfig)
	{
		List<ReplicationNode> nodes = new List<ReplicationNode>();
		CollectNodes(nodes, clusterConfig.Cluster);
		return nodes;
	}

	private async void RefreshNodeState(ConnectionStringParams cp, ReplicationNode node, Dictionary<string, NodeState> nodeStates, CancellationToken cancelToken)
	{
		cp = cp.Clone();
		cp.AddAddress(node.AdministrationAddress.ToString());
		cp.ServiceName = AdminAPIServiceNames.NodeAdministration;

		INodeAdministration nodeAdministration = ConnectionFactory.Get<INodeAdministration>(cp.GenerateConnectionString());

		while (true)
		{
			if (cancelToken.IsCancellationRequested)
				return;

			try
			{
				NodeState nodeState = await nodeAdministration.GetState();
				lock (nodeStates)
				{
					nodeStates[node.Name] = nodeState;
				}

				await Task.Delay(Interval);
			}
			catch (Exception e)
			{
				if (e is CommunicationException || e is ObjectDisposedException || e is ArgumentException ||
					e is TimeoutException || e is DbAPIErrorException)
				{
					nodeStates.Remove(node.Name);
				}
				else
				{
					throw;
				}
			}
		}
	}

	public static void CollectNodes(List<ReplicationNode> nodes, ReplicationElement element)
	{
		if (element.Type == ElementType.GlobalWrite)
		{
			GlobalWriteCluster cb = (GlobalWriteCluster)element;
			CollectNodes(nodes, cb.First);
			CollectNodes(nodes, cb.Second);
			foreach (ReplicationElement child in cb.Children)
			{
				CollectNodes(nodes, child);
			}
		}
		else if (element.Type == ElementType.LocalWrite)
		{
			LocalWriteCluster cb = (LocalWriteCluster)element;
			CollectNodes(nodes, cb.First);
			CollectNodes(nodes, cb.Second);
			foreach (ReplicationElement child in cb.Children)
			{
				CollectNodes(nodes, child);
			}
		}
		else if (element.Type == ElementType.LocalWriteNode)
		{
			nodes.Add((ReplicationNode)element);
		}
		else
		{
			nodes.Add((ReplicationNode)element);
			if ((element as StandaloneNode).Children != null)
			{
				foreach (ReplicationElement child in (element as StandaloneNode).Children)
				{
					CollectNodes(nodes, child);
				}
			}
		}
	}
}
