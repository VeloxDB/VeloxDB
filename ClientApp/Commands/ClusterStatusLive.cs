using System;
using System.Diagnostics.Metrics;
using VeloxDB.Common;
using VeloxDB.Config;
using VeloxDB.Server;

namespace VeloxDB.ClientApp.Commands;

internal sealed class ClusterStatusLive
{
	List<BoxElement> elements;

	public ClusterStatusLive(ClusterConfiguration clusterConfig, Dictionary<string, NodeState> nodeStates)
	{
		elements = new List<BoxElement>(4);
		ArrayQueue<ReplicationElement> todo = new ArrayQueue<ReplicationElement>(8);
		todo.Enqueue(clusterConfig.Cluster);
		CreateElements(todo, nodeStates);
	}

	public void Show(ScreenBuffer buffer, int top, ref int clearHeight)
	{
		int width = ConsoleHelper.WindowWidth;

		int currHeight = 0;
		int currTop = 0;
		int currLeft = 0;
		for (int i = 0; i < elements.Count; i++)
		{
			BoxElement element = elements[i];
			element.Measure();
			if (currLeft == 0 || currLeft + element.Width <= width)
			{
				element.ArrangedLeft = currLeft;
				element.ArrangedTop = currTop;
				currLeft += element.Width;
				currHeight = Math.Max(currHeight, element.Height);
			}
			else
			{
				currTop += currHeight;
				currHeight = element.Height;
				element.ArrangedLeft = 0;
				element.ArrangedTop = currTop;
				currLeft = element.Width;
			}
		}

		currTop += currHeight;
		buffer.Clear(clearHeight);
		int showHeight = Math.Max(clearHeight, currTop);
		clearHeight = currTop;

		for (int i = 0; i < elements.Count; i++)
		{
			BoxElement element = elements[i];
			element.Show(buffer, element.ArrangedLeft, element.ArrangedTop);
		}

		buffer.Show(top, clearHeight);
	}

	private void CreateElements(ArrayQueue<ReplicationElement> todo, Dictionary<string, NodeState> nodeStates)
	{
		while (todo.Count > 0)
		{
			ReplicationElement element = todo.Dequeue();
			if (element == null)
				continue;

			if (element.Type == ElementType.GlobalWrite)
			{
				todo.Enqueue((element as GlobalWriteCluster).First);
				todo.Enqueue((element as GlobalWriteCluster).Second);
				(element as GlobalWriteCluster).Children.ForEach(x => todo.Enqueue(x));
			}
			else if (element.Type == ElementType.LocalWrite)
			{
				elements.Add(BoxElement.FromLocalWriteCluster((element as LocalWriteCluster), nodeStates));
				(element as LocalWriteCluster).Children.ForEach(x => todo.Enqueue(x));
			}
			else if (element.Type == ElementType.Node)
			{
				elements.Add(BoxElement.FromNode((element as StandaloneNode), nodeStates));
				(element as StandaloneNode).Children?.ForEach(x => todo.Enqueue(x));
			}
		}
	}

	private sealed class BoxElement
	{
		const int maxNameWidth = 16;

		ConsoleColor? borderColor;
		RichTextItem title;
		List<RichTextItem>[] items;
		int[] columntWidhs;
		int width;
		int height;
		int arrangedLeft;
		int arrangedTop;

		private BoxElement(RichTextItem title, List<RichTextItem>[] items, ConsoleColor? borderColor)
		{
			this.title = title;
			this.items = items;
			this.borderColor = borderColor;
		}

		public int Width => width;
		public int Height => height;

		public int ArrangedLeft { get => arrangedLeft; set => arrangedLeft = value; }
		public int ArrangedTop { get => arrangedTop; set => arrangedTop = value; }

		public static BoxElement FromLocalWriteCluster(LocalWriteCluster cluster, Dictionary<string, NodeState> nodeStates)
		{
			LocalWriteNode[] nodes = new LocalWriteNode[(cluster.First != null ? 1 : 0) + (cluster.Second != null ? 1 : 0)];
			int c = 0;
			if (cluster.First != null)
				nodes[c++] = cluster.First;

			if (cluster.Second != null)
				nodes[c++] = cluster.Second;

			if (c == 0)
				return null;

			TextItem nameItem, connItem;

			LocalWriteNode primaryNode = null;
			List<RichTextItem>[] items = new List<RichTextItem>[nodes.Length];
			for (int i = 0; i < nodes.Length; i++)
			{
				items[i] = new List<RichTextItem>(8);
				nodeStates.TryGetValue(nodes[i].Name, out NodeState nodeState);

				if (nodeState == null)
				{
					items[i].Add(new RichTextItem(new TextItem[] { new TextItem() { Text = Text.LimitTextSize(nodes[i].Name, maxNameWidth),
						Color = Colors.NodeUnavailableColor} }));
				}
				else
				{
					ReplicaState lwState = GetReplicaState(nodeState, ReplicaType.LocalWrite);
					if (lwState == null)
					{
						items[i].Add(new RichTextItem(new TextItem[] { new TextItem() { Text = Text.LimitTextSize(nodes[i].Name, maxNameWidth),
							Color = Colors.StatusBadColor } }));
					}
					else
					{
						nameItem = new TextItem()
						{
							Text = Text.LimitTextSize(nodes[i].Name, maxNameWidth),
							Color = Colors.NodeNameColor,
							BackgroundColor = lwState.IsPrimary ? Colors.PrimaryNodeBkgColor : null
						};

						connItem = new TextItem()
						{
							Text = i == 0 ? "-->" : "<--",
							Color = GetConnectionColor(lwState, out var bkgColor),
							BackgroundColor = bkgColor
						};

						TextItem[] t = i == 0 ? new TextItem[] { nameItem, " ", connItem } : new TextItem[] { connItem, " ", nameItem };
						items[i].Add(new RichTextItem(t));

						if (lwState.IsPrimary)
						{
							primaryNode = nodes[i];

							ReplicaState sourceState = GetReplicaState(nodeState, ReplicaType.Source);
							if (sourceState != null)
							{
								ReplicationElement sourceElement = cluster.Parent;
								if (sourceElement != null)
								{
									items[i].Add(new RichTextItem(new TextItem[] { Text.LimitTextSize(sourceElement.Name, maxNameWidth),
										" ", GetConnectionStateItem(sourceState) }));
								}
							}

							for (int j = 0; j < cluster.Children.Length; j++)
							{
								ReplicaState localChildState = GetReplicaState(nodeState, ReplicaType.LocalRead, j);
								ReplicationElement localChildElement = cluster.Children[j];
								if (localChildElement != null)
								{
									items[i].Add(new RichTextItem(new TextItem[] { Text.LimitTextSize(localChildElement.Name, maxNameWidth),
										" ", GetConnectionStateItem(localChildState) }));
								}
							}

							if (cluster.IsMember && cluster.Parent.Type == ElementType.GlobalWrite)
							{
								ReplicaState gwState = GetReplicaState(nodeState, ReplicaType.GlobalWrite);
								GlobalWriteCluster gwCluster = cluster.Parent as GlobalWriteCluster;
								if (gwState != null && gwState.IsPrimary)
								{
									for (int j = 0; j < gwCluster.Children.Length; j++)
									{
										ReplicaState globalChildState = GetReplicaState(nodeState, ReplicaType.GlobalRead, j);
										ReplicationElement globalChildElement = gwCluster.Children[j];
										if (globalChildElement != null)
										{
											items[i].Add(new RichTextItem(new TextItem[] { Text.LimitTextSize(globalChildElement.Name, maxNameWidth),
												" ", GetConnectionStateItem(globalChildState) }));
										}
									}
								}
							}
						}

						items[i].Add(new RichTextItem(new TextItem[] { "Witness ", GetConnectionStateItem(nodeState.IsWitnessConnected) }));
						items[i].Add(new RichTextItem(new TextItem[] { "Elector ", GetConnectionStateItem(nodeState.IsElectorConnected) }));
					}
				}
			}

			nameItem = new TextItem() { Text = Text.LimitTextSize(cluster.Name, maxNameWidth), Color = Colors.NodeNameColor };
			connItem = null;
			bool isGWPrimary = false;
			if (cluster.IsMember && cluster.Parent.Type == ElementType.GlobalWrite && primaryNode != null)
			{
				nodeStates.TryGetValue(primaryNode.Name, out NodeState nodeState);
				ReplicaState gwState = GetReplicaState(nodeState, ReplicaType.GlobalWrite);
				isGWPrimary = gwState != null ? gwState.IsPrimary : false;
				GlobalWriteCluster gwCluster = cluster.Parent as GlobalWriteCluster;
				connItem = GetGWConnectionStateItem(gwState);
			}

			RichTextItem title = new RichTextItem(connItem == null ? new TextItem[] { nameItem } : new TextItem[] { nameItem, " ", connItem });

			return new BoxElement(title, items, isGWPrimary ? Colors.GlobalWritePrimaryColor : null);
		}

		public static BoxElement FromNode(StandaloneNode node, Dictionary<string, NodeState> nodeStates)
		{
			List<RichTextItem> items = new List<RichTextItem>(8);
			nodeStates.TryGetValue(node.Name, out NodeState nodeState);

			if (nodeState != null)
			{
				if (node.IsMember && node.Parent.Type == ElementType.GlobalWrite)
				{
					ReplicaState gwState = GetReplicaState(nodeState, ReplicaType.GlobalWrite);
					GlobalWriteCluster gwCluster = node.Parent as GlobalWriteCluster;
					if (gwState.IsPrimary)
					{
						for (int j = 0; j < gwCluster.Children.Length; j++)
						{
							ReplicaState globalChildState = GetReplicaState(nodeState, ReplicaType.GlobalRead, j);
							ReplicationElement globalChildElement = gwCluster.Children[j];
							if (globalChildElement != null)
							{
								items.Add(new RichTextItem(new TextItem[] { Text.LimitTextSize(globalChildElement.Name, maxNameWidth),
											" ", GetConnectionStateItem(globalChildState) }));
							}
						}
					}
				}
				else
				{
					ReplicaState sourceState = GetReplicaState(nodeState, ReplicaType.Source);
					if (sourceState != null)
					{
						ReplicationElement sourceElement = node.Parent;
						if (sourceElement != null)
						{
							items.Add(new RichTextItem(new TextItem[] { Text.LimitTextSize(sourceElement.Name, maxNameWidth),
										" ", GetConnectionStateItem(sourceState) }));
						}
					}
				}

				if (node.Children != null)
				{
					for (int j = 0; j < node.Children.Length; j++)
					{
						ReplicaState localChildState = GetReplicaState(nodeState, ReplicaType.LocalRead, j);
						ReplicationElement localChildElement = node.Children[j];
						if (localChildElement != null)
						{
							items.Add(new RichTextItem(new TextItem[] { Text.LimitTextSize(localChildElement.Name, maxNameWidth),
										" ", GetConnectionStateItem(localChildState) }));
						}
					}
				}
			}

			TextItem nameItem = new TextItem() { Text = Text.LimitTextSize(node.Name, maxNameWidth), Color = Colors.NodeNameColor };
			TextItem connItem = null;
			bool isGWPrimary = false;
			if (node.IsMember && node.Parent.Type == ElementType.GlobalWrite)
			{
				ReplicaState gwState = GetReplicaState(nodeState, ReplicaType.GlobalWrite);
				isGWPrimary = gwState != null ? gwState.IsPrimary : false;
				GlobalWriteCluster gwCluster = node.Parent as GlobalWriteCluster;
				connItem = GetGWConnectionStateItem(gwState);
			}

			RichTextItem title = new RichTextItem(connItem == null ? new TextItem[] { nameItem } : new TextItem[] { nameItem, " ", connItem });

			return new BoxElement(title, new List<RichTextItem>[] { items }, isGWPrimary ? Colors.GlobalWritePrimaryColor : null);
		}

		public void Measure()
		{
			columntWidhs = new int[items.Length];
			for (int i = 0; i < columntWidhs.Length; i++)
			{
				columntWidhs[i] = items[i].Select(x => x.Length).DefaultIfEmpty(0).Max();
			}

			this.width = columntWidhs.Sum() + 6;
			if (title != null)
				this.width = Math.Max(this.width, title.Length + 4);

			this.height = items.Select(x => x.Count).DefaultIfEmpty(0).Max() + 2;
		}

		public void Show(ScreenBuffer buffer, int left, int top)
		{
			ShowTitleLine(buffer, left, top);
			ShowBorder(buffer, left, top);
			for (int i = 0; i < items.Length; i++)
			{
				ShowColumn(buffer, left, top, i);
			}
		}

		private void ShowBorder(ScreenBuffer buffer, int left, int top)
		{
			ConsoleColor fcol = (ConsoleColor)ScreenBuffer.NoColor;
			if (borderColor.HasValue)
				fcol = borderColor.Value;

			for (int i = 1; i < height - 1; i++)
			{
				buffer.Write(left, top + i, '║', fcol);
				buffer.Write(left + width - 1, top + i, '║', fcol);
			}

			buffer.Write(left, top + height - 1, '╚', fcol);
			buffer.Write(left + width - 1, top + height - 1, '╝', fcol);

			for (int i = 1; i < width - 1; i++)
			{
				buffer.Write(left + i, top + height - 1, '═', fcol);
			}
		}

		private void ShowTitleLine(ScreenBuffer buffer, int left, int top)
		{
			ConsoleColor fcol = (ConsoleColor)ScreenBuffer.NoColor;
			if (borderColor.HasValue)
				fcol = borderColor.Value;

			buffer.Write(left, top, '╔', fcol);
			buffer.Write(left + width - 1, top, '╗', fcol);

			int t1 = (width - 4 - (title?.Length).GetValueOrDefault()) / 2;
			int t2 = width - 4 - (title?.Length).GetValueOrDefault() - t1;

			for (int i = 0; i < t1; i++)
			{
				buffer.Write(left + 1 + i, top, '═', fcol);
			}

			for (int i = 0; i < t2; i++)
			{
				buffer.Write(left + width - 1 - t2 + i, top, '═', fcol);
			}

			title?.Show(left + t1 + 2, top, buffer);
		}

		private void ShowColumn(ScreenBuffer buffer, int left, int top, int index)
		{
			left += 2;
			top += 1;
			for (int i = 0; i < index; i++)
			{
				left += columntWidhs[i] + 2;
			}

			List<RichTextItem> items = this.items[index];
			for (int i = 0; i < items.Count; i++)
			{
				RichTextItem item = items[i];
				if (index == 0)
				{
					item.Show(left + (columntWidhs[0] - item.Length), top + i, buffer);
				}
				else
				{
					item.Show(left, top + i, buffer);
				}
			}
		}

		private static TextItem GetGWConnectionStateItem(ReplicaState state)
		{
			TextItem item = new TextItem();
			if (state.StateType == ReplicaStateType.Disconnected)
				item.Text = "DISC";
			else
			{
				if (state.StateType == ReplicaStateType.ConnectedSync)
					item.Text = "CONN SYNC";
				else
					item.Text = "CONN ASYNC";
			}

			item.Color = GetConnectionColor(state, out var bkgColor);
			item.BackgroundColor = bkgColor;
			return item;
		}

		private static TextItem GetConnectionStateItem(ReplicaState state)
		{
			TextItem item = new TextItem();
			item.Text = state.StateType == ReplicaStateType.Disconnected ? "DISC" : "CONN";
			item.Color = GetConnectionColor(state, out var bkgColor);
			item.BackgroundColor = bkgColor;
			return item;
		}

		private static TextItem GetConnectionStateItem(bool isConnected)
		{
			TextItem item = new TextItem();
			item.Text = isConnected ? "CONN" : "DISC";
			item.BackgroundColor = isConnected ? Colors.StatusGoodColor : Colors.StatusBadColor;
			return item;
		}

		private static ConsoleColor? GetConnectionColor(ReplicaState state, out ConsoleColor? bkgColor)
		{
			if (state.StateType == ReplicaStateType.Disconnected)
			{
				bkgColor = Colors.StatusBadColor;
				return null;
			}
			else
			{
				bkgColor = Colors.StatusGoodColor;
				if (state.StateType == ReplicaStateType.ConnectedPendingSync || (!state.IsPrimary && !state.IsAligned))
					return Colors.StatusWarnColor;
				else
					return null;
			}
		}
	}

	public static ReplicaState GetReplicaState(NodeState state, ReplicaType replicaType, int childIndex = 0)
	{
		for (int i = 0; i < state.ReplicaStates.Count; i++)
		{
			if (state.ReplicaStates[i].ReplicaType == replicaType)
			{
				if (childIndex == 0)
					return state.ReplicaStates[i];
				else
					childIndex--;
			}
		}

		return null;
	}
}
