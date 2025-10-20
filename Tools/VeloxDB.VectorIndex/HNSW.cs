using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using VeloxDB.Descriptor;
using VeloxDB.ObjectInterface;

namespace VeloxDB.VectorIndex;

/// <summary>
/// Represents a type-safe client for interacting with an HNSW (Hierarchical Navigable Small World) vector index
/// stored within the VeloxDB object model.
/// <para>
/// **To create a new HNSW index, use the <c>CreateVectorIndex</c> extension method on the <see cref="ObjectModel"/>.**
/// </para>
/// </summary>
/// <typeparam name="T">The type of <see cref="DatabaseObject"/> whose vectors are indexed.</typeparam>
public class HNSW<T> where T : DatabaseObject
{
	private HNSW hnsw;
	private ObjectModel om;
	private Random random;


	internal HNSW(HNSW hnsw, ObjectModel om)
	{
		this.hnsw = hnsw;
		this.om = om;
#if !DEBUG
		random = new Random();
#else
		random = new Random(2);
#endif
	}

	/// <summary>
	/// The unique name of this index.
	/// </summary>
	public string Name => hnsw.Name;

	/// <summary>
	/// Adds a vector associated with a database object to the HNSW index.
	/// </summary>
	/// <param name="obj">The <typeparamref name="T"/> object whose vector is being added.</param>
	/// <param name="vector">The vector data wrapped in a <see cref="DatabaseArray{T}"/>.</param>
	public void Add(T obj, DatabaseArray<float> vector)
	{
		hnsw.Add(random, om, obj.Id, vector.AsSpan());
	}

	/// <summary>
	/// Adds a vector associated with a database object to the HNSW index.
	/// </summary>
	/// <param name="obj">The <typeparamref name="T"/> object whose vector is being added.</param>
	/// <param name="vector">The vector data as a <see cref="Span{T}"/> of floats.</param>
	public void Add(T obj, ReadOnlySpan<float> vector)
	{
		hnsw.Add(random, om, obj.Id, vector);
	}

	/// <summary>
	/// Deletes the HNSW vector index from the VeloxDB database.
	/// </summary>
	public void Delete()
	{
		hnsw.SafelyDelete();
	}

	/// <summary>
	/// Searches the HNSW index for the $k$ nearest neighbors to the query vector.
	/// </summary>
	/// <param name="vector">The query vector data wrapped in a <see cref="DatabaseArray{T}"/>.</param>
	/// <param name="k">The maximum number of nearest neighbors to return.</param>
	/// <param name="ef">
	/// The size of the dynamic list for the nearest neighbors during search. 
	/// Higher values increase search accuracy at the cost of performance. Defaults to 10.
	/// </param>
	/// <returns>An array of <typeparamref name="T"/> objects representing the $k$ nearest neighbors, 
	/// ordered from farthest to nearest.</returns>
	public T[] Search(DatabaseArray<float> vector, int k, int ef = 10)
	{
		return Search(vector.AsSpan(), k, ef);
	}

	/// <summary>
	/// Searches the HNSW index for the $k$ nearest neighbors to the query vector.
	/// </summary>
	/// <param name="vector">The query vector data as a <see cref="ReadOnlySpan{T}"/> of floats.</param>
	/// <param name="k">The maximum number of nearest neighbors to return.</param>
	/// <param name="ef">
	/// The size of the dynamic list for the nearest neighbors during search. 
	/// Higher values increase search accuracy at the cost of performance. Defaults to 10.
	/// </param>
	/// <returns>An array of <typeparamref name="T"/> objects representing the $k$ nearest neighbors, 
	/// ordered from farthest to nearest.</returns>
	public T[] Search(ReadOnlySpan<float> vector, int k, int ef = 10)
	{
		PriorityQueue<HNSWNode, float> queue = hnsw.Search(om, vector, ef);
		int count = Math.Min(k, queue.Count);
		T[] result = new T[count];

		if (queue.Count > k)
			for (int i = queue.Count - k; i > 0; i--)
				queue.Dequeue();

		for (int i = count - 1; i >= 0; i--)
		{
			queue.TryDequeue(out HNSWNode? node, out float _);
			Debug.Assert(node != null);
			T? obj = om.GetObject<T>(node.ReferenceId);
			Debug.Assert(obj != null);
			result[i] = obj;
		}

		return result;
	}

	/// <summary>
	/// Removes the vector associated with a database object from the HNSW index.
	/// </summary>
	/// <param name="obj">The <typeparamref name="T"/> object whose vector is to be removed.</param>
	public void Remove(T obj)
	{
		hnsw.Remove(om, obj.Id);
	}

#if DEBUG
	/// <summary>
	/// Generates a DOT language representation of the HNSW graph structure for debugging purposes.
	/// A separate graph is generated for each layer of the index.
	/// </summary>
	/// <returns>An array of strings, where each string is a DOT graph representation of an HNSW layer.</returns>
	public string[] ToDot()
	{
		string[] strings = new string[hnsw.MaxLevel + 1];
		StringBuilder sb = new StringBuilder();
		HashSet<(long, long)> visitedConnections = new HashSet<(long, long)>();
		for (int level = hnsw.MaxLevel; level >= 0; level--)
		{
			visitedConnections.Clear();
			sb.Clear();
			sb.AppendLine("graph G {");
			HashSet<long> visited = new HashSet<long>();
			Queue<HNSWNode> toVisit = new Queue<HNSWNode>();

			if (hnsw.EntryPoint == null)
				continue;

			toVisit.Enqueue(hnsw.EntryPoint);
			while (toVisit.Count > 0)
			{
				HNSWNode node = toVisit.Dequeue();
				if (!visited.Add(node.Id))
					continue;

				foreach (var neighbor in node.GetNeighborsAtLevel(level))
				{
					if (!visitedConnections.Add((Math.Min(node.Id, neighbor.Id), Math.Max(node.Id, neighbor.Id))))
						continue;
					sb.AppendLine($"  \"{(node as HNSWNode).DebugId:X}\" -- \"{(neighbor as HNSWNode).DebugId:X}\" [label=\"{hnsw.DistanceFunction(node.Vector.AsSpan(), neighbor.Vector.AsSpan()):F2}\"];");
					if (!visited.Contains(neighbor.Id))
						toVisit.Enqueue(neighbor);
				}
			}
			sb.AppendLine("}");
			strings[level] = sb.ToString();
		}


		return strings;
	}
#endif
}


/// <summary>
/// Hierarchical Navigable Small World graph (HNSW) vector index implementation. <b>Do not use directly</b>, use VectorIndexExtensions methods instead.
/// </summary>
[DatabaseClass]
[HashIndex("HNSWName", true, nameof(HNSW.Name))]
public abstract class HNSW : DatabaseObject
{

	/// <summary>
    /// Unique name of the index
    /// </summary>
	[DatabaseProperty]
	public abstract string Name { get; set; }

	/// <summary>
    /// The maximum number of neighbors (<c>M</c>) for a node in all layers *above* the base layer.
    /// </summary>
	[DatabaseProperty]
	public abstract int M { get; set; }

	/// <summary>
    /// The maximum number of neighbors (<c>M</c>) for a node in the base layer.
    /// </summary>
	[DatabaseProperty]
	public abstract int M0 { get; set; }


	/// <summary>
    /// the size of the dynamic candidate list ($\text{EfConstruction}$) used during the 
    /// insertion (construction) process.
    /// </summary>
	[DatabaseProperty]
	public abstract int EfConstruction { get; set; }

	/// <summary>
	/// The name of the distance function (e.g., "L2Distance") used by this index.
	/// </summary>
	[DatabaseProperty]
	public abstract string DistanceFunctionName { get; set; }

	/// <summary>
    /// The highest level index currently present in the HNSW graph
    /// </summary>
	[DatabaseProperty(-1)]
	public abstract int MaxLevel { get; set; }

	/// <summary>
    /// The expected dimension (length) of the vectors stored in this index.
    /// </summary>
	[DatabaseProperty]
	public abstract int Dimension { get; set; }

	/// <summary>
    /// </summary>
	[DatabaseReference()]
	public abstract HNSWNode? EntryPoint { get; set; }

#if DEBUG
	/// <summary>
    /// Internal counter used in DEBUG builds to assign a unique ID to new HNSWNode objects for tracking and diagnostics.
    /// </summary>
	[DatabaseProperty]
	public abstract long DebugIdCounter { get; set; }
#endif


	private DistanceFunction? distanceFunction;
	internal DistanceFunction DistanceFunction
	{
		get
		{
			if (distanceFunction == null)
				distanceFunction = DistanceCalculator.GetDistanceFunction(DistanceFunctionName);
			return distanceFunction;
		}
	}

	internal void Add(Random random, ObjectModel om, long id, ReadOnlySpan<float> vector)
	{
		if (vector.Length != Dimension)
			throw new ArgumentException($"Vector dimension {vector.Length} does not match index dimension {Dimension}");

		Indexes indexes = Indexes.Create(om);

		int level = RandomLevel(random);

		HNSWNode node = om.CreateObject<HNSWNode>();

#if DEBUG
		node.DebugId = DebugIdCounter++;
#endif

		PriorityQueue<HNSWNode, float> nearest;

		node.ReferenceId = id;
		node.Vector = DatabaseArray<float>.FromSpan(vector);

		HNSWNode? entryPoint = EntryPoint;

		for (int i = MaxLevel; i > level + 1; i--)
		{
			Debug.Assert(entryPoint != null);
			nearest = SearchLayer(vector, entryPoint, EfConstruction, i);

			Debug.Assert(nearest.TryPeek(out HNSWNode? _, out float _), "This has to succeed if for loop is executing, because it means that the HNSW is not empty");
			entryPoint = nearest.Peek();
		}

		if (entryPoint == null)
		{
			EntryPoint = node;
			MaxLevel = level;
			return;
		}

		for (int i = Math.Min(MaxLevel, level); i >= 0; i--)
		{
			int numNeighbors = (i == 0) ? M0 : M;
			PriorityQueue<HNSWNode, float> candidates = SearchLayer(vector, entryPoint, EfConstruction, i);
			HNSWNode[] neighbors = SelectNeighbors(vector, candidates, numNeighbors, i);

			for (var j = 0; j < neighbors.Length; j++)
			{
				var neighbor = neighbors[j];

				// It's possible that RefreshNeighborConnections already connected this neighbor by pushing a connection from the neighbor to this node
				if (HNSWConnection.IsConnected(indexes.ConnectionIndex, neighbor, node, i))
					continue;

				ConnectionInfo connInfo = HNSWConnection.Create(om, indexes.LevelCountIndex, node, neighbor, i);

				if (connInfo.SecondNeighborsCount > numNeighbors)
				{
					RefreshNeighborConnections(om, indexes, neighbor, i, numNeighbors);
				}
			}
		}

		if (level > MaxLevel)
		{
			MaxLevel = level;
			EntryPoint = node;
		}
	}

	private static int RandomLevel(Random random)
	{
		return (int)(-MathF.Log2(random.NextSingle()) * 0.3);
	}

    /// <summary>
    /// Verifies the structural integrity and connectivity of all HNSW indices in the ObjectModel.
    /// Checks for neighbor count limits, back-references, and node ownership across different HNSW instances.
    /// </summary>
    /// <param name="om">The ObjectModel context.</param>
    /// <returns>A list of tuples containing error messages and their severity levels.</returns>
	public static List<(string Error, ErrorLevel ErrorLevel)> VerifyAll(ObjectModel om)
	{
		Queue<(HNSWNode node, HNSWNode? origin)> toVisit = new();

		List<(string Error, ErrorLevel ErrorLevel)> errors = new();

		HNSW[] hnsws = om.GetAllObjects<HNSW>().ToArray();
		Dictionary<HNSW, HashSet<HNSWNode>[]> visitedSets = new();

		foreach (var hnsw in hnsws)
		{
			HashSet<HNSWNode>[] visitedOnAllLevels = new HashSet<HNSWNode>[hnsw.MaxLevel + 1];
			visitedSets[hnsw] = visitedOnAllLevels;
			for (int level = 0; level < hnsw.MaxLevel + 1; level++)
			{
				HashSet<HNSWNode> visited = new HashSet<HNSWNode>();
				visitedOnAllLevels[level] = visited;

				toVisit.Clear();
				int maxNeighbors = (level == 0) ? hnsw.M0 : hnsw.M;

				if (hnsw.EntryPoint == null)
					continue;

				toVisit.Enqueue((hnsw.EntryPoint, null));

				while (toVisit.Count > 0)
				{
					(HNSWNode node, HNSWNode? origin) = toVisit.Dequeue();
					if (visited.Contains(node))
						continue;

					visited.Add(node);

					HNSWNode[] neighbors = node.GetNeighborsAtLevel(level).ToArray();

					if (neighbors.Length > maxNeighbors)
					{
						errors.Add(($"Node {node.Id} of HNSW {hnsw.Name} at level {level} has {neighbors.Length} neighbors, exceeding the limit of {maxNeighbors}.", ErrorLevel.Warning));
					}

					bool hasOrigin = origin == null; // if origin is null it means that this is the entry point and it is fine
					foreach (var neighbor in neighbors)
					{
						if (!visited.Contains(neighbor))
						{
							toVisit.Enqueue((neighbor, node));
						}

						if (!hasOrigin && neighbor == origin)
						{
							hasOrigin = true;
						}
					}

					if (!hasOrigin)
					{
						errors.Add(($"Node {node.Id} at level {level} does not have a back-reference to its origin node {origin?.Id}.", ErrorLevel.Warning));
					}
				}
			}
		}

		Dictionary<int, HashSet<HNSWNode>> nodesByLevel = new();
		foreach ((HNSW hnsw, HashSet<HNSWNode>[] visitedPerLevel) in visitedSets)
		{
			for (int level = 0; level < visitedPerLevel.Length; level++)
			{
				HashSet<HNSWNode> visited = visitedPerLevel[level];
				if (!nodesByLevel.TryGetValue(level, out var allNodes))
				{
					allNodes = new();
					nodesByLevel[level] = allNodes;
				}

				foreach (HNSWNode node in visited)
				{
					if (allNodes.Contains(node))
					{
						errors.Add(($"Node {node.Id} at level {level} from HNSW {hnsw.Name} is already a member of some other HSWN", ErrorLevel.Error));
					}
					else
					{
						allNodes.Add(node);
					}
				}
			}
		}

		foreach (var node in om.GetAllObjects<HNSWNode>())
		{
			int nodeLevel = node.GetLevel();

			for (int level = 0; level <= nodeLevel; level++)
			{
				if (!nodesByLevel.TryGetValue(level, out var allNodes))
				{
					errors.Add(($"Node {node.Id} at level {nodeLevel} does not belong to any HNSW at level {level}", ErrorLevel.Error));
					continue;
				}

				if (!allNodes.Contains(node))
				{
					errors.Add(($"Node {node.Id} at level {nodeLevel} does not belong to any HNSW ate level {level}", ErrorLevel.Error));
				}
			}
		}

		return errors;
	}

	private void RefreshNeighborConnections(ObjectModel om, Indexes indexes, HNSWNode node, int level, int numNeighbors)
	{
		ReadOnlySpan<float> vector = node.Vector.AsSpan();

		List<NeighborConnectionPair> connections = [..node.GetConnectionsAtLevel(level)];

		PriorityQueue<HNSWNode, float> candidates = new(numNeighbors);
		foreach (var connection in connections)
		{
			HNSWNode neighbor = connection.Neighbor;
			float distance = DistanceFunction(vector, neighbor.Vector.AsSpan());
			candidates.Enqueue(neighbor, -distance);
		}

		Debug.Assert(node.GetConnectionCount(indexes.LevelCountIndex, level) == candidates.Count);

		HNSWNode[] newNeighbors = SelectNeighbors(vector, candidates, numNeighbors, level, node);
		HashSet<HNSWNode> newNeighborsSet = new HashSet<HNSWNode>(newNeighbors.Length);
		for (var i = 0; i < newNeighbors.Length; i++)
		{
			var n = newNeighbors[i];
			newNeighborsSet.Add(n);
		}

		List<NeighborConnectionPair> removedList = new();
		foreach (NeighborConnectionPair pair in connections)
		{
			if (!newNeighborsSet.Contains(pair.Neighbor))
			{
				removedList.Add(pair);
			}
			else
			{
				newNeighborsSet.Remove(pair.Neighbor);
			}
		}

		// Add new connections
		foreach (var neighbor in newNeighborsSet)
		{
			HNSWConnection.Create(om, indexes.LevelCountIndex, node, neighbor, level);
		}

		bool IsFarther(HNSWNode n1, float d1, HNSWNode n2, float d2)
		{
			bool firstEmpty = n1.GetConnectionCount(indexes.LevelCountIndex, level) < numNeighbors;
			bool secondEmpty = n2.GetConnectionCount(indexes.LevelCountIndex, level) < numNeighbors;

			if (firstEmpty == secondEmpty)
				return d1 > d2;

			return !firstEmpty;
		}

		// Remove old connections and find better place for removed connections, this is to prevent islands formation
		foreach (var removed in removedList)
		{
			removed.Connection.SafelyDelete(indexes.LevelCountIndex);
			HNSWNode removedNode = removed.Neighbor;

			float minDistance = float.MaxValue;
			HNSWNode? bestNeighbor = null;
			foreach (var neighbor in newNeighbors)
			{
				if (HNSWConnection.IsConnected(indexes.ConnectionIndex, neighbor, removedNode, level))
				{
					// break outer loop
					goto skip;
				}

				float dist = DistanceFunction(neighbor.Vector.AsSpan(), removedNode.Vector.AsSpan());

				if (bestNeighbor == null || IsFarther(bestNeighbor, minDistance, neighbor, dist))
				{
					bestNeighbor = neighbor;
					minDistance = dist;
				}
			}

			Debug.Assert(bestNeighbor != null);
			HNSWConnection.Create(om, indexes.LevelCountIndex, removedNode, bestNeighbor, level);

skip:; // label to break outer loop when a connection is already present
		}
	}

	private HNSWNode[] SelectNeighbors(ReadOnlySpan<float> vector, PriorityQueue<HNSWNode, float> candidates, int numNeighbors, int level, HNSWNode? exclude = null)
	{
		int count = Math.Min(numNeighbors, candidates.Count);

		HNSWNode[] result = new HNSWNode[count];

		bool excluded = false;

		HNSWNode? discarded = null;
		while (candidates.Count > count)
			discarded = candidates.Dequeue();

		for (int i = count - 1; i >= 0; i--)
		{
			candidates.TryDequeue(out HNSWNode? node, out float _);
			if (exclude != null && node == exclude)
			{
				excluded = true;
				i++;
				continue;
			}

			Debug.Assert(node != null);
			result[i] = node;
		}

		if (excluded)
		{
			if (discarded != null)
				result[0] = discarded;
			else
			{
				result = result.AsSpan(1).ToArray();
			}
		}

		return result;
	}

	private PriorityQueue<HNSWNode, float> SearchLayer(ReadOnlySpan<float> query, HNSWNode entryPoint, int ef, int level)
	{
		if (entryPoint == null)
			throw new ArgumentNullException(nameof(entryPoint));

		HashSet<long> visited = new HashSet<long>();
		visited.Add(entryPoint.Id);

		PriorityQueue<HNSWNode, float> candidates = new PriorityQueue<HNSWNode, float>();
		PriorityQueue<HNSWNode, float> result = new PriorityQueue<HNSWNode, float>();

		float distance = DistanceFunction(query, entryPoint.Vector.AsSpan());

		candidates.Enqueue(entryPoint, distance);
		result.Enqueue(entryPoint, -distance);

		while (candidates.Count > 0)
		{
			candidates.TryDequeue(out HNSWNode? nearest, out float nearestDistance);
			result.TryPeek(out HNSWNode? worst, out float worstDistance);
			worstDistance = -worstDistance;

			Debug.Assert(nearest != null && worst != null);

			if (nearestDistance > worstDistance)
				break;

			foreach (var neighbor in nearest.GetNeighborsAtLevel(level))
			{
				if (!visited.Add(neighbor.Id))
					continue;

				float dist = DistanceFunction(query, neighbor.Vector.AsSpan());

				if (result.Count < ef || dist < worstDistance)
				{
					candidates.Enqueue(neighbor, dist);
					result.Enqueue(neighbor, -dist);
					if (result.Count > ef)
						result.Dequeue();
				}
			}
		}

		return result;
	}

	internal PriorityQueue<HNSWNode, float> Search(ObjectModel om, ReadOnlySpan<float> vector, int ef)
	{
		if (EntryPoint == null)
			return new(0);

		HNSWNode entryPoint = EntryPoint;
		int maxLevel = MaxLevel;

		for (int level = maxLevel; level > 0; level--)
		{
			PriorityQueue<HNSWNode, float> nearest = SearchLayer(vector, entryPoint, 1, level);
			entryPoint = nearest.Peek();
		}

		PriorityQueue<HNSWNode, float> result = SearchLayer(vector, entryPoint, ef, 0);
		return result;

	}

	internal void Remove(ObjectModel om, long id)
	{
		Indexes indexes = Indexes.Create(om);
		HashIndexReader<HNSWNode, long> index = om.GetHashIndex<HNSWNode, long>(HNSWNode.IndexName);
		HNSWNode? node = index.GetObject(id);

		if (node == null)
			throw new InvalidOperationException($"Object {id} is not present in the index.");

		List<List<HNSWNode>> neighborsPerLevel = node.GetAllNeighborsPerLevel();

		if (EntryPoint == node)
		{
			int level = MaxLevel;
			HNSWNode? newRoot = null;

			while (level >= 0 && newRoot == null)
			{
				if (neighborsPerLevel.Count > level && neighborsPerLevel[level].Count > 0)
					newRoot = neighborsPerLevel[level][0];
				else
					level--;
			}

			EntryPoint = newRoot;
			MaxLevel = level;
		}

		// Test connectivity and reconnect neighbors if needed
		for (int level = 0; level < neighborsPerLevel.Count; level++)
		{
			int maxNeighbors = (level == 0) ? M0 : M;
			// find centroid
			List<HNSWNode> neighbors = neighborsPerLevel[level];
			if (neighbors.Count == 0)
				continue;

			float[] centroid = new float[Dimension];
			for (var i = 0; i < neighbors.Count; i++)
			{
				var neighbor = neighbors[i];
				ReadOnlySpan<float> vec = neighbor.Vector.AsSpan();
				AddVectors(centroid, vec);
			}

			ScaleVector(centroid, 1.0f / neighbors.Count);

			// Find best neighbor to be the new connection point
			HNSWNode bestNeighbor = neighbors[0];
			float bestDistance = DistanceFunction(centroid, bestNeighbor.Vector.AsSpan());
			for (var i = 1; i < neighbors.Count; i++)
			{
				var neighbor = neighbors[i];
				float dist = DistanceFunction(centroid, neighbor.Vector.AsSpan());
				if (dist < bestDistance)
				{
					bestDistance = dist;
					bestNeighbor = neighbor;
				}
			}

			// Reconnect other neighbors through the best neighbor
			int connectionCount = 0;
			for (var i = 0; i < neighbors.Count; i++)
			{
				var neighbor = neighbors[i];
				if (neighbor == bestNeighbor)
					continue;

				if (HNSWConnection.IsConnected(indexes.ConnectionIndex, bestNeighbor, neighbor, level))
					continue;

				ConnectionInfo connInfo = HNSWConnection.Create(om, indexes.LevelCountIndex, bestNeighbor, neighbor, level);
				connectionCount = connInfo.FirstNeighborsCount;
			}

			if(connectionCount > maxNeighbors)
			{
				RefreshNeighborConnections(om, indexes, bestNeighbor, level, maxNeighbors);
			}
		}

		node.SafelyDelete(indexes.LevelCountIndex);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void ScaleVector(float[] centroid, float v)
	{
		int i = 0;
		int simdLength = Vector<float>.Count;
		Vector<float> scale = new Vector<float>(v);
		for (; i <= centroid.Length - simdLength; i += simdLength)
		{
			var vec = new Vector<float>(centroid, i);
			(vec * scale).CopyTo(centroid, i);
		}

		for (; i < centroid.Length; i++)
			centroid[i] *= v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void AddVectors(float[] centroid, ReadOnlySpan<float> vec)
	{
		int i = 0;
		int simdLength = Vector<float>.Count;
		for (; i <= centroid.Length - simdLength; i += simdLength)
		{
			var v1 = new Vector<float>(centroid, i);
			var v2 = new Vector<float>(vec.Slice(i));
			(v1 + v2).CopyTo(centroid, i);
		}

		for (; i < centroid.Length; i++)
			centroid[i] += vec[i];
	}

	internal void SafelyDelete()
	{
		if (EntryPoint != null)
        {
			Queue<HNSWNode> toVisit = new();
			toVisit.Enqueue(EntryPoint);
			EntryPoint = null;

			HashSet<HNSWNode> visitedNodes = new();
			HashSet<HNSWConnection> connections = new();

			while (toVisit.Count > 0)
			{
				HNSWNode current = toVisit.Dequeue();
				visitedNodes.Add(current);

				ProcessConnections(current.ConnectionsAsFirst, c => c.Second);
				ProcessConnections(current.ConnectionsAsSecond, c=> c.First);
			}
			
			void ProcessConnections(ICollection<HNSWConnection> toProcess, Func<HNSWConnection, HNSWNode> select)
			{
				foreach (var connection in toProcess)
				{
					HNSWNode node = select(connection);
					connections.Add(connection);
					if (!visitedNodes.Contains(node))
					{
						toVisit.Enqueue(node);
					}
				}
			}

			foreach (HNSWConnection connection in connections)
			{
				connection.Delete();
			}
			
			foreach(HNSWNode node in visitedNodes)
            {
				node.Delete();
            }
        }
		Delete();
	}
}


/// <summary>
/// Defines the severity level of an error or warning reported by the index verification process.
/// </summary>
public enum ErrorLevel
{
	/// <summary>Indicates a critical structural error.</summary>
	Error,
    /// <summary>Indicates a non-critical issue, such as exceeding neighbor limits.</summary>
	Warning
}

internal record Indexes(HashIndexReader<HNSWConnection, long, long, int> ConnectionIndex, HashIndexReader<HNSWConnectionLevelCount, long, int> LevelCountIndex)
{
    public static Indexes Create(ObjectModel om)
    {
        var connIndex = HNSWConnection.GetIndex(om);
        var lvlCntIndex = HNSWConnectionLevelCount.GetIndex(om);

        return new Indexes(connIndex, lvlCntIndex);
    }
}
