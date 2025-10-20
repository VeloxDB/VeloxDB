using VeloxDB.ObjectInterface;

namespace VeloxDB.VectorIndex;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member


/// <summary>
/// <b>INTERNAL USE ONLY.</b> Represents a single node (vertex) in the HNSW graph.
/// <b>DO NOT</b> access or modify this class directly. It is part of the internal vector index implementation.
/// </summary>
[HashIndex(IndexName, true, nameof(HNSWNode.ReferenceId))]
[DatabaseClass]
public abstract class HNSWNode : DatabaseObject
{
	public const string IndexName = "HNSWNodeReferenceId";

#if DEBUG
	[DatabaseProperty]
	public abstract long DebugId { get; set; }
#endif

	[DatabaseProperty]
    public abstract long ReferenceId { get; set; }
    [DatabaseProperty]
    public abstract DatabaseArray<float> Vector { get; set; }

    [InverseReferences(nameof(HNSWConnection.First))]
    public abstract InverseReferenceSet<HNSWConnection> ConnectionsAsFirst { get; }

    [InverseReferences(nameof(HNSWConnection.Second))]
    public abstract InverseReferenceSet<HNSWConnection> ConnectionsAsSecond { get; }

    internal IEnumerable<HNSWNode> GetNeighborsAtLevel(int level)
    {
        foreach (HNSWConnection conn in ConnectionsAsFirst)
        {
            if (conn.Level == level)
                yield return conn.Second;
        }

        foreach (HNSWConnection conn in ConnectionsAsSecond)
        {
            if (conn.Level == level)
                yield return conn.First;
        }
    }

    internal IEnumerable<NeighborConnectionPair> GetConnectionsAtLevel(int level)
    {
        foreach (HNSWConnection conn in ConnectionsAsFirst)
        {
            if (conn.Level == level)
                yield return new NeighborConnectionPair(conn.Second, conn);
        }

        foreach (HNSWConnection conn in ConnectionsAsSecond)
        {
            if (conn.Level == level)
                yield return new NeighborConnectionPair(conn.First, conn);
        }
    }

    internal int GetLevel()
    {
        int maxLevel = -1;
        foreach (HNSWConnection conn in ConnectionsAsFirst)
        {
            if (conn.Level > maxLevel)
                maxLevel = conn.Level;
        }

        foreach (HNSWConnection conn in ConnectionsAsSecond)
        {
            if (conn.Level > maxLevel)
                maxLevel = conn.Level;
        }

        return maxLevel;
	}

    internal int GetConnectionCount(HashIndexReader<HNSWConnectionLevelCount, long, int> index, int level)
    {
        HNSWConnectionLevelCount? count = index.GetObject(Id, level);

        if (count == null)
            return 0;

        return count.Count;
	}

	internal List<List<HNSWNode>> GetAllNeighborsPerLevel()
	{
		List<List<HNSWNode>> result = new();
		foreach (HNSWConnection conn in ConnectionsAsFirst)
		{
			while (result.Count <= conn.Level)
				result.Add(new List<HNSWNode>());
			result[conn.Level].Add(conn.Second);
		}

		foreach (HNSWConnection conn in ConnectionsAsSecond)
		{
			while (result.Count <= conn.Level)
				result.Add(new List<HNSWNode>());
			result[conn.Level].Add(conn.First);
		}

		return result;
	}

	internal void SafelyDelete(HashIndexReader<HNSWConnectionLevelCount, long, int> levelCountIndex)
    {
        List<HNSWConnection> toDelete = [.. ConnectionsAsFirst, .. ConnectionsAsSecond];
        
        foreach (HNSWConnection conn in toDelete)
        {
            conn.SafelyDelete(levelCountIndex);
        }

		Delete();
	}
}

internal record struct NeighborConnectionPair(HNSWNode Neighbor, HNSWConnection Connection)
{
}

#pragma warning restore CS1591
