using System.Diagnostics;
using VeloxDB.ObjectInterface;

namespace VeloxDB.VectorIndex;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

/// <summary>
/// Represents a persistent internal structure for a connection between two HNSW nodes. 
/// <b>WARNING: This class is for internal framework use only and should not be accessed or modified directly by application developers.</b>
/// </summary>
[DatabaseClass]
[HashIndex(IndexName, true, nameof(HNSWConnection.First), nameof(HNSWConnection.Second), nameof(HNSWConnection.Level))]
public abstract class HNSWConnection : DatabaseObject
{
	private const string IndexName = "HNSWNodeConnection";

	[DatabaseProperty]
    public abstract int Level { get; set; }

    [DatabaseReference(false, Descriptor.DeleteTargetAction.PreventDelete)]
    public abstract HNSWNode First { get; set; }

    [DatabaseReference(false, Descriptor.DeleteTargetAction.PreventDelete)]
    public abstract HNSWNode Second { get; set; }

    internal static ConnectionInfo Create(ObjectModel om, HashIndexReader<HNSWConnectionLevelCount, long, int> levelCountIndex, HNSWNode first, HNSWNode second, int level)
    {
        HNSWConnectionLevelCount? connCountFirst = levelCountIndex.GetObject(first.Id, level);
        HNSWConnectionLevelCount? connCountSecond = levelCountIndex.GetObject(second.Id, level);

        if (connCountFirst == null)
        {
            connCountFirst = HNSWConnectionLevelCount.Create(om, first, level, 0);
        }

        if (connCountSecond == null)
        {
            connCountSecond = HNSWConnectionLevelCount.Create(om, second, level, 0);
        }

        HNSWConnection conn = om.CreateObject<HNSWConnection>();

        conn.First = first;
        conn.Second = second;
        conn.Level = level;

        connCountFirst.Count = connCountFirst.Count + 1;
        connCountSecond.Count = connCountSecond.Count + 1;

        return (conn, connCountFirst.Count, connCountSecond.Count);
    }

    public static HashIndexReader<HNSWConnection, long, long, int> GetIndex(ObjectModel om)
    {
        return om.GetHashIndex<HNSWConnection, long, long, int>(IndexName);
    }

	public static bool IsConnected(HashIndexReader<HNSWConnection, long, long, int> index, HNSWNode first, HNSWNode second, int level)
	{
        return index.GetObject(first.Id, second.Id, level) != null || index.GetObject(second.Id, first.Id, level) != null;
	}

	public void SafelyDelete(HashIndexReader<HNSWConnectionLevelCount, long, int> levelCountIndex)
    {
    	HNSWConnectionLevelCount? connCountFirst = levelCountIndex.GetObject(First.Id, Level);
        HNSWConnectionLevelCount? connCountSecond = levelCountIndex.GetObject(Second.Id, Level);

        Debug.Assert(connCountFirst != null);
		Debug.Assert(connCountSecond != null);

        connCountFirst.Count = connCountFirst.Count - 1;
        connCountSecond.Count = connCountSecond.Count - 1;

		Delete();
	}
}

internal record struct ConnectionInfo(HNSWConnection Connection, int FirstNeighborsCount, int SecondNeighborsCount)
{
    public static implicit operator (HNSWConnection Connection, int FirstNeighborsCount, int SecondNeighborsCount)(ConnectionInfo value)
    {
        return (value.Connection, value.FirstNeighborsCount, value.SecondNeighborsCount);
    }

    public static implicit operator ConnectionInfo((HNSWConnection Connection, int FirstNeighborsCount, int SecondNeighborsCount) value)
    {
        return new ConnectionInfo(value.Connection, value.FirstNeighborsCount, value.SecondNeighborsCount);
    }
}

#pragma warning restore CS1591
