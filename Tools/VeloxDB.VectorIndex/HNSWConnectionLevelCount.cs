using VeloxDB.ObjectInterface;

namespace VeloxDB.VectorIndex;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

/// <summary>
/// <b>INTERNAL USE ONLY.</b> Tracks the number of connections a node has at a specific level.
/// <b>DO NOT</b> access or modify this class directly. It is part of the internal vector index implementation.
/// </summary>
[DatabaseClass]
[HashIndex(IndexName, true, nameof(HNSWConnectionLevelCount.Node), nameof(HNSWConnectionLevelCount.Level))]
public abstract class HNSWConnectionLevelCount : DatabaseObject
{
	private const string IndexName = "HNSWConnectionLevelCount";

	[DatabaseReference(false, Descriptor.DeleteTargetAction.CascadeDelete)]
	public abstract HNSWNode Node { get; set; }

	[DatabaseProperty]
	public abstract int Level { get; set; }

	[DatabaseProperty]
	public abstract int Count { get; set; }

	public static HashIndexReader<HNSWConnectionLevelCount, long, int> GetIndex(ObjectModel om)
	{
		return om.GetHashIndex<HNSWConnectionLevelCount, long, int>(IndexName);
	}

	internal static HNSWConnectionLevelCount Create(ObjectModel om, HNSWNode first, int level, int count)
	{
		var connCount = om.CreateObject<HNSWConnectionLevelCount>();
		connCount.Node = first;
		connCount.Level = level;
		connCount.Count = count;
		return connCount;
	}
}

#pragma warning restore CS1591

