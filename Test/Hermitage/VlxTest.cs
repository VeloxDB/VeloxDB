using VeloxDB.ObjectInterface;

namespace VeloxDB.Samples.Hermitage;

[DatabaseClass]
[HashIndex("ObjId", true, nameof(VlxTest.ObjId))]
public abstract class VlxTest : DatabaseObject
{
	[DatabaseProperty]
	public abstract int ObjId { get; set; }

	[DatabaseProperty]
	public abstract int Value { get; set; }
}