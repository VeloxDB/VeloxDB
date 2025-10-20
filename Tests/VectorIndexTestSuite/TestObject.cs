using VeloxDB.ObjectInterface;

namespace VectorIndexTestSuite;

[DatabaseClass]
public abstract class TestObject : DatabaseObject
{
    [DatabaseProperty]
    public abstract string Name { get; set; }

    [DatabaseProperty]
    public abstract DatabaseArray<float> Vector { get; set; }
}
