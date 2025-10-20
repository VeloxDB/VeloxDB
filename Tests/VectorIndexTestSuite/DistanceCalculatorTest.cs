using NUnit.Framework;
using NUnit.Framework.Legacy;
using VeloxDB.VectorIndex;

[TestFixture]
public class DistanceCalculatorTest
{
    [Test]
    public void TestL2Distance()
    {
        float[] a = new float[] { 1, 2, 3 };
        float[] b = new float[] { 4, 5, 6 };
        float dist = DistanceCalculator.L2Distance(a, b);
        ClassicAssert.AreEqual(MathF.Sqrt(27) , dist);
    }

    [Test]
    public void TestCosineDistance()
    {
        float[] a = new float[] { 1, 0, 0 };
        float[] b = new float[] { 0, 1, 0 };
        float dist = DistanceCalculator.CosineDistance(a, b);
        ClassicAssert.AreEqual(1, dist);
    }
}