namespace VeloxDB.VectorIndex;

using System.Numerics;
using VeloxDB.ObjectInterface;


/// <summary>
/// Represents a method that calculates the distance between two floating-point vectors.
/// This delegate is used by the HNSW index to determine similarity during graph construction and search.
/// </summary>
public delegate float DistanceFunction(ReadOnlySpan<float> first, ReadOnlySpan<float> second);

/// <summary>
/// Provides static methods for calculating various distance metrics between two vectors (arrays of floats).
/// </summary>
public class DistanceCalculator
{

    /// <summary>
    /// Calculates the L1 (Manhattan or Taxicab) distance between two vectors.
    /// </summary>
    /// <param name="first">The first vector.</param>
    /// <param name="second">The second vector.</param>
    /// <returns>The L1 distance between the two vectors.</returns>
    /// <exception cref="ArgumentException">Thrown if the vectors are not of the same length.</exception>
    public static float L1Distance(DatabaseArray<float> first, DatabaseArray<float> second)
    {
        ReadOnlySpan<float> spanFirst = first.AsSpan();
        ReadOnlySpan<float> spanSecond = second.AsSpan();

        return L1Distance(spanFirst, spanSecond);
    }

    /// <summary>
    /// Calculates the L1 (Manhattan or Taxicab) distance between two vectors.
    /// </summary>
    /// <param name="first">The first vector.</param>
    /// <param name="second">The second vector.</param>
    /// <returns>The L1 distance between the two vectors.</returns>
    /// <exception cref="ArgumentException">Thrown if the vectors are not of the same length.</exception>
    public static float L1Distance(ReadOnlySpan<float> first, ReadOnlySpan<float> second)
    {
        if (first.Length != second.Length)
            throw new ArgumentException("Vectors must be of the same length");

        float sumAbsoluteDifferences = 0;

        int i = 0;

        for (; i <= first.Length - Vector<float>.Count; i += Vector<float>.Count)
        {
            Vector<float> va = new(first.Slice(i, Vector<float>.Count));
            Vector<float> vb = new(second.Slice(i, Vector<float>.Count));

            Vector<float> diff = Vector.Abs(va - vb);
            sumAbsoluteDifferences += Vector.Dot(diff, Vector<float>.One);
        }

        for (; i < first.Length; i++)
        {
            sumAbsoluteDifferences += MathF.Abs(first[i] - second[i]);
        }

        return sumAbsoluteDifferences;
    }

    /// <summary>
    /// Calculates the Inner Product distance between two vectors.
    /// </summary>
    /// <param name="first">The first vector.</param>
    /// <param name="second">The second vector.</param>
    /// <returns>The negated Inner Product.</returns>
    /// <exception cref="ArgumentException">Thrown if the vectors are not of the same length.</exception>
    public static float InnerProductDistance(DatabaseArray<float> first, DatabaseArray<float> second)
    {
        ReadOnlySpan<float> spanFirst = first.AsSpan();
        ReadOnlySpan<float> spanSecond = second.AsSpan();

        return InnerProductDistance(spanFirst, spanSecond);
    }

    /// <summary>
    /// Calculates the Inner Product distance between two vectors.
    /// </summary>
    /// <param name="first">The first vector.</param>
    /// <param name="second">The second vector.</param>
    /// <returns>The negated Inner Product.</returns>
    /// <exception cref="ArgumentException">Thrown if the vectors are not of the same length.</exception>
    public static float InnerProductDistance(ReadOnlySpan<float> first, ReadOnlySpan<float> second)
    {
        if (first.Length != second.Length)
            throw new ArgumentException("Vectors must be of the same length");

        float dotProduct = 0;

        int i = 0;

        // Process vectors in chunks using SIMD
        for (; i <= first.Length - Vector<float>.Count; i += Vector<float>.Count)
        {
            Vector<float> va = new(first.Slice(i, Vector<float>.Count));
            Vector<float> vb = new(second.Slice(i, Vector<float>.Count));

            dotProduct += Vector.Dot(va, vb); // Vectorized dot product part
        }

        // Process any remaining elements (tail)
        for (; i < first.Length; i++)
        {
            dotProduct += first[i] * second[i];
        }

        return -dotProduct;
    }

    /// <summary>
    /// Calculates the cosine distance between two vectors.
    /// The distance is defined as $ 1 - \cos(\theta) $, where $\cos(\theta) = \frac{\mathbf{A} \cdot \mathbf{B}}{|\mathbf{A}| |\mathbf{B}|}$.
    /// </summary>
    /// <param name="first">The first vector.</param>
    /// <param name="second">The second vector.</param>
    /// <returns>The Cosine distance (a value between 0 and 2).</returns>
    /// <exception cref="ArgumentException">Thrown if the vectors are not of the same length.</exception>
    public static float CosineDistance(DatabaseArray<float> first, DatabaseArray<float> second)
    {
        ReadOnlySpan<float> spanFirst = first.AsSpan();
        ReadOnlySpan<float> spanSecond = second.AsSpan();
        return CosineDistance(spanFirst, spanSecond);
    }

    /// <summary>
    /// Calculates the cosine distance between two vectors.
    /// The distance is defined as $ 1 - \cos(\theta) $, where $\cos(\theta) = \frac{\mathbf{A} \cdot \mathbf{B}}{|\mathbf{A}| |\mathbf{B}|}$.
    /// </summary>
    /// <param name="first">The first vector.</param>
    /// <param name="second">The second vector.</param>
    /// <returns>The Cosine distance (a value between 0 and 2).</returns>
    /// <exception cref="ArgumentException">Thrown if the vectors are not of the same length.</exception>
    public static float CosineDistance(ReadOnlySpan<float> first, ReadOnlySpan<float> second)
    {
        if (first.Length != second.Length)
            throw new ArgumentException("Vectors must be of the same length");

        float dotProduct = 0;
        float normASquared = 0;
        float normBSquared = 0;

        int i = 0;

        for (; i <= first.Length - Vector<float>.Count; i += Vector<float>.Count)
        {
            Vector<float> va = new(first.Slice(i, Vector<float>.Count));
            Vector<float> vb = new(second.Slice(i, Vector<float>.Count));

            dotProduct += Vector.Dot(va, vb); // Vectorized dot product part
            normASquared += Vector.Dot(va, va); // Vectorized squared norm part
            normBSquared += Vector.Dot(vb, vb);
        }

        // Process any remaining elements (tail)
        for (; i < first.Length; i++)
        {
            dotProduct += first[i] * second[i];
            normASquared += first[i] * first[i];
            normBSquared += second[i] * second[i];
        }

        if (normASquared == 0 || normBSquared == 0)
            return 1;

        return 1 - (dotProduct / MathF.Sqrt(normASquared * normBSquared));
    }

    /// <summary>
    /// Calculates the L2 (Euclidean) distance between two vectors.
    /// This is the "ordinary" straight-line distance between two points in Euclidean space, 
    /// calculated as the square root of the sum of the squared differences of their components.
    /// </summary>
    /// <param name="first">The first vector.</param>
    /// <param name="second">The second vector.</param>
    /// <returns>The L2 distance between the two vectors.</returns>
    /// <exception cref="ArgumentException">Thrown if the vectors are not of the same length.</exception>
    public static float L2Distance(DatabaseArray<float> first, DatabaseArray<float> second)
    {
        ReadOnlySpan<float> spanFirst = first.AsSpan();
        ReadOnlySpan<float> spanSecond = second.AsSpan();
        return L2Distance(spanFirst, spanSecond);
    }

    /// <summary>
    /// Calculates the L2 (Euclidean) distance between two vectors.
    /// </summary>
    /// <param name="first">The first vector.</param>
    /// <param name="second">The second vector.</param>
    /// <returns>The L2 distance between the two vectors.</returns>
    /// <exception cref="ArgumentException">Thrown if the vectors are not of the same length.</exception>
    public static float L2Distance(ReadOnlySpan<float> first, ReadOnlySpan<float> second)
    {
        if (first.Length != second.Length)
            throw new ArgumentException("Vectors must be of the same length");

        float sumOfSquares = 0;
        int i = 0;

        for (; i <= first.Length - Vector<float>.Count; i += Vector<float>.Count)
        {
            Vector<float> va = new(first.Slice(i, Vector<float>.Count));
            Vector<float> vb = new(second.Slice(i, Vector<float>.Count));

            Vector<float> diff = va - vb;

            sumOfSquares += Vector.Dot(diff, diff);
        }

        for (; i < first.Length; i++)
        {
            float diff = first[i] - second[i];
            sumOfSquares += diff * diff;
        }

        return MathF.Sqrt(sumOfSquares);
    }

    internal static DistanceFunction GetDistanceFunction(string distanceFunctionName)
    {
        return distanceFunctionName switch
        {
            nameof(L2Distance) => L2Distance,
            nameof(CosineDistance) => CosineDistance,
            nameof(InnerProductDistance) => InnerProductDistance,
            nameof(L1Distance) => L1Distance,
            _ => throw new ArgumentException($"Unknown distance function: {distanceFunctionName}")
        };
    }
}
