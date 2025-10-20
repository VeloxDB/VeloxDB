using VeloxDB.ObjectInterface;
namespace VeloxDB.VectorIndex;

/// <summary>
/// Provides extension methods for managing HNSW (Hierarchical Navigable Small World)
/// vector indices within a VeloxDB <see cref="ObjectModel"/>.
/// </summary>
public static class VectorIndexExtensions
{
    private const string IndexName = "HNSWName";

    /// <summary>
    /// Gets the HNSW vector index for objects of type <typeparamref name="T"/> with the specified name.
    /// </summary>
    /// <typeparam name="T">The type of <see cref="DatabaseObject"/> whose vectors are stored in the index.</typeparam>
    /// <param name="om">The <see cref="ObjectModel"/> instance.</param>
    /// <param name="name">The unique name of the vector index to retrieve.</param>
    /// <returns>
    /// An <see cref="HNSW{T}"/> instance representing the vector index, or <c>null</c> if an index with the given name is not found.
    /// </returns>
    public static HNSW<T>? GetVectorIndex<T>(this ObjectModel om, string name) where T : DatabaseObject
    {
        HashIndexReader<HNSW, string> index = om.GetHashIndex<HNSW, string>("HNSWName");
        HNSW? hnsw = index.GetObject(name);

        if (hnsw == null)
            return null;

        return new HNSW<T>(hnsw, om);
    }

    /// <summary>
    /// Creates a new HNSW vector index for objects of type <typeparamref name="T"/> with the specified parameters.
    /// </summary>
    /// <typeparam name="T">The type of <see cref="DatabaseObject"/> whose vectors will be stored in the index.</typeparam>
    /// <param name="om">The <see cref="ObjectModel"/> instance.</param>
    /// <param name="name">A unique name for the new vector index.</param>
    /// <param name="dimension">The dimensionality of the vectors that will be stored in the index.</param>
    /// <param name="m">The maximum number of outgoing connections for elements at layer $l > 0$. Defaults to 8.</param>
    /// <param name="m0">The maximum number of outgoing connections for elements at layer $l = 0$. Defaults to $2 \times M$.</param>
    /// <param name="efConstruction">The size of the dynamic list for the nearest neighbors during graph construction. Defaults to 200.</param>
    /// <param name="distanceFunction">
    /// The function used to calculate the distance between two vectors. It must be a static method
    /// defined in the <see cref="DistanceCalculator"/> class. Defaults to <see cref="DistanceCalculator.L2Distance(DatabaseArray{float}, DatabaseArray{float})"/>.
    /// </param>
    /// <returns>An <see cref="HNSW{T}"/> instance representing the newly created vector index.</returns>
    /// <exception cref="InvalidOperationException">Thrown if a vector index with the specified <paramref name="name"/> already exists.</exception>
    /// <exception cref="ArgumentException">Thrown if the provided <paramref name="distanceFunction"/> is not a supported method from the <see cref="DistanceCalculator"/> class.</exception>
    public static HNSW<T> CreateVectorIndex<T>(this ObjectModel om, string name, int dimension, int m = 8, int m0 = -1, int efConstruction = 200, DistanceFunction? distanceFunction = null) where T : DatabaseObject
    {
        if (distanceFunction == null)
        {
            distanceFunction = DistanceCalculator.L2Distance;
        }
        else
        {
            if (distanceFunction.Method.DeclaringType != typeof(DistanceCalculator))
            {
                throw new ArgumentException($"Only distance functions defined in the '{nameof(DistanceCalculator)}' class are supported for vector index creation.", nameof(distanceFunction));
            }
        }

        HashIndexReader<HNSW, string> index = om.GetHashIndex<HNSW, string>("HNSWName");
        HNSW? existing = index.GetObject(name);
        if (existing != null)
            throw new InvalidOperationException($"Vector index with name {name} already exists.");

        if (m0 == -1)
            m0 = m * 2;

        HNSW hnsw = om.CreateObject<HNSW>();
        hnsw.Name = name;
        hnsw.Dimension = dimension;
        hnsw.M = m;
        hnsw.M0 = m0;
        hnsw.EfConstruction = efConstruction;
        hnsw.DistanceFunctionName = distanceFunction.Method.Name;

        return new HNSW<T>(hnsw, om);
    }

    /// <summary>
    /// Deletes the HNSW vector index with the specified name from the <see cref="ObjectModel"/>.
    /// </summary>
    /// <param name="om">The <see cref="ObjectModel"/> instance.</param>
    /// <param name="name">The name of the vector index to delete.</param>
    /// <exception cref="InvalidOperationException">Thrown if a vector index with the specified <paramref name="name"/> does not exist.</exception>
    public static void DeleteVectorIndex(this ObjectModel om, string name)
    {
        HashIndexReader<HNSW, string> index = om.GetHashIndex<HNSW, string>(IndexName);
        HNSW? hnsw = index.GetObject(name);

        if (hnsw == null)
            throw new InvalidOperationException($"Vector index with name {name} already exists.");

        hnsw.SafelyDelete();
    }
}
