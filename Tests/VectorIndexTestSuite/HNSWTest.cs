using System.Diagnostics;
using NUnit.Framework;
using NUnit.Framework.Internal;
using NUnit.Framework.Legacy;
using VeloxDB.Embedded;
using VeloxDB.ObjectInterface;
using VeloxDB.VectorIndex;

namespace VectorIndexTestSuite;

[TestFixture]
public class HNSWTest
{
    private DirectoryInfo? tempDir;
    private VeloxDBEmbedded? db;

    private const int dimension = 128;

    [SetUp]
    public void SetUp()
    {
		tempDir = Directory.CreateTempSubdirectory();
        db = new VeloxDBEmbedded(tempDir.FullName, [typeof(HNSW).Assembly, typeof(TestObject).Assembly], false);
    }

    [Test]
    public void TestCreate()
    {
        Debug.Assert(db != null);
        using VeloxDBTransaction trans = db.BeginTransaction();

        HNSW<TestObject> hnsw = trans.ObjectModel.CreateVectorIndex<TestObject>("MyHNSW", dimension);
        trans.Commit();
    }

	[Test]
	[TestCase(2)]
    public void TestAdd(int seed)
	{
		Debug.Assert(db != null);
		const int count = 1000;

		using VeloxDBTransaction trans = db.BeginTransaction();
		ObjectModel om = trans.ObjectModel;

		if (seed == -1)
			seed = Random.Shared.Next();

		Random random = new Random(seed);

		HNSW<TestObject>? hnsw = om.CreateVectorIndex<TestObject>("MyHNSW", dimension);
		ClassicAssert.NotNull(hnsw);
		Debug.Assert(hnsw != null);

		List<TestObject> toAdd = GenerateObjects(om, count, dimension, random);

		foreach (TestObject obj in toAdd)
		{
			hnsw.Add(obj, obj.Vector);
		}

		trans.Commit();

		VerifyHNSW("MyHNSW");
	}

	private void VerifyHNSW(string hnswName)
	{
		Debug.Assert(db != null);
		using VeloxDBTransaction rtrans = db.BeginTransaction(TransactionType.Read);
		ObjectModel om = rtrans.ObjectModel;
		HNSW<TestObject>? hnsw = om.GetVectorIndex<TestObject>(hnswName);
		ClassicAssert.NotNull(hnsw);

		var errors = HNSW.VerifyAll(om);

		foreach (var error in errors)
		{
			Console.WriteLine(error);
		}

		Assert.That(() => errors.Where(e => e.ErrorLevel == ErrorLevel.Error).Count() == 0);
	}

	[Test]
	public void TestSearch()
	{
		Debug.Assert(db != null);

		using VeloxDBTransaction trans = db.BeginTransaction();
		ObjectModel om = trans.ObjectModel;

		HNSW<TestObject>? hnsw = om.CreateVectorIndex<TestObject>("MyHNSW", 3);
		ClassicAssert.NotNull(hnsw);

		TestObject obj1 = om.CreateObject<TestObject>();
		obj1.Name = "Object 1";
		obj1.Vector = DatabaseArray<float>.FromSpan([2, 0, 0]);

		TestObject obj2 = om.CreateObject<TestObject>();
		obj2.Name = "Object 2";
		obj2.Vector = DatabaseArray<float>.FromSpan([0, 1, 0]);

		TestObject obj3 = om.CreateObject<TestObject>();
		obj3.Name = "Object 3";
		obj3.Vector = DatabaseArray<float>.FromSpan([0, 0, 3]);

		hnsw.Add(obj1, obj1.Vector);
		hnsw.Add(obj2, obj2.Vector);
		hnsw.Add(obj3, obj3.Vector);

		TestObject[] result = hnsw.Search([0, 0, 0], 3);

		Assert.That(result.Length == 3);
		Assert.That(result[0].Id == obj2.Id);
		Assert.That(result[1].Id == obj1.Id);
		Assert.That(result[2].Id == obj3.Id);
	}

	[Test]
	[TestCase(94)]
	public void TestRecall(int seed)
	{
		Debug.Assert(db != null);
		const int count = 1000;

		using VeloxDBTransaction trans = db.BeginTransaction();
		ObjectModel om = trans.ObjectModel;

		Console.WriteLine($"TestRecall with seed {seed}");
		Random random = new Random(seed);
		HNSW<TestObject>? hnsw = om.CreateVectorIndex<TestObject>("MyHNSW", dimension, 32, 64, 40, distanceFunction: DistanceCalculator.CosineDistance);
		ClassicAssert.NotNull(hnsw);
		Debug.Assert(hnsw != null);

		List<TestObject> toAdd = GenerateObjects(om, count, dimension, random);

		foreach (TestObject obj in toAdd)
		{
			hnsw.Add(obj, obj.Vector);
		}

		float recall = CalculateRecall(random, hnsw, toAdd, 100);
		float totalRecall = CalculateTotalRecall(random, hnsw, toAdd);

		Console.WriteLine($"Recall: {recall * 100}%");
		Console.WriteLine($"Total Recall: {totalRecall * 100}%");

		Debug.Assert(recall > 0.90);
		Debug.Assert(totalRecall > 0.95);

		trans.Commit();
		VerifyHNSW("MyHNSW");
	}

	private float CalculateTotalRecall(Random random, HNSW<TestObject> hnsw, List<TestObject> all)
	{
		int correct = 0;
		for (var i = 0; i < all.Count; i++)
		{
			if (i == 1)
			{

			}
			var obj = all[i];
			var result = hnsw.Search(obj.Vector, 1);
			if (result.Length > 0 && result[0].Id == obj.Id)
				correct++;
			else
			{

			}
		}
		return correct / (float)all.Count;
	}

	private static float CalculateRecall(Random random, HNSW<TestObject> hnsw, List<TestObject> all, int count)
	{
		PriorityQueue<TestObject, float> pq = new();
		int correct = 0;
		int total = 0;
		for (int i = 0; i < count; i++)
		{
			float[] q = RandomVector(random, dimension);
			TestObject[] resultHNSW = hnsw.Search(q, 10, 16);

			foreach (TestObject obj in all)
			{
				float dist = DistanceCalculator.CosineDistance(q, obj.Vector.ToArray());

				pq.Enqueue(obj, -dist);
				if (pq.Count > 10)
					pq.Dequeue();
			}

			int resCount = Math.Min(10, pq.Count);
			total += resCount;
			TestObject[] resultExact = new TestObject[resCount];
			for (int j = resCount - 1; j >= 0; j--)
			{
				pq.TryDequeue(out TestObject? obj, out float dist);
				Debug.Assert(obj != null);
				resultExact[j] = obj;
			}
			pq.Clear();

			HashSet<long> exactIds = resultExact.Select(o => o.Id).ToHashSet();
			for (int j = 0; j < resCount; j++)
			{
				if (exactIds.Contains(resultHNSW[j].Id))
				{
					correct++;
				}
			}

		}

		return correct/(float)total;
	}

	[Test]
	public void SearchBenchmark()
	{
#if DEBUG
		Assert.Ignore("Benchmark tests are ignored in DEBUG mode.");
#endif
		int count = 10000;
		using VeloxDBTransaction trans = db.BeginTransaction();
		ObjectModel om = trans.ObjectModel;

		Random random = new Random();

		HNSW<TestObject>? hnsw = om.CreateVectorIndex<TestObject>("MyHNSW", dimension);
		ClassicAssert.NotNull(hnsw);
		Debug.Assert(hnsw != null);

		List<TestObject> toAdd = GenerateObjects(om, count, dimension, random);

		Stopwatch sw = Stopwatch.StartNew();
		Console.WriteLine("Adding objects...");
		foreach (TestObject obj in toAdd)
		{
			hnsw.Add(obj, obj.Vector);
		}
		sw.Stop();
		Console.WriteLine($"Added {count} objects in {sw.ElapsedMilliseconds} ms");

		trans.Commit();

		using var transRead = db.BeginTransaction(TransactionType.Read);
		om = transRead.ObjectModel;

		hnsw = om.GetVectorIndex<TestObject>("MyHNSW");
		ClassicAssert.NotNull(hnsw);

		sw.Restart();
		const int searchCount = 10000;
		for (int i = 0; i < searchCount; i++)
		{
			float[] q = RandomVector(random, dimension);

			var watch = Stopwatch.StartNew();
			var result = hnsw.Search(q, 10);
		}
		sw.Stop();
		Console.WriteLine($"Performed {searchCount} searches in {sw.ElapsedMilliseconds} ms");
	}


	[Test]
	[TestCase(2)]
	public void TestRemove(int seed)
	{
		if (seed == -1)
			seed = Random.Shared.Next();
		Debug.Assert(db != null);

		using VeloxDBTransaction trans = db.BeginTransaction();
		ObjectModel om = trans.ObjectModel;

		HNSW<TestObject>? hnsw = om.CreateVectorIndex<TestObject>("MyHNSW", dimension);
		ClassicAssert.NotNull(hnsw);
		Random random = new Random(seed);
		List<TestObject> objects = GenerateObjects(om, 100, dimension, random);

		foreach (TestObject obj in objects)
		{
			hnsw.Add(obj, obj.Vector);
		}

		TestObject objToRemove = objects[50];
		objects[50] = objects[objects.Count - 1];
		objects.RemoveAt(objects.Count - 1);
		hnsw.Remove(objToRemove);

		Assert.Throws<InvalidOperationException>(() => hnsw.Remove(objToRemove));
		Assert.That(!om.GetAllObjects<HNSWNode>().Any(n => n.ReferenceId == objToRemove.Id));

		for (int i = 0; i < 90; i++)
		{
			int index = random.Next(objects.Count);
			TestObject obj = objects[index];
			hnsw.Remove(obj);
			objects[index] = objects[objects.Count - 1];
			objects.RemoveAt(objects.Count - 1);
		}

		trans.Commit();

		VerifyHNSW("MyHNSW");

	}

	[Test]
	public void TestDelete()
	{
		Debug.Assert(db != null);

		using VeloxDBTransaction trans = db.BeginTransaction();
		ObjectModel om = trans.ObjectModel;

		HNSW<TestObject>? hnsw = om.CreateVectorIndex<TestObject>("MyHNSW", dimension);
		ClassicAssert.NotNull(hnsw);
		Random random = new Random(2);
		List<TestObject> objects = GenerateObjects(om, 1000, dimension, random);

		foreach (TestObject obj in objects)
		{
			hnsw.Add(obj, obj.Vector);
		}

		om.DeleteVectorIndex("MyHNSW");

		Assert.That(!om.GetAllObjects<HNSW>().Any());
		Assert.That(!om.GetAllObjects<HNSWNode>().Any());
		Assert.That(!om.GetAllObjects<HNSWConnection>().Any());
		Assert.That(!om.GetAllObjects<HNSWConnectionLevelCount>().Any());
	}


	[TearDown]
    public void TearDown()
    {
        if (db != null)
        {
            db.Dispose();
            db = null;
        }
        // Delete temporary directory
        if (tempDir != null && tempDir.Exists)
        {
            tempDir.Delete(true);
        }
    }

	static List<TestObject> GenerateObjects(ObjectModel om, int count, int dim, Random random)
	{
		List<TestObject> result = new(count);
		for (int i = 0; i < count; i++)
		{
			float[] vector = RandomVector(random, dim);
			TestObject obj = om.CreateObject<TestObject>();
			obj.Name = $"Object {i}";
			obj.Vector = DatabaseArray<float>.FromSpan((ReadOnlySpan<float>)vector);
			result.Add(obj);
		}
		return result;
	}

	static float[] RandomVector(Random random, int dim)
	{
		float[] vector = new float[dim];
		for (int j = 0; j < vector.Length; j++)
		{
			vector[j] = random.NextSingle();
		}

		return vector;
	}

}
