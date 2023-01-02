using System;
using System.Collections.Generic;
using System.IO;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage.Replication;

namespace VeloxDB.Storage;

#if TEST_BUILD
internal unsafe sealed partial class StorageEngine
{
	internal void WaitGCComplete()
	{
		for (int i = 0; i < databases.Length; i++)
		{
			databases[i].TryFlushGCAndWaitCompletion(0);
		}
	}

	internal void DrainPersistenceSnapshots()
	{
		snapshotController.Block();
		for (int i = 0; i < databases.Length; i++)
		{
			databases[i].DrainPersistenceSnapshot();
		}

		snapshotController.Unblock();
	}

	internal void CreateSnapshot(long databaseId, int logIndex)
	{
		databases[databaseId].Persister.CreateLogSnapshotAfterRestore(logIndex + 1);	// Skip master log file
	}

	internal void PreventLogging(long databaseId, int logIndex)
	{
		databases[databaseId].Persister.PreventLogging(logIndex + 1);	// Skip master log file
	}

	internal void ValidateGarbage()
	{
		for (int i = 0; i < databases.Length; i++)
		{
			databases[i].ValidateGarbage();
		}
	}

	internal void Validate(Transaction tran)
	{
		if (tran != null)
		{
			if (tran.Type == TransactionType.ReadWrite)
				throw new InvalidOperationException();

			TTTrace.Write(traceId, tran.Id);
		}

		TTTrace.Flush();

		DrainPersistenceSnapshots();
		for (int i = 0; i < databases.Length; i++)
		{
			databases[i].ValidateGarbage(1);
		}

		Dictionary<ulong, int> strings = new Dictionary<ulong, int>(1024);
		Dictionary<ulong, int> blobs = new Dictionary<ulong, int>(1024);

		for (int i = 0; i < databases.Length; i++)
		{
			databases[i].ValidateAndCollectBlobs(tran, strings, blobs);
		}

		stringStorage.ValidateRefCounts(strings);
		BlobStorage.Validate(blobs);

		contextPool.Validate();
	}

	internal bool TryFlushAndWaitGCComplete()
	{
		for (int i = 0; i < databases.Length; i++)
		{
			if (!databases[i].TryFlushGCAndWaitCompletion(0))
				return false;
		}

		return true;
	}

	public void DrainGC()
	{
		LocalSystemDatabase.DrainGC();
		GlobalSystemDatabase.DrainGC();
		UserDatabase.DrainGC();
	}

	public ulong GetReadVersion(long databaseId)
	{
		return databases[databaseId].ReadVersion;
	}

	internal IReplicator GetReplicator()
	{
		return replicator;
	}

	public bool ObjectExists(ClassDescriptor cd, long id)
	{
		Database db = databases[DatabaseId.User];
		Class @class = db.GetClass(cd.Index).MainClass;

		ulong commitVersion;
		bool b = (@class as Class).CommitedObjectExistsInDatabase(id, out commitVersion);

		if (db.ReadVersion >= commitVersion)
			return b;

		return false;
	}

	internal uint GetTerm(long databaseId)
	{
		return databases[databaseId].LocalTerm;
	}

	internal T GetReplica<T>(string replicaName) where T : IReplica
	{
		return (T)replicator.FindReplica(replicaName);
	}

	public List<ObjectReader> TestScan(Transaction tran, ClassDescriptor cd)
	{
		ClassBase @class = tran.Database.GetClass(cd.Index, out ClassLocker locker);
		return @class.TestScan(tran);
	}

	public long PickRandomObject(Transaction tran, ClassDescriptor classDesc,
		bool includeInherited, Func<long, long> rand, out ObjectReader r)
	{
		ClassBase @class = UserDatabase.GetClassById(classDesc.Id);
		return @class.PickRandomObject(tran, classDesc, includeInherited, rand, out r);
	}
}
#endif
