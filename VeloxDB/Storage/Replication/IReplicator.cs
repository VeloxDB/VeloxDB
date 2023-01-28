using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage.Replication.HighAvailability;

namespace VeloxDB.Storage.Replication;

internal enum ReplicatedDatabases
{
	Global = 1,
	All = 2,
}

internal interface IAsyncCleanup
{
	void WaitCleanup();
}

internal interface IReplicator
{
	public ReplicationDescriptor ReplicationDesc { get; }
	void Start();
	bool IsTransactionAllowed(long databaseId, TransactionSource source, IReplica sourceReplica, TransactionType type, out DatabaseErrorDetail reason);
	void PreTransactionCommit(Transaction tran);
	void PostTransactionCommit(Transaction tran, bool isCommited);
	void CommitTransaction(Transaction tran);
	void TransactionFailed();
	IAsyncCleanup Dispose();

#if TEST_BUILD
	internal IReplica FindReplica(string replicaName);
	internal void DrainLocks();
#endif
}

internal interface IReplicatorFactory
{
	const string replicationAssemblyName = "vlxrep";

	IReplicator CreateReplicator(StorageEngine engine, ReplicationSettings replicationSettings,
		ILeaderElector localElector, ILeaderElector globalElector);

	public static Assembly FindReplicatorAssembly()
	{
		Assembly assembly = AppDomain.CurrentDomain.GetAssemblies().
			FirstOrDefault(x => string.Equals(x.GetName().Name, replicationAssemblyName, StringComparison.OrdinalIgnoreCase));
		if (assembly != null)
			return assembly;

		string filePath = Path.GetFullPath(replicationAssemblyName + ".dll");
		if (File.Exists(filePath))
			return Assembly.LoadFile(filePath);

		return null;
	}
}
