using System;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage.Replication;

internal sealed class UnreplicatedReplicator : IReplicator
{
	StorageEngine engine;
	ReplicationDescriptor replicationDesc;

	public UnreplicatedReplicator(StorageEngine engine, ReplicationSettings replicationSettings)
	{
		this.engine = engine;
		replicationDesc = new ReplicationDescriptor(replicationSettings);
	}

	public ReplicationDescriptor ReplicationDesc => replicationDesc;

	public void CommitTransaction(Transaction tran)
	{
	}

	public IAsyncCleanup Dispose()
	{
		return new Cleanup();
	}

	public bool IsTransactionAllowed(long databaseId, TransactionSource source, IReplica sourceReplica, TransactionType type, out DatabaseErrorDetail reason)
	{
		reason = null;
		return true;
	}

	public void PostTransactionCommit(Transaction tran, bool isCommited)
	{
	}

	public void PreTransactionCommit(Transaction tran)
	{
	}

	public void Start()
	{
		engine.NodeWriteStateUpdated(true);
	}

	public void TransactionFailed()
	{
	}

#if TEST_BUILD
	public IReplica FindReplica(string replicaName)
	{
		return null;
	}

	public void DrainLocks()
	{
		throw new NotImplementedException();
	}
#endif

	private sealed class Cleanup : IAsyncCleanup
	{
		public void WaitCleanup()
		{
		}
	}
}
