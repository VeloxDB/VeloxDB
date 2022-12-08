using System;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.Storage.Replication;

internal sealed class UnreplicatedReplicator : IReplicator
{
	ReplicationDescriptor replicationDesc;

	public UnreplicatedReplicator(ReplicationSettings replicationSettings)
	{
		replicationDesc = new ReplicationDescriptor(replicationSettings);
	}

	public ReplicationDescriptor ReplicationDesc => replicationDesc;

	public void CommittingTransaction(Transaction tran)
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

	public void PostTransactionCommit(Transaction tran, bool isCommited, int handle)
	{
		Checker.AssertTrue(handle == -1);
	}

	public void PreTransactionCommit(Transaction tran, out int handle)
	{
		handle = -1;
	}

	public void Start()
	{
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
