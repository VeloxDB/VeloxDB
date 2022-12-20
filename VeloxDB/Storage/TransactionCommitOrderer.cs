using System;
using System.Runtime.CompilerServices;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal delegate void TransactionOrderedCallback(Transaction tran);

internal sealed class TransactionCommitOrderer
{
	ArrayQueue<Transaction> queue;

	public TransactionCommitOrderer(StorageEngineSettings settings)
	{
		queue = new ArrayQueue<Transaction>(settings.CommitWorkerCount + 1);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void TranCommited(Transaction tran, TransactionOrderedCallback finalizedCallback)
	{
		finalizedCallback(tran);
		tran.CommitEvent.Set();

		ulong commitVersion = tran.CommitVersion;
		while (Dequeue(commitVersion + 1, out Transaction p))
		{
			commitVersion++;
			finalizedCallback(p);
			p.CommitEvent.Set();
		}
	}

	public void AddPending(Transaction item)
	{
		queue.Enqueue(null);
		int i = queue.Count - 1;
		while (i > 0 && queue[i - 1].CommitVersion > item.CommitVersion)
		{
			queue[i] = queue[i - 1];
			i--;
		}

		queue[i] = item;
	}

	private bool Dequeue(ulong version, out Transaction tran)
	{
		if (queue.Count == 0)
		{
			tran = null;
			return false;
		}

		tran = queue.Peek();
		if (tran.CommitVersion != version)
			return false;

		queue.Dequeue();
		return true;
	}
}
