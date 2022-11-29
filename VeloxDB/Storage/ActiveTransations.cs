using System;
using System.Collections;
using System.Collections.Generic;
using Velox.Common;

namespace Velox.Storage;

internal sealed class ActiveTransations : IEnumerable<Transaction>
{
	long traceId;
	int count;
	ArrayQueue<Transaction> queue;

	public ActiveTransations(long traceId)
	{
		this.traceId = traceId;
		queue = new ArrayQueue<Transaction>(1024, (t, i) => { if (t != null) t.ActiveListIndex = i; });
	}

	public bool IsEmpty => count == 0;
	public int Count => count;

	public void AddTran(Transaction tran)
	{
		TTTrace.Write(traceId, tran.Id, tran.ReadVersion, queue.Count);
		tran.ActiveListIndex = queue.Enqueue(tran);
		count++;
	}

	public void TransactionCompleted(Transaction tran, ulong engineReadVersion)
	{
		TTTrace.Write(traceId, tran.Id, tran.ReadVersion, tran.CommitVersion, tran.ActiveListIndex, queue.Count, engineReadVersion);

		int index = tran.ActiveListIndex;
		Checker.AssertTrue(object.ReferenceEquals(queue.GetAtAbsolute(index), tran));

		queue.SetAtAbsolute(index, null);
		count--;

		while (queue.Count > 0 && queue.Peek() == null)
		{
			queue.Dequeue();
		}

		Checker.AssertFalse(count == 0 && queue.Count > 0);
		Checker.AssertTrue(queue.Count >= count);

		TTTrace.Write(traceId, tran.Id, queue.Count == 0 ? 0 : queue.Peek().ReadVersion, queue.Count == 0 ? 0 : queue.Peek().Id);
	}

	public Transaction GetFirst()
	{
		if (queue.Count == 0)
			return null;

		return queue.Peek();
	}

	public IEnumerator<Transaction> GetEnumerator()
	{
		for (int i = 0; i < queue.Count; i++)
		{
			Transaction tran = queue[i];
			if (tran != null)
				yield return tran;
		}
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		return GetEnumerator();
	}
}
