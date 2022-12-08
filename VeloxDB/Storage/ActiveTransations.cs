using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Velox.Common;

namespace Velox.Storage;

internal sealed class ActiveTransations : IEnumerable<Transaction>
{
	long traceId;
	Transaction queueHead;

	public ActiveTransations(long traceId)
	{
		this.traceId = traceId;
	}

	public bool IsEmpty => queueHead == null;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddTran(Transaction tran)
	{
		TTTrace.Write(traceId, tran.Id, tran.ReadVersion);

		tran.NextActiveTran = queueHead;
		if (queueHead != null)
			queueHead.PrevActiveTran = tran;

		queueHead = tran;
	}

	public Transaction TransactionCompleted(Transaction tran)
	{
		TTTrace.Write(traceId, tran.Id, tran.ReadVersion, tran.CommitVersion);

		Transaction res;
		if (tran.NextActiveTran == null)
		{
			res = tran.PrevActiveTran;
		}
		else
		{
			res = null;
			tran.NextActiveTran.PrevActiveTran = tran.PrevActiveTran;
		}

		if (tran.PrevActiveTran != null)
		{
			tran.PrevActiveTran.NextActiveTran = tran.NextActiveTran;
		}
		else
		{
			queueHead = tran.NextActiveTran;
		}

		return res;
	}

	public IEnumerator<Transaction> GetEnumerator()
	{
		Transaction curr = queueHead;
		while (curr != null)
		{
			yield return curr;
			curr = curr.NextActiveTran;
		}
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		return GetEnumerator();
	}

#if TEST_BUILD
	public int Count => this.Count();

	public Transaction GetOldestReader()
	{
		return this.LastOrDefault();
	}
#endif
}
