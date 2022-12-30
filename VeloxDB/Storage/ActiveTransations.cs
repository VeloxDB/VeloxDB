using System;
using System.Linq;
using System.Runtime.CompilerServices;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal sealed class ActiveTransations
{
	MultiSpinLock sync;
	Queue[] perCPUQueues;

	public ActiveTransations()
	{
		sync = new MultiSpinLock();
		perCPUQueues = new Queue[ProcessorNumber.CoreCount];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddTran(Transaction tran)
	{
		tran.ActiveListIndex = sync.Enter();
		try
		{
			// Creating the queue here (lazy) will likely create it in the GC heap of the current CPU (away from other queues)
			perCPUQueues[tran.ActiveListIndex] ??= new Queue();
			perCPUQueues[tran.ActiveListIndex].AddTran(tran);
		}
		finally
		{
			sync.Exit(tran.ActiveListIndex);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Remove(Transaction tran)
	{
		sync.Enter(tran.ActiveListIndex);
		try
		{
			perCPUQueues[tran.ActiveListIndex].Remove(tran);
		}
		finally
		{
			sync.Exit(tran.ActiveListIndex);
		}

		tran.ActiveListIndex = -1;
	}

	public bool IsEmpty
	{
		get
		{
			for (int i = 0; i < perCPUQueues.Length; i++)
			{
				if (perCPUQueues[i] != null && !perCPUQueues[i].isEmpty)
					return false;
			}

			return true;
		}
	}

	public Transaction CancelTransactions()
	{
		Transaction res = null;
		for (int i = 0; i < perCPUQueues.Length; i++)
		{
			sync.Enter(i);
			try
			{
				if (perCPUQueues[i] != null)
					perCPUQueues[i].CancelTransactions();

			}
			finally
			{
				sync.Exit(i);
			}
		}

		return res;
	}

	public Transaction GetOldestReader()
	{
		Transaction res = null;
		for (int i = 0; i < perCPUQueues.Length; i++)
		{
			if (perCPUQueues[i] != null)
			{
				Transaction curr = perCPUQueues[i].Oldest;
				if (curr != null && (res == null || res.ReadVersion > curr.ReadVersion))
					res = curr;
			}
		}

		return res;
	}

	public void Dispose()
	{
		sync.Dispose();
	}

	private sealed class Queue
	{
		Transaction head;
		Transaction tail;

		public bool isEmpty => head == null;
		public Transaction Oldest => tail;
		public int Count
		{
			get
			{
				int c = 0;
				Transaction t = head;
				while (t != null)
				{
					c++;
					t = t.NextActiveTran;
				}

				return c;
			}
		}

		public Queue()
		{
		}

		public void AddTran(Transaction tran)
		{
			TTTrace.Write(tran.Engine.TraceId, tran.Id, tran.ReadVersion);

			tran.NextActiveTran = head;
			if (head != null)
			{
				head.PrevActiveTran = tran;
			}
			else
			{
				tail = tran;
			}

			head = tran;
		}

		public void Remove(Transaction tran)
		{
			TTTrace.Write(tran.Engine.TraceId, tran.Id, tran.ReadVersion, tran.CommitVersion);

			if (tran.NextActiveTran == null)
			{
				tail = tran.PrevActiveTran;
			}
			else
			{
				tran.NextActiveTran.PrevActiveTran = tran.PrevActiveTran;
			}

			if (tran.PrevActiveTran == null)
			{
				head = tran.NextActiveTran;
			}
			else
			{
				tran.PrevActiveTran.NextActiveTran = tran.NextActiveTran;
			}
		}

		public void CancelTransactions()
		{
			Transaction t = head;
			while (t != null)
			{
				t.CancelRequested = true;
				t = t.NextActiveTran;
			}
		}
	}

#if TEST_BUILD
	public int Count => perCPUQueues.Where(x => x != null).Select(x => x.Count).Sum();
#endif
}
