using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace VeloxDB.Common;

internal sealed class JobWorkers<TItem>
{
	string workerName;

	volatile Action<TItem>[] actions;
	volatile Action<List<TItem>>[] groupedActions;

	JobQueue<Item<TItem>> queue;
	List<Thread> workers;

	CountdownEvent drainEvent;
	volatile CountedManualResetEvent stopDrain;

	volatile Action<int> finalizer;

	private JobWorkers(string workerName, int workerCount, Action<TItem>[] actions,
		Action<List<TItem>>[] groupedActions, JobQueueMode mode, int maxItemCount = -1)
	{
		queue = new JobQueue<Item<TItem>>(maxItemCount != -1 ? maxItemCount : 32, mode, maxItemCount);

		this.workerName = workerName;
		workers = new List<Thread>(workerCount);
		this.actions = Utils.CreateCopy(actions);
		this.groupedActions = Utils.CreateCopy(groupedActions);

		for (int i = 0; i < workerCount; i++)
		{
			if (mode == JobQueueMode.Normal)
			{
				workers.Add(Utils.RunThreadWithSupressedFlow(Worker, i, workerName, true));
			}
			else
			{
				workers.Add(Utils.RunThreadWithSupressedFlow(GroupedWorker, i, workerName, true));
			}
		}
	}

	public int WorkerCount => workers.Count;

	public static JobWorkers<TItem> Create(string workerName, int workerCount, int maxItemCount = -1)
	{
		Checker.AssertTrue(workerCount > 0 && (maxItemCount == -1 || maxItemCount > 0));

		return new JobWorkers<TItem>(workerName, workerCount, null, null, JobQueueMode.Normal, maxItemCount);
	}

	public static JobWorkers<TItem> Create(string workerName, int workerCount, Action<TItem> action, int maxItemCount = -1)
	{
		Checker.AssertNotNull(action);
		Checker.AssertTrue(workerCount > 0 && (maxItemCount == -1 || maxItemCount > 0));

		return new JobWorkers<TItem>(workerName, workerCount,
			Enumerable.Range(0, workerCount).Select(x => action).ToArray(), null, JobQueueMode.Normal, maxItemCount);
	}

	public static JobWorkers<TItem> Create(string workerName, int workerCount, Action<TItem>[] actions, int maxItemCount = -1)
	{
		Checker.AssertNotNull(actions);
		Checker.AssertTrue(workerCount > 0 && (maxItemCount == -1 || maxItemCount > 0));

		return new JobWorkers<TItem>(workerName, workerCount, actions, null, JobQueueMode.Normal, maxItemCount);
	}

	public static void Execute(string workerName, int workerCount, Action<TItem> action, IEnumerable<TItem> items, int maxItemCount = -1)
	{
		Checker.AssertNotNull(action);

		if (workerCount == 0)
			return;

		if (workerCount == 1)
		{
			action(items.First());
		}
		else
		{
			Checker.AssertTrue(workerCount > 0 && (maxItemCount == -1 || maxItemCount > 0));
			JobWorkers<TItem> workers = new JobWorkers<TItem>(workerName, workerCount,
				Enumerable.Range(0, workerCount).Select(x => action).ToArray(), null, JobQueueMode.Normal, maxItemCount);
			foreach (TItem item in items)
			{
				workers.EnqueueWork(item);
			}

			workers.WaitAndClose();
		}
	}

	public static JobWorkers<TItem> CreateGrouped(string workerName, int workerCount, Action<List<TItem>> action, int maxItemCount = -1)
	{
		Checker.AssertNotNull(action);
		Checker.AssertTrue(maxItemCount == -1 || maxItemCount > 0);

		return new JobWorkers<TItem>(workerName, 1, null,
			Enumerable.Range(0, workerCount).Select(x => action).ToArray(), JobQueueMode.Grouped, maxItemCount);
	}

	public static JobWorkers<TItem> CreateGrouped(string workerName, int workerCount, Action<List<TItem>>[] actions, int maxItemCount = -1)
	{
		Checker.AssertNotNull(actions);
		Checker.AssertTrue(maxItemCount == -1 || maxItemCount > 0);

		return new JobWorkers<TItem>(workerName, 1, null, actions, JobQueueMode.Grouped, maxItemCount);
	}

	public void SetMaxItemCount(int maxItemCount)
	{
		queue.SetMaxItemCount(maxItemCount);
	}

	public void SetAction(Action<TItem> action)
	{
		this.actions = Enumerable.Range(0, WorkerCount).Select(x => action).ToArray();
		Drain();
	}

	public void SetActions(Action<TItem>[] actions)
	{
		Checker.AssertTrue(actions.Length == workers.Count);
		this.actions = actions;
		Drain();
	}

	public void SetThreadPriority(ThreadPriority priority)
	{
		for (int i = 0; i < workers.Count; i++)
		{
			workers[i].Priority = priority;
		}
	}

	public void EnqueueWork(TItem item)
	{
		queue.Enqueue(new Item<TItem>(item));
	}

	public void EnqueueWork(IEnumerable<TItem> items)
	{
		foreach (TItem item in items)
		{
			EnqueueWork(item);
		}
	}

	public bool TryEnqueueWork(TItem item, int timeout, bool immediateWorkerRequired = false)
	{
		return queue.TryEnqueue(new Item<TItem>(item), timeout, immediateWorkerRequired);
	}

	public void WaitAndClose(Action<int> finalizer = null)
	{
		this.finalizer = finalizer;

		List<Thread> tempWorkers = new List<Thread>(workers);
		for (int i = 0; i < workers.Count; i++)
		{
			queue.TryEnqueue(new Item<TItem>(Item<TItem>.ItemType.Termination), -1);

			// In grouped mode, worker threads dequeue all the items in the queue. For this reason we must not enqueue all the
			// termination commands at once, but instead for each termination command wait for a single worker to finish.
			if (queue.Mode == JobQueueMode.Grouped)
				JoinAndRemoveSingleWorker(tempWorkers);
		}

		if (queue.Mode != JobQueueMode.Grouped)
		{
			for (int i = 0; i < workers.Count; i++)
			{
				workers[i].Join();
			}
		}

		queue.Dispose();
		finalizer = null;
	}

	public void Drain()
	{
		if (queue.Mode == JobQueueMode.Grouped)
			throw new NotSupportedException("Grouped mode workers do not support draining.");

		drainEvent = new CountdownEvent(workers.Count);
		stopDrain = new CountedManualResetEvent(workers.Count);

		for (int i = 0; i < workers.Count; i++)
		{
			queue.Enqueue(new Item<TItem>(Item<TItem>.ItemType.Drain));
		}

		drainEvent.Wait();
		stopDrain.Set();
		drainEvent.Dispose();
		drainEvent = null;
	}

	private void JoinAndRemoveSingleWorker(List<Thread> workers)
	{
		while (true)
		{
			for (int i = 0; i < workers.Count; i++)
			{
				if (workers[i].Join(1))
				{
					workers.RemoveAt(i);
					return;
				}
			}
		}
	}

	private void Worker(object state)
	{
		int index = (int)state;
		Action<TItem> action = actions == null ? null : actions[index];

		while (true)
		{
			Item<TItem> item = queue.Dequeue();

			if (item.Type == Item<TItem>.ItemType.Action)
			{
				action(item.Value);
				continue;
			}

			if (item.Type == Item<TItem>.ItemType.Termination)
			{
				finalizer?.Invoke(index);
				return;
			}

			if (item.Type == Item<TItem>.ItemType.Drain)
			{
				CountedManualResetEvent sd = stopDrain;
				drainEvent.Signal();
				sd.Wait();
				action = actions == null ? null : actions[index];
				continue;
			}
		}
	}

	private void GroupedWorker(object state)
	{
		int index = (int)state;
		Action<List<TItem>> action = groupedActions == null ? null : groupedActions[index];

		List<Item<TItem>> workItems = new List<Item<TItem>>(16);
		List<TItem> items = new List<TItem>(16);
		while (true)
		{
			queue.DequeueAll(workItems);

			bool shouldFinish = false;
			if (workItems[workItems.Count - 1].Type == Item<TItem>.ItemType.Termination)
			{
				shouldFinish = true;
				workItems.RemoveAt(workItems.Count - 1);
			}

			if (workItems.Count > 0)
			{
				for (int i = 0; i < workItems.Count; i++)
				{
					items.Add(workItems[i].Value);
				}

				action(items);
			}

			items.Clear();
			workItems.Clear();

			if (shouldFinish)
			{
				finalizer?.Invoke(index);
				return;
			}
		}
	}

	private sealed class CountedManualResetEvent
	{
		int count;
		ManualResetEvent signal;

		public CountedManualResetEvent(int count)
		{
			signal = new ManualResetEvent(false);
			this.count = count;
		}

		public void Set()
		{
			signal.Set();
		}

		public void Wait()
		{
			signal.WaitOne();
			if (Interlocked.Decrement(ref count) == 0)
				signal.Dispose();
		}
	}

	private struct Item<T>
	{
		public enum ItemType
		{
			Action,
			Termination,
			Drain
		}

		public T Value { get; private set; }
		public ItemType Type { get; private set; }

		public Item(T value)
		{
			this.Value = value;
			this.Type = ItemType.Action;
		}

		public Item(ItemType type)
		{
			this.Value = default(T);
			this.Type = type;
		}
	}
}
