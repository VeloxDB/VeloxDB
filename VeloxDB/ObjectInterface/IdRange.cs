using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Velox.Common;
using Velox.Storage;

namespace Velox.ObjectInterface;

internal sealed class IdRange
{
	const long idRangeSize = 1002173;
	const int reqIdRangeSize = 331;
	const long nextIdRangeLimit = (long)(idRangeSize * 0.2);

	readonly object sync = new object();
	readonly object querySync = new object();

	StorageEngine engine;

	long currId;
	long idRangeLimit;
	bool nextRangeScheduled = false;

	DatabaseException rangeError;
	long nextRange;

	WaitCallback nextRangeWorker;

	public IdRange(StorageEngine engine)
	{
		this.engine = engine;
		nextRangeWorker = x => ScheduledPrepareNextRange();
	}

	public long GetIdRange(out long count)
	{
		count = reqIdRangeSize;

		lock (sync)
		{
			while (true)
			{
				if (currId != idRangeLimit)
					break;

				if (rangeError != null)
				{
					Checker.AssertTrue(nextRange == 0);
					DatabaseException e = rangeError;
					rangeError = null;
					throw e;
				}

				if (nextRange != 0)
				{
					currId = nextRange;
					idRangeLimit = currId + idRangeSize;
					nextRange = 0;
					break;
				}

				PrepareNextRange();
			}

			long result = currId;
			if (currId + count <= idRangeLimit)
			{
				currId += count;
			}
			else
			{
				count = idRangeLimit - currId;
				currId = idRangeLimit;
			}

			if (idRangeLimit - currId < nextIdRangeLimit && nextRange == 0 && !nextRangeScheduled)
				ScheduleNextRange();

			return result;
		}
	}

	private void ScheduleNextRange()
	{
		nextRangeScheduled = true;
		ThreadPool.UnsafeQueueUserWorkItem(nextRangeWorker, null);
	}

	private void ScheduledPrepareNextRange()
	{
		PrepareNextRange();
		nextRangeScheduled = false;
	}

	private void PrepareNextRange()
	{
		lock (querySync)
		{
			if (currId != idRangeLimit || rangeError != null || nextRange != 0)
				return;

			try
			{
				nextRange = engine.ReserveIdRange(idRangeSize);
			}
			catch (DatabaseException e)
			{
				rangeError = e;
			}
		}
	}
}
