using System;
using System.Threading;
using Velox.Common;

namespace VeloxDB.Storage.Persistence;

internal sealed class SnapshotSemaphore
{
	ReaderWriterLockSlim sync = new ReaderWriterLockSlim();
	int count;

	public SnapshotSemaphore()
	{
		count = 0;
	}

	public void Block()
	{
		sync.EnterWriteLock();
		try
		{
			count++;
		}
		finally
		{
			sync.ExitWriteLock();
		}
	}

	public void Unblock()
	{
		sync.EnterWriteLock();
		try
		{
			count--;
			if (count < 0)
				throw new CriticalDatabaseException();
		}
		finally
		{
			sync.ExitWriteLock();
		}
	}

	public bool Enter(out IDisposable @lock)
	{
		sync.EnterReadLock();
		if (count == 0)
		{
			@lock = new SnapshotLock(this);
			return true;
		}

		sync.ExitReadLock();
		@lock = null;
		return false;
	}

	private void Exit()
	{
		sync.ExitReadLock();
	}

	private sealed class SnapshotLock : IDisposable
	{
		SnapshotSemaphore semaphore;

		public SnapshotLock(SnapshotSemaphore semaphore)
		{
			this.semaphore = semaphore;
		}

		public void Dispose()
		{
			semaphore.Exit();
		}
	}
}
