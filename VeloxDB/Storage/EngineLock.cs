﻿using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Velox.Common;

namespace Velox.Storage;

internal sealed class EngineLock
{
	[ThreadStatic]
	static int ownershipCount;

	StorageEngine engine;
	MultiSpinRWLock sync;

	RWSpinLock drainSync;
	bool isDraining;

	public EngineLock(StorageEngine engine)
	{
		this.engine = engine;
		sync = new MultiSpinRWLock();
	}

	public bool IsReentrancy => ownershipCount > 0;
	public bool IsDraining => isDraining;

	public bool IsTakenReadOrWrite()
	{
		return ownershipCount > 0;
	}

	public void BorrowOwnerhsip()
	{
		if (ownershipCount > 0)
			throw new InvalidOperationException();

		ownershipCount++;
	}

	public void RelinquishOwnership()
	{
		ownershipCount--;

		if (ownershipCount > 0)
			throw new InvalidOperationException();
	}

	private void EnterDraining()
	{
		TTTrace.Write(engine.TraceId);

		drainSync.EnterWriteLock(); // Must be thread agnostic
		Checker.AssertFalse(isDraining);

		sync.EnterWriteLock();
		isDraining = true;
		sync.ExitWriteLock();

		engine.CancelAllTransactions();
		SpinWait.SpinUntil(() => !engine.HasTransactions());

		engine.Trace.Verbose("Engine entered draining mode.");
	}

	private void ExitDraining()
	{
		TTTrace.Write(engine.TraceId);

		isDraining = false;
		drainSync.ExitWriteLock();  // Must be thread agnostic

		engine.Trace.Verbose("Engine exited draining mode.");
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool TryCreateTransactionAndEnterReadLock(out int handle, int timeout = -1)
	{
		bool isReentrancy = ownershipCount > 0;
		if (!TryEnterReadLock(timeout, out handle))
			return false;

		if (isDraining && !isReentrancy)
		{
			ExitReadLock(handle);
			return false;
		}

		return true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int EnterReadLockNoDraining()
	{
		while (true)
		{
			int handle = EnterReadLock();

			if (!isDraining)
				return handle;

			ExitReadLock(handle);
			Thread.Sleep(1);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public int EnterReadLock()
	{
		int handle = -1;
		if (ownershipCount == 0)
			handle = sync.EnterReadLock();

		ownershipCount++;
		return handle;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool TryEnterReadLock(int timeout, out int handle)
	{
		handle = -1;
		if (ownershipCount == 0)
		{
			if (!sync.TryEnterReadLock(timeout, out handle))
			{
				handle = -1;
				return false;
			}
		}

		ownershipCount++;
		return true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitReadLock(int handle)
	{
		ownershipCount--;

		if (ownershipCount == 0)
		{
			Checker.AssertTrue(handle >= 0);
			sync.ExitReadLock(handle);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void EnterWriteLock(bool drainTransactions = false)
	{
		if (ownershipCount == 0)
		{
			engine.PreventPersistanceSnapshots();

			if (drainTransactions)
				EnterDraining();

			sync.EnterWriteLock();
		}

		ownershipCount++;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ThreadAgnosticLocker EnterWriteLockThreadAgnostic(bool drainTransactions)
	{
		ThreadAgnosticLocker lockOwnership = new ThreadAgnosticLocker(engine, this, drainTransactions);
		lockOwnership.Enter();
		return lockOwnership;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitWriteLock(bool enteredWithDraining = false)
	{
		ownershipCount--;

		if (ownershipCount == 0)
		{
			sync.ExitWriteLock();
			if (enteredWithDraining)
				ExitDraining();

			engine.AllowPersistanceSnapshots();
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitWriteLockThreadAgnostic(ThreadAgnosticLocker lockOwnership)
	{
		lockOwnership.Exit();
		engine.AllowPersistanceSnapshots();
	}

	public sealed class ThreadAgnosticLocker
	{
		JobWorkers<bool> locker;
		bool drainTransactions;

		public ThreadAgnosticLocker(StorageEngine engine, EngineLock engineLock, bool drainTransactions)
		{
			this.drainTransactions = drainTransactions;

			string name = string.Format("{0}: vlx-DatabaseThreadAgnosticLocker", engine.Trace.Name);
			locker = JobWorkers<bool>.Create(name, 1, b =>
			{
				if (b) { engineLock.EnterWriteLock(drainTransactions); }
				else { engineLock.ExitWriteLock(drainTransactions); }
			}, 1);
		}

		public void Enter()
		{
			locker.EnqueueWork(true);
			locker.Drain();
		}

		public void Exit()
		{
			locker.EnqueueWork(false);
			locker.WaitAndClose();
			locker = null;
		}
	}

}
