using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace VeloxDB.Common;

internal struct RWLock
{
	const int spinCount = 500;

	// rh - reader signal handle
	// wh - writer signal handle
	// crw - count of waiting readers
	// cww - count of waiting writers
	// cr - count of entered readers
	// cw - count of entered writers
	long rh_wh_crw_cww_cr_cw;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool TryChangeState(long newState, long prevState)
	{
		return Interlocked.CompareExchange(ref rh_wh_crw_cww_cr_cw, newState, prevState) == prevState;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void EnterReadLock()
	{
		long state = rh_wh_crw_cww_cr_cw;
		int cw = GetCW(state);
		int cww = GetCWW(state);
		if (cw == 0 && cww == 0 && TryChangeState(MakeState(0, 0, 0, 0, GetCR(state) + 1, 0), state))
		{
			Checker.AssertTrue(GetCRW(state) == 0 && GetRH(state) == 0);
			return;
		}

		EnterReadLockContested();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool TryEnterReadLock()
	{
		long state = rh_wh_crw_cww_cr_cw;
		int cw = GetCW(state);
		int cww = GetCWW(state);
		return cw == 0 && cww == 0 && TryChangeState(MakeState(0, 0, 0, 0, GetCR(state) + 1, 0), state);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public unsafe static bool TryEnterReadLock(RWLock* p, int timeout)
	{
		return SpinWait.SpinUntil(() => p->TryEnterReadLockSingle(), timeout);
	}

	private bool TryEnterReadLockSingle()
	{
		long state = rh_wh_crw_cww_cr_cw;
		int cw = GetCW(state);
		int cww = GetCWW(state);
		if (cw == 0 && cww == 0 && TryChangeState(MakeState(0, 0, 0, 0, GetCR(state) + 1, 0), state))
		{
			Checker.AssertTrue(GetCRW(state) == 0 && GetRH(state) == 0);
			return true;
		}

		return false;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void EnterReadLockContested()
	{
		int spinsLeft = spinCount;
		while (true)
		{
			long state = rh_wh_crw_cww_cr_cw;
			int cw = GetCW(state);
			int cww = GetCWW(state);
			while (spinsLeft > 0 || (cw == 0 && cww == 0))
			{
				if (cw == 0 && cww == 0 && TryChangeState(MakeState(0, 0, 0, 0, GetCR(state) + 1, 0), state))
				{
					Checker.AssertTrue(GetCRW(state) == 0 && GetRH(state) == 0);
					return;
				}

				state = rh_wh_crw_cww_cr_cw;
				cw = GetCW(state);
				cww = GetCWW(state);
				spinsLeft--;
			}

			int crw = GetCRW(state);
			int cr = GetCR(state);
			Checker.AssertTrue(cw != 0 || cww != 0);
			Checker.AssertTrue(cr == 0 || cw == 0);
			Checker.AssertTrue(cr != 0 || cw != 0);

			int rh;
			if (crw == 0)
			{
				rh = ManualEventPool.Alloc();
				if (ManualEventPool.Get(rh).IsSignaled())
					throw new InvalidOperationException();

				if (!TryChangeState(MakeState(rh, GetWH(state), 1, cww, cr, cw), state))
				{
					ManualEventPool.Get(rh).DecRefCount(true);
					continue;
				}
			}
			else
			{
				rh = GetRH(state);
				bool incSucceeded = ManualEventPool.Get(rh).TryIncRefCount();
				if (!incSucceeded)
					continue;

				if (!TryChangeState(MakeState(rh, GetWH(state), crw + 1, cww, cr, cw), state))
				{
					ManualEventPool.Get(rh).DecRefCount(incSucceeded);
					continue;
				}
			}

			ManualEventPool.Get(rh).Wait();
			ManualEventPool.Get(rh).DecRefCount(true);
			break;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool IsWriteLockTaken(RWLock sync)
	{
		return GetCW(sync.rh_wh_crw_cww_cr_cw) != 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void EnterWriteLock()
	{
		long state = rh_wh_crw_cww_cr_cw;
		int cw = GetCW(state);
		int cr = GetCR(state);
		if (cw == 0 && cr == 0 && TryChangeState(MakeState(0, 0, 0, 0, 0, 1), state))
		{
			Checker.AssertTrue(GetCWW(state) == 0 && GetWH(state) == 0);
			Checker.AssertTrue(GetCRW(state) == 0 && GetRH(state) == 0);
			return;
		}

		EnterWriteLockContested();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool TryEnterWriteLock()
	{
		long state = rh_wh_crw_cww_cr_cw;
		int cw = GetCW(state);
		int cr = GetCR(state);
		return cw == 0 && cr == 0 && TryChangeState(MakeState(0, 0, 0, 0, 0, 1), state);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void EnterWriteLockContested()
	{
		int spinsLeft = spinCount;
		while (true)
		{
			long state = rh_wh_crw_cww_cr_cw;
			int cw = GetCW(state);
			int cr = GetCR(state);

			while (spinsLeft > 0 || (cw == 0 && cr == 0))
			{
				if (cw == 0 && cr == 0 && TryChangeState(MakeState(0, 0, 0, 0, 0, 1), state))
				{
					Checker.AssertTrue(GetCWW(state) == 0 && GetWH(state) == 0);
					Checker.AssertTrue(GetCRW(state) == 0 && GetRH(state) == 0);
					return;
				}

				state = rh_wh_crw_cww_cr_cw;
				cw = GetCW(state);
				cr = GetCR(state);
				spinsLeft--;
			}

			int cww = GetCWW(state);
			int wh;

			if (cww == 0)
			{
				wh = SemaphorePool.Alloc();
				Checker.AssertTrue(!SemaphorePool.Get(wh).IsSignaled());

				if (!TryChangeState(MakeState(GetRH(state), wh, GetCRW(state), cww + 1, cr, cw), state))
				{
					SemaphorePool.Get(wh).DecRefCount(true);
					continue;
				}
			}
			else
			{
				wh = GetWH(state);
				bool incSucceeded = SemaphorePool.Get(wh).TryIncRefCount();
				if (!incSucceeded)
					continue;

				if (!TryChangeState(MakeState(GetRH(state), wh, GetCRW(state), cww + 1, cr, cw), state))
				{
					SemaphorePool.Get(wh).DecRefCount(incSucceeded);
					continue;
				}
			}

			SemaphorePool.Get(wh).Wait();
			SemaphorePool.Get(wh).DecRefCount(true);
			break;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitReadLock()
	{
		long state = rh_wh_crw_cww_cr_cw;
		int cr = GetCR(state);
		int cww = GetCWW(state);
		Checker.AssertTrue(GetCW(state) == 0 && cr > 0);
		if ((cr > 1 || cww == 0) && TryChangeState(MakeState(GetRH(state), GetWH(state), GetCRW(state), cww, cr - 1, 0), state))
		{
			if (cww == 0)
				Checker.AssertTrue(GetCRW(state) == 0 && GetRH(state) == 0 && GetWH(state) == 0);

			return;
		}

		ExitReadLockContested();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void ExitReadLockContested()
	{
		while (true)
		{
			long state = rh_wh_crw_cww_cr_cw;
			int cr = GetCR(state);
			int cww = GetCWW(state);
			Checker.AssertTrue(GetCW(state) == 0 && cr > 0);

			if (cr > 1 || cww == 0)
			{
				if (TryChangeState(MakeState(GetRH(state), GetWH(state), GetCRW(state), cww, cr - 1, 0), state))
				{
					if (cww == 0)
						Checker.AssertTrue(GetCRW(state) == 0 && GetRH(state) == 0 && GetWH(state) == 0);

					return;
				}

				continue;
			}

			int wh = GetWH(state);
			if (cww == 1)
			{
				if (!TryChangeState(MakeState(GetRH(state), 0, GetCRW(state), 0, 0, 1), state))
					continue;
			}
			else
			{
				if (!TryChangeState(MakeState(GetRH(state), wh, GetCRW(state), cww - 1, 0, 1), state))
					continue;
			}

			SemaphorePool.Get(wh).Set();
			break;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ExitWriteLock()
	{
		long state = rh_wh_crw_cww_cr_cw;
		int crw = GetCRW(state);
		int cww = GetCWW(state);
		Checker.AssertTrue(GetCR(state) == 0 && GetCW(state) == 1);
		if (cww == 0 && crw == 0 && TryChangeState(MakeState(0, 0, 0, 0, 0, 0), state))
		{
			Checker.AssertTrue(GetRH(state) == 0 && GetWH(state) == 0);
			return;
		}

		ExitWriteLockContested();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void DowngradeWriteToReadLock()
	{
		long state = rh_wh_crw_cww_cr_cw;
		int crw = GetCRW(state);
		int cww = GetCWW(state);
		int wh = GetWH(state);
		Checker.AssertTrue(GetCR(state) == 0 && GetCW(state) == 1);
		if (crw == 0 && TryChangeState(MakeState(0, wh, 0, cww, 1, 0), state))
		{
			Checker.AssertTrue(GetRH(state) == 0);
			return;
		}

		DowngradeWriteToReadLockContested();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void ExitWriteLockContested()
	{
		while (true)
		{
			long state = rh_wh_crw_cww_cr_cw;
			int crw = GetCRW(state);
			int cww = GetCWW(state);
			Checker.AssertTrue(GetCR(state) == 0 && GetCW(state) == 1);
			if (cww == 0 && crw == 0)
			{
				if (TryChangeState(MakeState(0, 0, 0, 0, 0, 0), state))
				{
					Checker.AssertTrue(GetRH(state) == 0 && GetWH(state) == 0);
					return;
				}

				continue;
			}

			int wh = GetWH(state);
			if (cww == 1)
			{
				if (!TryChangeState(MakeState(GetRH(state), 0, GetCRW(state), 0, 0, 1), state))
					continue;

				SemaphorePool.Get(wh).Set();
			}
			else if (cww > 1)
			{
				if (!TryChangeState(MakeState(GetRH(state), wh, GetCRW(state), cww - 1, 0, 1), state))
					continue;

				SemaphorePool.Get(wh).Set();
			}
			else
			{
				Checker.AssertTrue(wh == 0);
				int rh = GetRH(state);
				if (!TryChangeState(MakeState(0, 0, 0, 0, crw, 0), state))
					continue;

				ManualEventPool.Get(rh).Set();
			}

			break;
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void DowngradeWriteToReadLockContested()
	{
		while (true)
		{
			long state = rh_wh_crw_cww_cr_cw;
			int crw = GetCRW(state);
			int cww = GetCWW(state);
			int wh = GetWH(state);
			Checker.AssertTrue(GetCR(state) == 0 && GetCW(state) == 1);
			if (crw == 0)
			{
				if (TryChangeState(MakeState(0, wh, 0, cww, 1, 0), state))
				{
					Checker.AssertTrue(GetRH(state) == 0);
					return;
				}

				continue;
			}

			int rh = GetRH(state);
			Checker.AssertTrue(rh != 0);
			if (!TryChangeState(MakeState(0, wh, 0, cww, crw + 1, 0), state))
				continue;

			ManualEventPool.Get(rh).Set();

			break;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static int GetRH(long state)
	{
		return (int)(state >> 50) & 0x3fff;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static int GetWH(long state)
	{
		return (int)(state >> 40) & 0x03ff;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static int GetCRW(long state)
	{
		return (int)(state >> 26) & 0x3fff;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static int GetCWW(long state)
	{
		return (int)(state >> 16) & 0x03ff;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static int GetCR(long state)
	{
		return (int)(state >> 1) & 0x7fff;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static int GetCW(long state)
	{
		return (int)state & 0x01;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static long MakeState(int rh, int wh, int crw, int cww, int cr, int cw)
	{
		long t1 = ((long)rh << 50) | ((long)wh << 40) | ((long)crw << 26);
		long t2 = ((long)cww << 16) | ((long)cr << 1) | (long)cw;
		return t1 | t2;
	}
}
