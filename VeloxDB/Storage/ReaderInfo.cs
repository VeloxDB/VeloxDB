using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using VeloxDB.Common;

namespace VeloxDB.Storage;

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = ReaderInfo.Size)]
internal unsafe struct ReaderInfo
{
	public const int Size = 16;

	// First bit of this field is shared with the ClassTempData.hasNextVersion_nextVersion field so slot must not be greater than 15 bits.
	[FieldOffset(0)]
	ushort slot0;

	[FieldOffset(2)]
	ushort slot1;

	[FieldOffset(4)]
	ushort slot2;

	[FieldOffset(6)]
	public ushort lockCount_slotCount;

	[FieldOffset(8)]
	public ulong unusedBit_commReadLockVer;

	public ulong CommReadLockVer
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		get => unusedBit_commReadLockVer & 0x7fffffffffffffff;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		set => unusedBit_commReadLockVer = (unusedBit_commReadLockVer & 0x8000000000000000) | value;
	}

	public byte UnusedBit
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		get => (byte)(unusedBit_commReadLockVer >> 63);

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		set => unusedBit_commReadLockVer = (unusedBit_commReadLockVer & 0x7fffffffffffffff) | ((ulong)value << 63);
	}

	public int LockCount
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		get => lockCount_slotCount >> 2;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		set => lockCount_slotCount = (ushort)((lockCount_slotCount & 0x03) | (value << 2));
	}

	public int SlotCount
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		get => lockCount_slotCount & 0x03;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		set => lockCount_slotCount = (ushort)((lockCount_slotCount & 0xfffc) | value);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Init(ReaderInfo* p, ulong commitedVersion)
	{
		*((ulong*)p) = 0;
		p->unusedBit_commReadLockVer = (p->unusedBit_commReadLockVer & 0x8000000000000000) | commitedVersion;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Init(ReaderInfo* p)
	{
		*((ulong*)p) = 0;
		p->unusedBit_commReadLockVer &= 0x8000000000000000;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InitWithUnusedBit(ReaderInfo* p)
	{
		*((ulong*)p) = 0;
		p->unusedBit_commReadLockVer = 0;
	}

	public static bool IsObjectInConflict(Transaction tran, long id, ReaderInfo* rd)
	{
		TTTrace.Write(tran.Slot, tran.ReadVersion, id, rd->CommReadLockVer, rd->LockCount);

		if (rd->CommReadLockVer > tran.ReadVersion)
			return true;

		int lc = rd->LockCount;
		int sc = rd->SlotCount;

		ushort* slots = (ushort*)rd;
		for (int i = 0; i < sc; i++)
		{
			if (slots[i] != tran.Slot)
				return true;
		}

		if (lc == sc)
			return false;

		if (lc > 1)
			return true;

		LongHashSet set = tran.Context.ObjectReadLocksSet;
		return !set.Contains(id);
	}

	public static bool IsInvRefInConflict(Transaction tran, long id, int propertyId, ReaderInfo* rd)
	{
		TTTrace.Write(tran.Slot, tran.Id, tran.ReadVersion, id, rd->CommReadLockVer, rd->LockCount);

		if (rd->CommReadLockVer > tran.ReadVersion)
			return true;

		int lc = rd->LockCount;
		int sc = rd->SlotCount;
		Checker.AssertTrue(lc >= sc);

		ushort* slots = (ushort*)rd;
		for (int i = 0; i < sc; i++)
		{
			if (slots[i] != tran.Slot)
				return true;
		}

		if (lc == sc)
			return false;

		if (lc > 1)
			return true;

		FastHashSet<InverseReferenceKey> set = tran.Context.InvRefReadLocksSet;
		return !set.Contains(new InverseReferenceKey(id, propertyId));
	}

	public static bool IsKeyInConflict(Transaction tran, ReaderInfo* rd)
	{
		TTTrace.Write(tran.Slot, tran.ReadVersion, rd->CommReadLockVer, rd->LockCount);

		if (rd->CommReadLockVer > tran.ReadVersion)
			return true;

		int sc = rd->SlotCount;
		Checker.AssertTrue(rd->LockCount >= sc);

		ushort* slots = (ushort*)rd;
		for (int i = 0; i < sc; i++)
		{
			if (slots[i] != tran.Slot)
				return true;
		}

		return false;
	}

	public static void TakeObjectLock(Transaction tran, long id, ushort classIndex, ReaderInfo* rd, ulong handle)
	{
		TTTrace.Write(tran.Id, tran.Slot, id, classIndex, rd->CommReadLockVer, rd->LockCount);

		TransactionContext tc = tran.Context;
		ushort tranSlot = tran.Slot;

		int lc = rd->LockCount;
		int sc = rd->SlotCount;
		Checker.AssertTrue(lc >= sc);

		ushort* slots = (ushort*)rd;
		for (int i = 0; i < sc; i++)
		{
			TTTrace.Write(slots[i]);
			if (slots[i] == tranSlot)
				return;
		}

		LongHashSet set = tc.ObjectReadLocksSet;
		if (lc > sc && set.Contains(id))
			return;

		if (sc < 3)
		{
			slots[sc++] = tranSlot;
			rd->SlotCount = sc;
		}
		else
		{
			set.Add(id);
		}

		rd->LockCount = lc + 1;
		Checker.AssertTrue(rd->LockCount >= rd->SlotCount);

		tc.AddReadLock(tranSlot, handle, classIndex, true, true);
	}

	public static void TakeInvRefLock(Transaction tran, long id, int propertyId, ushort classIndex,
		bool eligibleForGC, ReaderInfo* rd, ulong handle)
	{
		TTTrace.Write(tran.Id, tran.Slot, id, propertyId, classIndex, rd->CommReadLockVer, rd->LockCount);

		TransactionContext tc = tran.Context;
		ushort tranSlot = tran.Slot;

		int lc = rd->LockCount;
		int sc = rd->SlotCount;
		Checker.AssertTrue(lc >= sc);

		ushort* slots = (ushort*)rd;
		for (int i = 0; i < sc; i++)
		{
			TTTrace.Write(slots[i]);
			if (slots[i] == tranSlot)
				return;
		}

		FastHashSet<InverseReferenceKey> set = tc.InvRefReadLocksSet;
		if (lc > sc && set.Contains(new InverseReferenceKey(id, propertyId)))
			return;

		if (sc < 3)
		{
			slots[sc++] = tranSlot;
			rd->SlotCount = sc;
		}
		else
		{
			set.Add(new InverseReferenceKey(id, propertyId));
		}

		rd->LockCount = lc + 1;
		Checker.AssertTrue(rd->LockCount >= rd->SlotCount);

		tc.AddReadLock(tranSlot, handle, classIndex, false, eligibleForGC);
	}

	public static bool TryTakeKeyLock(Transaction tran, ReaderInfo* rd, ulong handle, int index, ulong hash)
	{
		TTTrace.Write(tran.Id, tran.Slot, index, hash, rd->CommReadLockVer, rd->LockCount, handle);

		TransactionContext tc = tran.Context;
		ushort tranSlot = tran.Slot;

		int lc = rd->LockCount;
		int sc = rd->SlotCount;
		Checker.AssertTrue(lc >= sc);

		ushort* slots = (ushort*)rd;
		for (int i = 0; i < sc; i++)
		{
			if (slots[i] == tranSlot)
				return true;
		}

		if (sc == 3)
			return false;

		slots[sc] = tranSlot;
		rd->SlotCount = sc + 1;
		rd->LockCount = lc + 1;
		Checker.AssertTrue(rd->LockCount >= rd->SlotCount);

		tc.AddKeyReadLock(tranSlot, handle, index, hash);

		return true;
	}

	public static void FinalizeObjectLock(Transaction tran, long id, ReaderInfo* rd, bool isCommit, ushort slot)
	{
		TTTrace.Write(tran.Id, id, tran.Slot, slot, rd->CommReadLockVer, rd->LockCount, tran.CommitVersion, isCommit);

		if (isCommit)
		{
			if (tran.CommitVersion > rd->CommReadLockVer)
				rd->CommReadLockVer = tran.CommitVersion;
		}
		else
		{
			if (rd->LockCount == 0)
				return;
		}

		Checker.AssertTrue(rd->LockCount != 0);

		int lc = rd->LockCount;
		int sc = rd->SlotCount;
		Checker.AssertTrue(lc >= sc);

		ushort* slots = (ushort*)rd;

		for (int i = 0; i < sc; i++)
		{
			if (slots[i] == slot)
			{
				slots[i] = slots[sc - 1];
				rd->SlotCount = sc - 1;
				rd->LockCount = lc - 1;
				Checker.AssertTrue(rd->LockCount >= rd->SlotCount);
				return;
			}
		}

		rd->LockCount = lc - 1;
		Checker.AssertTrue(rd->LockCount >= rd->SlotCount);
	}

	public static void FinalizeInvRefLock(Transaction tran, long id, int propertyId, ReaderInfo* rd, bool isCommit, ushort slot)
	{
		TTTrace.Write(tran.Id, tran.Slot, id, propertyId, slot, rd->CommReadLockVer, rd->LockCount, tran.CommitVersion, isCommit);

		if (isCommit)
		{
			if (tran.CommitVersion > rd->CommReadLockVer)
				rd->CommReadLockVer = tran.CommitVersion;
		}
		else
		{
			if (rd->LockCount == 0)
				return;
		}

		Checker.AssertTrue(rd->LockCount != 0);

		int lc = rd->LockCount;
		int sc = rd->SlotCount;
		Checker.AssertTrue(lc >= sc);

		ushort* slots = (ushort*)rd;

		for (int i = 0; i < sc; i++)
		{
			if (slots[i] == slot)
			{
				slots[i] = slots[sc - 1];
				rd->SlotCount = sc - 1;
				rd->LockCount = lc - 1;
				Checker.AssertTrue(rd->LockCount >= rd->SlotCount);
			}
		}

		rd->LockCount = lc - 1;
		Checker.AssertTrue(rd->LockCount >= rd->SlotCount);
	}

	public static void FinalizeKeyLock(Transaction tran, ReaderInfo* rd, bool isCommit, ushort slot)
	{
		TTTrace.Write(tran.Id, tran.Slot, slot, rd->CommReadLockVer, rd->LockCount, tran.CommitVersion, isCommit);

		if (isCommit)
		{
			if (tran.CommitVersion > rd->CommReadLockVer)
				rd->CommReadLockVer = tran.CommitVersion;
		}
		else
		{
			if (rd->LockCount == 0)
				return;
		}

		Checker.AssertTrue(rd->LockCount != 0);

		int lc = rd->LockCount;
		int sc = rd->SlotCount;
		Checker.AssertTrue(lc >= sc);

		ushort* slots = (ushort*)rd;

		for (int i = 0; i < sc; i++)
		{
			if (slots[i] == slot)
			{
				slots[i] = slots[sc - 1];
				rd->SlotCount = sc - 1;
				rd->LockCount = lc - 1;
				Checker.AssertTrue(rd->LockCount >= rd->SlotCount);
				return;
			}
		}

		throw new CriticalDatabaseException();
	}

	public static void RemapSlot(ReaderInfo* rd, ushort prevSlot, ushort newSlot)
	{
		TTTrace.Write(prevSlot, newSlot, rd->CommReadLockVer, rd->LockCount);

		int sc = rd->SlotCount;
		ushort* slots = (ushort*)rd;

		for (int i = 0; i < sc; i++)
		{
			if (slots[i] == prevSlot)
			{
				slots[i] = newSlot;
				return;
			}
		}
	}
}
