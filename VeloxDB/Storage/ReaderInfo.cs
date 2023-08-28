using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal enum LockType : ushort
{
	Standard = 0x0000,
	Existance = 0x2000,
}

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = ReaderInfo.Size)]
internal unsafe struct ReaderInfo
{
	public const int Size = 8;

	// 1-bit - Is in newer version mode
	// 1-bit - IsDeleted in ClassObject
	// 9-bit standard lock count
	// 9-bit existance lock count
	// 2-bit slot count
	// 3*14=42 bit slots (13 bit slots + 1 bit lock type)

	[FieldOffset(0)]
	public ulong bits;

	public int HighestTwoBits => (int)((bits & 0xc000000000000000) >> 62);

	public int SecondHighestBit
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		get => (int)((bits & 0x4000000000000000) >> 62);

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		set => bits = (bits & 0xbfffffffffffffff) | ((ulong)value << 62);
	}

	public int StandardLockCount
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		get => (int)((bits & 0x3fe0000000000000) >> 53);

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		set
		{
			Checker.AssertTrue(value >= 0 && value < 512);
			bits = (bits & 0xc01fffffffffffff) | ((ulong)value << 53);
		}
	}

	public int ExistanceLockCount
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		get => (int)((bits & 0x001ff00000000000) >> 44);

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		set
		{
			Checker.AssertTrue(value >= 0 && value < 512);
			bits = (bits & 0xffe00fffffffffff) | ((ulong)value << 44);
		}
	}

	public int TotalLockCount => StandardLockCount + ExistanceLockCount;

	public int SlotCount
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		get => (int)((bits & 0x00000c0000000000) >> 42);

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		set
		{
			Checker.AssertTrue(value >= 0 && value <= 3);
			bits = (bits & 0xfffff3ffffffffff) | ((ulong)value << 42);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static ushort GetSlot(ReaderInfo* p, int index)
	{
		int shift = index * 14;
		return (ushort)((p->bits >> shift) & 0x3fff);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void SetSlot(ReaderInfo* p, int index, ushort slot)
	{
		Checker.AssertTrue(((uint)slot & 0xc000) == 0);
		int shift = index * 14;
		ulong mask = ~((ulong)0x3fff << shift);
		p->bits = (p->bits & mask) | ((ulong)slot << shift);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Clear(ReaderInfo* p)
	{
		*((ulong*)p) = 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void ClearWithoutSecondHighestBit(ReaderInfo* p)
	{
		*((ulong*)p) &= 0x4000000000000000;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static ushort UnpackValue(ushort value, out LockType lockType)
	{
		lockType = (LockType)(value & (uint)LockType.Existance);
		return (ushort)(value & ~(uint)LockType.Existance);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static ushort PackValue(ushort value, LockType lockType)
	{
		return (ushort)(value | (uint)lockType);
	}

	public static bool IsObjectInUpdateConflict(Transaction tran, long id, ReaderInfo* rd)
	{
		TTTrace.Write(tran.Slot, tran.ReadVersion, id, rd->SlotCount, rd->StandardLockCount, rd->ExistanceLockCount);

		LockType type;
		int slc = rd->StandardLockCount;
		if (slc != 1)
			return slc > 1;

		int sc = rd->SlotCount;
		for (int i = 0; i < sc; i++)
		{
			ushort slot = UnpackValue(GetSlot(rd, i), out type);
			TTTrace.Write(tran.Slot, slot, (ushort)type);
			if (type == LockType.Standard)
				return slot != tran.Slot;
		}

		LongDictionary<LockType> set = tran.Context.ObjectReadLocksSet;
		if (!set.TryGetValue(id, out type))
			return true;

		TTTrace.Write(tran.Slot, (ushort)type);
		return type != LockType.Standard;
	}

	public static bool IsObjectInDeleteConflict(Transaction tran, long id, ReaderInfo* rd)
	{
		TTTrace.Write(tran.Slot, tran.ReadVersion, id, rd->SlotCount, rd->StandardLockCount, rd->ExistanceLockCount);

		int lc = rd->TotalLockCount;
		if (lc != 1)
			return lc > 1;

		int sc = rd->SlotCount;
		Checker.AssertTrue(sc <= 1);

		if (sc == 1)
		{
			ushort slot = UnpackValue(GetSlot(rd, 0), out LockType type);
			TTTrace.Write(tran.Slot, slot, (ushort)type);
			return slot != tran.Slot;
		}

		LongDictionary<LockType> set = tran.Context.ObjectReadLocksSet;
		return !set.TryGetValue(id, out _);
	}

	public static bool IsInvRefInConflict(Transaction tran, long id, int propertyId, ReaderInfo* rd)
	{
		TTTrace.Write(tran.Slot, tran.Id, tran.ReadVersion, id, rd->StandardLockCount);

		int lc = rd->StandardLockCount;
		if (lc != 1)
			return lc > 1;

		int sc = rd->SlotCount;
		Checker.AssertTrue(sc <= 1);

		if (sc == 1)
			return GetSlot(rd, 0) != tran.Slot;

		FastHashSet<InverseReferenceKey> set = tran.Context.InvRefReadLocksSet;
		return !set.Contains(new InverseReferenceKey(id, propertyId));
	}

	public static bool IsKeyInConflict(Transaction tran, ReaderInfo* rd)
	{
		TTTrace.Write(tran.Slot, tran.Id, tran.ReadVersion, rd->StandardLockCount);

		int lc = rd->StandardLockCount;
		if (lc != 1)
			return lc > 1;

		int sc = rd->SlotCount;
		Checker.AssertTrue(sc == lc);

		return GetSlot(rd, 0) != tran.Slot;
	}

	public static void TakeObjectStandardLock(Transaction tran, long id, ushort classIndex, ReaderInfo* rd, ulong handle)
	{
		TTTrace.Write(tran.Id, tran.Slot, id, classIndex, rd->StandardLockCount, rd->ExistanceLockCount, rd->SlotCount, rd->SecondHighestBit);

		ushort tranSlot = tran.Slot;

		LockType type;
		int sc = rd->SlotCount;

		for (int i = 0; i < sc; i++)
		{
			ushort slot = UnpackValue(GetSlot(rd, i), out type);
			if (slot == tranSlot)
			{
				if (type != LockType.Standard)
					SetSlot(rd, i, PackValue(slot, LockType.Standard));

				return;
			}
		}

		TransactionContext tc = tran.Context;
		LongDictionary<LockType> set = tc.ObjectReadLocksSet;

		int slc = rd->StandardLockCount;
		int elc = rd->ExistanceLockCount;

		if (slc + elc > sc && set.TryGetValue(id, out type))
		{
			if (type != LockType.Standard)
				set[id] = LockType.Standard;

			return;
		}

		if (sc < 3)
		{
			SetSlot(rd, sc++, PackValue(tranSlot, LockType.Standard));
			rd->SlotCount = sc;
		}
		else
		{
			set.Add(id, LockType.Standard);
		}

		rd->StandardLockCount = slc + 1;
		Checker.AssertTrue(rd->StandardLockCount + elc >= rd->SlotCount);

		tc.AddReadLock(tranSlot, handle, classIndex, true, true);
	}

	public static void TakeObjectExistanceLock(Transaction tran, long id, ushort classIndex, ReaderInfo* rd, ulong handle)
	{
		TTTrace.Write(tran.Id, tran.Slot, id, classIndex, rd->StandardLockCount, rd->ExistanceLockCount, rd->SlotCount);

		ushort tranSlot = tran.Slot;

		int sc = rd->SlotCount;

		for (int i = 0; i < sc; i++)
		{
			ushort slot = UnpackValue(GetSlot(rd, i), out _);
			if (slot == tranSlot)
				return;
		}

		TransactionContext tc = tran.Context;
		LongDictionary<LockType> set = tc.ObjectReadLocksSet;

		int slc = rd->StandardLockCount;
		int elc = rd->ExistanceLockCount;

		if (slc + elc > sc && set.TryGetValue(id, out _))
			return;

		if (sc < 3)
		{
			SetSlot(rd, sc++, PackValue(tranSlot, LockType.Existance));
			rd->SlotCount = sc;
		}
		else
		{
			set.Add(id, LockType.Existance);
		}

		rd->ExistanceLockCount = elc + 1;
		Checker.AssertTrue(rd->ExistanceLockCount + slc >= rd->SlotCount);

		tc.AddReadLock(tranSlot, handle, classIndex, true, true);
	}

	public static void TakeInvRefLock(Transaction tran, long id, int propertyId, ushort classIndex,
		bool eligibleForGC, ReaderInfo* rd, ulong handle)
	{
		TTTrace.Write(tran.Id, tran.Slot, id, propertyId, classIndex, rd->StandardLockCount);

		TransactionContext tc = tran.Context;
		ushort tranSlot = tran.Slot;

		int lc = rd->StandardLockCount;
		int sc = rd->SlotCount;
		Checker.AssertTrue(lc >= sc);

		for (int i = 0; i < sc; i++)
		{
			TTTrace.Write(GetSlot(rd, i));
			if (GetSlot(rd, i) == tranSlot)
				return;
		}

		FastHashSet<InverseReferenceKey> set = tc.InvRefReadLocksSet;
		if (lc > sc && set.Contains(new InverseReferenceKey(id, propertyId)))
			return;

		if (sc < 3)
		{
			SetSlot(rd, sc++, tranSlot);
			rd->SlotCount = sc;
		}
		else
		{
			set.Add(new InverseReferenceKey(id, propertyId));
		}

		rd->StandardLockCount = lc + 1;
		Checker.AssertTrue(rd->StandardLockCount >= rd->SlotCount);

		tc.AddReadLock(tranSlot, handle, classIndex, false, eligibleForGC);
	}

	public static bool TryTakeKeyLock(Transaction tran, ReaderInfo* rd, ulong handle, int index, ulong hash)
	{
		TTTrace.Write(tran.Id, tran.Slot, index, hash, rd->StandardLockCount, handle);

		TransactionContext tc = tran.Context;
		ushort tranSlot = tran.Slot;

		int lc = rd->StandardLockCount;
		int sc = rd->SlotCount;
		Checker.AssertTrue(lc >= sc);

		for (int i = 0; i < sc; i++)
		{
			if (GetSlot(rd, i) == tranSlot)
				return true;
		}

		if (sc == 3)
			return false;

		SetSlot(rd, sc, tranSlot);
		rd->SlotCount = sc + 1;
		rd->StandardLockCount = lc + 1;
		Checker.AssertTrue(rd->StandardLockCount >= rd->SlotCount);

		tc.AddKeyReadLock(tranSlot, handle, index, hash);

		return true;
	}

	public static void FinalizeObjectLock(Transaction tran, long id, ReaderInfo* rd, bool isCommit, ushort slot)
	{
		TTTrace.Write(tran.Id, id, tran.Slot, slot, rd->StandardLockCount, rd->ExistanceLockCount, rd->SecondHighestBit, tran.CommitVersion, isCommit);

		if (!isCommit)
		{
			if (rd->StandardLockCount == 0)
				return;
		}

		Checker.AssertTrue(rd->StandardLockCount != 0);

		int lc = rd->StandardLockCount;
		int sc = rd->SlotCount;
		Checker.AssertTrue(lc >= sc);

		for (int i = 0; i < sc; i++)
		{
			if (GetSlot(rd, i) == slot)
			{
				SetSlot(rd, i, GetSlot(rd, sc - 1));
				rd->SlotCount = sc - 1;
				rd->StandardLockCount = lc - 1;
				Checker.AssertTrue(rd->StandardLockCount >= rd->SlotCount);
				return;
			}
		}

		rd->StandardLockCount = lc - 1;
		Checker.AssertTrue(rd->StandardLockCount >= rd->SlotCount);
	}

	public static void FinalizeInvRefLock(Transaction tran, long id, int propertyId, ReaderInfo* rd, bool isCommit, ushort slot)
	{
		TTTrace.Write(tran.Id, tran.Slot, id, propertyId, slot, rd->StandardLockCount, tran.CommitVersion, isCommit);

		if (!isCommit)
		{
			if (rd->StandardLockCount == 0)
				return;
		}

		Checker.AssertTrue(rd->StandardLockCount != 0);

		int lc = rd->StandardLockCount;
		int sc = rd->SlotCount;
		Checker.AssertTrue(lc >= sc);

		for (int i = 0; i < sc; i++)
		{
			if (GetSlot(rd, i) == slot)
			{
				SetSlot(rd, i, GetSlot(rd, sc - 1));
				rd->SlotCount = sc - 1;
				rd->StandardLockCount = lc - 1;
				Checker.AssertTrue(rd->StandardLockCount >= rd->SlotCount);
			}
		}

		rd->StandardLockCount = lc - 1;
		Checker.AssertTrue(rd->StandardLockCount >= rd->SlotCount);
	}

	public static bool FinalizeKeyLock(Transaction tran, ReaderInfo* rd, bool isCommit, ushort slot)
	{
		TTTrace.Write(tran.Id, tran.Slot, slot, rd->StandardLockCount, tran.CommitVersion, isCommit);

		if (!isCommit)
		{
			if (rd->StandardLockCount == 0)
				return true;
		}

		Checker.AssertTrue(rd->StandardLockCount != 0);

		int lc = rd->StandardLockCount;
		int sc = rd->SlotCount;
		Checker.AssertTrue(lc >= sc);

		for (int i = 0; i < sc; i++)
		{
			if (GetSlot(rd, i) == slot)
			{
				SetSlot(rd, i, GetSlot(rd, sc - 1));
				rd->SlotCount = sc - 1;
				rd->StandardLockCount = lc - 1;
				Checker.AssertTrue(rd->StandardLockCount >= rd->SlotCount);
				return rd->StandardLockCount == 0;
			}
		}

		throw new CriticalDatabaseException();
	}

	public static void RemapSlot(ReaderInfo* rd, ushort prevSlot, ushort newSlot)
	{
		TTTrace.Write(prevSlot, newSlot, rd->StandardLockCount);

		int sc = rd->SlotCount;
		for (int i = 0; i < sc; i++)
		{
			if (GetSlot(rd, i) == prevSlot)
			{
				SetSlot(rd, i, newSlot);
				return;
			}
		}
	}
}
