using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal unsafe sealed partial class SortedIndex
{
	[StructLayout(LayoutKind.Explicit, Pack = 1, Size = 1)]
	public unsafe struct Node
	{
#if TEST_BUILD
		public const int HeaderSize = 56;

		[ThreadStatic]
		static HashSet<IntPtr> writeLocked;

		[ThreadStatic]
		static HashSet<IntPtr> readLocked;

		[FieldOffset(48)]
		Node* thisPointer;

		private static HashSet<IntPtr> WriteLocked
		{
			get
			{
				if (writeLocked == null)
					writeLocked = new HashSet<IntPtr>();

				return writeLocked;
			}
		}

		private static HashSet<IntPtr> ReadLocked
		{
			get
			{
				if (readLocked == null)
					readLocked = new HashSet<IntPtr>();

				return readLocked;
			}
		}
#else
		public const int HeaderSize = 48;
#endif

		[FieldOffset(0)]
		ulong nextGarbage;   // This has to be at offset 0

		// Must not be used while holding optimistic read locks (because the location is shared with nextGarbage)
		[FieldOffset(0)]
		Node* parent;

		[FieldOffset(8)]
		Node* left;

		[FieldOffset(16)]
		Node* right;

		[FieldOffset(24)]
		RWLock sync;

		[FieldOffset(32)]
		Range* lockedRanges;

		[FieldOffset(40)]
		public int version;

		[FieldOffset(44)]
		ushort notUsed_isLeaf_count;

		[FieldOffset(46)]
		byte bufferMetaData;

		public Range* LockedRanges
		{
			get
			{
				CheckLocked(false);
				return lockedRanges;
			}

			set
			{
				CheckLocked(true);
				lockedRanges = value;
			}
		}

		public Range* LockedRangesUnsafe => lockedRanges;

		public Node* Parent
		{
			get
			{
				CheckLocked(false);
				return parent;
			}

			set
			{
				CheckLocked(true);
				parent = value;
			}
		}

		public Node* ParentUnsafe => parent;

		public Node* Left
		{
			get
			{
				CheckLocked(false);
				return left;
			}

			set
			{
				CheckLocked(true);
				left = value;
			}
		}

		public Node* LeftUnsafe => left;

		public Node* Right
		{
			get
			{
				CheckLocked(false);
				return right;
			}

			set
			{
				CheckLocked(true);
				right = value;
			}
		}

		public Node* RightUnsafe => right;

		// IsLeaf never changes, so no lock is neccessary to read it
		public bool IsLeaf => (notUsed_isLeaf_count & (ushort)NodeFlags.Leaf) != 0;

		public bool NotUsed
		{
			get
			{
				return (notUsed_isLeaf_count & (ushort)NodeFlags.Used) != 0;
			}

			set
			{
				if (value)
					notUsed_isLeaf_count |= (ushort)NodeFlags.Used;
				else
					notUsed_isLeaf_count &= (ushort)(~NodeFlags.Used);
			}
		}

		public int Count
		{
			get => notUsed_isLeaf_count & (ushort)NodeFlags.CountMask;
			set
			{
				notUsed_isLeaf_count = (ushort)((notUsed_isLeaf_count & (ushort)~NodeFlags.CountMask) | (ushort)value);
			}
		}

		public static Node* Create(MemoryManager.FixedAccessor memoryManager, bool isLeaf)
		{
			ulong handle = memoryManager.Allocate();
			Node* node = (Node*)memoryManager.GetBuffer(handle);
#if TEST_BUILD
			node->thisPointer = node;
#endif
			node->parent = null;
			node->left = null;
			node->right = null;
			node->lockedRanges = null;
			node->version = 0;
			node->bufferMetaData = (byte)(handle >> 56);
			node->sync = new RWLock();
			node->notUsed_isLeaf_count = Make(0, isLeaf);
			TTTrace.Write(isLeaf, (ulong)node, handle);
			return node;
		}

		public static Node* Create(MemoryManager.FixedAccessor nodeManager, MemoryManager.FixedAccessor leafManager,
			Node* other, int offset, int count, int entrySize)
		{
			CheckLocked(other, false);

			MemoryManager.FixedAccessor memoryManager = other->IsLeaf ? leafManager : nodeManager;
			ulong handle = memoryManager.Allocate();
			Node* node = (Node*)memoryManager.GetBuffer(handle);
#if TEST_BUILD
			node->thisPointer = node;
#endif
			node->parent = null;
			node->left = null;
			node->right = null;
			node->lockedRanges = null;
			node->version = 0;
			node->sync = new RWLock();
			node->bufferMetaData = (byte)(handle >> 56);
			node->notUsed_isLeaf_count = Make(count, other->IsLeaf);

			if (other->IsLeaf)
			{
				byte* src = Entries(other) + offset * 8;
				CopyKeyRange(src, Entries(node), count);
			}
			else
			{
				byte* src = Entries(other) + offset * entrySize;
				CopyKeyRange(src, Entries(node), count * entrySize / 8);
			}

			TTTrace.Write((ulong)node, handle);
			return node;
		}

		[Conditional("TEST_BUILD")]
		public static void CheckLocked(Node* node, bool isWrite)
		{
#if TEST_BUILD
			if (isWrite)
			{
				if (!WriteLocked.Contains((IntPtr)node))
					throw new InvalidOperationException();
			}
			else
			{
				if (!WriteLocked.Contains((IntPtr)node) && !ReadLocked.Contains((IntPtr)node))
					throw new InvalidOperationException();
			}
#endif
		}

		[Conditional("TEST_BUILD")]
		public void CheckLocked(bool isWrite)
		{
#if TEST_BUILD
			if (isWrite)
			{
				if (!WriteLocked.Contains((IntPtr)thisPointer))
					throw new InvalidOperationException();
			}
			else
			{
				if (!WriteLocked.Contains((IntPtr)thisPointer) && !ReadLocked.Contains((IntPtr)thisPointer))
					throw new InvalidOperationException();
			}
#endif
		}

		[Conditional("TEST_BUILD")]
		public void CheckNotLocked()
		{
#if TEST_BUILD
			if (ReadLocked.Contains((IntPtr)thisPointer) || WriteLocked.Contains((IntPtr)thisPointer))
				throw new InvalidOperationException();
#endif
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static byte* Entries(Node* node)
		{
			return (byte*)node + HeaderSize;
		}

		private static ushort Make(int count, bool isLeaf)
		{
			return (ushort)((ushort)(isLeaf ? NodeFlags.Leaf : 0) | count);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static ulong GetHandle(Node* node)
		{
			return (ulong)node | ((ulong)node->bufferMetaData << 56);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void EnterWriteLock()
		{
			CheckNotLocked();

#if TEST_BUILD
			TTTrace.Write((ulong)thisPointer, version);
			WriteLocked.Add((IntPtr)thisPointer);
#endif

			sync.EnterWriteLock();
			int newVersion = version + 1;
			if (newVersion == int.MaxValue)
				newVersion = int.MinValue;

			version = newVersion;

			// On ARM, stores can be reordered with other stores, and we must ensure that
			// version has been modified before any changes to the node are made.
#if !X86_64
			Thread.MemoryBarrier();
#endif
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void ExitWriteLock()
		{
#if TEST_BUILD
			TTTrace.Write((ulong)thisPointer);
			if (!WriteLocked.Remove((IntPtr)thisPointer))
				throw new InvalidOperationException();
#endif

			sync.ExitWriteLock();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool TryEnterWriteLock()
		{
			CheckNotLocked();

#if TEST_BUILD
			TTTrace.Write((ulong)thisPointer);
			WriteLocked.Add((IntPtr)thisPointer);
#endif

			if (sync.TryEnterWriteLock())
			{
				int newVersion = version + 1;
				if (newVersion == int.MaxValue)
					newVersion = int.MinValue;

				version = newVersion;

				// On ARM, stores can be reordered with other stores, and we must ensure that
				// version has been modified before any changes to the node are made.
#if !X86_64
				Thread.MemoryBarrier();
#endif

				return true;
			}

#if TEST_BUILD
			TTTrace.Write((ulong)thisPointer);
			WriteLocked.Remove((IntPtr)thisPointer);
#endif

			return false;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void DowngradeWriteLockToRead()
		{
#if TEST_BUILD
			TTTrace.Write((ulong)thisPointer, version);
			if (!WriteLocked.Remove((IntPtr)thisPointer))
				throw new InvalidOperationException();

			ReadLocked.Add((IntPtr)thisPointer);
#endif

			sync.DowngradeWriteToReadLock();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public int EnterReadLock(bool isOptimistic)
		{
			CheckNotLocked();

#if TEST_BUILD
			TTTrace.Write((ulong)thisPointer, version);
			ReadLocked.Add((IntPtr)thisPointer);
#endif

			if (isOptimistic)
			{
				while (true)
				{
					// We need volatile read, otherwise compiler might optimize this read and the one after this
					int seenVersion = Thread.VolatileRead(ref this.version);

					RWLock sync = this.sync;
#if !X86_64
					Thread.MemoryBarrier();
#endif
					if (!RWLock.IsWriteLockTaken(sync) && seenVersion == this.version)
						return seenVersion;
				}
			}
			else
			{
				sync.EnterReadLock();
				return 0;
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool TryEnterReadLock()
		{
			CheckNotLocked();

#if TEST_BUILD
			TTTrace.Write((ulong)thisPointer);
			ReadLocked.Add((IntPtr)thisPointer);
#endif

			if (sync.TryEnterReadLock())
				return true;

#if TEST_BUILD
			TTTrace.Write((ulong)thisPointer);
			ReadLocked.Remove((IntPtr)thisPointer);
#endif

			return false;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool ExitReadLock(int version, bool isOptimistic)
		{
#if TEST_BUILD
			TTTrace.Write((ulong)thisPointer, version);
			if (!ReadLocked.Remove((IntPtr)thisPointer))
				throw new InvalidOperationException();
#endif

			if (isOptimistic)
			{
				// On arm, no reads before this.version should be reordered after it.
#if !X86_64
				Thread.MemoryBarrier();
#endif
				return this.version == version;
			}
			else
			{
				sync.ExitReadLock();
				return true;
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public int EnterLock(bool isWrite, bool isOptimistic)
		{
			if (isWrite)
			{
				EnterWriteLock();
				return int.MaxValue;
			}
			else
			{
				return EnterReadLock(isOptimistic);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool TryEnterLock(bool isWrite)
		{
			if (isWrite)
			{
				return TryEnterWriteLock();
			}
			else
			{
				return TryEnterReadLock();
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool ExitLock(int version, bool isOptimistic)
		{
			if (version == int.MaxValue)
			{
				ExitWriteLock();
				return true;
			}

			return ExitReadLock(version, isOptimistic);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void ExitLock(bool isWrite)
		{
			if (isWrite)
			{
				ExitWriteLock();
			}
			else
			{
				ExitReadLock(0, false);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void AddLockedRange(Node* node, Range* range)
		{
			TTTrace.Write((ulong)node, (ulong)range);

			range->owner = node;
			range->next = node->lockedRanges;
			range->prev = null;
			if (node->lockedRanges != null)
				node->lockedRanges->prev = range;

			node->lockedRanges = range;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void AddLockedRangeMultiple(Node* node, Range* range)
		{
			TTTrace.Write((ulong)node, (ulong)range);

			while (range != null)
			{
				Range* next = range->next;
				AddLockedRange(node, range);
				range = next;
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void RemoveLockedRange(Range* range)
		{
#if TEST_BUILD
			TTTrace.Write((ulong)thisPointer, (ulong)range);
#endif

			if (range->next != null)
				range->next->prev = range->prev;

			if (range->prev != null)
				range->prev->next = range->next;
			else
				lockedRanges = range->next;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static Entry* GetEntry(Node* node, int index, int entrySize)
		{
			return (Entry*)(Entries(node) + index * entrySize);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static ulong* GetLeafEntry(Node* node, int index)
		{
			return (ulong*)(Entries(node) + index * 8);
		}

		public static void InitLeafWithMaxKey(Node* node)
		{
			*((ulong*)Entries(node)) = KeyComparer.MaxKey;
			node->Count = 1;
		}

		public static void InitNodeWithMaxKey(Node* node, Node* child, int keySize)
		{
			Entry* entry = (Entry*)Entries(node);
			entry->handle = KeyComparer.MaxKey;
			entry->child = child;
			entry->id = 0;
			byte* key = Entry.Key(entry);
			Utils.ZeroMemory(key, keySize);
			node->Count = 1;
		}
	}

	[StructLayout(LayoutKind.Explicit, Pack = 1, Size = 1)]
	public unsafe struct Entry
	{
		public const int Size = 24;
		public const int LeafSize = 8;

		[FieldOffset(0)]
		public Node* child;

		[FieldOffset(8)]
		public long id;

		[FieldOffset(16)]
		public ulong handle;

		public static byte* Key(Entry* entry)
		{
			return (byte*)entry + Size;
		}
	}

	[StructLayout(LayoutKind.Explicit, Pack = 1, Size = 1)]
	public unsafe struct Range
	{
		public const int Size = 80;

		[FieldOffset(0)]
		ulong nextGarbage;  // This has to be at offset 0

		// Must not be used while holding optimistic read locks (because the location is shared with nextGarbage)
		[FieldOffset(0)]
		public Range* prev;

		[FieldOffset(8)]
		public Range* next;

		[FieldOffset(16)]
		public ulong version;

		[FieldOffset(24)]
		public Range* nextInScan;

		[FieldOffset(32)]
		public Node* owner;

		[FieldOffset(40)]
		public long startId;

		[FieldOffset(48)]
		public ulong startHandle;

		[FieldOffset(56)]
		public long endId;

		[FieldOffset(64)]
		public ulong endHandle;

		[FieldOffset(72)]
		public bool isStartOpen;

		[FieldOffset(73)]
		public bool isEndOpen;

		[FieldOffset(74)]
		byte bufferMetaData;

		public static Range* CreateForwardInitial(MemoryManager.FixedAccessor rangeManager,
			StringStorage stringStorage, RangeScanBase scan, byte* lastKey, long lastId, ulong lastHandle, KeyComparer keyComparer)
		{
			ulong handle = rangeManager.Allocate();
			Range* range = (Range*)rangeManager.GetBuffer(handle);
			range->prev = null;
			range->next = null;
			range->version = scan.Tran.Id;
			range->nextInScan = null;
			range->owner = null;
			range->bufferMetaData = (byte)(handle >> 56);

			range->isStartOpen = scan.IsStartOpen;
			range->startId = scan.StartId;
			range->startHandle = scan.StartHandle;
			byte* start = GetStart(range);
			scan.Comparer.CopyWithStringStorage(scan.StartKey, scan.StartStrings, start, stringStorage);

			byte* end = GetEnd(range, scan.Comparer.KeySize);

			if (scan.Comparer.IsAfter(scan.EndKey, scan.EndId, scan.EndHandle, scan.EndStrings,
				lastKey, lastId, lastHandle, keyComparer, stringStorage, out _))
			{
				range->isEndOpen = false;
				range->endId = lastId;
				range->endHandle = lastHandle;
				keyComparer.CopyWithStringStorage(lastKey, null, end, stringStorage);
			}
			else
			{
				range->isEndOpen = scan.IsEndOpen;
				range->endId = scan.EndId;
				range->endHandle = scan.EndHandle;
				scan.Comparer.CopyWithStringStorage(scan.EndKey, scan.EndStrings, end, stringStorage);
			}

			return range;
		}

		public static Range* CreateBackwardInitial(MemoryManager.FixedAccessor rangeManager,
			StringStorage stringStorage, RangeScanBase scan, byte* lastKey, long lastId, ulong lastHandle, KeyComparer keyComparer)
		{
			ulong handle = rangeManager.Allocate();
			Range* range = (Range*)rangeManager.GetBuffer(handle);
			range->prev = null;
			range->next = null;
			range->version = scan.Tran.Id;
			range->nextInScan = null;
			range->owner = null;
			range->bufferMetaData = (byte)(handle >> 56);

			range->isEndOpen = scan.IsEndOpen;
			range->endId = scan.EndId;
			range->endHandle = scan.EndHandle;
			byte* end = GetEnd(range, scan.Comparer.KeySize);
			scan.Comparer.CopyWithStringStorage(scan.EndKey, scan.EndStrings, end, stringStorage);

			byte* start = GetStart(range);
			if (lastKey != null && scan.Comparer.IsBefore(scan.StartKey, scan.StartId, scan.StartHandle, scan.StartStrings,
				lastKey, lastId, lastHandle, keyComparer, stringStorage, out _))
			{
				range->isStartOpen = true;
				range->startId = lastId;
				range->startHandle = lastHandle;
				keyComparer.CopyWithStringStorage(lastKey, null, start, stringStorage);
			}
			else
			{
				range->isStartOpen = scan.IsStartOpen;
				range->startId = scan.StartId;
				range->startHandle = scan.StartHandle;
				scan.Comparer.CopyWithStringStorage(scan.StartKey, scan.StartStrings, start, stringStorage);
			}

			return range;
		}

		public static Range* CreateForwardNext(MemoryManager.FixedAccessor rangeManager, StringStorage stringStorage,
			RangeScanBase scan, byte* lastKey, long lastId, ulong lastHandle, KeyComparer keyComparer)
		{
			Range* prev = scan.LastAddedLeafLock;

			ulong handle = rangeManager.Allocate();
			Range* range = (Range*)rangeManager.GetBuffer(handle);
			range->prev = null;
			range->next = null;
			range->version = scan.Tran.Id;
			range->nextInScan = null;
			range->owner = null;
			range->bufferMetaData = (byte)(handle >> 56);

			range->isStartOpen = true;
			range->startId = prev->endId;
			range->startHandle = prev->endHandle;
			byte* start = GetStart(range);
			scan.Comparer.CopyWithStringStorage(Range.GetEnd(prev, scan.Comparer.KeySize), null, start, stringStorage);

			byte* end = GetEnd(range, scan.Comparer.KeySize);

			if (scan.Comparer.IsAfter(scan.EndKey, scan.EndId, scan.EndHandle, scan.EndStrings,
				lastKey, lastId, lastHandle, keyComparer, stringStorage, out _))
			{
				range->isEndOpen = false;
				range->endId = lastId;
				range->endHandle = lastHandle;
				keyComparer.CopyWithStringStorage(lastKey, null, end, stringStorage);
			}
			else
			{
				range->isEndOpen = scan.IsEndOpen;
				range->endId = scan.EndId;
				range->endHandle = scan.EndHandle;
				scan.Comparer.CopyWithStringStorage(scan.EndKey, scan.EndStrings, end, stringStorage);
			}

			return range;
		}

		public static Range* CreateBackwardNext(MemoryManager.FixedAccessor rangeManager, StringStorage stringStorage,
			RangeScanBase scan, byte* lastKey, long lastId, ulong lastHandle, KeyComparer keyComparer)
		{
			Range* prev = scan.LastAddedLeafLock;

			ulong handle = rangeManager.Allocate();
			Range* range = (Range*)rangeManager.GetBuffer(handle);
			range->prev = null;
			range->next = null;
			range->version = scan.Tran.Id;
			range->nextInScan = null;
			range->owner = null;
			range->bufferMetaData = (byte)(handle >> 56);

			range->isEndOpen = false;
			range->endId = prev->startId;
			range->endHandle = prev->startHandle;
			byte* end = GetEnd(range, scan.Comparer.KeySize);
			scan.Comparer.CopyWithStringStorage(Range.GetStart(prev), null, end, stringStorage);

			byte* start = GetStart(range);
			if (lastKey != null && scan.Comparer.IsBefore(scan.StartKey, scan.StartId, scan.StartHandle, scan.StartStrings,
				lastKey, lastId, lastHandle, keyComparer, stringStorage, out _))
			{
				range->isStartOpen = true;
				range->startId = lastId;
				range->startHandle = lastHandle;
				keyComparer.CopyWithStringStorage(lastKey, null, start, stringStorage);
			}
			else
			{
				range->isStartOpen = scan.IsStartOpen;
				range->startId = scan.StartId;
				range->startHandle = scan.StartHandle;
				scan.Comparer.CopyWithStringStorage(scan.StartKey, scan.StartStrings, start, stringStorage);
			}

			return range;
		}

		public static Range* CreateEnvelope(MemoryManager.FixedAccessor rangeManager, StringStorage stringStorage,
			Range* low, Range* high, KeyComparer comparer)
		{
			ulong handle = rangeManager.Allocate();
			Range* range = (Range*)rangeManager.GetBuffer(handle);
			range->prev = null;
			range->next = null;
			range->version = low->version;
			Checker.AssertTrue(low->version == high->version);
			range->nextInScan = null;
			range->owner = null;
			range->bufferMetaData = (byte)(handle >> 56);

			range->isStartOpen = low->isStartOpen;
			range->startId = low->startId;
			range->startHandle = low->startHandle;
			byte* start = GetStart(range);
			comparer.CopyWithStringStorage(Range.GetStart(low), null, start, stringStorage);

			range->isEndOpen = high->isEndOpen;
			range->endId = high->endId;
			range->endHandle = high->endHandle;
			byte* end = GetEnd(range, comparer.KeySize);
			comparer.CopyWithStringStorage(Range.GetEnd(high, comparer.KeySize), null, end, stringStorage);

			return range;
		}

		public static void Rollback(Range* range)
		{
			range->version = 0;
		}

		public static bool IsInConflict(Transaction tran, Range* range, KeyComparer comparer,
			byte* key, long id, ulong handle, KeyComparer keyComparer, StringStorage stringStorage)
		{
			if (range->version == tran.Id)
				return false;

			bool isBefore = keyComparer.IsBefore(key, id, handle, null,
				GetStart(range), range->startId, range->startHandle, comparer, stringStorage, out bool isEqual);

			if (isBefore || (range->isStartOpen && isEqual))
				return false;

			bool isAfter = keyComparer.IsAfter(key, id, handle, null,
				GetEnd(range, comparer.KeySize), range->endId, range->endHandle, comparer, stringStorage, out isEqual);

			if (isAfter || (range->isEndOpen && isEqual))
				return false;

			// Conflict is either uncommited version or commited after the transaction started
			return true;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void ReleaseStrings(Range* range, KeyComparer comparer, StringStorage stringStorage)
		{
			comparer.ReleaseStrings(Range.GetStart(range), stringStorage);
			comparer.ReleaseStrings(Range.GetEnd(range, comparer.KeySize), stringStorage);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static byte* GetStart(Range* range)
		{
			return (byte*)range + Size;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static byte* GetEnd(Range* range, int keySize)
		{
			return (byte*)range + Size + keySize;
		}

		public static ulong GetHandle(Range* range)
		{
			return (ulong)range | ((ulong)range->bufferMetaData << 56);
		}
	}
}
