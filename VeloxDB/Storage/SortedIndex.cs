using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal unsafe sealed partial class SortedIndex : Index
{
	const bool isOptimistic = true;

	readonly int N;
	readonly int NHalf;
	public readonly int NodeCapacity;

	SortedIndexDescriptor indexDesc;
	Database database;

	MemoryManager memoryManager;
	MemoryManager.FixedAccessor rootManager;
	MemoryManager.FixedAccessor nodeManager;
	MemoryManager.FixedAccessor leafManager;
	MemoryManager.FixedAccessor rangeManager;
	StringStorage stringStorage;

	long traceId;

	bool pendingRefill;

	int entrySize, entryOctetSize;
	KeyComparer localComparer;

	GarbageCollector gc;

	Node* rootParent;
	Node* root;

	public SortedIndex(Database database, SortedIndexDescriptor indexDesc)
	{
		TTTrace.Write(database.TraceId, indexDesc.Id);

		StorageEngine engine = database.Engine;

		this.traceId = database.TraceId;
		this.indexDesc = indexDesc;
		this.database = database;
		this.memoryManager = database.Engine.MemoryManager;
		this.stringStorage = engine.StringStorage;

		this.N = database.Engine.Settings.BTreeNodeSize;
		this.NHalf = N / 2;
		this.NodeCapacity = N + 2;

		gc = (GarbageCollector)database.Engine.SortedIndexGC;

		Init();

		rootParent = Node.Create(rootManager, false);
		root = Node.Create(leafManager, true);
		Node.InitLeafWithMaxKey(root);

		TTTrace.Write(traceId);
	}

	public SortedIndexDescriptor SortedIndexDesc => indexDesc;
	public KeyComparer LocalComparer => localComparer;
	public override IndexDescriptor IndexDesc => indexDesc;
	public override bool PendingRefill => pendingRefill;

	public override void ModelUpdated(IndexDescriptor indexDesc)
	{
		this.indexDesc = (SortedIndexDescriptor)indexDesc;
		Init();
	}

	public override DatabaseErrorDetail Insert(Transaction tran, long id, ulong objectHandle, byte* key,
		KeyComparer comparer, Func<short, KeyComparer> comparerFinder = null)
	{
		TTTrace.Write(traceId, indexDesc.Id, tran == null ? 0 : tran.Id, id, objectHandle, comparerFinder != null);
		comparer.TTTraceKeys(traceId, tran == null ? 0 : tran.Id, indexDesc.Id, key, null, stringStorage, 14);
		Checker.AssertFalse(tran != null && comparerFinder != null);

		long searchId = id;
		ulong searchObjectHandle = objectHandle;
		if (indexDesc.IsUnique)
		{
			// In unique indexes we want to find the first item with a given key
			searchId = long.MinValue;
			searchObjectHandle = ulong.MinValue;
		}

		GarbageCollector.Epoch epoch = gc.ThreadEntered();
		try
		{
beggining:
			Node* parent = rootParent;
			Node* curr = null;
			int indexInParent = 0;
			int index = -1;

			int parentVersion = parent->EnterReadLock(isOptimistic);
			curr = root;

			DatabaseErrorDetail error = null;
			int currVersion = curr->EnterLock(curr->IsLeaf, isOptimistic);

			TTTrace.Write(traceId, indexDesc.Id, id, (ulong)parent, (ulong)root, parentVersion, currVersion, root->Count);

			while (true)
			{
				// While holding optimistic read lock, we must not read parent property of the node (since it is used by the GC)!

				if (!parent->ExitReadLock(parentVersion, isOptimistic))
				{
					TTTrace.Write(traceId, id, (ulong)parent);
					curr->ExitLock(currVersion, isOptimistic);
					goto beggining;
				}

				if (error != null)
					break;

				index = BinarySearchEqualOrLarger(curr, key, searchId, searchObjectHandle, comparer, null, comparerFinder);
				if (curr->IsLeaf)
					break;

				if (tran != null && IsRangeLocked(tran, curr, key, id, objectHandle, comparer))
					error = DatabaseErrorDetail.Create(DatabaseErrorType.IndexConflict);

				Node* child = null;
				if (index < NodeCapacity)
				{
					TTTrace.Write(traceId, id, (ulong)parent);
					child = Node.GetEntry(curr, index, entrySize)->child;
				}

				if (child == null)
				{
					TTTrace.Write(traceId, id, (ulong)parent);
					curr->ExitLock(currVersion, isOptimistic);
					goto beggining;
				}

				parentVersion = currVersion;
				parent = curr;
				curr = child;
				indexInParent = index;
				currVersion = curr->EnterLock(curr->IsLeaf, isOptimistic);

				TTTrace.Write(traceId, indexDesc.Id, id, (ulong)parent, (ulong)curr, error != null, curr->IsLeaf, parentVersion, currVersion);
			}

			TTTrace.Write(traceId, indexDesc.Id, id, (ulong)curr, curr->Count, index, indexInParent, indexDesc.IsUnique, error != null);

			if (error == null && indexDesc.IsUnique)
				error = CheckUniqueness(tran, ref curr, ref index, key, id, objectHandle, comparer, comparerFinder);

			if (tran != null && error == null && IsRangeLocked(tran, curr, key, id, objectHandle, comparer))
				error = DatabaseErrorDetail.Create(DatabaseErrorType.IndexConflict);

			if (error != null)
			{
				TTTrace.Write(traceId, id, (int)error.ErrorType);
				curr->ExitLock(currVersion, isOptimistic);
				return error;
			}

			if (curr->Count == NodeCapacity)
			{
				TTTrace.Write(traceId, id);
				curr->ExitWriteLock();
				goto beggining;
			}

			InsertNewKey(curr, index, objectHandle);

			while (curr->Count >= N)
			{
				TTTrace.Write(traceId, indexDesc.Id, id, (ulong)curr, curr->Count, (ulong)curr->Parent);

				parent = curr->Parent;
				Node* parentSyncNode = parent == null ? rootParent : parent;

				if (parentSyncNode->TryEnterWriteLock())
				{
					Split(curr, indexInParent, comparerFinder);
					parent = curr->Parent;
					curr->ExitWriteLock();
					curr = parent;
				}
				else
				{
					TTTrace.Write(traceId, id, (ulong)curr, (ulong)curr->Parent);

					curr->ExitWriteLock();
					parentSyncNode->EnterWriteLock();
					curr->EnterWriteLock();

					if (curr->NotUsed)
					{
						TTTrace.Write(traceId, id, (ulong)curr);
						parentSyncNode->ExitWriteLock();
						curr->ExitWriteLock();
						return null;
					}

					if (curr->Parent != parent)
					{
						TTTrace.Write(traceId, id, (ulong)curr);
						parentSyncNode->ExitWriteLock();
						continue;
					}

					if (curr->Count < N)
					{
						TTTrace.Write(traceId, id, (ulong)curr, curr->Count);
						parentSyncNode->ExitWriteLock();
						continue;
					}

					Split(curr, indexInParent, comparerFinder);
					parent = curr->Parent;
					curr->ExitWriteLock();
					curr = parent;
				}
			}

			curr->ExitWriteLock();
			return null;
		}
		finally
		{
			gc.ThreadExited(epoch);
		}
	}

	public override void ReplaceObjectHandle(ulong objectHandle, ulong newObjectHandle, byte* key, long id, KeyComparer comparer)
	{
		TTTrace.Write(traceId, indexDesc.Id, id);

		int index;
		Node* curr = root;
		while (true)
		{
			if (curr->IsLeaf)
			{
				TTTrace.Write(traceId, id, (ulong)curr, (ulong)curr->ParentUnsafe);
				index = LinearSearchHandle(curr, objectHandle, true);
				Checker.AssertTrue(index != -1);
				*Node.GetLeafEntry(curr, index) = newObjectHandle;
				return;
			}

			index = BinarySearchEqualOrLarger(curr, key, id, objectHandle, comparer, null, null, true);
			TTTrace.Write(traceId, id, (ulong)curr, (ulong)curr->ParentUnsafe, index);
			Entry* entry = Node.GetEntry(curr, index, entrySize);
			if (entry->handle == objectHandle)
				entry->handle = newObjectHandle;

			curr = entry->child;
		}
	}

	public override void Delete(ulong objectHandle, byte* key, long id, KeyComparer comparer)
	{
		TTTrace.Write(traceId, indexDesc.Id, id, objectHandle);
		comparer.TTTraceKeys(traceId, 0, indexDesc.Id, key, null, stringStorage, 15);

		GarbageCollector.Epoch epoch = gc.ThreadEntered();
		try
		{
beggining:
			Node* parent = rootParent;
			Node* curr = null;
			int indexInParent = 0;
			int index = -1;

			int parentVersion = parent->EnterReadLock(isOptimistic);
			curr = root;

			int currVersion = curr->EnterLock(curr->IsLeaf, isOptimistic);

			TTTrace.Write(traceId, indexDesc.Id, id, (ulong)parent, (ulong)root, parentVersion, currVersion);

			bool parentLimitModified = false;
			bool releaseParent = true;
			while (true)
			{
				// While holding optimistic read lock, we must not read parent property of the node (since it is used by the GC)

descend:
				index = curr->IsLeaf ? LinearSearchHandle(curr, objectHandle) :
					BinarySearchEqualOrLarger(curr, key, id, objectHandle, comparer, null);

				if (!parentLimitModified && !curr->IsLeaf && index <= NodeCapacity &&
					Node.GetEntry(curr, index, entrySize)->handle == objectHandle)
				{
					TTTrace.Write(traceId, indexDesc.Id, id, (ulong)curr, (ulong)parent, index);

					parentLimitModified = true;
					if (!curr->ExitReadLock(currVersion, isOptimistic))
					{
						TTTrace.Write(traceId, id, (ulong)curr);
						parent->ExitReadLock(parentVersion, isOptimistic);
						goto beggining;
					}

					currVersion = curr->EnterLock(true, isOptimistic);
					index = BinarySearchEqualOrLarger(curr, key, id, objectHandle, comparer, null);
					if (index >= NodeCapacity || Node.GetEntry(curr, index, entrySize)->handle != objectHandle)
					{
						TTTrace.Write(traceId, id, (ulong)curr);
						parentLimitModified = false;
						curr->ExitLock(currVersion, isOptimistic);
						currVersion = curr->EnterLock(false, isOptimistic);
						goto descend;
					}
				}

				if (releaseParent && !parent->ExitReadLock(parentVersion, isOptimistic))
				{
					TTTrace.Write(traceId, id, (ulong)curr, (ulong)parent);
					curr->ExitLock(currVersion, isOptimistic);
					goto beggining;
				}

				releaseParent = !parentLimitModified;

				TTTrace.Write(traceId, indexDesc.Id, id, (ulong)curr, (ulong)parent, releaseParent, curr->IsLeaf, index, curr->Count);

				if (curr->IsLeaf)
					break;

				Node* child = null;
				if (index < NodeCapacity)
					child = child = Node.GetEntry(curr, index, entrySize)->child;

				if (child == null)
				{
					curr->ExitLock(currVersion, isOptimistic);
					goto beggining;
				}

				parentVersion = currVersion;
				parent = curr;
				curr = child;
				indexInParent = index;

				currVersion = curr->EnterLock(curr->IsLeaf | parentLimitModified, isOptimistic);
			}

			TTTrace.Write(traceId, id, (ulong)curr, (ulong)parent, index, indexInParent, curr->Count);

			if (curr->Count == 1)
			{
				Checker.AssertFalse(root == curr);
				if (parentLimitModified)
					UpdateParentLimitsAndReleaseLocks(curr, indexInParent);

				curr->ExitWriteLock();
				goto beggining;
			}

			if (index == -1)	// Item not found in leaf
			{
				// This can happen if the original insert produced an error which caused the transaction to raollback
				// (which further caused this Delete). The insert didn't execute in the first place.
				Checker.AssertFalse(parentLimitModified);
				curr->ExitWriteLock();
				return;
			}

			Checker.AssertFalse(parentLimitModified && *Node.GetLeafEntry(curr, curr->Count - 1) != objectHandle);

			RemoveRange(curr, index, 1, true);

			if (parentLimitModified)
				UpdateParentLimitsAndReleaseLocks(curr, indexInParent);

			while (curr->Count < NHalf)
			{
				parent = curr->Parent;
				if (parent == null)
				{
					TTTrace.Write(traceId, indexDesc.Id, id, (ulong)curr, curr->Count);

					// curr is root
					if (!root->IsLeaf && root->Count == 1)
						RemoveRoot();

					root->ExitWriteLock();
					return;
				}

				if (parent->TryEnterWriteLock())
				{
					Rebalance(curr, indexInParent);
					curr->ExitWriteLock();
					curr = parent;
				}
				else
				{
					curr->ExitWriteLock();
					parent->EnterWriteLock();
					curr->EnterWriteLock();

					TTTrace.Write(traceId, id, (ulong)curr);

					if (curr->NotUsed)
					{
						TTTrace.Write(traceId, id, (ulong)curr);
						parent->ExitWriteLock();
						curr->ExitWriteLock();
						return;
					}

					if (curr->Parent != parent)
					{
						TTTrace.Write(traceId, id, (ulong)curr);
						parent->ExitWriteLock();
						continue;
					}

					if (curr->Count >= NHalf)
					{
						TTTrace.Write(traceId, id, (ulong)curr);
						parent->ExitWriteLock();
						continue;
					}

					Rebalance(curr, indexInParent);
					curr->ExitWriteLock();
					curr = parent;
				}
			}

			curr->ExitWriteLock();
		}
		finally
		{
			gc.ThreadExited(epoch);
		}
	}

	public DatabaseErrorDetail ScanNext(RangeScanBase scan, IList<ObjectReader> objects, int fetchCountLimit)
	{
		DatabaseErrorDetail error = null;

		if (scan.Finished)
			return null;

		GarbageCollector.Epoch epoch = gc.ThreadEntered();
		try
		{
			if (scan.IsForward)
				error = ScanForward(scan, objects, fetchCountLimit);
			else
				error = ScanBackward(scan, objects, fetchCountLimit);

			if (error != null)
				return error;

			error = ReadLockObjects(scan, objects);
			if (error != null)
				return error;
		}
		finally
		{
			gc.ThreadExited(epoch);
		}

		GroupLockRanges(scan);

		return null;
	}

	public override IndexScanRange[] SplitScanRange()
	{
		if (root->IsLeaf)
			return new SortedIndexScanRange[] { new SortedIndexScanRange(root, null) };

		Node* curr = root;
		while (true)
		{
			Node* child = Node.GetEntry(curr, 0, entrySize)->child;
			if (child->IsLeaf)
				break;

			curr = child;
		}

		long leafCount = 0;
		Node* temp = curr;
		while (temp != null)
		{
			leafCount += temp->Count;
			temp = temp->RightUnsafe;
		}

		curr = Node.GetEntry(curr, 0, entrySize)->child;

		Utils.Range[] ranges = Utils.SplitRange(leafCount, database.Engine.Settings.ResizeSplitSize, ProcessorNumber.CoreCount);
		List<IndexScanRange> l = new List<IndexScanRange>(ranges.Length);

		for (int i = 0; i < ranges.Length; i++)
		{
			long count = ranges[i].Count;
			Node* start = curr;
			for (int j = 0; j < count; j++)
			{
				curr = curr->RightUnsafe;
			}

			l.Add(new SortedIndexScanRange(start, curr));
		}

		Checker.AssertTrue(curr == null);
		return l.ToArray();
	}

	public override DatabaseErrorDetail CheckUniqueness(IndexScanRange scanRange)
	{
		SortedIndexScanRange range = (SortedIndexScanRange)scanRange;
		Dictionary<short, KeyComparer> localCache = null;

		Node* curr = range.StartNode;
		while (curr != range.EndNode)
		{
			ulong leftHandle = *Node.GetLeafEntry(curr, 0);

			byte* leftKey = GetKeyAndComparer(leftHandle, false, out var leftComparer, out _, out var @class);

			if (leftComparer == null)
				leftComparer = ProvideComparerInModelUpdate(ref localCache, @class.ClassDesc);

			int c = curr->Count;
			for (int i = 0; i < c; i++)
			{
				ulong rightHandle = 0;
				if (i == c - 1)
				{
					if (curr->RightUnsafe != null)
						rightHandle = *Node.GetLeafEntry(curr->RightUnsafe, 0);
				}
				else
				{
					rightHandle = *Node.GetLeafEntry(curr, i + 1);
				}

				if (rightHandle != 0)
				{
					byte* rightKey = GetKeyAndComparer(rightHandle, false, out var rightComparer, out _, out @class);

					if (rightKey != null)
					{
						if (rightComparer == null)
							rightComparer = ProvideComparerInModelUpdate(ref localCache, @class.ClassDesc);

						if (leftComparer.Equals(leftKey, null, rightKey, rightComparer, stringStorage))
						{
							leftComparer.TTTraceKeys(traceId, 0, indexDesc.Id, leftKey, null, stringStorage, 16);
							rightComparer.TTTraceKeys(traceId, 0, indexDesc.Id, rightKey, null, stringStorage, 17);
							return DatabaseErrorDetail.CreateUniquenessConstraint(SortedIndexDesc.FullName);
						}
					}

					leftKey = rightKey;
					leftComparer = rightComparer;
				}
			}

			curr = curr->RightUnsafe;
		}

		return null;
	}

	public override void PrepareForPendingRefill(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(traceId, indexDesc.Id);

		FreeBuffers(workers);
		pendingRefill = true;
	}

	public override void PendingRefillStarted(long capacity)
	{
		TTTrace.Write(traceId, indexDesc.Id);

		rootParent = Node.Create(rootManager, false);
		root = Node.Create(leafManager, true);
		Node.InitLeafWithMaxKey(root);
	}

	public override void PendingRefillFinished()
	{
		TTTrace.Write(traceId, indexDesc.Id);

		pendingRefill = false;
	}

	public void CommitRange(ulong rangeHandle, Transaction tran)
	{
		Range* range = (Range*)memoryManager.GetBuffer(rangeHandle);

		while (range != null)
		{
			Node* node = LockRangeNode(true, range);
			TTTrace.Write(traceId, indexDesc.Id, (ulong)range, (ulong)node, tran.Id, tran.CommitVersion);
			Checker.AssertTrue(range->version == tran.Id);
			range->version = tran.CommitVersion;
			range = range->nextInScan;
			node->ExitWriteLock();
		}
	}

	public void GarbageCollectRange(ulong rangeHandle)
	{
		Range* range = (Range*)memoryManager.GetBuffer(rangeHandle);
		while (range != null)
		{
			Node* node = LockRangeNode(true, range);
			TTTrace.Write(traceId, indexDesc.Id, (ulong)range, (ulong)node);
			node->RemoveLockedRange(range);
			node->ExitWriteLock();

			Range* next = range->nextInScan;
			Range.ReleaseStrings(range, localComparer, stringStorage);
			gc.AddGarbage(range);
			range = next;
		}
	}

	public void RollbackRange(ulong rangeHandle, Transaction tran)
	{
		Range* range = (Range*)memoryManager.GetBuffer(rangeHandle);
		while (range != null)
		{
			Range.Rollback(range);
			range = range->nextInScan;
		}
	}

	public override void Dispose(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(traceId, indexDesc.Id);

		FreeBuffers(workers);
	}

	private void FreeSubtree(Node* curr)
	{
		Range* currRange = curr->LockedRangesUnsafe;
		while (currRange != null)
		{
			Range* nextRange = currRange->next;
			memoryManager.Free(Range.GetHandle(currRange));
			currRange = nextRange;
		}

		if (curr->IsLeaf)
		{
			memoryManager.Free(Node.GetHandle(curr));
			return;
		}

		for (int i = 0; i < curr->Count; i++)
		{
			Entry* entry = Node.GetEntry(curr, i, entrySize);
			Node* child = entry->child;
			localComparer.ReleaseStrings(Entry.Key(entry), stringStorage);
			FreeSubtree(child);
		}

		memoryManager.Free(Node.GetHandle(curr));
	}

	private void FreeBuffers(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(traceId, indexDesc.Id);
		Checker.AssertTrue(root->LockedRangesUnsafe == null);

		if (root->IsLeaf)
		{
			memoryManager.Free(Node.GetHandle(root));
			return;
		}

		Utils.Range[] ranges = Utils.SplitRange(root->Count, 1, ProcessorNumber.CoreCount);
		workers.SetAction(o =>
		{
			Utils.Range r = (Utils.Range)o.ReferenceParam;
			for (int i = (int)r.Offset; i < r.Offset + r.Count; i++)
			{
				Node* curr = Node.GetEntry(root, i, entrySize)->child;
				FreeSubtree(curr);
			}
		});

		Array.ForEach(ranges, x => workers.EnqueueWork(new CommonWorkerParam() { ReferenceParam = x }));
		workers.Drain();

		for (int i = 0; i < root->Count; i++)
		{
			localComparer.ReleaseStrings(Entry.Key(Node.GetEntry(root, i, entrySize)), stringStorage);
		}

		memoryManager.Free(Node.GetHandle(root));
		memoryManager.Free(Node.GetHandle(rootParent));
		root = null;
		rootParent = null;
		TTTrace.Write(traceId, indexDesc.Id);
	}

	private KeyComparer ProvideComparerInModelUpdate(ref Dictionary<short, KeyComparer> localCache, ClassDescriptor classDesc)
	{
		if (localCache == null)
			localCache = new Dictionary<short, KeyComparer>(2);

		if (localCache.TryGetValue(classDesc.Id, out KeyComparer comparer))
			return comparer;

		KeyComparerDesc kad = classDesc.GetIndexAccessDescByPropertyName(indexDesc);
		comparer = new KeyComparer(kad);
		localCache.Add(classDesc.Id, comparer);

		return comparer;
	}

	private DatabaseErrorDetail ScanForward(RangeScanBase scan, IList<ObjectReader> objects, int fetchCountLimit)
	{
		TTTrace.Write(traceId, indexDesc.Id, scan.Tran.Id, (int)scan.Tran.Type);

		Transaction tran = scan.Tran;

beggining:
		Node* parent = rootParent;
		Node* curr = null;
		int index = -1;

		int parentVersion = parent->EnterReadLock(isOptimistic);
		curr = root;

		bool isWrite = curr->IsLeaf && tran.Type == TransactionType.ReadWrite;
		int currVersion = curr->EnterLock(isWrite, isOptimistic && !curr->IsLeaf);

		TTTrace.Write(traceId, indexDesc.Id, isWrite, (ulong)root, currVersion);

		while (true)
		{
			// While holding optimistic read lock, we must not read parent property of the node (since it is used by the GC)!

			if (!parent->ExitReadLock(parentVersion, isOptimistic))
			{
				TTTrace.Write(traceId, (ulong)curr, (ulong)parent);
				curr->ExitLock(currVersion, isOptimistic && !curr->IsLeaf);
				goto beggining;
			}

			index = BinarySearchEqualOrLarger(curr, scan.StartKey, scan.StartId, scan.StartHandle, scan.Comparer, scan.StartStrings);
			if (curr->IsLeaf)
				break;

			Node* child = null;
			if (index < NodeCapacity)
			{
				TTTrace.Write(traceId, (ulong)curr, (ulong)parent);
				child = child = Node.GetEntry(curr, index, entrySize)->child;
			}

			if (child == null)
			{
				TTTrace.Write(traceId, (ulong)curr, (ulong)parent);
				curr->ExitLock(currVersion, isOptimistic);
				goto beggining;
			}

			parentVersion = currVersion;
			parent = curr;
			curr = child;
			isWrite = curr->IsLeaf && tran.Type == TransactionType.ReadWrite;
			currVersion = curr->EnterLock(isWrite, isOptimistic && !curr->IsLeaf);

			TTTrace.Write(traceId, (ulong)curr, (ulong)parent, currVersion);
		}

		while (objects.Count <= fetchCountLimit - curr->Count)
		{
			TTTrace.Write(traceId, (ulong)curr, currVersion, index, curr->Count);

			if (tran.Type == TransactionType.ReadWrite)
			{
				CreateForwardRangeLock(scan, curr);
				curr->DowngradeWriteLockToRead();
			}

			for (int i = index; i < curr->Count; i++)
			{
				ulong* entry = Node.GetLeafEntry(curr, i);

				byte* objectKey = GetKeyAndComparer(*entry, true, out var itemComparer, out long id, out Class @class);

				bool isBefore = scan.Comparer.IsBefore(scan.EndKey, scan.EndId,
					scan.EndHandle, scan.EndStrings, objectKey, id, *entry, itemComparer, stringStorage, out bool isEqual);

				if (isBefore || (scan.IsEndOpen && isEqual))
				{
					scan.Finished = true;
					curr->ExitLock(false);
					return null;
				}

				if (!scan.InitialClearingDone)
				{
					isBefore = scan.Comparer.IsBefore(scan.StartKey, scan.StartId, scan.StartHandle, scan.StartStrings,
						objectKey, id, *entry, itemComparer, stringStorage, out isEqual);
					scan.InitialClearingDone = isBefore || (!scan.IsStartOpen && isEqual);
					if (scan.InitialClearingDone)
						TTTrace.Write(traceId, indexDesc.Id, id, i);
				}

				if (scan.InitialClearingDone)
				{
					ClassObject* obj = Class.GetObjectByHandle(*entry);
					bool isVisible = IsVisible(tran, obj, out bool isConflicting);
					TTTrace.Write(traceId, indexDesc.Id, id, obj->version, isVisible, isConflicting, i);
					if (tran.Type == TransactionType.ReadWrite && isConflicting)
					{
						scan.Finished = true;
						curr->ExitLock(false);
						return DatabaseErrorDetail.Create(DatabaseErrorType.IndexConflict);
					}

					if (isVisible)
						objects.Add(new ObjectReader((byte*)*entry, @class));
				}
			}

			Node* right = curr->Right;
			Checker.AssertTrue(right != null);

			currVersion = right->EnterLock(tran.Type == TransactionType.ReadWrite, false);
			curr->ExitLock(false);
			curr = right;
			index = 0;
		}

		WriteFirstKeyAsStart(scan, curr);
		curr->ExitLock(tran.Type == TransactionType.ReadWrite);
		return null;
	}

	private DatabaseErrorDetail ScanBackward(RangeScanBase scan, IList<ObjectReader> objects, int fetchCountLimit)
	{
		TTTrace.Write(traceId, indexDesc.Id, scan.Tran.Id, (int)scan.Tran.Type);

		Transaction tran = scan.Tran;

beggining:
		Node* parent = rootParent;
		Node* curr = null;
		int index = -1;

		int parentVersion = parent->EnterReadLock(isOptimistic);
		curr = root;

		bool isWrite = curr->IsLeaf && tran.Type == TransactionType.ReadWrite;
		int currVersion = curr->EnterLock(isWrite, isOptimistic && !curr->IsLeaf);

		TTTrace.Write(traceId, indexDesc.Id, isWrite, (ulong)root, currVersion);

		while (true)
		{
			// While holding optimistic read lock, we must not read parent property of the node (since it is used by the GC)!

			if (!parent->ExitReadLock(parentVersion, isOptimistic))
			{
				TTTrace.Write(traceId, (ulong)curr, (ulong)parent);
				curr->ExitLock(currVersion, isOptimistic && !curr->IsLeaf);
				goto beggining;
			}

			index = BinarySearchEqualOrLarger(curr, scan.EndKey, scan.EndId, scan.EndHandle, scan.Comparer, scan.EndStrings);
			if (curr->IsLeaf)
				break;

			Node* child = null;
			if (index < NodeCapacity)
			{
				TTTrace.Write(traceId, (ulong)curr, (ulong)parent);
				child = child = Node.GetEntry(curr, index, entrySize)->child;
			}

			if (child == null)
			{
				TTTrace.Write(traceId, (ulong)curr, (ulong)parent);
				curr->ExitLock(currVersion, isOptimistic);
				goto beggining;
			}

			parentVersion = currVersion;
			parent = curr;
			curr = child;
			isWrite = curr->IsLeaf && tran.Type == TransactionType.ReadWrite;
			currVersion = curr->EnterLock(isWrite, isOptimistic && !curr->IsLeaf);

			TTTrace.Write(traceId, (ulong)curr, (ulong)parent, currVersion);
		}

		scan.InitialClearingDone = false;
		while (objects.Count <= fetchCountLimit - curr->Count)
		{
			TTTrace.Write(traceId, (ulong)curr, currVersion, index);

			Node* left = curr->Left;
			if (tran.Type == TransactionType.ReadWrite)
			{
				if (!TryLockLeftLeaf(curr, left, true, false))
				{
					curr->ExitLock(true);
					goto beggining;
				}

				CreateBackwardRangeLock(scan, curr, left);
				if (left != null)
					left->ExitReadLock(0, false);

				curr->DowngradeWriteLockToRead();
			}

			bool added = false;
			for (int i = index; i >= 0; i--)
			{
				ulong* entry = Node.GetLeafEntry(curr, i);

				byte* objectKey = GetKeyAndComparer(*entry, true, out var itemComparer, out long id, out Class @class);
				bool isAfter = scan.Comparer.IsAfter(scan.StartKey, scan.StartId, scan.StartHandle, scan.StartStrings,
					objectKey, id, *entry, itemComparer, stringStorage, out bool isEqual);

				if (isAfter || (scan.IsStartOpen && isEqual))
				{
					scan.Finished = true;
					curr->ExitLock(false);
					return null;
				}

				if (!scan.InitialClearingDone)
				{
					isAfter = scan.Comparer.IsAfter(scan.EndKey, scan.EndId, scan.EndHandle, scan.EndStrings,
						objectKey, id, *entry, itemComparer, stringStorage, out isEqual);
					scan.InitialClearingDone = isAfter || (!scan.IsEndOpen && isEqual);
				}

				if (scan.InitialClearingDone)
				{
					ClassObject* obj = Class.GetObjectByHandle(*entry);
					bool isVisible = IsVisible(scan.Tran, obj, out bool isConflicting);
					if (scan.Tran.Type == TransactionType.ReadWrite && isConflicting)
					{
						scan.Finished = true;
						curr->ExitLock(false);
						return DatabaseErrorDetail.Create(DatabaseErrorType.IndexConflict);
					}

					if (isVisible)
					{
						objects.Add(new ObjectReader((byte*)*entry, @class));
						added = true;
					}
				}
			}

			if (added)
				WriteLastScannedToEndKey(scan, objects);

			if (left == null)
			{
				scan.Finished = true;
				curr->ExitLock(false);
				return null;
			}

			if (!TryLockLeftLeaf(curr, left, false, tran.Type == TransactionType.ReadWrite))
			{
				curr->ExitLock(false);
				goto beggining;
			}

			curr->ExitLock(false);
			index = left->Count - 1;
			curr = left;
		}

		curr->ExitLock(tran.Type == TransactionType.ReadWrite);
		return null;
	}

	private bool TryLockLeftLeaf(Node* curr, Node* left, bool isCurrWrite, bool isWrite)
	{
		if (left == null || left->TryEnterLock(isWrite))
			return true;

		int version = curr->version;
		curr->ExitLock(isCurrWrite);
		left->EnterLock(isWrite, false);
		curr->EnterLock(isCurrWrite, false);
		if (curr->version != version)
		{
			left->ExitLock(isWrite);
			return false;
		}

		return true;
	}

	private Node* LockRangeNode(bool isWrite, Range* range)
	{
		while (true)
		{
			Node* node = range->owner;
			node->EnterLock(isWrite, false);
			if (range->owner == node)
				return node;

			node->ExitLock(isWrite);
		}
	}

	private Range* CreateForwardRangeLock(RangeScanBase scan, Node* curr)
	{
		Range* range;
		ulong* entry = Node.GetLeafEntry(curr, curr->Count - 1);
		byte* lastKey = GetKeyAndComparer(*entry, true, out KeyComparer comparer, out long id, out _);

		if (scan.LeafLockCount == 0)
		{
			range = Range.CreateForwardInitial(rangeManager, stringStorage, scan, lastKey, id, *entry, comparer);
			scan.TransactionReadLock = scan.Tran.Context.AddKeyRangeReadLock(Range.GetHandle(range), IndexDesc.Index);
		}
		else
		{
			range = Range.CreateForwardNext(rangeManager, stringStorage, scan, lastKey, id, *entry, comparer);
		}

		TTTrace.Write(traceId, indexDesc.Id, (ulong)range, (ulong)curr);
		localComparer.TTTraceKeys(traceId, scan.Tran.Id, indexDesc.Id, Range.GetStart(range), null, stringStorage, 18);
		localComparer.TTTraceKeys(traceId, scan.Tran.Id, indexDesc.Id, Range.GetEnd(range, localComparer.KeySize), null, stringStorage, 19);

		Node.AddLockedRange(curr, range);
		scan.AddLeafLock(range, stringStorage);

		return range;
	}

	private Range* CreateBackwardRangeLock(RangeScanBase scan, Node* curr, Node* prev)
	{
		Range* range;

		ulong* entry = null;
		ulong entryVal = 0;
		byte* lastKey = null;
		long id = 0;
		KeyComparer comparer = null;

		if (prev != null)
		{
			entry = Node.GetLeafEntry(prev, prev->Count - 1);
			entryVal = *entry;
			lastKey = GetKeyAndComparer(*entry, true, out comparer, out id, out _);
		}

		if (scan.LeafLockCount == 0)
		{
			range = Range.CreateBackwardInitial(rangeManager, stringStorage, scan, lastKey, id, entryVal, comparer);
			scan.TransactionReadLock = scan.Tran.Context.AddKeyRangeReadLock(Range.GetHandle(range), IndexDesc.Index);
		}
		else
		{
			range = Range.CreateBackwardNext(rangeManager, stringStorage, scan, lastKey, id, entryVal, comparer);
		}

		TTTrace.Write(traceId, indexDesc.Id, (ulong)range, (ulong)prev,(ulong)curr);
		localComparer.TTTraceKeys(traceId, scan.Tran.Id, indexDesc.Id, Range.GetStart(range), null, stringStorage, 20);
		localComparer.TTTraceKeys(traceId, scan.Tran.Id, indexDesc.Id, Range.GetEnd(range, localComparer.KeySize), null, stringStorage, 21);

		Node.AddLockedRange(curr, range);
		scan.AddLeafLock(range, stringStorage);

		return range;
	}

	private void GroupLockRanges(RangeScanBase scan)
	{
		TTTrace.Write(traceId, indexDesc.Id, scan.Tran.Id);
		while (scan.LeafLockCount > NodeCapacity)
		{
			Range* firstRange = scan.PeekLeafLock();

			// Read parent node of the first locked leaf, and write lock it
			Node* node, parent;
			while (true)
			{
				node = firstRange->owner;
				parent = node->ParentUnsafe;
				if (parent == null) // Tree was modified so that it only has single leaf (root)
					return;

				parent->EnterWriteLock();
				// Since the parent  is locked, there is no way for the range to change owner node or for the owner node to change its parent.
				if (firstRange->owner == node && node->ParentUnsafe == parent)
					break;

				parent->ExitWriteLock();
			}

			// We now hold the parent lock

			TTTrace.Write(traceId, indexDesc.Id, scan.Tran.Id, (ulong)parent);

			Range* lastRange = null;
			Range* currRange = firstRange;
			while (currRange != null && currRange->owner->ParentUnsafe == parent)
			{
				lastRange = currRange;
				currRange = currRange->nextInScan;
			}

			Range* groupedRange;
			if (scan.IsForward)
			{
				groupedRange = Range.CreateEnvelope(rangeManager, stringStorage, firstRange, lastRange, localComparer);
			}
			else
			{
				groupedRange = Range.CreateEnvelope(rangeManager, stringStorage, lastRange, firstRange, localComparer);
			}

			Node.AddLockedRange(parent, groupedRange);

			TTTrace.Write(traceId, indexDesc.Id, (ulong)groupedRange, (ulong)parent);
			localComparer.TTTraceKeys(traceId, scan.Tran.Id, indexDesc.Id, Range.GetStart(groupedRange), null, stringStorage, 20);
			localComparer.TTTraceKeys(traceId, scan.Tran.Id, indexDesc.Id, Range.GetEnd(groupedRange, localComparer.KeySize), null, stringStorage, 21);

			currRange = firstRange;
			while (true)
			{
				currRange->owner->EnterWriteLock();
				currRange->owner->RemoveLockedRange(currRange);
				currRange->owner->ExitWriteLock();

				scan.RemoveLeafLock();
				Range* next = currRange->nextInScan;
				Range.ReleaseStrings(currRange, localComparer, stringStorage);
				gc.AddGarbage(currRange);

				if (currRange == lastRange)
					break;

				currRange = next;
			}

			parent->ExitWriteLock();

			if (scan.PeekLeafLock() == null)
			{
				scan.TransactionReadLock.Modify(Range.GetHandle(groupedRange));
			}
			else
			{
				scan.TransactionReadLock.Modify(Range.GetHandle(scan.PeekLeafLock()));
				scan.Tran.Context.AddKeyRangeReadLock(Range.GetHandle(groupedRange), IndexDesc.Index);
			}
		}
	}

	private DatabaseErrorDetail ReadLockObjects(RangeScanBase scan, IList<ObjectReader> objects)
	{
		int c = objects.Count;
		for (int i = 0; i < c; i++)
		{
			ulong objectHandle = (ulong)objects[i].RawObject;
			if (scan.Tran.Type == TransactionType.ReadWrite)
			{
				DatabaseErrorDetail error = objects[i].Class.ReadLockObjectFromIndex(scan.Tran, objectHandle, indexDesc.Id);
				if (error != null)
					return error;
			}

			ObjectReader r = new ObjectReader(ClassObject.ToDataPointer(Class.GetObjectByHandle(objectHandle)), objects[i].Class);
			objects[i] = r;
		}

		return null;
	}

	private void WriteLastScannedToEndKey(RangeScanBase scan, IList<ObjectReader> objects)
	{
		ulong entry = (ulong)objects[objects.Count - 1].RawObject;

		byte* objectKey = GetKeyAndComparer(entry, true, out KeyComparer itemComparer, out long id, out _);
		scan.WriteEndKey(objectKey, id, entry, true, itemComparer, stringStorage);

		TTTrace.Write(traceId, indexDesc.Id, scan.Tran.Id);
		itemComparer.TTTraceKeys(traceId, scan.Tran.Id, indexDesc.Id, objectKey, null, stringStorage, 20);
	}

	private void WriteFirstKeyAsStart(RangeScanBase scan, Node* node)
	{
		ulong* entry = Node.GetLeafEntry(node, 0);
		byte* objectKey = GetKeyAndComparer(*entry, true, out KeyComparer itemComparer, out long id, out _);
		scan.WriteStartKey(objectKey, id, *entry, false, itemComparer, stringStorage);

		TTTrace.Write(traceId, indexDesc.Id, scan.Tran.Id);
		itemComparer.TTTraceKeys(traceId, scan.Tran.Id, indexDesc.Id, objectKey, null, stringStorage, 20);
	}

	private void RemoveRoot()
	{
		if (rootParent->TryEnterWriteLock())
		{
			root->NotUsed = true;
			Node* child = Node.GetEntry(root, 0, entrySize)->child;
			child->EnterWriteLock();
			root->ExitWriteLock();
			gc.AddGarbage(root);
			root = child;
			root->Parent = null;
			rootParent->ExitWriteLock();
		}
		else
		{
			root->ExitWriteLock();
			rootParent->EnterWriteLock();
			root->EnterWriteLock();

			if (root->NotUsed || root->Parent != null || root->Count > 1)
			{
				rootParent->ExitWriteLock();
				return;
			}

			root->NotUsed = true;
			Node* child = Node.GetEntry(root, 0, entrySize)->child;
			child->EnterWriteLock();
			root->ExitWriteLock();
			gc.AddGarbage(root);
			root = child;
			root->Parent = null;
			rootParent->ExitWriteLock();
		}
	}

	private void Rebalance(Node* node, int indexInParent)
	{
		TTTrace.Write(traceId, indexDesc.Id, (ulong)node, indexInParent, node->Count);

		Node* parent = node->Parent;
		indexInParent = FindIndexInParent(node, indexInParent);

		Node* left = node->Left;
		Node* right = node->Right;

		bool rebalancedLeft = false;
		if (left != null && left->TryEnterWriteLock())
		{
			if (left->Parent == parent)
			{
				if (left->Count + node->Count < N)
				{
					if (right != null)
						right->EnterWriteLock();

					MergeRight(left, indexInParent - 1);
					if (right != null)
						right->ExitWriteLock();

					TTTrace.Write(traceId, indexDesc.Id, (ulong)node, (ulong)left, left->Count);
					rebalancedLeft = true;
				}
				else if (left->Count - node->Count >= 2)
				{
					TTTrace.Write(traceId, indexDesc.Id, (ulong)node, (ulong)left, left->Count);
					RebalanceRight(left, indexInParent - 1);
					rebalancedLeft = true;
				}
			}

			left->ExitWriteLock();
		}

		if (!rebalancedLeft && right != null)
		{
			right->EnterWriteLock();
			if (parent == right->Parent)
			{
				if (right->Count + node->Count < N)
				{
					Node* rightRight = right->Right;
					if (rightRight != null)
						rightRight->EnterWriteLock();

					TTTrace.Write(traceId, indexDesc.Id, (ulong)node, (ulong)right, right->Count);
					MergeRight(node, indexInParent);

					if (rightRight != null)
						rightRight->ExitWriteLock();
				}
				else if (right->Count - node->Count >= 2)
				{
					TTTrace.Write(traceId, indexDesc.Id, (ulong)node, (ulong)right, right->Count);
					RebalanceLeft(right, indexInParent + 1);
				}
			}

			right->ExitWriteLock();
		}
	}

	private void RebalanceRight(Node* node, int indexInParent)
	{
		Node* right = node->Right;
		Node* parent = node->Parent;

		int c = (node->Count - right->Count) / 2;

		if (!node->IsLeaf)
		{
			for (int i = node->Count - c; i < node->Count; i++)
			{
				Node* child = Node.GetEntry(node, i, entrySize)->child;
				child->EnterWriteLock();
				child->Parent = right;
			}
		}

		InsertRangeBeginning(right, node, node->Count - c, c);
		RemoveRange(node, node->Count - c, c, false);

		RefreshKey(parent, indexInParent, node, true);

		RearrangeRangeLocksRight(node, right, parent);

		if (!node->IsLeaf)
		{
			for (int i = 0; i < c; i++)
			{
				Node.GetEntry(right, i, entrySize)->child->ExitWriteLock();
			}
		}
	}

	private void RebalanceLeft(Node* node, int indexInParent)
	{
		Node* left = node->Left;
		Node* parent = node->Parent;

		int c = (node->Count - left->Count) / 2;

		if (!node->IsLeaf)
		{
			for (int i = 0; i < c; i++)
			{
				Node* child = Node.GetEntry(node, i, entrySize)->child;
				child->EnterWriteLock();
				child->Parent = left;
			}
		}

		AddRange(left, node, 0, c);
		RemoveRange(node, 0, c, false);

		RefreshKey(parent, indexInParent - 1, left, true);

		RearrangeRangeLocksLeft(node, left, parent);

		if (!node->IsLeaf)
		{
			for (int i = left->Count - c; i < left->Count; i++)
			{
				Node.GetEntry(left, i, entrySize)->child->ExitWriteLock();
			}
		}
	}

	private void MergeRight(Node* node, int indexInParent)
	{
		Node* right = node->Right;
		Node* parent = node->Parent;

		if (!node->IsLeaf)
		{
			int c = right->Count;
			for (int i = 0; i < c; i++)
			{
				Node* child = Node.GetEntry(right, i, entrySize)->child;
				child->EnterWriteLock();
				child->Parent = node;
			}
		}

		AddRange(node, right);
		Node.AddLockedRangeMultiple(node, right->LockedRanges);
		right->LockedRanges = null;

		node->Right = right->Right;
		if (right->Right != null)
			right->Right->Left = node;

		right->NotUsed = true;
		gc.AddGarbage(right);

		RefreshKey(parent, indexInParent, node, true);
		RemoveRange(parent, indexInParent + 1, 1, true);

		if (!node->IsLeaf)
		{
			int c = right->Count;
			for (int i = 0; i < right->Count; i++)
			{
				Node* child = Node.GetEntry(right, i, entrySize)->child;
				child->ExitWriteLock();
			}
		}
	}

	public void RearrangeRangeLocksRight(Node* node, Node* right, Node* parent)
	{
		Range* currRange = node->LockedRanges;
		while (currRange != null)
		{
			if (!IsAfterLastKey(node, Range.GetEnd(currRange, localComparer.KeySize),
				currRange->endId, currRange->endHandle, false))
			{
				currRange = currRange->next;
			}
			else
			{
				Range* next = currRange->next;
				node->RemoveLockedRange(currRange);
				if (right != null && IsAfterLastKey(node, Range.GetStart(currRange), currRange->startId, currRange->startHandle, currRange->isStartOpen))
					Node.AddLockedRange(right, currRange);
				else
					Node.AddLockedRange(parent, currRange);

				currRange = next;
			}
		}
	}

	public void RearrangeRangeLocksLeft(Node* node, Node* left, Node* parent)
	{
		Range* currRange = node->LockedRanges;
		while (currRange != null)
		{
			if (IsAfterLastKey(left, Range.GetStart(currRange), currRange->startId, currRange->startHandle, currRange->isStartOpen))
			{
				currRange = currRange->next;
			}
			else
			{
				Range* next = currRange->next;
				node->RemoveLockedRange(currRange);
				if (!IsAfterLastKey(left, Range.GetEnd(currRange, localComparer.KeySize), currRange->endId, currRange->endHandle, false))
					Node.AddLockedRange(left, currRange);
				else
					Node.AddLockedRange(parent, currRange);

				currRange = next;
			}
		}
	}

	private void UpdateParentLimitsAndReleaseLocks(Node* curr, int indexInParent)
	{
		bool isLastInParent;
		bool isFirst = true;

		do
		{
			indexInParent = FindIndexInParent(curr, indexInParent);
			Node* parent = curr->Parent;
			isLastInParent = parent->Count - 1 == indexInParent;
			RefreshKey(parent, indexInParent, curr, true);
			RearrangeRangeLocksRight(curr, null, parent);

			if (!isFirst)
				curr->ExitWriteLock();

			curr = parent;
			isFirst = false;
		}
		while (isLastInParent && curr->Parent != null);

		curr->ExitWriteLock();
	}

	private int FindIndexInParent(Node* node, int index, Func<short, KeyComparer> comparerFinder = null)
	{
		Node* parent = node->Parent;
		if (index < parent->Count && Node.GetEntry(parent, index, entrySize)->child == node)
			return index;

		if (node->IsLeaf)
		{
			ulong* lastEntry = Node.GetLeafEntry(node, node->Count - 1);
			byte* objectKey = GetKeyAndComparer(comparerFinder, *lastEntry, true, out KeyComparer itemComparer, out long id, out _);
			if (objectKey == null)
				return parent->Count - 1;

			return BinarySearchEqualOrLarger(parent, objectKey, id, *lastEntry, itemComparer, null);
		}
		else
		{
			Entry* lastEntry = Node.GetEntry(node, node->Count - 1, entrySize);
			if (lastEntry->handle == KeyComparer.MaxKey)
				return parent->Count - 1;

			return BinarySearchEqualOrLarger(parent, Entry.Key(lastEntry), lastEntry->id, lastEntry->handle, localComparer, null);
		}
	}

	private bool IsRangeLocked(Transaction tran, Node* node, byte* key, long id, ulong handle, KeyComparer keyComparer)
	{
		Range* curr = node->LockedRanges;
		while (curr != null)
		{
			if (Range.IsInConflict(tran, curr, localComparer, key, id, handle, keyComparer, stringStorage))
				return true;

			curr = curr->next;
		}

		return false;
	}

	private void Split(Node* node, int indexInParent, Func<short, KeyComparer> comparerFinder)
	{
		TTTrace.Write(traceId, indexDesc.Id, (ulong)node, indexInParent);

		Node* parent = node->Parent;
		Node* right = node->Right;
		if (right != null)
			right->EnterWriteLock();

		if (parent == null)
		{
			TTTrace.Write(traceId, indexDesc.Id, (ulong)node, indexInParent);

			parent = Node.Create(nodeManager, false);
			parent->EnterWriteLock();
			root = parent;
			Node.InitNodeWithMaxKey(root, node, localComparer.KeySize);
			node->Parent = parent;
			rootParent->ExitWriteLock();
		}

		indexInParent = FindIndexInParent(node, indexInParent, comparerFinder);
		if (indexInParent >= parent->Count)
			throw new CriticalDatabaseException();

		if (!node->IsLeaf)
		{
			for (int i = NHalf; i < node->Count; i++)
			{
				Node.GetEntry(node, i, entrySize)->child->EnterWriteLock();
			}
		}

		int c = node->Count - NHalf;
		Node* newNode = Node.Create(nodeManager, leafManager, node, c, NHalf, entrySize);
		newNode->EnterWriteLock();

		newNode->Parent = parent;
		newNode->Left = node;
		node->Right = newNode;

		if (!node->IsLeaf)
		{
			for (int i = 0; i < newNode->Count; i++)
			{
				Node.GetEntry(newNode, i, entrySize)->child->Parent = newNode;
			}
		}

		RemoveRange(node, c, NHalf, false);
		newNode->Right = right;
		if (right != null)
			right->Left = newNode;

		RefreshKey(parent, indexInParent, node, true, comparerFinder);
		InsertNewChild(parent, indexInParent + 1, newNode, comparerFinder);

		Checker.AssertFalse(node->LockedRanges != null && comparerFinder != null);
		RearrangeRangeLocksRight(node, newNode, parent);

		if (right != null)
			right->ExitWriteLock();

		if (!node->IsLeaf)
		{
			for (int i = 0; i < newNode->Count; i++)
			{
				Node.GetEntry(newNode, i, entrySize)->child->ExitWriteLock();
			}
		}

		newNode->ExitWriteLock();
	}

	private DatabaseErrorDetail CheckUniqueness(Transaction tran, ref Node* node, ref int index, byte* key, long id, ulong handle,
		KeyComparer comparer, Func<short, KeyComparer> comparerFinder = null)
	{
		Node* currNode = node;
		int currIndex = index;
		node = null;

		try
		{
			while (true)
			{
				ulong* entry = Node.GetLeafEntry(currNode, currIndex);
				byte* objectKey = GetKeyAndComparer(comparerFinder, *entry, true, out var itemComparer, out long objId, out Class @class);
				ClassObject* obj = Class.GetObjectByHandle(*entry);

				if (comparer.IsBefore(key, long.MaxValue, ulong.MaxValue, null,
					objectKey, objId, *entry, itemComparer, stringStorage, out bool equal))
				{
					if (node == null)
					{
						node = currNode;
						index = currIndex;
					}

					return null;
				}
				else
				{
					if (objId != id)
					{
						bool isVisible = IsVisible(tran, obj, out bool isConflicting);
						if (isConflicting)
							return DatabaseErrorDetail.Create(DatabaseErrorType.IndexConflict);

						if (isVisible)
							return DatabaseErrorDetail.CreateUniquenessConstraint(indexDesc.Name);
					}

					if (node == null && (objId > id || (objId == id && *entry > handle)))
					{
						node = currNode;
						index = currIndex;
					}
				}

				currIndex++;
				if (currIndex == currNode->Count)
				{
					currIndex = 0;
					Node* right = currNode->Right;
					right->EnterWriteLock();
					if (node != currNode)
						currNode->ExitWriteLock();

					currNode = right;
				}
			}
		}
		finally
		{
			if (node == null)
				node = currNode;

			if (node != currNode)
				currNode->ExitWriteLock();

			node->CheckLocked(true);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool IsVisible(Transaction tran, ClassObject* obj, out bool isConflicting)
	{
		if (tran == null)
		{
			isConflicting = false;
			return true;
		}

		if (obj->version <= tran.ReadVersion)
		{
			isConflicting = false;
			ulong newerVersion = obj->NewerVersion;
			return newerVersion == 0 || (newerVersion > tran.ReadVersion && newerVersion != tran.Id);
		}

		if (obj->version == tran.Id)
		{
			isConflicting = false;
			return true;
		}

		isConflicting = true;
		return false;
	}

	private void Init()
	{
		KeyProperty[] properties = new KeyProperty[indexDesc.Properties.Length];
		int offset = 0;
		for (int i = 0; i < properties.Length; i++)
		{
			properties[i] = new KeyProperty(indexDesc.Properties[i].PropertyType, offset, indexDesc.PropertySortOrder[i]);
			offset += PropertyTypesHelper.GetItemSize(indexDesc.Properties[i].PropertyType);
		}

		localComparer = localComparer = new KeyComparer(new KeyComparerDesc(properties, indexDesc.CultureName, indexDesc.CaseSensitive));

		entrySize = (int)Utils.GetNextDivisible(localComparer.KeySize + Entry.Size, 8);
		entryOctetSize = entrySize / 8;
		rootManager = memoryManager.RegisterFixedConsumer(Node.HeaderSize);
		nodeManager = memoryManager.RegisterFixedConsumer(Node.HeaderSize + entrySize * NodeCapacity);
		leafManager = memoryManager.RegisterFixedConsumer(Node.HeaderSize + Entry.LeafSize * NodeCapacity);
		rangeManager = memoryManager.RegisterFixedConsumer(Range.Size + localComparer.KeySize * 2);

#if TEST_BUILD
#if !HUNT_CORRUPT
		if (N == 118)
		{
			Checker.AssertTrue(leafManager.BufferSize == 1024 || leafManager.BufferSize == 1016);
			Checker.AssertTrue(nodeManager.BufferSize <= MemoryManager.MaxManagedSize);
		}
#endif
#endif
	}

	private bool IsAfterLastKey(Node* node, byte* key, long id, ulong handle, bool takeEqual)
	{
		if (node->IsLeaf)
		{
			ulong* lastEntry = Node.GetLeafEntry(node, node->Count - 1);
			byte* lastKey = GetKeyAndComparer(*lastEntry, true, out KeyComparer itemComparer, out long lastId, out _);
			return localComparer.IsAfter(key, id, handle, null, lastKey, lastId, *lastEntry, itemComparer, stringStorage, out bool equal) ||
				(takeEqual && equal);
		}
		else
		{
			Entry* lastEntry = Node.GetEntry(node, node->Count - 1, entrySize);
			return localComparer.IsAfter(key, id, handle, null,
				Entry.Key(lastEntry), lastEntry->id, lastEntry->handle, localComparer, stringStorage, out bool equal) ||
				(takeEqual && equal);
		}
	}

	private int LinearSearchHandle(Node* node, ulong handle, bool isUnsafe = false)
	{
#if TEST_BUILD
		if (!isUnsafe)
			Node.CheckLocked(node, false);
#endif

		ulong* handles = (ulong*)Node.Entries(node);
		int c = node->Count;
		for (int i = 0; i < c; i++)
		{
			if (handles[i] == handle)
				return i;
		}

		return -1;
	}

	private int BinarySearchEqualOrLarger(Node* node, byte* key, long id, ulong handle, KeyComparer comparer,
		string[] requestStrings, Func<short, KeyComparer> comparerFinder = null, bool isUnsafe = false)
	{
#if TEST_BUILD
		if (!isUnsafe)
			Node.CheckLocked(node, false);
#endif

		byte* entries = Node.Entries(node);

		int l = 0;
		Entry* le = (Entry*)entries;

		int h = node->Count - 1;

		if (node->IsLeaf)
		{
			while (l <= h)
			{
				int m = (l + h) >> 1;
				ulong* me = (ulong*)(entries + m * Entry.LeafSize);

				byte* objectKey = GetKeyAndComparer(comparerFinder, *me, true, out KeyComparer itemComparer, out long objId, out _);
				bool isAfter = comparer.IsAfter(key, id, handle, requestStrings, objectKey, objId, *me,
					itemComparer, stringStorage, out bool isEqual);

				if (isEqual)
					return m;

				if (isAfter)
					l = m + 1;
				else
					h = m - 1;
			}

			return l;
		}
		else
		{
			while (l <= h)
			{
				int m = (l + h) >> 1;
				Entry* me = (Entry*)(entries + m * entrySize);

				bool isAfter = comparer.IsAfter(key, id, handle, requestStrings, Entry.Key(me),
					me->id, me->handle, localComparer, stringStorage, out bool isEqual);

				if (isEqual)
					return m;

				if (isAfter)
					l = m + 1;
				else
					h = m - 1;
			}

			return l;
		}
	}

	public void RefreshKey(Node* parent, int index, Node* child, bool releaseStrings, Func<short, KeyComparer> comparerFinder = null)
	{
		parent->CheckLocked(true);
		child->CheckLocked(false);

		Entry* parentEntry = Node.GetEntry(parent, index, entrySize);
		byte* parentKey = Entry.Key(parentEntry);
		if (releaseStrings)
			localComparer.ReleaseStrings(parentKey, stringStorage);

		Checker.AssertTrue(parentEntry->child == child);

		if (child->IsLeaf)
		{
			ulong* lastChildEntry = Node.GetLeafEntry(child, child->Count - 1);

			if (*lastChildEntry == KeyComparer.MaxKey)
			{
				Utils.ZeroMemory(parentKey, localComparer.KeySize);
				parentEntry->id = 0;
			}
			else
			{
				byte* objectKey = GetKeyAndComparer(comparerFinder, *lastChildEntry, true, out KeyComparer itemComparer, out long id, out _);
				itemComparer.CopyWithStringStorage(objectKey, null, parentKey, stringStorage);
				parentEntry->id = id;
			}

			parentEntry->handle = *lastChildEntry;
		}
		else
		{
			Entry* lastChildEntry = Node.GetEntry(child, child->Count - 1, entrySize);
			if (lastChildEntry == null)
			{
				Utils.ZeroMemory(parentKey, localComparer.KeySize);
			}
			else
			{
				byte* childKey = Entry.Key(lastChildEntry);
				localComparer.CopyWithStringStorage(childKey, null, parentKey, stringStorage);
			}

			parentEntry->id = lastChildEntry->id;
			parentEntry->handle = lastChildEntry->handle;
		}
	}

	public void AddRange(Node* dst, Node* src)
	{
		AddRange(dst, src, 0, src->Count);
	}

	public void AddRange(Node* dst, Node* src, int offset, int count)
	{
		dst->CheckLocked(true);
		src->CheckLocked(false);

		if (dst->IsLeaf)
		{
			Checker.AssertTrue(src->IsLeaf);
			ulong* dstEntries = Node.GetLeafEntry(dst, dst->Count);
			ulong* srcEntries = Node.GetLeafEntry(src, offset);
			for (int i = 0; i < count; i++)
			{
				dstEntries[i] = srcEntries[i];
			}
		}
		else
		{
			Checker.AssertFalse(src->IsLeaf);
			Entry* dstEntries = Node.GetEntry(dst, dst->Count, entrySize);
			Entry* srcEntries = Node.GetEntry(src, offset, entrySize);
			CopyKeyRange((byte*)srcEntries, (byte*)dstEntries, count * entryOctetSize);
		}

		dst->Count += count;
	}

	public static void InsertNewKey(Node* node, int index, ulong objectHandle)
	{
		node->CheckLocked(true);

		ulong* entries = (ulong*)Node.Entries(node);
		int c = node->Count;
		for (int i = c - 1; i >= index; i--)
		{
			entries[i + 1] = entries[i];
		}

		entries[index] = objectHandle;
		node->Count++;
	}

	public void InsertNewChild(Node* parent, int index, Node* child, Func<short, KeyComparer> comparerFinder)
	{
		parent->CheckLocked(true);
		child->CheckLocked(false);

		Entry* parentEntry = Node.GetEntry(parent, index, entrySize);
		CopyKeyRangeBackwards((byte*)parentEntry, (byte*)parentEntry + entrySize, (parent->Count - index) * entryOctetSize);
		parent->Count++;
		parentEntry->child = child;
		RefreshKey(parent, index, child, false, comparerFinder);
	}

	public void InsertRangeBeginning(Node* node, Node* other, int offset, int count)
	{
		node->CheckLocked(true);
		other->CheckLocked(false);
		Checker.AssertTrue(node->IsLeaf == other->IsLeaf);

		byte* nodeEntries = Node.Entries(node);
		byte* otherEntries = Node.Entries(other);
		if (node->IsLeaf)
		{
			CopyKeyRangeBackwards(nodeEntries, nodeEntries + count * 8, node->Count);
			CopyKeyRange(otherEntries + offset * 8, nodeEntries, count);
		}
		else
		{
			CopyKeyRangeBackwards(nodeEntries, nodeEntries + count * entrySize, node->Count * entryOctetSize);
			CopyKeyRange(otherEntries + offset * entrySize, nodeEntries, count * entryOctetSize);
		}

		node->Count += count;
	}

	public void RemoveRange(Node* node, int index, int count, bool releaseStrings)
	{
		node->CheckLocked(true);

		int t = node->Count - index - count;
		if (node->IsLeaf)
		{
			CopyKeyRange(Node.Entries(node) + (index + count) * 8, Node.Entries(node) + index * 8, t);
		}
		else
		{
			Entry* entries = Node.GetEntry(node, index, entrySize);
			if (releaseStrings && localComparer.HasStringProperties)
			{
				for (int i = 0; i < count; i++)
				{
					localComparer.ReleaseStrings(Entry.Key(entries), stringStorage);
					entries = (Entry*)((byte*)entries + entrySize);
				}
			}

			CopyKeyRange(Node.Entries(node) + (index + count) * entrySize, Node.Entries(node) + index * entrySize, t * entryOctetSize);
		}

		node->Count -= count;
	}

	public static void CopyKeyRange(byte* src, byte* dst, int count)
	{
		long* lsrc = (long*)src;
		long* ldst = (long*)dst;
		for (int i = 0; i < count; i++)
		{
			ldst[i] = lsrc[i];
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* GetKeyAndComparer(Func<short, KeyComparer> comparerFinder, ulong objectHandle,
		bool mandatoryFind, out KeyComparer comparer, out long id, out Class @class)
	{
		if (objectHandle == KeyComparer.MaxKey)
		{
			comparer = null;
			@class = null;
			id = 0;
			return null;
		}

		ClassObject* @object = (ClassObject*)ObjectStorage.GetBuffer(objectHandle);
		id = @object->id;
		@class = database.GetClassById(IdHelper.GetClassId(@object->id)).MainClass;
		comparer = comparerFinder != null ? comparerFinder(@class.ClassDesc.Id) : @class.GetKeyComparer(indexDesc.Id, mandatoryFind);
		return ClassObject.ToDataPointer(@object);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* GetKeyAndComparer(ulong objectHandle, bool mandatoryFind, out KeyComparer comparer, out long id, out Class @class)
	{
		if (objectHandle == KeyComparer.MaxKey)
		{
			comparer = null;
			@class = null;
			id = 0;
			return null;
		}

		ClassObject* @object = (ClassObject*)ObjectStorage.GetBuffer(objectHandle);
		id = @object->id;
		@class = database.GetClassById(IdHelper.GetClassId(@object->id)).MainClass;
		comparer = @class.GetKeyComparer(indexDesc.Id, mandatoryFind);
		return ClassObject.ToDataPointer(@object);
	}

	public static void CopyKeyRangeBackwards(byte* src, byte* dst, int count)
	{
		long* lsrc = (long*)src;
		long* ldst = (long*)dst;
		for (int i = count - 1; i >= 0; i--)
		{
			ldst[i] = lsrc[i];
		}
	}

	private enum NodeFlags : ushort
	{
		Used = 0x8000,
		Leaf = 0x4000,
		CountMask = 0x3fff,
	}

	private sealed class SortedIndexScanRange : IndexScanRange
	{
		public Node* StartNode { get; private set; }
		public Node* EndNode { get; private set; }

		public SortedIndexScanRange(Node* startNode, Node* endNode)
		{
			this.StartNode = startNode;
			this.EndNode = endNode;
		}
	}

#if TEST_BUILD
	public override unsafe bool HasObject(ClassObject* tobj, byte* key, ulong objectHandle, KeyComparer comparer)
	{
		Node* curr = root;
		while (true)
		{
			curr->EnterReadLock(false);
			Node* temp = curr;
			try
			{
				int index = BinarySearchEqualOrLarger(curr, key, tobj->id, objectHandle, comparer, null);
				if (curr->IsLeaf)
				{
					ulong* leafEntry = Node.GetLeafEntry(curr, index);
					return objectHandle == *leafEntry;
				}

				Entry* entry = Node.GetEntry(curr, index, entrySize);
				curr = entry->child;
			}
			finally
			{
				temp->ExitReadLock(0, false);
			}
		}
	}

	public override void Validate(ulong readVersion)
	{
		ValidateSubTree(null, null, root, readVersion, false);
	}

	public void ValidateMidTransaction()
	{
		ValidateSubTree(null, null, root, 0, true);
	}

	private void ValidateSubTree(Node* parent, Entry* parentEntry, Node* node, ulong readVersion, bool allowLocks)
	{
		if (node->Count == 0 || node->Count > NodeCapacity)
			throw new InvalidOperationException();

		if (node->ParentUnsafe != parent)
			throw new InvalidOperationException();

		if (node->LeftUnsafe != null && node->LeftUnsafe->RightUnsafe != node)
			throw new InvalidOperationException();

		if (node->RightUnsafe != null && node->RightUnsafe->LeftUnsafe != node)
			throw new InvalidOperationException();

		if (node->LockedRangesUnsafe != null && !allowLocks)
			throw new InvalidOperationException();

		if (allowLocks)
			ValidateLockRanges(node);

		if (node->IsLeaf)
		{
			if (parentEntry != null)
			{
				ulong* entry = Node.GetLeafEntry(node, node->Count - 1);
				if (*entry == KeyComparer.MaxKey)
				{
					if (parentEntry->handle != KeyComparer.MaxKey)
						throw new InvalidOperationException();
				}
				else
				{
					byte* key = GetKeyAndComparer(*entry, true, out KeyComparer itemComparer, out long id, out _);
					itemComparer.IsBefore(key, id, *entry, null, Entry.Key(parentEntry),
						parentEntry->id, parentEntry->handle, localComparer, stringStorage, out bool equal);
					if (!equal)
						throw new InvalidOperationException();
				}
			}

			for (int i = 0; i < node->Count; i++)
			{
				ulong* entry = Node.GetLeafEntry(node, i);
				ulong* pentry = null;
				if (i > 0)
					pentry = Node.GetLeafEntry(node, i - 1);
				else if (node->LeftUnsafe != null)
					pentry = Node.GetLeafEntry(node->LeftUnsafe, node->LeftUnsafe->Count - 1);

				if (pentry != null)
				{
					byte* key = GetKeyAndComparer(*entry, true, out KeyComparer itemComparer, out long id, out _);
					byte* pkey = GetKeyAndComparer(*pentry, true, out KeyComparer pitemComparer, out long pid, out _);
					if (key == null)    // Max key
					{
						if (pkey == null)
							throw new InvalidOperationException();
					}
					else
					{
						if (itemComparer.IsBefore(key, id, *entry, null, pkey, pid, *pentry,
							pitemComparer, stringStorage, out bool equal) || equal)
						{
							throw new InvalidOperationException();
						}
					}
				}
			}
		}
		else
		{
			if (parentEntry != null)
			{
				Entry* entry = Node.GetEntry(node, node->Count - 1, entrySize);
				if (entry->handle == KeyComparer.MaxKey || parentEntry->handle == KeyComparer.MaxKey)
				{
					if (entry->handle != KeyComparer.MaxKey || parentEntry->handle != KeyComparer.MaxKey)
						throw new InvalidOperationException();
				}
				else
				{
					localComparer.IsBefore(Entry.Key(entry), entry->id, entry->handle, null, Entry.Key(parentEntry),
						parentEntry->id, parentEntry->handle, localComparer, stringStorage, out bool equal);
					if (!equal)
						throw new InvalidOperationException();
				}
			}

			for (int i = 0; i < node->Count; i++)
			{
				Entry* entry = Node.GetEntry(node, i, entrySize);
				Entry* pentry = null;
				if (i > 0)
					pentry = Node.GetEntry(node, i - 1, entrySize);
				else if (node->LeftUnsafe != null)
					pentry = Node.GetEntry(node->LeftUnsafe, node->LeftUnsafe->Count - 1, entrySize);

				if (pentry != null)
				{
					if (localComparer.IsBefore(Entry.Key(entry), entry->id, entry->handle, null, Entry.Key(pentry),
						pentry->id, pentry->handle, localComparer, stringStorage, out bool equal) || equal)
					{
						throw new InvalidOperationException();
					}
				}

				ValidateSubTree(node, entry, entry->child, readVersion, allowLocks);
			}
		}
	}

	private void ValidateLockRanges(Node* node)
	{
		KeyComparer startComparer = null;
		byte* startKey = null;
		long startId = 0;
		ulong startHandle = 0;
		KeyComparer endComparer;
		byte* endKey;
		long endId;
		ulong endHandle;

		if (node->IsLeaf)
		{
			if (node->LeftUnsafe != null)
			{
				startHandle = *Node.GetLeafEntry(node->LeftUnsafe, node->LeftUnsafe->Count - 1);
				startKey = GetKeyAndComparer(startHandle, true, out startComparer, out startId, out _);
			}

			endHandle = *Node.GetLeafEntry(node, node->Count - 1);
			endKey = GetKeyAndComparer(endHandle, true, out endComparer, out endId, out _);
		}
		else
		{
			startComparer = localComparer;
			endComparer = localComparer;
			Entry* entry;
			if (node->LeftUnsafe != null)
			{
				entry = Node.GetEntry(node->LeftUnsafe, node->LeftUnsafe->Count - 1, entrySize);
				startHandle = entry->handle;
				startId = entry->id;
				startKey = Entry.Key(entry);
			}

			entry = Node.GetEntry(node, node->Count - 1, entrySize);
			endHandle = entry->handle;
			endId = entry->id;
			endKey = Entry.Key(entry);
		}

		Range* curr = node->LockedRangesUnsafe;
		while (curr != null)
		{
			if (startKey != null)
			{
				if (localComparer.IsBefore(Range.GetStart(curr), curr->startId, curr->startHandle, null,
					startKey, startId, startHandle, startComparer, stringStorage, out bool equal) || (equal && !curr->isStartOpen))
				{
					throw new InvalidOperationException();
				}
			}

			if (localComparer.IsAfter(Range.GetEnd(curr, localComparer.KeySize), curr->endId, curr->endHandle, null,
				endKey, endId, endHandle, endComparer, stringStorage, out _))
			{
				throw new InvalidOperationException();
			}

			curr = curr->next;
		}
	}

	public void CollectBlobRefCounts(Dictionary<ulong, int> strings)
	{
		Node* curr = root;
		while (curr != null)
		{
			Node* currLevel = curr;
			while (currLevel != null)
			{
				if (currLevel->LockedRangesUnsafe != null)
					throw new InvalidOperationException();

				if (!curr->IsLeaf)
				{
					for (int i = 0; i < currLevel->Count; i++)
					{
						localComparer.CollectKeyStrings(Entry.Key(Node.GetEntry(currLevel, i, entrySize)), strings);
					}
				}

				currLevel = currLevel->RightUnsafe;
			}

			if (curr->IsLeaf)
				curr = null;
			else
				curr = Node.GetEntry(curr, 0, entrySize)->child;
		}
	}

	public string ShowKey(Node* node, int index)
	{
		if (node->IsLeaf)
		{
			ulong* entry = Node.GetLeafEntry(node, index);
			if (*entry == KeyComparer.MaxKey)
				return "MaxKey";

			byte* key = GetKeyAndComparer(*entry, true, out var comparer, out long id, out _);
			return comparer.ShowKey(key, null, stringStorage);
		}
		else
		{
			Entry* entry = Node.GetEntry(node, index, entrySize);
			if (entry->handle == KeyComparer.MaxKey)
				return "MaxKey";

			return localComparer.ShowKey(Entry.Key(entry), null, stringStorage);
		}
	}
#endif
}
