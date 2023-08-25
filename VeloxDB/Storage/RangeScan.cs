using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal abstract class RangeScan
{
	public abstract bool Next(IList<ObjectReader> objects, int fetchCountLimit);
}

internal unsafe abstract class RangeScanBase : RangeScan
{
	Transaction tran;

	protected byte* startKey;
	long startId;
	ulong startHandle;
	bool isStartOpen;

	protected byte* endKey;
	long endId;
	ulong endHandle;
	bool isEndOpen;

	bool isForward;

	bool finished;
	bool initialClearingDone;

	int leafLockCount;
	SortedIndex.Range* leafLocksHead;
	SortedIndex.Range* leafLocksTail;
	KeyReadLockItem transactionReadLock;

	protected SortedIndex index;

	protected string[] startStrings;
	protected string[] endStrings;

	protected KeyComparer comparer;

	public RangeScanBase(Transaction tran, KeyComparer comparer, SortedIndex index, bool isForward)
	{
		this.tran = tran;
		this.comparer = comparer;
		this.index = index;
		this.isForward = isForward;

		if (comparer.StringPropertyCount > 0)
		{
			startStrings = new string[comparer.StringPropertyCount];
			endStrings = new string[comparer.StringPropertyCount];
		}
	}

	public byte* StartKey => startKey;
	public long StartId { get => startId; set => startId = value; }
	public ulong StartHandle { get => startHandle; set => startHandle = value; }
	public bool IsStartOpen { get => isStartOpen; set => isStartOpen = value; }

	public byte* EndKey => endKey;
	public long EndId { get => endId; set => endId = value; }
	public ulong EndHandle { get => endHandle; set => endHandle = value; }
	public bool IsEndOpen { get => isEndOpen; set => isEndOpen = value; }

	public bool Finished { get => finished; set => finished = value; }
	public bool InitialClearingDone { get => initialClearingDone; set => initialClearingDone = value; }

	public Transaction Tran => tran;
	public bool IsForward => isForward;

	public KeyComparer Comparer => comparer;

	public string[] StartStrings => startStrings;
	public string[] EndStrings => endStrings;

	public int LeafLockCount => leafLockCount;
	public SortedIndex.Range* LastAddedLeafLock => leafLocksTail;

	public KeyReadLockItem TransactionReadLock { get => transactionReadLock; set => transactionReadLock = value; }

	protected abstract DatabaseErrorDetail NextInternal(IList<ObjectReader> objects, int fetchCountLimit);

	public void AddLeafLock(SortedIndex.Range* range, StringStorage stringStorage)
	{
		if (leafLocksHead == null)
		{
			leafLocksHead = range;
			leafLocksTail = range;
		}
		else
		{
			leafLocksTail->nextInScan = range;
			leafLocksTail = range;
		}

		leafLockCount++;

#if TEST_BUILD
		AddLastLockedKey(range, stringStorage);
#endif
	}

	public SortedIndex.Range* PeekLeafLock()
	{
		return leafLocksHead;
	}

	public void RemoveLeafLock()
	{
		if (leafLocksHead->nextInScan == null)
			leafLocksTail = null;

		leafLocksHead = leafLocksHead->nextInScan;
		leafLockCount--;
	}

	public override bool Next(IList<ObjectReader> objects, int fetchCountLimit)
	{
		Tran.ValidateUsage();
		if (Tran.CancelRequested)
			Tran.Engine.CheckErrorAndRollback(DatabaseErrorDetail.Create(DatabaseErrorType.TransactionCanceled), Tran);

		if (fetchCountLimit < index.NodeCapacity * 2 || objects.Count > 0)
			Tran.Engine.CheckErrorAndRollback(DatabaseErrorDetail.Create(DatabaseErrorType.InvalidArgument), Tran);

		if (Finished)
			return false;

		DatabaseErrorDetail error = NextInternal(objects, fetchCountLimit);
		Tran.Engine.CheckErrorAndRollback(error, Tran);
		return objects.Count > 0;
	}

	protected abstract void CopyStartKey(byte* key, KeyComparer comparer, StringStorage stringStorage);
	protected abstract void CopyEndKey(byte* key, KeyComparer comparer, StringStorage stringStorage);

	public void WriteStartKey(byte* key, long id, ulong handle, bool isOpen, KeyComparer comparer, StringStorage stringStorage)
	{
		StartId = id;
		StartHandle = handle;
		IsStartOpen = isOpen;
		CopyStartKey(key, comparer, stringStorage);
	}

	public void WriteEndKey(byte* key, long id, ulong handle, bool isOpen, KeyComparer comparer, StringStorage stringStorage)
	{
		EndId = id;
		EndHandle = handle;
		IsEndOpen = isOpen;
		CopyEndKey(key, comparer, stringStorage);
	}

#if TEST_BUILD
	public object[] LastLockedKey { get; set; }

	private void AddLastLockedKey(SortedIndex.Range* range, StringStorage stringStorage)
	{
		LastLockedKey = new object[index.IndexDesc.Properties.Length + 1];
		byte* src = IsForward ? SortedIndex.Range.GetEnd(range, index.LocalComparer.KeySize) : SortedIndex.Range.GetStart(range);
		LastLockedKey[LastLockedKey.Length - 1] = IsForward ? range->isEndOpen : range->isStartOpen;

		for (int i = 0; i < index.IndexDesc.Properties.Length; i++)
		{
			PropertyDescriptor propDesc = index.IndexDesc.Properties[i];
			switch (propDesc.PropertyType)
			{
				case PropertyType.Byte:
					LastLockedKey[i] = *src;
					break;

				case PropertyType.Short:
					LastLockedKey[i] = *(short*)src;
					break;

				case PropertyType.Int:
					LastLockedKey[i] = *(int*)src;
					break;

				case PropertyType.Long:
					LastLockedKey[i] = *(long*)src;
					break;

				case PropertyType.Float:
					LastLockedKey[i] = *(float*)src;
					break;

				case PropertyType.Double:
					LastLockedKey[i] = *(double*)src;
					break;

				case PropertyType.Bool:
					LastLockedKey[i] = *(bool*)src;
					break;

				case PropertyType.DateTime:
					LastLockedKey[i] = DateTime.FromBinary(*(long*)src);
					break;

				case PropertyType.String:
					LastLockedKey[i] = stringStorage.GetString(*(ulong*)src);
					break;
			}

			src += (int)PropertyTypesHelper.GetItemSize(propDesc.PropertyType);
		}

		LastLockedKey[LastLockedKey.Length - 1] = IsForward ? range->endId : range->startId;
	}
#endif
}

#pragma warning disable CS7036

internal unsafe sealed class RangeScan<TKey1> : RangeScanBase
{
	KeyStruct startKeyStruct;
	KeyStruct endKeyStruct;

	public RangeScan(Transaction tran, KeyComparer comparer, SortedIndex index, bool isForward) :
		base(tran, comparer, index, isForward)
	{
		startKeyStruct.key[0] = 0;
		endKeyStruct.key[0] = 0;
	}

	public void SetRange(TKey1 startKey1, long startId, ulong startHandle, bool isStartOpen,
		TKey1 endKey1, long endId, ulong endHandle, bool isEndOpen, KeyWriter<TKey1> keyWriter)
	{
		base.StartId = startId;
		base.StartHandle = startHandle;
		base.IsStartOpen = isStartOpen;
		base.EndId = endId;
		base.EndHandle = endHandle;
		base.IsEndOpen = isEndOpen;

		fixed (long* p = startKeyStruct.key)
		{
			keyWriter(startKey1, (byte*)p, startStrings);
		}

		fixed (long* p = endKeyStruct.key)
		{
			keyWriter(endKey1, (byte*)p, endStrings);
		}
	}

	protected override void CopyStartKey(byte* key, KeyComparer comparer, StringStorage stringStorage)
	{
		fixed (long* p = startKeyStruct.key)
		{
			comparer.CopyWithRequestStrings(key, stringStorage, (byte*)p, startStrings);
		}
	}

	protected override void CopyEndKey(byte* key, KeyComparer comparer, StringStorage stringStorage)
	{
		fixed (long* p = endKeyStruct.key)
		{
			comparer.CopyWithRequestStrings(key, stringStorage, (byte*)p, endStrings);
		}
	}

	protected override DatabaseErrorDetail NextInternal(IList<ObjectReader> objects, int fetchCountLimit)
	{
		fixed (long* startp = startKeyStruct.key)
		{
			fixed (long* endp = endKeyStruct.key)
			{
				startKey = (byte*)startp;
				endKey = (byte*)endp;
				DatabaseErrorDetail error = base.index.ScanNext(this, objects, fetchCountLimit);
				startKey = null;
				endKey = null;
				return error;
			}
		}
	}

	private unsafe struct KeyStruct
	{
		public fixed long key[1];
	}
}

internal unsafe sealed class RangeScan<TKey1, TKey2> : RangeScanBase
{
	KeyStruct startKeyStruct;
	KeyStruct endKeyStruct;

	public RangeScan(Transaction tran, KeyComparer comparer, SortedIndex index, bool isForward) :
		base(tran, comparer, index, isForward)
	{
		startKeyStruct.key[0] = 0;
		endKeyStruct.key[0] = 0;
	}

	public void SetRange(TKey1 startKey1, TKey2 startKey2, long startId, ulong startHandle, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, long endId, ulong endHandle, bool isEndOpen, KeyWriter<TKey1, TKey2> keyWriter)
	{
		base.StartId = startId;
		base.StartHandle = startHandle;
		base.IsStartOpen = isStartOpen;
		base.EndId = endId;
		base.EndHandle = endHandle;
		base.IsEndOpen = isEndOpen;

		fixed (long* p = startKeyStruct.key)
		{
			keyWriter(startKey1, startKey2, (byte*)p, startStrings);
		}

		fixed (long* p = endKeyStruct.key)
		{
			keyWriter(endKey1, endKey2, (byte*)p, endStrings);
		}
	}

	protected override void CopyStartKey(byte* key, KeyComparer comparer, StringStorage stringStorage)
	{
		fixed (long* p = startKeyStruct.key)
		{
			comparer.CopyWithRequestStrings(key, stringStorage, (byte*)p, startStrings);
		}
	}

	protected override void CopyEndKey(byte* key, KeyComparer comparer, StringStorage stringStorage)
	{
		fixed (long* p = endKeyStruct.key)
		{
			comparer.CopyWithRequestStrings(key, stringStorage, (byte*)p, endStrings);
		}
	}

	protected override DatabaseErrorDetail NextInternal(IList<ObjectReader> objects, int fetchCountLimit)
	{
		fixed (long* startp = startKeyStruct.key)
		{
			fixed (long* endp = endKeyStruct.key)
			{
				startKey = (byte*)startp;
				endKey = (byte*)endp;
				DatabaseErrorDetail error = base.index.ScanNext(this, objects, fetchCountLimit);
				startKey = null;
				endKey = null;
				return error;
			}
		}
	}

	private unsafe struct KeyStruct
	{
		public fixed long key[2];
	}
}

internal unsafe sealed class RangeScan<TKey1, TKey2, TKey3> : RangeScanBase
{
	KeyStruct startKeyStruct;
	KeyStruct endKeyStruct;

	public RangeScan(Transaction tran, KeyComparer comparer, SortedIndex index, bool isForward) :
		base(tran, comparer, index, isForward)
	{
		startKeyStruct.key[0] = 0;
		endKeyStruct.key[0] = 0;
	}

	public void SetRange(TKey1 startKey1, TKey2 startKey2, TKey3 startKey3, long startId, ulong startHandle, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, long endId, ulong endHandle, bool isEndOpen,
		KeyWriter<TKey1, TKey2, TKey3> keyWriter)
	{
		base.StartId = startId;
		base.StartHandle = startHandle;
		base.IsStartOpen = isStartOpen;
		base.EndId = endId;
		base.EndHandle = endHandle;
		base.IsEndOpen = isEndOpen;

		fixed (long* p = startKeyStruct.key)
		{
			keyWriter(startKey1, startKey2, startKey3, (byte*)p, startStrings);
		}

		fixed (long* p = endKeyStruct.key)
		{
			keyWriter(endKey1, endKey2, endKey3, (byte*)p, endStrings);
		}
	}

	protected override void CopyStartKey(byte* key, KeyComparer comparer, StringStorage stringStorage)
	{
		fixed (long* p = startKeyStruct.key)
		{
			comparer.CopyWithRequestStrings(key, stringStorage, (byte*)p, startStrings);
		}
	}

	protected override void CopyEndKey(byte* key, KeyComparer comparer, StringStorage stringStorage)
	{
		fixed (long* p = endKeyStruct.key)
		{
			comparer.CopyWithRequestStrings(key, stringStorage, (byte*)p, endStrings);
		}
	}

	protected override DatabaseErrorDetail NextInternal(IList<ObjectReader> objects, int fetchCountLimit)
	{
		fixed (long* startp = startKeyStruct.key)
		{
			fixed (long* endp = endKeyStruct.key)
			{
				startKey = (byte*)startp;
				endKey = (byte*)endp;
				DatabaseErrorDetail error = base.index.ScanNext(this, objects, fetchCountLimit);
				startKey = null;
				endKey = null;
				return error;
			}
		}
	}

	private unsafe struct KeyStruct
	{
		public fixed long key[3];
	}
}

internal unsafe sealed class RangeScan<TKey1, TKey2, TKey3, TKey4> : RangeScanBase
{
	KeyStruct startKeyStruct;
	KeyStruct endKeyStruct;

	public RangeScan(Transaction tran, KeyComparer comparer, SortedIndex index, bool isForward) :
		base(tran, comparer, index, isForward)
	{
		startKeyStruct.key[0] = 0;
		endKeyStruct.key[0] = 0;
	}

	public void SetRange(TKey1 startKey1, TKey2 startKey2, TKey3 startKey3, TKey4 startKey4, long startId, ulong startHandle, bool isStartOpen,
		TKey1 endKey1, TKey2 endKey2, TKey3 endKey3, TKey4 endKey4, long endId, ulong endHandle, bool isEndOpen,
		KeyWriter<TKey1, TKey2, TKey3, TKey4> keyWriter)
	{
		base.StartId = startId;
		base.StartHandle = startHandle;
		base.IsStartOpen = isStartOpen;
		base.EndId = endId;
		base.EndHandle = endHandle;
		base.IsEndOpen = isEndOpen;

		fixed (long* p = startKeyStruct.key)
		{
			keyWriter(startKey1, startKey2, startKey3, startKey4, (byte*)p, startStrings);
		}

		fixed (long* p = endKeyStruct.key)
		{
			keyWriter(endKey1, endKey2, endKey3, endKey4, (byte*)p, endStrings);
		}
	}

	protected override void CopyStartKey(byte* key, KeyComparer comparer, StringStorage stringStorage)
	{
		fixed (long* p = startKeyStruct.key)
		{
			comparer.CopyWithRequestStrings(key, stringStorage, (byte*)p, startStrings);
		}
	}

	protected override void CopyEndKey(byte* key, KeyComparer comparer, StringStorage stringStorage)
	{
		fixed (long* p = endKeyStruct.key)
		{
			comparer.CopyWithRequestStrings(key, stringStorage, (byte*)p, endStrings);
		}
	}

	protected override DatabaseErrorDetail NextInternal(IList<ObjectReader> objects, int fetchCountLimit)
	{
		fixed (long* startp = startKeyStruct.key)
		{
			fixed (long* endp = endKeyStruct.key)
			{
				startKey = (byte*)startp;
				endKey = (byte*)endp;
				DatabaseErrorDetail error = base.index.ScanNext(this, objects, fetchCountLimit);
				startKey = null;
				endKey = null;
				return error;
			}
		}
	}

	private unsafe struct KeyStruct
	{
		public fixed long key[4];
	}
}
