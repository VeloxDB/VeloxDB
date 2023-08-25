using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal unsafe abstract class Index
{
	public abstract IndexDescriptor IndexDesc { get; }
	public abstract bool PendingRefill { get; }
	public abstract void ModelUpdated(IndexDescriptor indexDesc);
	public abstract DatabaseErrorDetail Insert(Transaction tran, long id, ulong objectHandle, byte* key,
		KeyComparer comparer, Func<short, KeyComparer> comparerFinder = null);
	public abstract void ReplaceObjectHandle(ulong objectHandle, ulong newObjectHandle, byte* key, long id, KeyComparer comparer);
	public abstract void Delete(ulong objectHandle, byte* key, long id, KeyComparer comparer);
	public abstract IndexScanRange[] SplitScanRange();
	public abstract DatabaseErrorDetail CheckUniqueness(IndexScanRange scanRange);
	public abstract void PrepareForPendingRefill(JobWorkers<CommonWorkerParam> workers);
	public abstract void PendingRefillStarted(long capacity);
	public abstract void PendingRefillFinished();
	public abstract void Dispose(JobWorkers<CommonWorkerParam> workers);

#if TEST_BUILD
	public abstract void Validate(ulong readVersion);
	public abstract bool HasObject(ClassObject* tobj, byte* key, ulong objectHandle, KeyComparer comparer);
#endif
}

internal abstract class IndexScanRange
{
}
