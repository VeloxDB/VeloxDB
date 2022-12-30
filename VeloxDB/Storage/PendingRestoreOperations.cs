using System;
using System.Collections.Generic;
using System.Diagnostics;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal delegate void PendingRestoreDelegate(IntPtr ph, IntPtr prevObjPtr, bool hasMore);

internal sealed unsafe class PendingRestoreOperations
{
	readonly object sync = new object();

	long traceId;
	MemoryManager memoryManager;
	Dictionary<long, ulong> map;

	public PendingRestoreOperations(long traceId, MemoryManager memoryManager)
	{
		this.traceId = traceId;
		this.memoryManager = memoryManager;
		map = new Dictionary<long, ulong>(8);
	}

	public int Count => map.Count;

	public void Add(long id, ulong handle)
	{
		TTTrace.Write(traceId, id, handle);

		lock (sync)
		{
			PendingRestoreObjectHeader* ph = (PendingRestoreObjectHeader*)memoryManager.GetBuffer(handle);
			ph->nextPendingHandle = 0;
			ph->nextPendingInSameTranHandle = 0;

			TTTrace.Write(traceId, id, handle, ph->isDelete, ph->prevVersion, ph->version, ph->IsFirstInTransaction, ph->isLastInTransaction);

			if (!map.TryGetValue(id, out ulong existing))
			{
				Checker.AssertTrue(ph->IsFirstInTransaction);
				TTTrace.Write(traceId, id);
				map.Add(id, handle);
				return;
			}

			PendingRestoreObjectHeader* eph = (PendingRestoreObjectHeader*)memoryManager.GetBuffer(existing);
			TTTrace.Write(traceId, id, existing, ph->version, ph->prevVersion, eph->isDelete,
				eph->prevVersion, eph->version, eph->IsFirstInTransaction, eph->IsFirstInTransaction);

			// If the operation is not the first operation on the object than there is already a pending operation
			// for the same object with the same version.
			if (!ph->IsFirstInTransaction)
			{
				while (eph->version != ph->version)
				{
					TTTrace.Write(traceId, id, eph->isDelete, eph->prevVersion, eph->version, eph->IsFirstInTransaction, eph->IsFirstInTransaction);
					eph = (PendingRestoreObjectHeader*)memoryManager.GetBuffer(eph->nextPendingHandle);
				}

				while (eph->nextPendingInSameTranHandle != 0)
				{
					TTTrace.Write(traceId, id, eph->isDelete, eph->prevVersion, eph->version, eph->IsFirstInTransaction, eph->IsFirstInTransaction);
					eph = (PendingRestoreObjectHeader*)memoryManager.GetBuffer(eph->nextPendingInSameTranHandle);
				}

				eph->nextPendingInSameTranHandle = handle;
				return;
			}

			if (ph->prevVersion < eph->prevVersion)
			{
				Checker.AssertTrue(ph->IsFirstInTransaction);
				ph->nextPendingHandle = existing;
				map[id] = handle;
				return;
			}

			while (eph->nextPendingHandle != 0)
			{
				PendingRestoreObjectHeader* nph = (PendingRestoreObjectHeader*)memoryManager.GetBuffer(eph->nextPendingHandle);
				TTTrace.Write(traceId, id, existing, ph->version, ph->prevVersion, nph->isDelete,
					nph->prevVersion, nph->version, nph->IsFirstInTransaction, nph->isLastInTransaction);

				if (ph->prevVersion < nph->prevVersion)
				{
					ph->nextPendingHandle = eph->nextPendingHandle;
					eph->nextPendingHandle = handle;
					return;
				}

				eph = nph;
			}

			ph->nextPendingHandle = 0;
			eph->nextPendingHandle = handle;
		}
	}

	public bool TryPrune(long id, ulong currVersion, PendingRestoreDelegate action, IntPtr param)
	{
		TTTrace.Write(traceId, id, currVersion);

		lock (sync)
		{
			ulong handle = map[id];
			PendingRestoreObjectHeader* ph = (PendingRestoreObjectHeader*)memoryManager.GetBuffer(handle);
			TTTrace.Write(traceId, id, handle, ph->version, ph->prevVersion, currVersion);
			Checker.AssertTrue(ph->IsFirstInTransaction);

			bool pruned = false;
			while (ph != null && ph->prevVersion == currVersion)
			{
				TTTrace.Write(traceId, id, handle, ph->version, ph->prevVersion, currVersion, ph->nextPendingHandle);

				pruned = true;
				ulong nextHandle = ph->nextPendingHandle;

				while (ph != null)
				{
					ulong nextSameTranHandle = ph->nextPendingInSameTranHandle;
					currVersion = ph->version;
					if (!ph->isLastInTransaction)
						currVersion |= OperationHeader.NotLastInTranFlag;

					action((IntPtr)ph, param, nextHandle != 0);
					memoryManager.Free(handle);

					handle = nextSameTranHandle;
					ph = (PendingRestoreObjectHeader*)memoryManager.GetBuffer(handle);
				}

				handle = nextHandle;
				ph = (PendingRestoreObjectHeader*)memoryManager.GetBuffer(handle);
			}

			if (handle == 0)
			{
				TTTrace.Write(traceId, id);
				map.Remove(id);
			}
			else
			{
				TTTrace.Write(traceId, id, handle, ph->version, ph->prevVersion, currVersion, ph->nextPendingHandle);
				map[id] = handle;
			}

			return pruned;
		}
	}

	[Conditional("DEBUG")]
	public void ValidateEmpty()
	{
		Checker.AssertTrue(map.Count == 0);
	}
}
