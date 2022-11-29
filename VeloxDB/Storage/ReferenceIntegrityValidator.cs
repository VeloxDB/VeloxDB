using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.Storage;

internal unsafe sealed class ReferenceIntegrityValidator
{
	const int deletedCountLimit = 1024;

	StorageEngine engine;

	DataModelDescriptor modelDesc;

	public ReferenceIntegrityValidator(StorageEngine engine, Database database)
	{
		TTTrace.Write(engine.TraceId);

		this.engine = engine;
		this.modelDesc = database.ModelDesc;
	}

	public void ModelUpdated(DataModelDescriptor modelDesc)
	{
		this.modelDesc = modelDesc;
	}

	public DatabaseErrorDetail ValidateReference(Transaction tran, InverseReferenceOperation* rc, int count)
	{
		Checker.AssertFalse(rc->directReference == 0);

		ClassDescriptor classDesc = modelDesc.GetClassByIndex(rc->ClassIndex);
		ReferencePropertyDescriptor mrp = (ReferencePropertyDescriptor)classDesc.GetProperty(rc->PropertyId);

		TTTrace.Write(engine.TraceId, tran.Id, count, classDesc.Id, mrp.Id);

		ClassDescriptor targetClassDesc = IdHelper.GetClass(modelDesc, rc->directReference);
		if (targetClassDesc == null)
		{
			if (!RefereceStillExists(tran, classDesc, rc, count))
				return null;

			return DatabaseErrorDetail.CreateUnknownReference(rc->inverseReference, classDesc.FullName, mrp.Name, rc->directReference);
		}

		TTTrace.Write(engine.TraceId, targetClassDesc.Id);
		if (!mrp.ReferencedTypeValid(targetClassDesc.Id))
		{
			if (!RefereceStillExists(tran, classDesc, rc, count))
				return null;

			return DatabaseErrorDetail.CreateInvalidReferencedClass(rc->inverseReference, classDesc.FullName, targetClassDesc.FullName, mrp.Name, rc->directReference);
		}

		Class @class = tran.Database.GetClass(targetClassDesc.Index).MainClass;
		TTTrace.Write(engine.TraceId, rc->directReference);

		if (!@class.ObjectExists(tran, rc->directReference))
		{
			if (!RefereceStillExists(tran, classDesc, rc, count))
				return null;

			return DatabaseErrorDetail.CreateUnknownReference(rc->inverseReference, classDesc.FullName, mrp.Name, rc->directReference);
		}

		return null;
	}

	public Changeset PropagateDeletes(Transaction tran, bool onlySetToNull, out DatabaseErrorDetail error)
	{
		TransactionContext tc = tran.Context;
		if (tc.Deleted.IsEmpty)
		{
			error = null;
			return null;
		}

		TTTrace.Write(engine.TraceId, tran.Id);

		error = CollectPropagationAffectedUsingInverseReferences(tran, onlySetToNull,
			out HashSet<ushort> classesToScan, out DeletedSet deleted);
		if (error != null)
			return null;

		error = CollectPropagationAffectedUsingClassScans(tran, onlySetToNull, classesToScan, deleted);
		if (error != null)
			return null;

		tc.SortPropagatedInverseReferences();

		GeneratePropagatedOperations(tran, out ChangesetWriter writer, out error);
		try
		{
			if (error != null || writer == null)
				return null;

			return writer.FinishWriting();
		}
		finally
		{
			if (writer != null)
				engine.ChangesetWriterPool.Put(writer);
		}
	}

	private DatabaseErrorDetail CollectPropagationAffectedUsingInverseReferences(Transaction tran, bool onlySetToNull,
	out HashSet<ushort> classesToScan, out DeletedSet deletedSet)
	{
		TTTrace.Write(engine.TraceId, tran.Id);

		TransactionContext tc = tran.Context;
		Database database = tran.Database;

		DeletedObject* deleted = (DeletedObject*)tc.Deleted.Buffer;
		long deletedCount = tc.Deleted.Count;

		classesToScan = null;
		deletedSet = new DeletedSet();

		for (long i = 0; i < deletedCount; i++)
		{
			InverseReferenceMap invRefMap = database.GetInvRefs(deleted->classIndex);
			ReadOnlyArray<ReferencePropertyDescriptor> invRefPropDescs = onlySetToNull ?
				invRefMap?.ClassDesc.SetToNullInverseReferences : invRefMap?.ClassDesc.InverseReferences;

			TTTrace.Write(engine.TraceId, deleted->id, deleted->classIndex, invRefPropDescs == null ? -1 : invRefPropDescs.Length, onlySetToNull);
			if (invRefPropDescs == null || invRefPropDescs.Length == 0)
			{
				deleted++;
				continue;
			}

			ReadInverseReferenceMap(tran, tc, invRefMap, deleted, invRefPropDescs, out long* refs, out int* invRefCounts);

			int offset = 0;
			for (int j = 0; j < invRefPropDescs.Length; j++)
			{
				ReferencePropertyDescriptor invRefPropDesc = invRefPropDescs[j];
				int invRefCount = invRefCounts[j];

				if (invRefCount >= 0)
				{
					for (int k = 0; k < invRefCount; k++)
					{
						TTTrace.Write(engine.TraceId, deleted->id, refs[offset + k], invRefPropDesc.Id, (byte)invRefPropDesc.DeleteTargetAction);
						tc.AddInverseReferenceChange(0, refs[offset + k], deleted->id, invRefPropDesc.Id, false, (byte)invRefPropDesc.DeleteTargetAction);
					}

					offset += invRefCount;
				}
				else
				{
					classesToScan ??= new HashSet<ushort>(invRefPropDesc.OnDeleteScanClasses.Length);
					ReadOnlyArray<ClassDescriptor> dsc = invRefPropDesc.OnDeleteScanClasses;
					for (int k = 0; k < dsc.Length; k++)
					{
						classesToScan.Add((ushort)dsc[k].Index);
					}

					deletedSet.Add(deleted->id);
				}
			}

			deleted++;
		}

		return null;
	}

	private DatabaseErrorDetail GenerateSetToNullOperation(ChangesetWriter writer, Transaction tran,
		ClassDescriptor classDesc, InverseReferenceOperation* ops, long count)
	{
		ClassBase @class = tran.Database.GetClass(classDesc.Index);

		ObjectReader reader = new ObjectReader();

		int index = classDesc.GetPropertyIndex(ops->PropertyId);
		ReferencePropertyDescriptor propDesc = (ReferencePropertyDescriptor)classDesc.Properties[index];
		int byteOffset = classDesc.PropertyByteOffsets[index];

		TTTrace.Write(engine.TraceId, classDesc.Id, propDesc.Id, ops->inverseReference, ops->directReference);

		reader = @class.GetObject(tran, ops->inverseReference, out DatabaseErrorDetail error);
		if (error != null)
			return error;

		if (propDesc.Multiplicity != Multiplicity.Many)
		{
			if (ContainsDirectReference(ops, count, reader.GetReference(byteOffset)))
			{
				writer.StartUpdateBlock(classDesc).Add(propDesc.Id);
				writer.AddLong(ops->inverseReference).AddReference(0);
			}
		}
		else
		{
			TransactionContext tc = tran.Context;
			long* refs = tc.TempInvRefs;
			int bufferSize = TransactionContext.TempInvRefSize;
			reader.GetReferenceArray(engine, byteOffset, ref refs, ref bufferSize, out int refCount);
			tc.TempInvRefs = refs;

			if (FilterOutReferences(refs, ref refCount, ops, count))
			{
				writer.StartUpdateBlock(classDesc).Add(propDesc.Id);
				writer.AddLong(ops->inverseReference).AddReferenceArray(refs, refCount);
			}
		}

		return null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool ContainsDirectReference(InverseReferenceOperation* ops, long count, long reference)
	{
		TTTrace.Write(engine.TraceId, ops->directReference, reference, count);

		long low = 0;
		long high = (long)count - 1;
		while (low <= high)
		{
			long mid = (low + high) >> 1;
			InverseReferenceOperation* m = ops + mid;

			if (m->directReference == reference)
				return true;

			if (m->directReference > reference)
			{
				high = mid - 1;
			}
			else
			{
				low = mid + 1;
			}
		}

		return false;
	}

	private void GeneratePropagatedOperations(Transaction tran, out ChangesetWriter writer, out DatabaseErrorDetail error)
	{
		NativeList l = tran.Context.InverseRefChanges;

		writer = null;
		error = null;

		long count = l.Count;
		if (count == 0)
			return;

		InverseReferenceOperation* startOp = (InverseReferenceOperation*)l.Buffer;
		InverseReferenceOperation* endOp = startOp + 1;
		long rangeCount = 1;

		writer = engine.ChangesetWriterPool.Get();

		for (long i = 0; i < count - 1; i++)
		{
			if (endOp->inverseReference != startOp->inverseReference)
			{
				error = GenerateRangePropagatedOperations(writer, tran, startOp, rangeCount);
				if (error != null)
					return;

				rangeCount = 0;
				startOp = endOp;
			}

			rangeCount++;
			endOp++;
		}

		error = GenerateRangePropagatedOperations(writer, tran, startOp, rangeCount);
	}

	private DatabaseErrorDetail GenerateRangePropagatedOperations(ChangesetWriter cw, Transaction tran,
		InverseReferenceOperation* ops, long count)
	{
		ClassDescriptor classDesc = IdHelper.GetClass(modelDesc, ops->inverseReference);

		if (ops->Type == (byte)DeleteTargetAction.PreventDelete)
		{
			PropertyDescriptor pd = classDesc.GetProperty(ops->PropertyId);
			return DatabaseErrorDetail.CreateReferencedDelete(ops->directReference, ops->inverseReference, classDesc.FullName, pd.Name);
		}

		if (ops->Type == (byte)DeleteTargetAction.CascadeDelete)
		{
			cw.StartDeleteBlock(classDesc);
			cw.AddDelete(ops->inverseReference);
			return null;
		}

		InverseReferenceOperation* endOp = ops + 1;
		long rangeCount = 1;
		int prevPropId = ops->PropertyId;

		for (int i = 0; i < count - 1; i++)
		{
			if (endOp->PropertyId != prevPropId)
			{
				DatabaseErrorDetail error = GenerateSetToNullOperation(cw, tran, classDesc, ops, rangeCount);
				if (error != null)
					return error;

				rangeCount = 0;
				ops = endOp;
				prevPropId = ops->PropertyId;
			}

			rangeCount++;
			endOp++;
		}

		return GenerateSetToNullOperation(cw, tran, classDesc, ops, rangeCount);
	}

	private bool FilterOutReferences(long* refs, ref int refCount, InverseReferenceOperation* ops, long opCount)
	{
		int count = 0;
		bool filtered = false;
		for (int j = 0; j < refCount; j++)
		{
			if (!ContainsDirectReference(ops, opCount, refs[j]))
			{
				refs[count++] = refs[j];
			}
			else
			{
				filtered = true;
			}
		}

		refCount = count;
		return filtered;
	}

	private DatabaseErrorDetail CollectPropagationAffectedUsingClassScans(Transaction tran, bool onlySetToNull,
		HashSet<ushort> classesToScan, DeletedSet deletedSet)
	{
		if (classesToScan == null)
			return null;

		TTTrace.Write(engine.TraceId, tran.Id);

		foreach (ClassDescriptor classDesc in classesToScan.Select(x => modelDesc.GetClassByIndex(x)))
		{
			DatabaseErrorDetail error = CollectPropagationAffectedUsingClassScan(tran, onlySetToNull, classDesc, deletedSet);
			if (error != null)
				return error;
		}

		return null;
	}

	private DatabaseErrorDetail CollectPropagationAffectedUsingClassScan(Transaction tran, bool onlySetToNull,
		ClassDescriptor classDesc, DeletedSet deletedSet)
	{
		TTTrace.Write(engine.TraceId, tran.Id, classDesc.Id);

		TransactionContext tc = tran.Context;

		ObjectReader[] readers = tc.TempRecReaders;
		using ClassScan scan = engine.BeginClassScanInternal(tran, classDesc, false, false, out DatabaseErrorDetail error);
		if (error != null)
			return error;

		ReadOnlyArray<int> refProps = classDesc.UntrackedRefeferencePropertyIndexes;
		int count = readers.Length;
		while (scan.Next(readers, 0, ref count))
		{
			for (int i = 0; i < count; i++)
			{
				long id = readers[i].GetId();

				for (int j = 0; j < refProps.Length; j++)
				{
					ReferencePropertyDescriptor mrp = (ReferencePropertyDescriptor)classDesc.Properties[refProps[j]];
					if (mrp.DeleteTargetAction != DeleteTargetAction.SetToNull && onlySetToNull)
						continue;

					int byteOffset = classDesc.PropertyByteOffsets[refProps[j]];
					CollectAffectedForProperty(tran, id, mrp, byteOffset, readers[i], deletedSet);
				}
			}
		}

		return null;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void CollectAffectedForProperty(Transaction tran, long id, ReferencePropertyDescriptor propDesc,
		int byteOffset, ObjectReader reader, DeletedSet deletedSet)
	{
		TransactionContext tc = tran.Context;
		if (propDesc.Multiplicity == Multiplicity.Many)
		{
			long* refs = tc.TempInvRefs;
			int bufferSize = TransactionContext.TempInvRefSize;
			reader.GetReferenceArray(engine, byteOffset, ref refs, ref bufferSize, out int refCount);
			tc.TempInvRefs = refs;

			for (int i = 0; i < refCount; i++)
			{
				if (ObjectDeletedInTran(tran, refs[i], deletedSet))
				{
					TTTrace.Write(engine.TraceId, id, propDesc.Id, refs[i], propDesc.Id, (byte)propDesc.DeleteTargetAction);
					tc.AddInverseReferenceChange(0, id, refs[i], propDesc.Id, false, (byte)propDesc.DeleteTargetAction);
				}
			}
		}
		else
		{
			long reference = reader.GetReference(byteOffset);
			if (ObjectDeletedInTran(tran, reference, deletedSet))
			{
				TTTrace.Write(engine.TraceId, id, propDesc.Id, reference, propDesc.Id, (byte)propDesc.DeleteTargetAction);
				tc.AddInverseReferenceChange(0, id, reference, propDesc.Id, false, (byte)propDesc.DeleteTargetAction);
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void ReadInverseReferenceMap(Transaction tran, TransactionContext tc, InverseReferenceMap invRefMap, DeletedObject* p,
		ReadOnlyArray<ReferencePropertyDescriptor> invRefs, out long* refs, out int* invRefCounts)
	{
		invRefCounts = tc.TempInvRefCounts;
		refs = tc.TempInvRefs;
		int bufferSize = TransactionContext.TempInvRefSize;
		invRefMap.GetReferences(tran, p->id, invRefs, ref refs, ref bufferSize, invRefCounts);
		tc.TempInvRefs = refs;
	}

	private bool ObjectDeletedInTran(Transaction tran, long id, DeletedSet deletedSet)
	{
		if (id == 0)
			return false;

		if (deletedSet.TryGetIsDeleted(id, out bool isDeleted))
			return isDeleted;

		ClassDescriptor classDesc = IdHelper.GetClass(modelDesc, id);
		Class @class = tran.Database.GetClass(classDesc.Index).MainClass;
		return @class.ObjectDeletedInTransaction(tran, id);
	}

	private bool RefereceStillExists(Transaction tran, ClassDescriptor classDesc, InverseReferenceOperation* rc, int count)
	{
		for (int i = 0; i < count; i++)
		{
			if (RefereceStillExists(tran, classDesc, rc + i))
				return true;
		}

		return false;
	}

	private unsafe bool RefereceStillExists(Transaction tran, ClassDescriptor classDesc, InverseReferenceOperation* op)
	{
		TTTrace.Write(engine.TraceId, tran.Id, classDesc.Id, op->PropertyId, op->inverseReference, op->directReference);

		ClassBase @class = tran.Database.GetClass(classDesc.Index);
		ObjectReader reader = @class.GetObjectNoReadLock(tran, op->inverseReference);

		// If object was deleted after introducing new reference
		if (reader.IsEmpty())
			return false;

		int index = classDesc.GetPropertyIndex(op->PropertyId);
		ReferencePropertyDescriptor mrp = (ReferencePropertyDescriptor)classDesc.Properties[index];
		int byteOffset = classDesc.PropertyByteOffsets[index];

		// Reference might not exists if it was introduced and later overridden in the same changeset

		if (mrp.Multiplicity != Multiplicity.Many)
			return reader.GetReference(byteOffset) == op->directReference;

		return reader.ContainsReference(engine, byteOffset, op->directReference);
	}

	private struct DeletedSet
	{
		HashSet<long> set;
		bool invalidated;

		public void Add(long id)
		{
			if (invalidated)
				return;

			set ??= new HashSet<long>(8);
			set.Add(id);

			if (set.Count > deletedCountLimit)
			{
				invalidated = true;
				set = null;
			}
		}

		public bool TryGetIsDeleted(long id, out bool isDeleted)
		{
			if (invalidated)
			{
				isDeleted = false;
				return false;
			}

			isDeleted = set != null && set.Contains(id);
			return true;
		}
	}
}
