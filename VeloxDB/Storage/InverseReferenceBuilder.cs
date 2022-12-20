using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal unsafe sealed class InverseReferenceBuilder
{
	Database database;
	DataModelDescriptor model;
	ulong commitVersion;

	Dictionary<short, IndexedReferences> filter;

	public InverseReferenceBuilder(Database database, ulong commitVersion = 0)
	{
		this.database = database;
		this.commitVersion = commitVersion;
		model = this.database.ModelDesc;
	}

	public void SetFilter(Dictionary<short, HashSet<PropertyFilter>> filter)
	{
		if (filter == null)
		{
			this.filter = null;
			return;
		}

		this.filter = new Dictionary<short, IndexedReferences>(2);
		foreach (short classId in filter.Keys)
		{
			ClassDescriptor classDesc = database.ModelDesc.GetClass(classId);
			HashSet<PropertyFilter> propFilter = filter[classId];

			IndexedReferences r = new IndexedReferences()
			{
				ReferenceIndexes = ReadOnlyArray<int>.FromNullable(propFilter.Select(x => classDesc.GetPropertyIndex(x.PropertyId))),
				TrackingOverride = propFilter.Select(x => x.IsTracked).ToArray()
			};

			this.filter.Add(classId, r);
		}
	}

	public void Build(JobWorkers<CommonWorkerParam> workers)
	{
		TransactionContext tc = CollectInvRefs(workers);
		BuildInvRefs(workers, tc);
		tc.Clear();
		database.Engine.ContextPool.Put(tc);
	}

	public void Update(JobWorkers<CommonWorkerParam> workers, TransactionContext tc, long[] groupCounts)
	{
		tc.GroupSortInvRefs(groupCounts);
		UpdateInvRefs(workers, tc);
	}

	private TransactionContext CollectInvRefs(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(database.TraceId);

		long estRefCount = CalculateEstimatedRefCount();

		List<TransactionContext> tcs = new List<TransactionContext>(workers.WorkerCount);
		Action<CommonWorkerParam>[] actions = new Action<CommonWorkerParam>[workers.WorkerCount];
		for (int i = 0; i < actions.Length; i++)
		{
			TransactionContext tc = database.Engine.ContextPool.Get();
			tc.Init(database, 0);
			tc.ClearInvRefGroupCounts();

			if (i == 0)
			{
				tc.ResizeInvRefChange((long)(estRefCount * 1.5));   // We will collect all references to this context
			}
			else
			{
				tc.ResizeInvRefChange((long)(estRefCount * 2.0 / workers.WorkerCount));
			}

			tcs.Add(tc);

			ObjectReader[] objects = new ObjectReader[128];
			actions[i] = x =>
			{
				ClassScan scan = (ClassScan)x.ReferenceParam;
				ReadOnlyArray<int> refPropIndexes = scan.Class.ClassDesc.RefeferencePropertyIndexes;
				bool[] trackingOverride = null;

				if (filter != null)
				{
					refPropIndexes = filter[scan.Class.ClassDesc.Id].ReferenceIndexes;
					trackingOverride = filter[scan.Class.ClassDesc.Id].TrackingOverride;
				}

				int count = objects.Length;
				while (scan.Next(objects, 0, ref count))
				{
					for (int j = 0; j < count; j++)
					{
						// Since we are scanning outside of transaction, we can get deleted objects during alignment, so just skip them
						if (!objects[j].ClassObject->IsDeleted)
						{
							scan.Class.CreateGroupingInvRefChanges(tc, objects[j].ClassObject,
								InvRefChangeType.Insert, refPropIndexes, trackingOverride);
						}
					}

					count = objects.Length;
				}
			};
		}

		workers.SetActions(actions);

		EnqueueClassScans(workers);

		workers.Drain();

		IntPtr[] ps = tcs[0].MergeInvRefChanges(tcs.Skip(1).ToArray());
		workers.SetAction(x =>
		{
			Tuple<IntPtr, TransactionContext> vt = (Tuple<IntPtr, TransactionContext>)x.ReferenceParam;
			vt.Item2.CopyInvRefChanges(vt.Item1);
		});

		for (int i = 0; i < ps.Length; i++)
		{
			workers.EnqueueWork(new CommonWorkerParam()
			{
				ReferenceParam = new Tuple<IntPtr, TransactionContext>(ps[i], tcs[i + 1])
			});
		}

		workers.Drain();

		for (int i = 1; i < tcs.Count; i++)
		{
			tcs[i].Clear();
			database.Engine.ContextPool.Put(tcs[i]);
		}

		long[] l = Enumerable.Range(0, GroupingReferenceSorter.GroupCount).Select(x => (long)tcs[0].InvRefGroupCounts[x]).ToArray();
		tcs[0].GroupSortInvRefs(l);
		return tcs[0];
	}

	private void EnqueueClassScans(JobWorkers<CommonWorkerParam> workers)
	{
		if (filter != null)
		{
			foreach (short classId in filter.Keys)
			{
				ClassDescriptor classDesc = database.ModelDesc.GetClass(classId);
				Class @class = database.GetClass(classDesc.Index).MainClass;
				ClassScan[] scans = @class.GetClassScans(null, false, out long totalCount);
				for (int j = 0; j < scans.Length; j++)
				{
					workers.EnqueueWork(new CommonWorkerParam() { ReferenceParam = scans[j] });
				}
			}
		}
		else
		{
			for (int i = 0; i < database.ModelDesc.ClassCount; i++)
			{
				Class @class = database.GetClass(i).MainClass;
				if (@class != null && @class.ClassDesc.RefeferencePropertyIndexes.Length > 0)
				{
					ClassScan[] scans = @class.GetClassScans(null, false, out long totalCount);
					for (int j = 0; j < scans.Length; j++)
					{
						workers.EnqueueWork(new CommonWorkerParam() { ReferenceParam = scans[j] });
					}
				}
			}
		}
	}

	private long CalculateEstimatedRefCount()
	{
		long estRefCount = 0;
		if (filter != null)
		{
			foreach (short classId in filter.Keys)
			{
				ClassDescriptor classDesc = database.ModelDesc.GetClass(classId);
				Class @class = database.GetClass(classDesc.Index).MainClass;
				if (@class != null)
				{
					long estObjCount = @class.EstimatedObjectCount *
						filter[classId].ReferenceIndexes.Length / @class.ClassDesc.RefeferencePropertyIndexes.Length;

					estRefCount += estObjCount * (@class.ClassDesc.RefeferencePropertyIndexes.Where(
							x => (@class.ClassDesc.Properties[x] as ReferencePropertyDescriptor).Multiplicity == Multiplicity.Many)
							.Count() * 16 + @class.ClassDesc.RefeferencePropertyIndexes.Length);
				}
			}
		}
		else
		{
			for (int i = 0; i < model.ClassCount; i++)
			{
				Class @class = database.GetClass(i).MainClass;
				if (@class != null)
				{
					estRefCount += @class.EstimatedObjectCount * (@class.ClassDesc.RefeferencePropertyIndexes.Where(
							x => (@class.ClassDesc.Properties[x] as ReferencePropertyDescriptor).Multiplicity == Multiplicity.Many)
							.Count() * 16 + @class.ClassDesc.RefeferencePropertyIndexes.Length);
				}
			}
		}

		return estRefCount;
	}

	private void BuildInvRefs(JobWorkers<CommonWorkerParam> workers, TransactionContext tc)
	{
		TTTrace.Write(database.TraceId);

		NativeList l = tc.InverseRefChanges;
		InverseReferenceOperation* rc = (InverseReferenceOperation*)l.Buffer;

		List<Tuple<long, long>> workItems = SplitInverseRefs(rc, l.Count, workers.WorkerCount);
		if (workItems.Count == 0)
			return;

		if (filter == null)
			PreSizeInvRefMaps(workers, workItems, rc);

		workers.SetAction(x =>
		{
			Tuple<long, long> t = (Tuple<long, long>)x.ReferenceParam;
			BuildInvRefsWorker(rc + t.Item1, t.Item2 - t.Item1);
		});

		for (int i = 0; i < workItems.Count; i++)
		{
			workers.EnqueueWork(new CommonWorkerParam() { ReferenceParam = workItems[i] });
		}

		workers.Drain();
	}

	private void UpdateInvRefs(JobWorkers<CommonWorkerParam> workers, TransactionContext tc)
	{
		TTTrace.Write(database.TraceId);

		NativeList l = tc.InverseRefChanges;
		InverseReferenceOperation* rc = (InverseReferenceOperation*)l.Buffer;

		List<Tuple<long, long>> workItems = SplitInverseRefs(rc, l.Count, workers.WorkerCount);
		if (workItems.Count == 0)
			return;

		Action<CommonWorkerParam>[] actions = new Action<CommonWorkerParam>[workers.WorkerCount];
		TransactionContext[] workerTranContexts = new TransactionContext[workers.WorkerCount];
		for (int i = 0; i < actions.Length; i++)
		{
			TransactionContext wtc = database.Engine.ContextPool.Get();
			wtc.Init(database, 0);

			actions[i] = x =>
			{
				Tuple<long, long> t = (Tuple<long, long>)x.ReferenceParam;
				UpdateInvRefsWorker(wtc, rc + t.Item1, t.Item2 - t.Item1);
			};

			workerTranContexts[i] = wtc;
		}

		workers.SetActions(actions);

		for (int i = 0; i < workItems.Count; i++)
		{
			workers.EnqueueWork(new CommonWorkerParam() { ReferenceParam = workItems[i] });
		}

		workers.Drain();

		for (int i = 0; i < workerTranContexts.Length; i++)
		{
			workerTranContexts[i].Clear();
			database.Engine.ContextPool.Put(workerTranContexts[i]);
		}
	}

	private void BuildInvRefsWorker(InverseReferenceOperation* rc, long count)
	{
		InverseReferenceOperation* begRange;
		long rangeCount;

		while (count > 0)
		{
			GetNextRange(ref rc, ref count, out begRange, out rangeCount);
			CreateRangeInvRefs(begRange, rangeCount);
		}
	}

	private void UpdateInvRefsWorker(TransactionContext tc, InverseReferenceOperation* rc, long count)
	{
		InverseReferenceOperation* begRange;
		long rangeCount;

		while (count > 0)
		{
			GetNextRange(ref rc, ref count, out begRange, out rangeCount);
			UpdateRangeInvRefs(tc, begRange, rangeCount);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void CreateRangeInvRefs(InverseReferenceOperation* rc, long count)
	{
		IdHelper.TryGetClassIndex(model, rc->directReference, out ushort typeIndex);
		Checker.AssertTrue(typeIndex != ushort.MaxValue);
		InverseReferenceMap invRefMap = database.GetInvRefs(typeIndex);
		invRefMap.Insert(1, rc->directReference, rc->PropertyId, rc->IsTracked, false, (int)count, rc);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void UpdateRangeInvRefs(TransactionContext tc, InverseReferenceOperation* rc, long count)
	{
		IdHelper.TryGetClassIndex(model, rc->directReference, out ushort typeIndex);
		Checker.AssertTrue(typeIndex != ushort.MaxValue);
		InverseReferenceMap invRefMap = database.GetInvRefs(typeIndex);

		int insertCount = 0;
		while (rc[insertCount].Type == (int)InvRefChangeType.Insert && insertCount < count)
		{
			insertCount++;
		}

		invRefMap.ApplyAlignmentModification(tc, commitVersion, rc->directReference, rc->PropertyId,
			rc->IsTracked, insertCount, (int)(count - insertCount), rc, rc + insertCount);
	}

	private void PreSizeInvRefMaps(JobWorkers<CommonWorkerParam> workers, List<Tuple<long, long>> workItems, InverseReferenceOperation* rc)
	{
		TTTrace.Write(database.TraceId);

		Dictionary<InverseReferenceMap, long>[] invRefSizes = new Dictionary<InverseReferenceMap, long>[workers.WorkerCount];
		Action<CommonWorkerParam>[] actions = new Action<CommonWorkerParam>[workers.WorkerCount];
		for (int i = 0; i < workers.WorkerCount; i++)
		{
			Dictionary<InverseReferenceMap, long> hm =
				new Dictionary<InverseReferenceMap, long>(model.ClassCount, ReferenceEqualityComparer<InverseReferenceMap>.Instance);

			invRefSizes[i] = hm;

			actions[i] = x =>
			{
				Tuple<long, long> t = (Tuple<long, long>)x.ReferenceParam;
				CountInvRefEntriesWorker(rc + t.Item1, t.Item2 - t.Item1, hm);
			};
		}

		workers.SetActions(actions);
		for (int i = 0; i < workItems.Count; i++)
		{
			workers.EnqueueWork(new CommonWorkerParam() { ReferenceParam = workItems[i] });
		}

		workers.Drain();

		for (int i = 1; i < invRefSizes.Length; i++)
		{
			foreach (KeyValuePair<InverseReferenceMap, long> kv in invRefSizes[i])
			{
				long c = 0;
				invRefSizes[0].TryGetValue(kv.Key, out c);
				invRefSizes[0][kv.Key] = c + kv.Value;
			}
		}

		workers.SetAction(x =>
		{
			KeyValuePair<InverseReferenceMap, long> kv = (KeyValuePair<InverseReferenceMap, long>)x.ReferenceParam;
			kv.Key.Resize(kv.Value);
		});

		foreach (KeyValuePair<InverseReferenceMap, long> kv in invRefSizes[0])
		{
			workers.EnqueueWork(new CommonWorkerParam() { ReferenceParam = kv });
		}

		workers.Drain();
	}

	private void CountInvRefEntriesWorker(InverseReferenceOperation* rc, long count, Dictionary<InverseReferenceMap, long> invRefSizes)
	{
		InverseReferenceOperation* begRange;
		long rangeCount;

		while (count > 0)
		{
			GetNextRange(ref rc, ref count, out begRange, out rangeCount);

			IdHelper.TryGetClassIndex(model, begRange->directReference, out ushort typeIndex);
			Checker.AssertTrue(typeIndex != ushort.MaxValue);
			InverseReferenceMap invRefMap = database.GetInvRefs(typeIndex);

			long c = 0;
			invRefSizes.TryGetValue(invRefMap, out c);
			invRefSizes[invRefMap] = c + 1;
		}
	}

	public static List<Tuple<long, long>> SplitInverseRefs(InverseReferenceOperation* rc, long refCount, int workerCount)
	{
		List<Tuple<long, long>> res = new List<Tuple<long, long>>(workerCount);

		long diff = refCount / workerCount;

		long start = 0;
		for (int i = 0; i < workerCount; i++)
		{
			long end = Math.Min(refCount, start + diff);
			if (i == workerCount - 1)
				end = refCount;

			MoveToEndOfRefInterval(rc, refCount, ref end);

			if (end > start)
				res.Add(new Tuple<long, long>(start, end));

			start = end;
		}

		return res;
	}

	private static void MoveToEndOfRefInterval(InverseReferenceOperation* rc, long refCount, ref long end)
	{
		if (end >= refCount)
			return;

		rc += end;
		long currRefId = rc->directReference;
		while (end < refCount && rc->directReference == currRefId)
		{
			rc++;
			end++;
		}
	}

	public static void GetNextRange(ref InverseReferenceOperation* pch, ref long count, out InverseReferenceOperation* rangeBeg, out long rangeCount)
	{
		Checker.AssertTrue(count > 0);

		rangeBeg = pch;
		rangeCount = 1;

		pch++;
		count--;
		while (count > 0)
		{
			if (pch->directReference != rangeBeg->directReference || pch->PropertyId != rangeBeg->PropertyId)
				return;

			rangeCount++;
			count--;
			pch++;
		}
	}

	public struct PropertyFilter
	{
		public int PropertyId { get; private set; }
		public bool IsTracked { get; private set; }

		public PropertyFilter(int propertyId, bool isTracked)
		{
			this.PropertyId = propertyId;
			this.IsTracked = isTracked;
		}
	}

	private struct IndexedReferences
	{
		public ReadOnlyArray<int> ReferenceIndexes { get; set; }
		public bool[] TrackingOverride { get; set; }
	}
}
