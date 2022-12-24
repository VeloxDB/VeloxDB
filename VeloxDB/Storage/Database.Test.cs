using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage.Persistence;

namespace VeloxDB.Storage;

internal unsafe sealed partial class Database
{
	[Conditional("TEST_BUILD")]
	private void ValidateStructure()
	{
		for (int i = 0; i < classes.Length; i++)
		{
			ClassBase @class = classes[i].Class;

			if (@class == null)
				throw new InvalidOperationException();

			if (!object.ReferenceEquals(@class.ClassDesc, modelDesc.GetClass(@class.ClassDesc.Id)))
				throw new InvalidOperationException();

			if (!object.ReferenceEquals(@class, classes[@class.ClassDesc.Index].Class))
				throw new InvalidOperationException();

			if ((@class.ClassDesc.IsAbstract || @class.ClassDesc.DescendentClassIds.Length > 0) != (@class is InheritedClass))
				throw new InvalidOperationException();

			if (@class.ClassDesc.IsAbstract)
			{
				if (classes[i].InvRefMap != null || classes[i].Locker != null)
					throw new InvalidOperationException();
			}
			else
			{
				if (((@class.ClassDesc.InverseReferences.Length > 0) != (classes[i].InvRefMap != null)) || classes[i].Locker == null)
					throw new InvalidOperationException();

				if (classes[i].InvRefMap != null && !object.ReferenceEquals(classes[i].InvRefMap.ClassDesc, @class.ClassDesc))
					throw new InvalidOperationException();
			}

			InheritedClass inhClass = @class as InheritedClass;
			if (inhClass != null)
			{
				foreach (ClassDescriptor childClassDesc in @class.ClassDesc.SubtreeClasses)
				{
					if (childClassDesc.BaseClass != null && childClassDesc.BaseClass.Id == @class.ClassDesc.Id &&
						inhClass.InheritedClasses.FirstOrDefault(x => x.ClassDesc.Id == childClassDesc.Id) == null)
					{
						throw new InvalidOperationException();
					}
				}

				foreach (ClassBase childClass in inhClass.InheritedClasses)
				{
					if (childClass.ClassDesc.BaseClass.Id != @class.ClassDesc.Id)
						throw new InvalidOperationException();

					if (!object.ReferenceEquals(childClass, classes[childClass.ClassDesc.Index].Class))
						throw new InvalidOperationException();
				}
			}
		}
		foreach (ClassDescriptor classDesc in modelDesc.GetAllClasses())
		{
			ClassBase @class = GetClass(classDesc.Index);
			if (@class == null)
				throw new InvalidOperationException();

			if (!object.ReferenceEquals(@class.ClassDesc, classDesc))
				throw new InvalidOperationException();
		}
	}

#if TEST_BUILD
	[Conditional("TEST_BUILD")]
	internal void WaitSnapshotFinished()
	{
		snapshotController.Block();

		if (persister != null)
			persister.WaitSnapshotFinished();

		snapshotController.Unblock();
	}

	internal void ValidateGarbage(int allowedTranCount = 0)
	{
		TTTrace.Write(engine.TraceId);

		TryFlushGCAndWaitCompletion(allowedTranCount);

		gc.GetOldestReaders(versions, out ulong readVer);

		bool checkEmpty = !gc.HasActiveTransactions();

		foreach (ClassEntry classEntry in classes)
		{
			if (classEntry.Class.MainClass != null)
				classEntry.Class.MainClass.ValidateGarbage(readVer);

			if (classEntry.InvRefMap != null)
				classEntry.InvRefMap.ValidateGarbage(readVer, checkEmpty);
		}
	}

	public bool TryFlushGCAndWaitCompletion(int allowedActiveTranCount)
	{
		if (gc.ActiveTransactionCount > allowedActiveTranCount)
			return false;

		DrainGC();
		return true;
	}

	internal void ValidateAndCollectBlobs(Transaction tran, Dictionary<ulong, int> strings, Dictionary<ulong, int> blobs)
	{
		if (versions.ReadVersion != versions.CommitVersion)
			throw new InvalidOperationException();

		versions.ValidateOrderAndUniqueness();

		if (tran != null)
			ValidateInvRefs(tran);

		List<long> tempList = new List<long>();
		foreach (ClassEntry classEntry in classes)
		{
			Class @class = classEntry.Class.MainClass;
			if (@class != null)
			{
				@class.CollectBlobRefCounts(strings, blobs);
				@class.Validate(tempList, versions.ReadVersion);
			}
		}

		foreach (ClassEntry classEntry in classes)
		{
			if (classEntry.Locker != null)
				classEntry.Locker.Validate();
		}

		foreach (HashIndexEntry hashEntry in hashIndexes)
		{
			hashEntry.Locker.ValidateAndCollectBlobs(versions.ReadVersion, strings);
			hashEntry.Index.Validate(versions.ReadVersion);
		}
	}

	private void ValidateInvRefs(Transaction tran)
	{
		TTTrace.Write(engine.TraceId, tran.Id);

		if (tran == null)
			return;

		Dictionary<ValueTuple<long, int>, List<long>> validInvRefs = new Dictionary<ValueTuple<long, int>, List<long>>(1024, new InvRefKeyEqComparer());
		foreach (ClassEntry classEntry in classes)
		{
			if (classEntry.Class.MainClass != null)
				classEntry.Class.MainClass.CollectInverseRefsAndValidateTran(tran, validInvRefs);
		}

		foreach (ClassEntry classEntry in classes)
		{
			if (classEntry.InvRefMap != null)
				classEntry.InvRefMap.ValidateInverseRefs(tran, validInvRefs);
		}

		if (validInvRefs.Count > 0)
			throw new InvalidOperationException();
	}

	private class InvRefKeyEqComparer : IEqualityComparer<ValueTuple<long, int>>
	{
		public bool Equals(ValueTuple<long, int> x, ValueTuple<long, int> y)
		{
			return x.Item1 == y.Item1 && x.Item2 == y.Item2;
		}

		public int GetHashCode(ValueTuple<long, int> obj)
		{
			return (int)((ulong)obj.Item1 * HashUtils.PrimeMultiplier64 + (uint)obj.Item2);
		}
	}

	internal void Persist(string dir, int logIndex)
	{
		TTTrace.Write(engine.TraceId);

		LogDescriptor logDesc = new LogDescriptor("main", false, dir, dir, 1024 * 8);

		string[] logs;
		string[] snapshots;
		LogPersister.GetLogNames(Engine, logDesc, out logs);
		LogPersister.GetSnapshotNames(Engine, logDesc, out snapshots);

		LogFileWriter.CreateEmpty(logs[0], 1, true, logDesc.MaxSize, logDesc.IsPackedFormat);
		LogFileHeader head = LogFileWriter.CreateEmpty(logs[1], 2, true, logDesc.MaxSize, logDesc.IsPackedFormat);

		SnapshotFileWriter sfw = new SnapshotFileWriter(this, null, versions, logIndex);
		sfw.CreateSnapshot(snapshots[1]);
	}

#endif
}
