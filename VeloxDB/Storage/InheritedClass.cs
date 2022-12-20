using System;
using System.Collections.Generic;
using System.Linq;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using static VeloxDB.Storage.Database;

namespace VeloxDB.Storage;

internal unsafe sealed partial class InheritedClass : ClassBase
{
	ClassBase[] inheritedClasses;

	public InheritedClass(Database database, ClassDescriptor classDesc, long capacity) :
		base(database, classDesc)
	{
		TTTrace.Write(Engine.TraceId, classDesc.Id);

		if (!classDesc.IsAbstract)
			MainClass = new Class(database, classDesc, capacity);
	}

	public InheritedClass(Class mainClass) :
		base(mainClass.Database, mainClass.ClassDesc)
	{
		TTTrace.Write(Engine.TraceId, mainClass.ClassDesc.Id);
		this.MainClass = mainClass;
	}

	public void SetInheritedClasses(ClassEntry[] classes)
	{
		inheritedClasses = new ClassBase[ClassDesc.DirectDescendentClasses.Count()];
		int c = 0;
		foreach (ClassDescriptor descClass in ClassDesc.DirectDescendentClasses)
		{
			inheritedClasses[c++] = classes[descClass.Index].Class;
		}
	}

	public override ObjectReader GetObject(Transaction tran, long id, out DatabaseErrorDetail err)
	{
		if (MainClass == null)
		{
			err = null;
			return new ObjectReader();
		}

		return MainClass.GetObject(tran, id, out err);
	}

	public override ObjectReader GetObjectNoReadLock(Transaction tran, long id)
	{
		return MainClass.GetObjectNoReadLock(tran, id);
	}

	public override ObjectStorage.ScanRange[] GetScanRanges(bool scanInhereted, out long totalCount)
	{
		if (!scanInhereted && MainClass != null)
			return MainClass.GetScanRanges(false, out totalCount);

		List<ObjectStorage.ScanRange> ranges = new List<ObjectStorage.ScanRange>((scanInhereted ? inheritedClasses.Length : 0 + 1) * 2);
		totalCount = 0;

		if (MainClass != null)
			ranges.AddRange(MainClass.GetScanRanges(scanInhereted, out totalCount));

		if (scanInhereted)
		{
			for (int i = 0; i < inheritedClasses.Length; i++)
			{
				ranges.AddRange(inheritedClasses[i].GetScanRanges(scanInhereted, out long tc));
				totalCount += tc;
			}
		}

		return ranges.ToArray();
	}

	public override ObjectStorage.ScanRange[] GetDisposingScanRanges(bool scanInhereted, out long totalCount)
	{
		if (!scanInhereted && MainClass != null)
			return MainClass.GetDisposingScanRanges(false, out totalCount);

		List<ObjectStorage.ScanRange> ranges = new List<ObjectStorage.ScanRange>((scanInhereted ? inheritedClasses.Length : 0 + 1) * 2);
		totalCount = 0;

		if (MainClass != null)
			ranges.AddRange(MainClass.GetDisposingScanRanges(scanInhereted, out totalCount));

		if (scanInhereted)
		{
			for (int i = 0; i < inheritedClasses.Length; i++)
			{
				ranges.AddRange(inheritedClasses[i].GetDisposingScanRanges(scanInhereted, out long tc));
				totalCount += tc;
			}
		}

		return ranges.ToArray();
	}

	public override DatabaseErrorDetail TakeReadLock(Transaction tran, ClassLocker locker)
	{
		TTTrace.Write(Engine.TraceId, ClassDesc.Id, tran.Id);

		DatabaseErrorDetail err = MainClass?.TakeReadLock(tran, locker);
		if (err != null)
			return err;

		for (int i = 0; i < inheritedClasses.Length; i++)
		{
			ClassBase @class = inheritedClasses[i];
			locker = tran.Database.GetClassLocker(@class.ClassDesc.Index);
			err = @class.TakeReadLock(tran, locker);
			if (err != null)
				return err;
		}

		return null;
	}

	protected override void OnModelUpdated()
	{
		if (MainClass != null)
			MainClass.ModelUpdated(ClassDesc);
	}

	protected override void OnStartPropertyUpdate(ClassDescriptor newClassDesc, bool propertyListModified)
	{
		if (MainClass != null)
			MainClass.StartPropertyUpdate(newClassDesc, propertyListModified);
	}

	protected override void OnFinishPropertyUpdate()
	{
		if (MainClass != null)
			MainClass.FinishPropertyUpdate();
	}

	public override void Dispose(JobWorkers<CommonWorkerParam> workers)
	{
		TTTrace.Write(Engine.TraceId, ClassDesc.Id);

		if (MainClass != null)
			MainClass.Dispose(workers);
	}
}
