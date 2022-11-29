using System;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.Storage;

internal unsafe abstract partial class ClassBase
{
	StorageEngine engine;
	Database database;
	ClassDescriptor classDesc;
	ClassDescriptor updatedClassDesc;
	Class mainClass;

	public ClassBase(Database database, ClassDescriptor classDesc)
	{
		this.database = database;
		this.engine = database.Engine;
		this.classDesc = classDesc;
	}

	internal Database Database => database;
	internal StorageEngine Engine => engine;
	internal ClassDescriptor ClassDesc => classDesc;
	protected long TraceId => Engine.TraceId;
	public Class MainClass { get => mainClass; protected set => mainClass = value; }

	public static ClassBase CreateEmptyClass(Database database, ClassDescriptor classDesc, long capacity)
	{
		if (classDesc.DescendentClassIds.Length > 0 || classDesc.IsAbstract)
		{
			return new InheritedClass(database, classDesc, capacity);
		}
		else
		{
			return new Class(database, classDesc, capacity);
		}
	}

	public static ClassBase ChangeHierarchyType(ClassBase @class)
	{
		TTTrace.Write(@class.TraceId, @class.ClassDesc.Id, @class.GetType().FullName);
		if (@class is InheritedClass)
		{
			return @class.MainClass;
		}
		else
		{
			return new InheritedClass((Class)@class);
		}
	}

	public abstract ObjectStorage.ScanRange[] GetScanRanges(bool scanInhereted, out long totalCount);
	public abstract ObjectStorage.ScanRange[] GetDisposingScanRanges(bool scanInhereted, out long totalCount);

	public abstract ObjectReader GetObject(Transaction tran, long id, out DatabaseErrorDetail err);
	public abstract ObjectReader GetObjectNoReadLock(Transaction tran, long id);
	public abstract DatabaseErrorDetail TakeReadLock(Transaction tran, ClassLocker locker);
	public abstract void Dispose(JobWorkers<CommonWorkerParam> workers);

	protected virtual void OnModelUpdated()
	{
	}

	protected abstract void OnStartPropertyUpdate(ClassDescriptor newClassDesc, bool propertiesModified);
	protected abstract void OnFinishPropertyUpdate();

	public void ModelUpdated(ClassDescriptor classDesc)
	{
		this.classDesc = classDesc;
		OnModelUpdated();
		TTTrace.Write(classDesc.Id, classDesc.StringPropertyIndexes.Length, classDesc.BlobPropertyIndexes.Length);
	}

	public void StartPropertyUpdate(ClassDescriptor newClassDesc, bool propertyListModified)
	{
		updatedClassDesc = newClassDesc;
		OnStartPropertyUpdate(newClassDesc, propertyListModified);
	}

	public void FinishPropertyUpdate()
	{
		this.classDesc = updatedClassDesc;
		OnFinishPropertyUpdate();
		updatedClassDesc = null;
	}

	public ClassScan GetClassScan(Transaction tran, bool scanInherited, out long totalCount)
	{
		ObjectStorage.ScanRange[] ranges = GetScanRanges(scanInherited, out totalCount);
		return new ClassScan(tran, ranges);
	}

	public ClassScan[] GetClassScans(Transaction tran, bool scanInherited, out long totalCount)
	{
		ObjectStorage.ScanRange[] ranges = GetScanRanges(scanInherited, out totalCount);
		ClassScan[] scans = new ClassScan[ranges.Length];
		for (int i = 0; i < scans.Length; i++)
		{
			scans[i] = new ClassScan(tran, new ObjectStorage.ScanRange[] { ranges[i] });
		}

		return scans;
	}

	public ClassScan[] GetDisposingClassScans(Transaction tran, bool scanInherited, out long totalCount)
	{
		ObjectStorage.ScanRange[] ranges = GetDisposingScanRanges(scanInherited, out totalCount);
		ClassScan[] scans = new ClassScan[ranges.Length];
		for (int i = 0; i < scans.Length; i++)
		{
			scans[i] = new ClassScan(tran, new ObjectStorage.ScanRange[] { ranges[i] });
		}

		return scans;
	}
}
