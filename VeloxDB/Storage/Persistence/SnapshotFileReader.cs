using System;
using System.Collections.Generic;
using System.IO;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using static System.Math;

namespace VeloxDB.Storage.Persistence;

internal unsafe sealed class SnapshotFileReader : IDisposable
{
	Database database;
	NativeFile file;
	string fileName;
	int logIndex;
	JobWorkers<CommonWorkerParam> workers;

	Dictionary<short, RestoredClassHeader> classes;

	public SnapshotFileReader(Database database, JobWorkers<CommonWorkerParam> workers, string fileName, int logIndex)
	{
		TTTrace.Write(database.TraceId, database.Id, fileName, logIndex);

		this.database = database;
		this.workers = workers;
		this.fileName = fileName;
		this.logIndex = logIndex;

		file = NativeFile.Create(fileName, FileMode.Open, FileAccess.Read, FileShare.None, FileFlags.Sequential);
	}

	public static DatabaseVersions ReadVersions(Database database, string fileName, int logIndex)
	{
		using (NativeFile file = NativeFile.Create(fileName, FileMode.Open, FileAccess.Read, FileShare.None, FileFlags.Sequential))
		{
			SnapshotFileHeader head = new SnapshotFileHeader();
			file.Read(new IntPtr(&head), SnapshotFileHeader.Size);

			if (head.version > DatabasePersister.FormatVersion)
				Checker.NotSupportedException("Unsupported snapshot format.");

			return ReadVersions(database, logIndex, file);
		}
	}

	public void Restore(out DatabaseVersions versions)
	{
		TTTrace.Write(database.TraceId, database.Id, logIndex);

		SnapshotFileHeader head = new SnapshotFileHeader();

		file.Read(new IntPtr(&head), SnapshotFileHeader.Size);

		if (head.version > DatabasePersister.FormatVersion)
			Checker.NotSupportedException("Snapshot format {0} not supported.", head.version);

		versions = ReadVersions(database, logIndex, file);
		classes = ReadClassHeaders(file, database);

		ResizeClasses();

		int maxLoadedItems = (int)Math.Max(1, Math.Min(int.MaxValue, database.Engine.Settings.SnapshotMaxMemorySize / SnapshotFileWriter.BlockSize));
		workers.SetMaxItemCount(maxLoadedItems);
		workers.SetAction(SnapshotWorker);

		LoadClassSnapshots();
		workers.Drain();
		workers.SetMaxItemCount(-1);
	}

	private void ResizeClasses()
	{
		foreach (RestoredClassHeader ch in classes.Values)
		{
			ClassDescriptor classDesc = database.ModelDesc.GetClass(ch.ClassId);
			if (classDesc == null || classDesc.IsAbstract)
				continue;

			Class @class = database.GetClass(classDesc.Index).MainClass;
			@class.ResizeEmpty(ch.ObjectCount);
		}
	}

	private static DatabaseVersions ReadVersions(Database database, int logIndex, NativeFile file)
	{
		long size;
		file.Read((IntPtr)(&size), sizeof(long));

		IntPtr buffer = NativeAllocator.Allocate(size);
		file.Read((IntPtr)buffer, size);

		SegmentBinaryReader reader = new SegmentBinaryReader((byte*)buffer, size);

		uint localTerm = (uint)reader.ReadInt();
		ulong maxLogSeqNum = (ulong)reader.ReadLong();
		int count = reader.ReadInt();

		List<GlobalVersion> globalVersions = new List<GlobalVersion>(count);

		for (int i = 0; i < count; i++)
		{
			long low = reader.ReadLong();
			long high = reader.ReadLong();
			ulong version = (ulong)reader.ReadLong();
			globalVersions.Add(new GlobalVersion(new SimpleGuid(low, high), version));
		}

		reader.ValidateFinishedReading();
		NativeAllocator.Free(buffer);

		GlobalVersion lastVersion = globalVersions[globalVersions.Count - 1];
		DatabaseVersions versions = new DatabaseVersions(database, lastVersion.Version,
			lastVersion.Version, maxLogSeqNum, localTerm, globalVersions);

		versions.TTTraceState();

		return versions;
	}

	private static Dictionary<short, RestoredClassHeader> ReadClassHeaders(NativeFile file, Database database)
	{
		long size;
		file.Read((IntPtr)(&size), sizeof(long));

		IntPtr buffer = NativeAllocator.Allocate(size);
		file.Read((IntPtr)buffer, size);

		SegmentBinaryReader reader = new SegmentBinaryReader((byte*)buffer, size);

		int count = reader.ReadInt();

		Dictionary<short, RestoredClassHeader> classes = new Dictionary<short, RestoredClassHeader>(count);
		for (int i = 0; i < count; i++)
		{
			ClassSnapshotFileHeader* h = (ClassSnapshotFileHeader*)reader.SkipBytes(ClassSnapshotFileHeader.Size);

			ClassDescriptor classDesc = database.ModelDesc.GetClass(h->classId);

			TTTrace.Write(database.TraceId, database.Id, h->classId, h->propertyCount, classDesc != null);

			PropertyType[] propertyTypes = new PropertyType[h->propertyCount];
			int[] propertyIndexes = new int[h->propertyCount];

			bool altered = classDesc != null && h->propertyCount != classDesc.Properties.Length - 2;

			for (int j = 0; j < h->propertyCount; j++)
			{
				int propertyId = reader.ReadInt();
				propertyTypes[j] = (PropertyType)reader.ReadByte();
				propertyIndexes[j] = classDesc != null ? classDesc.GetPropertyIndex(propertyId) : -1;

				TTTrace.Write(database.TraceId, database.Id, h->classId, classDesc != null,
					propertyId, (int)propertyTypes[j], propertyIndexes[j]);

				altered |= propertyIndexes[j] != (j + 2);
			}

			if (classDesc != null)
				classes.Add(h->classId, new RestoredClassHeader(h->classId, h->objectCount, propertyTypes, propertyIndexes, altered));
		}

		reader.ValidateFinishedReading();
		NativeAllocator.Free(buffer);

		return classes;
	}

	private void LoadClassSnapshots()
	{
		while (true)
		{
			long size;
			file.Read((IntPtr)(&size), sizeof(long), out long read);

			if (read < sizeof(long))
				return;

			IntPtr buffer = NativeAllocator.Allocate(size);
			file.Read(buffer, size);

			workers.EnqueueWork(new CommonWorkerParam() { ReferenceParam = (buffer, size) });
		}
	}

	private void SnapshotWorker(CommonWorkerParam p)
	{
		ValueTuple<IntPtr, long> item = (ValueTuple<IntPtr, long>)p.ReferenceParam;
		SegmentBinaryReader reader = new SegmentBinaryReader((byte*)item.Item1, item.Item2);

		ClassSnapshotBlockFileHeader* head = (ClassSnapshotBlockFileHeader*)reader.SkipBytes(ClassSnapshotBlockFileHeader.Size);

		ClassDescriptor classDesc = database.ModelDesc.GetClass(head->classId);
		if (classDesc == null || classDesc.IsAbstract)
		{
			TTTrace.Write(database.TraceId, database.Id, head->classId);
			NativeAllocator.Free(item.Item1);
			return;
		}

		Class @class = database.GetClass(classDesc.Index).MainClass;
		RestoredClassHeader rch = classes[classDesc.Id];

		TTTrace.Write(database.TraceId, database.Id, classDesc.Id, rch.Altered);

		@class.RestoreSnapshot(rch.PropertyIndexes, rch.PropertyTypes, rch.Altered, head->objectCount, reader);

		reader.ValidateFinishedReading();
		NativeAllocator.Free(item.Item1);
	}

	public void Dispose()
	{
		file.Dispose();
	}
}

internal sealed class RestoredClassHeader
{
	public short ClassId { get; private set; }
	public long ObjectCount { get; private set; }
	public PropertyType[] PropertyTypes { get; private set; }
	public int[] PropertyIndexes { get; private set; }
	public bool Altered { get; private set; }

	public RestoredClassHeader(short classId, long objectCount, PropertyType[] propertyTypes, int[] propertyIndexes, bool altered)
	{
		this.ClassId = classId;
		this.ObjectCount = objectCount;
		this.PropertyTypes = propertyTypes;
		this.PropertyIndexes = propertyIndexes;
		this.Altered = altered;
	}
}
