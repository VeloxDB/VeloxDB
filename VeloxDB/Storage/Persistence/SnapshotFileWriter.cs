using System;
using System.IO;
using System.Runtime.InteropServices;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.Storage.Persistence;

internal unsafe sealed class SnapshotFileWriter
{
	const int segmentSize = 1024 * 1024 * 16;

	// 90% of segment size so that we rarely need more than one segment to create a block
	public const int BlockSize = (int)(segmentSize * 9 / 10);

	StorageEngine engine;
	Database database;

	int logIndex;

	Transaction tran;

	ulong lastLogSeqNum;
	GlobalVersion[] globalVersions;
	uint lastLocalTerm;

	public SnapshotFileWriter(Database database, Transaction tran, DatabaseVersions versions, int logIndex)
	{
		TTTrace.Write(database.TraceId, database.Id, logIndex);
		versions.TTTraceState();

		this.engine = database.Engine;
		this.database = database;
		this.tran = tran;
		this.logIndex = logIndex;

		this.lastLogSeqNum = versions.LogSeqNum;
		globalVersions = versions.UnpackClusterVersions(out lastLocalTerm);
	}

	public static void CreateEmpty(Database database, int logIndex, string fileName)
	{
		TTTrace.Write(database.TraceId, database.Id, logIndex, fileName);

		string dir = Path.GetDirectoryName(fileName);
		if (!Directory.Exists(dir))
			Directory.CreateDirectory(dir);

		if (File.Exists(fileName))
			File.Delete(fileName);

		using (NativeFile file = NativeFile.Create(fileName, FileMode.Create, FileAccess.Write, FileShare.None, FileFlags.Sequential))
		{
			WriteHeader(file, database, logIndex, 0);
			file.Flush();
		}
	}

	public void CreateSnapshot(string fileName)
	{
		TTTrace.Write(database.TraceId, database.Id, fileName);

		using (NativeFile file = NativeFile.Create(fileName, FileMode.Create, FileAccess.Write, FileShare.None, FileFlags.Sequential))
		{
			WriteHeader(file, database, logIndex, lastLogSeqNum);
			WriteSnapshots(file);
			file.Flush();
		}
	}

	private static void WriteHeader(NativeFile file, Database database, int logIndex, ulong logSeqNum)
	{
		using (SegmentBinaryWriter writer = new SegmentBinaryWriter(segmentSize))
		{
			SnapshotFileHeader head = new SnapshotFileHeader();
			head.version = DatabasePersister.FormatVersion;
			writer.Write((byte*)&head, SnapshotFileHeader.Size);

			GlobalVersion[] globalVersions = database.GetGlobalVersions(out uint localTerm);

			SerializeVersions(writer, globalVersions, localTerm, logSeqNum);
			SerializeClassList(writer, database, logIndex);

			writer.WriteToFile(file);
		}
	}

	private static void SerializeVersions(SegmentBinaryWriter writer, GlobalVersion[] globalVersions, uint localTerm, ulong maxLogSeqNum)
	{
		TTTrace.Write(localTerm, maxLogSeqNum);

		SegmentBinaryWriter.Position pos = writer.ReserveSpace(sizeof(long));
		long size = writer.Size;

		writer.Write((int)localTerm);
		writer.Write((long)maxLogSeqNum);

		writer.Write(globalVersions.Length);
		for (int i = 0; i < globalVersions.Length; i++)
		{
			TTTrace.Write(globalVersions[i].GlobalTerm.Low, globalVersions[i].GlobalTerm.Hight, globalVersions[i].Version);

			writer.Write(globalVersions[i].GlobalTerm.Low);
			writer.Write(globalVersions[i].GlobalTerm.Hight);
			writer.Write((long)globalVersions[i].Version);
		}

		size = writer.Size - size;
		writer.Write(pos, (byte*)(&size), sizeof(long));
	}

	private static void SerializeClassList(SegmentBinaryWriter writer, Database database, int logIndex)
	{
		DataModelDescriptor modelDesc = database.ModelDesc;
		int count = 0;
		for (int i = 0; i < modelDesc.ClassCount; i++)
		{
			ClassDescriptor cd = modelDesc.GetClassByIndex(i);
			if (!cd.IsAbstract && cd.LogIndex == logIndex)
				count++;
		}

		SegmentBinaryWriter.Position pos = writer.ReserveSpace(sizeof(long));
		long size = writer.Size;

		writer.Write(count);
		for (int i = 0; i < modelDesc.ClassCount; i++)
		{
			ClassDescriptor cd = modelDesc.GetClassByIndex(i);
			TTTrace.Write(database.TraceId, cd.Id, logIndex, cd.LogIndex, cd.IsAbstract);

			if (cd.IsAbstract || cd.LogIndex != logIndex)
				continue;

			Class @class = database.GetClass(cd.Index).MainClass;
			ClassSnapshotFileHeader classHeader = new ClassSnapshotFileHeader()
			{
				classId = cd.Id,
				objectCount = @class.EstimatedObjectCount,
				propertyCount = (short)(cd.Properties.Length - 2)
			};

			writer.Write((byte*)&classHeader, ClassSnapshotFileHeader.Size);

			for (int j = 2; j < cd.Properties.Length; j++)  // Skip version and id
			{
				Descriptor.PropertyDescriptor pd = cd.Properties[j];
				TTTrace.Write(database.TraceId, cd.Id, pd.Id, (byte)pd.PropertyType);
				writer.Write(pd.Id);
				writer.Write((byte)pd.PropertyType);
			}
		}

		size = writer.Size - size;
		writer.Write(pos, (byte*)(&size), sizeof(long));
	}

	private void WriteSnapshots(NativeFile file)
	{
		JobWorkers<SegmentBinaryWriter> persistJobs;

		using (tran)
		{
			int workerCount = ProcessorNumber.CoreCount;
			if (!database.Engine.Settings.AllowInternalParallelization)
				workerCount = 1;

			string snapshotWorkerName = string.Format("{0}: vlx-SnapshotCreator", database.Engine.Trace.Name);
			string persistWorkerName = string.Format("{0}: vlx-SnapshotPersister", database.Engine.Trace.Name);

			int maxPersistItems = (int)Math.Max(1, Math.Min(int.MaxValue, database.Engine.Settings.SnapshotMaxMemorySize / BlockSize));

			persistJobs = JobWorkers<SegmentBinaryWriter>.Create(persistWorkerName, 1, x => PersistWorker(file, x), maxPersistItems);

			Action<ClassScan>[] actions = new Action<ClassScan>[workerCount];
			for (int i = 0; i < workerCount; i++)
			{
				SnapshotWorkerParam param = new SnapshotWorkerParam() { PersistJobs = persistJobs, Objects = new ObjectReader[64] };
				actions[i] = item => SnapshotWorker(param, item);
			}

			JobWorkers<ClassScan> snapshotJobs = JobWorkers<ClassScan>.Create(snapshotWorkerName, workerCount, actions);

			foreach (ClassDescriptor cd in database.ModelDesc.GetAllClasses())
			{
				if (!cd.IsAbstract && cd.LogIndex == logIndex)
				{
					ClassBase @class = database.GetClass(cd.Index);
					ClassScan[] scans = @class.GetClassScans(tran, false, out long totalCount);
					TTTrace.Write(database.TraceId, cd.Id, scans.Length, totalCount, @class.MainClass.EstimatedObjectCount);
					for (int i = 0; i < scans.Length; i++)
					{
						snapshotJobs.EnqueueWork(scans[i]);
					}
				}
			}

			snapshotJobs.WaitAndClose();
		}

		persistJobs.WaitAndClose();
	}

	private void PersistWorker(NativeFile file, SegmentBinaryWriter persistItem)
	{
		using (persistItem)
		{
			persistItem.WriteToFile(file);
		}
	}

	private void SnapshotWorker(SnapshotWorkerParam param, ClassScan scan)
	{
		ObjectReader[] objects = param.Objects;
		ClassDescriptor cd = scan.Class.ClassDesc;
		Class @class = scan.Class;

		SegmentBinaryWriter writer = new SegmentBinaryWriter(segmentSize);
		SegmentBinaryWriter.Position sizePos = writer.ReserveSpace(sizeof(long));
		long size = writer.Size;
		ClassSnapshotBlockFileHeader head = new ClassSnapshotBlockFileHeader(cd.Id);
		SegmentBinaryWriter.Position headPos = writer.ReserveSpace(ClassSnapshotBlockFileHeader.Size);

		TTTrace.Write(database.TraceId, cd.Id, @class.EstimatedObjectCount);

		int count = objects.Length;
		while (scan.Next(objects, 0, ref count))
		{
			TTTrace.Write(database.TraceId, cd.Id, count, @class.EstimatedObjectCount);
			for (int i = 0; i < count; i++)
			{
				TTTrace.Write(database.TraceId, cd.Id, writer.Size, BlockSize, objects[i].ClassObject->id);

				if (writer.Size >= BlockSize)
				{
					writer.Write(headPos, (byte*)&head, ClassSnapshotBlockFileHeader.Size);
					size = writer.Size - size;
					writer.Write(sizePos, (byte*)&size, sizeof(long));
					param.PersistJobs.EnqueueWork(writer);

					writer = new SegmentBinaryWriter(segmentSize);
					sizePos = writer.ReserveSpace(sizeof(long));
					size = writer.Size;
					head = new ClassSnapshotBlockFileHeader(cd.Id);
					headPos = writer.ReserveSpace(ClassSnapshotBlockFileHeader.Size);
				}

				@class.CreateObjectSnapshot(writer, objects[i].ClassObject);
				head.objectCount++;
			}

			count = objects.Length;
		}

		TTTrace.Write(database.TraceId, cd.Id, head.objectCount);

		writer.Write(headPos, (byte*)&head, ClassSnapshotBlockFileHeader.Size);
		size = writer.Size - size;
		writer.Write(sizePos, (byte*)&size, sizeof(long));

		param.PersistJobs.EnqueueWork(writer);
	}

	private class SnapshotWorkerParam
	{
		public JobWorkers<SegmentBinaryWriter> PersistJobs { get; set; }
		public ObjectReader[] Objects { get; set; }
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal unsafe struct SnapshotFileHeader
{
	public static readonly int Size = sizeof(SnapshotFileHeader);

	public short version;
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal unsafe struct ClassSnapshotFileHeader
{
	public static readonly int Size = sizeof(ClassSnapshotFileHeader);

	public short classId;
	public long objectCount;

	public short propertyCount;     // Number of properties in the list that follows
									// Here comes a list of ids of snapshoted properties (without Id and Version)
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal unsafe struct ClassSnapshotBlockFileHeader
{
	public static readonly int Size = sizeof(ClassSnapshotBlockFileHeader);

	public short classId;
	public long objectCount;

	public ClassSnapshotBlockFileHeader(short classId)
	{
		this.classId = classId;
		objectCount = 0;
	}
}
