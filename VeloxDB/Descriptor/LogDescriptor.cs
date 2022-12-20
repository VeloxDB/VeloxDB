using System;
using System.IO;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal sealed class LogDescriptor
{
	public const long SysLogDefaultMaxSize = 1024 * 1024 * 4;

	public const string NodeNameTemplate = "${NodeName}";
	public const string SysDirTemplate = "${SysDir}";

	public const string MasterLogName = "master";

	public const string TempSufix = "__temp";

	string name;
	string directory;
	string snapshotDirectory;
	long maxSize;
	bool isPackedFormat;

	private LogDescriptor()
	{
	}

	public LogDescriptor(string name, bool isPackedFormat, string directory, string snapshotDirectory, long maxSize)
	{
		this.name = name;
		this.isPackedFormat = isPackedFormat;
		this.directory = directory;
		this.snapshotDirectory = snapshotDirectory;
		this.maxSize = maxSize;
	}

	public string Name => name;
	public bool IsPackedFormat => isPackedFormat;
	public string Directory => directory;
	public string SnapshotDirectory => snapshotDirectory;
	public long MaxSize => maxSize;

	public LogDescriptor Clone()
	{
		return new LogDescriptor(Name, isPackedFormat, Directory, SnapshotDirectory, maxSize);
	}

	public string FinalDirectory(Storage.StorageEngine engine)
	{
		string dir = directory;

		if (dir.Contains(SysDirTemplate))
		{
			if (engine.SystemDbPath == null)
				throw new InvalidOperationException();

			dir = dir.Replace(SysDirTemplate, engine.SystemDbPath);
		}

		if (dir.Contains(NodeNameTemplate))
		{
			if (engine.ReplicationDesc.NodeName == null)
				throw new InvalidOperationException();

			dir = dir.Replace(NodeNameTemplate, engine.ReplicationDesc.NodeName);
		}

		return dir;
	}

	public string FinalSnapshotDirectory(Storage.StorageEngine engine)
	{
		string dir = snapshotDirectory;

		if (dir.Contains(SysDirTemplate))
		{
			if (engine.SystemDbPath == null)
				throw new InvalidOperationException();

			dir = dir.Replace(SysDirTemplate, engine.SystemDbPath);
		}

		if (dir.Contains(NodeNameTemplate))
		{
			if (engine.ReplicationDesc.NodeName == null)
				throw new InvalidOperationException();

			dir = dir.Replace(NodeNameTemplate, engine.ReplicationDesc.NodeName);
		}

		return dir;
	}

	public void Serialize(BinaryWriter writer)
	{
		writer.Write(isPackedFormat);
		writer.Write(name);
		writer.Write(directory);
		writer.Write(snapshotDirectory);
		writer.Write(maxSize);
	}

	public static LogDescriptor Deserialize(BinaryReader reader)
	{
		LogDescriptor d = new LogDescriptor();
		d.isPackedFormat = reader.ReadBoolean();
		d.name = reader.ReadString();
		d.directory = reader.ReadString();
		d.snapshotDirectory = reader.ReadString();
		d.maxSize = reader.ReadInt64();
		return d;
	}

	public void MarkDirectoriesAsTemp()
	{
		directory = Path.Combine(directory, TempSufix);
		snapshotDirectory = Path.Combine(snapshotDirectory, TempSufix);
	}

	public void UnmarkDirectoriesAsTemp()
	{
		Checker.AssertTrue(directory.EndsWith(TempSufix));
		Checker.AssertTrue(snapshotDirectory.EndsWith(TempSufix));

		directory = directory.Substring(0, directory.Length - TempSufix.Length - 1);
		snapshotDirectory = snapshotDirectory.Substring(0, snapshotDirectory.Length - TempSufix.Length - 1);
	}
}
