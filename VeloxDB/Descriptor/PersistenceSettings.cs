using System;
using System.Collections.Generic;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal sealed class PersistenceSettings
{
	const short serializerVersion = 1;

	string systemDirectory;
	LogSettings mainLog;
	LogSettings[] secondaryLogs;

	public PersistenceSettings(LogSettings mainLog, LogSettings[] secondaryLogs)
	{
		this.mainLog = mainLog;
		this.secondaryLogs = secondaryLogs;
	}

	public PersistenceSettings(string systemDirectory, LogSettings mainLog, LogSettings[] secondaryLogs)
	{
		Checker.NotNull(systemDirectory, nameof(systemDirectory));
		Checker.NotNull(mainLog, nameof(mainLog));
		Checker.NotNull(secondaryLogs, nameof(secondaryLogs));

		if (secondaryLogs.Length > PersistenceDescriptor.MaxLogGroups - 1)
			throw new ArgumentException("Maximum number of secondary logs exceeded.");

		this.systemDirectory = systemDirectory;
		this.mainLog = mainLog;
		this.secondaryLogs = secondaryLogs;
	}

	public LogSettings MainLog => mainLog;
	public LogSettings[] SecondaryLogs => secondaryLogs;
	public string SystemDirectory => systemDirectory;
}

internal sealed class LogSettings
{
	string name;
	bool isPackedFormat;
	short[] classIds;
	long maxSize;
	string directory;
	string snapshotDirectory;

	public LogSettings(string name, bool isPackedFormat, string snapshotDirectory, string directory, short[] classIds, long maxSize)
	{
		this.name = name;
		this.isPackedFormat = isPackedFormat;
		this.directory = directory;
		this.classIds = classIds;
		this.maxSize = maxSize;
		this.snapshotDirectory = snapshotDirectory;
	}

	public string Name => name;
	public bool IsPackedFormat => isPackedFormat;
	public string Directory => directory;
	public string SnapshotDirectory => snapshotDirectory;
	public short[] Classes => classIds;
	public long MaxSize => maxSize;
}
