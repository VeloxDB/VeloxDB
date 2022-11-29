using System;
using System.Text.RegularExpressions;
using Velox.ClientApp.Modes;
using Velox.Server;

namespace Velox.ClientApp.Commands;

[Command("modify", "Modifies a log in the persistence configuration.", DirectModeName = "modify-log", ProgramMode = ProgramMode.Both)]
internal sealed class ModifyLogCommand : BindableCommand
{
	[Param("name", "Name of the log.", ShortName = "n", IsMandatory = true)]
	public string Name { get; set; }

	[Param("packed", "Specifies whether the log uses packed format or not.", ShortName = "p")]
	public bool IsPackedFormat { get; set; } = false;

	[Param("dir", "Path to a directory where database will store the log files. This path needs to be avaiable to each node in the cluster.",
		ShortName = "d")]
	public string Directory { get; set; }

	[Param("snapshot-dir", "Path to a directory where database will store the snapshot files. This path needs to be avaiable " +
		"to each node in the cluster.", ShortName = "sd")]
	public string SnapshotDirectory { get; set; }

	[Param("size", "Size of the log file in MB.", ShortName = "s")]
	public long Size { get; set; } = -1;

	public override bool IsModeValid(Mode mode)
	{
		return mode is PersistenceConfigMode;
	}

	protected override bool OnExecute(Program program)
	{
		if (!PersistenceConfigMode.ProvidePersistenceConfigMode(program, out PersistenceConfigMode mode))
			return false;

		int index = mode.PersistenceConfig.LogDescriptors.FindIndex(x => x.Name == Name);
		if (index == -1)
		{
			ConsoleHelper.ShowError("Given log could not be found.");
			return false;
		}

		if (Name != null && !CreateLogCommand.LogNameRegex.Match(Name).Success)
		{
			ConsoleHelper.ShowError("Log name contains illegal characters.");
			return false;
		}

		if (Directory != null)
		{
			if (!CreateLogCommand.DirnameRegex.Match(Directory).Success ||
				Directory.Length > CreateLogCommand.MaxPathLength - Name.Length)
			{
				ConsoleHelper.ShowError("Log directory path is invalid.");
				return false;
			}
		}

		if (SnapshotDirectory != null)
		{
			if (!CreateLogCommand.DirnameRegex.Match(SnapshotDirectory).Success ||
				SnapshotDirectory.Length > CreateLogCommand.MaxPathLength - -Name.Length)
			{
				ConsoleHelper.ShowError("Snapshot directory path is invalid.");
				return false;
			}
		}

		if (Size == 0 || Size < -1)
		{
			ConsoleHelper.ShowError("Invalid log size.");
			return false;
		}

		LogDescriptor logDescriptor = mode.PersistenceConfig.LogDescriptors[index];
		if (IsPackedFormat != logDescriptor.IsPackedFormat)
			logDescriptor.IsPackedFormat = IsPackedFormat;

		if (Directory != null && !Directory.Equals(logDescriptor.Directory, StringComparison.Ordinal))
			logDescriptor.Directory = Directory;

		if (SnapshotDirectory != null && !SnapshotDirectory.Equals(logDescriptor.SnapshotDirectory, StringComparison.Ordinal))
			logDescriptor.SnapshotDirectory = SnapshotDirectory;

		if (Size != -1 && Size * 1024 * 1024 != logDescriptor.MaxSize)
			logDescriptor.MaxSize = Size * 1024 * 1024;

		mode.PersistenceModified();

		if (program.ProgramMode == ProgramMode.Direct)
			mode.ApplyToDatabase();

		return true;
	}
}
