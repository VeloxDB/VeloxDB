using System;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Velox.ClientApp.Modes;
using Velox.Server;

namespace Velox.ClientApp.Commands;

[Command("create", "Creates a new log in the persistence configuration. The first created log is considered to be main log.",
	DirectModeName = "create-log", ProgramMode = ProgramMode.Both)]
internal sealed class CreateLogCommand : BindableCommand
{
	public const int MaxPathLength = 200;

	public static readonly Regex LogNameRegex = new Regex("^[a-z,A-Z,0-9,\\., ,_,\\-,:]+$", RegexOptions.IgnoreCase);
	public static readonly Regex DirnameRegex = new Regex("^([a-z,A-Z,0-9,\\., ,_,\\-,/,\\\\,:]+|\\$\\{NodeName\\})*$", RegexOptions.IgnoreCase);

	const int maxLogCount = 8;

	[Param("name", "Name of the log.", ShortName = "n", IsMandatory = true)]
	public string Name { get; set; }

	[Param("packed", "Specifies whether the log uses packed format or not. Default is false.", ShortName = "p")]
	public bool IsPackedFormat { get; set; } = false;

	[Param("dir", "Path to a directory where database will store the log files. This path needs to be avaiable to each node in the cluster.",
		ShortName = "d", IsMandatory = true)]
	public string Directory { get; set; }

	[Param("snapshot-dir", "Path to a directory where database will store the snapshot files. This path needs to be avaiable " +
		"to each node in the cluster.", ShortName = "sd", IsMandatory = true)]
	public string SnapshotDirectory { get; set; }

	[Param("size", "Size of the log file in MB.", ShortName = "s", IsMandatory = true)]
	public long Size { get; set; }

	public override bool IsModeValid(Mode mode)
	{
		return mode is PersistenceConfigMode;
	}

	protected override bool OnExecute(Program program)
	{
		if (!PersistenceConfigMode.ProvidePersistenceConfigMode(program, out PersistenceConfigMode mode))
			return false;

		if (mode.PersistenceConfig.LogDescriptors.Count == maxLogCount)
		{
			ConsoleHelper.ShowError("Maximum allowed number of logs exceeded.");
			return false;
		}

		if (!LogNameRegex.Match(Name).Success)
		{
			ConsoleHelper.ShowError("Log name contains illegal characters.");
			return false;
		}

		if (!DirnameRegex.Match(Directory).Success || Directory.Length > MaxPathLength - Name.Length)
		{
			ConsoleHelper.ShowError("Log directory path is invalid.");
			return false;
		}

		if (!DirnameRegex.Match(SnapshotDirectory).Success || SnapshotDirectory.Length > MaxPathLength - -Name.Length)
		{
			ConsoleHelper.ShowError("Snapshot directory path is invalid.");
			return false;
		}

		if (Size <= 0)
		{
			ConsoleHelper.ShowError("Invalid log size.");
			return false;
		}

		LogDescriptor logDescriptor = new LogDescriptor(Name, IsPackedFormat, Directory, SnapshotDirectory, Size * 1024 * 1024);
		mode.PersistenceConfig.LogDescriptors.Add(logDescriptor);
		mode.PersistenceModified();

		if (program.ProgramMode == ProgramMode.Direct)
			mode.ApplyToDatabase();

		return true;
	}
}
