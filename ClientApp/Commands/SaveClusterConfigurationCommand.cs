using System;
using Velox.ClientApp.Modes;

namespace Velox.ClientApp.Commands;

[Command("save", "Saves the cluster configuration to a file.", DirectModeName = "save-cluster", ProgramMode = ProgramMode.Both)]
internal sealed class SaveClusterConfigurationCommand : BindableCommand
{
	[Param("file", "File name where to save the cluster configuration.", ShortName = "f")]
	public string FileName { get; set; }

	public override bool IsModeValid(Mode mode)
	{
		return mode is ClusterConfigMode;
	}

	protected override bool OnExecute(Program program)
	{
		if (!ClusterConfigMode.ProvideClusterConfigMode(program, out ClusterConfigMode mode))
			return false;

		return mode.SaveToFile(FileName);
	}
}
