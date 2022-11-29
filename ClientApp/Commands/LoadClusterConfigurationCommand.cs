using System;
using Velox.ClientApp.Modes;

namespace Velox.ClientApp.Commands;

[Command("load", "Loads the cluster configuration for editing. The configuration is either loaded from the file, " +
	"or from the current bound configuration if no file is provided.")]
internal sealed class LoadClusterConfigurationCommand : Command
{
	[Param("file", "Cluster configuration file to load.", ShortName = "f")]
	public string FileName { get; set; }

	public override bool IsModeValid(Mode mode)
	{
		return mode is ClusterConfigMode;
	}

	protected override bool OnExecute(Program program)
	{
		ClusterConfigMode mode = (ClusterConfigMode)program.Mode;
		if (FileName != null)
		{
			return mode.LoadFromFile(FileName);
		}
		else
		{
			InitialMode initMode = (InitialMode)mode.Parent;
			if (initMode.ClusterConfig == null)
			{
				ConsoleHelper.ShowError("Cluster binding has not been established.");
				return false;
			}

			mode.SetConfiguration(initMode.ClusterConfig);
			return true;
		}
	}
}
