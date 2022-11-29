using System;
using Velox.ClientApp.Modes;

namespace Velox.ClientApp.Commands;

[Command("cluster-config", "Enters the cluster configuration editor mode. This mode is used to create or modify cluster configuration.")]
internal sealed class EnterClusterConfigModeCommand : Command
{
	public override bool IsModeValid(Mode mode)
	{
		return mode is InitialMode;
	}

	protected override bool OnExecute(Program program)
	{
		program.EnterMode(new ClusterConfigMode((InitialMode)program.Mode));
		return true;
	}
}
