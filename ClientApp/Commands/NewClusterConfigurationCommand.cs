using System;
using VeloxDB.ClientApp.Modes;

namespace VeloxDB.ClientApp.Commands;

[Command("new", "Creates new cluster configuration.")]
internal sealed class NewClusterConfigurationCommand : Command
{
	public override bool IsModeValid(Mode mode)
	{
		return mode is ClusterConfigMode;
	}

	protected override bool OnExecute(Program program)
	{
		ClusterConfigMode mode = (ClusterConfigMode)program.Mode;
		mode.CreateNew();
		return true;
	}
}
