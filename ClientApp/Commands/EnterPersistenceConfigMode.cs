using System;
using VeloxDB.ClientApp.Modes;

namespace VeloxDB.ClientApp.Commands;

[Command("persist-config", "Enters the persistence configuration editor mode. This mode is used to create or modify persistence configuration.")]
internal sealed class EnterPersistenceConfigMode : Command
{
	public override bool IsModeValid(Mode mode)
	{
		return mode is InitialMode;
	}

	protected override bool OnExecute(Program program)
	{
		if (!CheckClusterBinding(program))
			return false;

		PersistenceConfigMode mode = new PersistenceConfigMode((InitialMode)program.Mode);
		if (!mode.PullFromDatabase())
			return false;

		program.EnterMode(mode);
		return true;
	}
}
