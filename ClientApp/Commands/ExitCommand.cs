using System;

namespace Velox.ClientApp.Commands;

[Command("exit", "Exits the current mode, or exits the program if no mode is active.")]
internal sealed class ExitCommand : Command
{
	public override bool IsModeValid(Mode mode)
	{
		return true;
	}

	protected override bool OnExecute(Program program)
	{
		Mode mode = program.Mode;
		if (!mode.Confirmation())
			return true;

		program.ExitMode();
		return true;
	}
}
