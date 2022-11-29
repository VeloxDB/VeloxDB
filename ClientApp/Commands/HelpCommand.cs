using System;

namespace Velox.ClientApp.Commands;

[Command("help", "Displays the help section.", ProgramMode = ProgramMode.Both)]
internal sealed class HelpCommand : Command
{
	public override bool IsModeValid(Mode mode)
	{
		return true;
	}

	protected override bool OnExecute(Program program)
	{
		program.ShowHelp();
		return true;
	}
}
