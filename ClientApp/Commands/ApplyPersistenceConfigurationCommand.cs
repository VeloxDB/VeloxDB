using System;
using Velox.ClientApp.Modes;

namespace Velox.ClientApp.Commands;

[Command("apply", "Applies the persistence configuration to the database.", DirectModeName = "apply-persistence", ProgramMode = ProgramMode.Both)]
internal sealed class ApplyPersistenceConfigurationCommand : BindableCommand
{
	[Param("file", "Name of the file containing persistence configuration that should be applied to the database.",
		ShortName = "f", ProgramMode = ProgramMode.Direct)]
	public string FileName { get; set; }

	public override bool IsModeValid(Mode mode)
	{
		return mode is PersistenceConfigMode;
	}

	protected override bool OnExecute(Program program)
	{
		if (!PersistenceConfigMode.ProvidePersistenceConfigMode(program, FileName, out PersistenceConfigMode mode))
			return false;

		return mode.ApplyToDatabase();
	}
}
