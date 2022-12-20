using System;
using VeloxDB.ClientApp.Modes;

namespace VeloxDB.ClientApp.Commands;

[Command("save", "Saves the persistence configuration to a file.", DirectModeName = "save-persistence", ProgramMode = ProgramMode.Both)]
internal class SavePersistenceConfigurationCommand : BindableCommand
{
	[Param("file", "File name where to save the persistence configuration.", ShortName = "f", IsMandatory = true)]
	public string FileName { get; set; }

	public override bool IsModeValid(Mode mode)
	{
		return mode is PersistenceConfigMode;
	}

	protected override bool OnExecute(Program program)
	{
		if (!PersistenceConfigMode.ProvidePersistenceConfigMode(program, out PersistenceConfigMode mode))
			return false;

		return mode.SaveToFile(FileName);
	}
}
