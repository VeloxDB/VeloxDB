using System;
using System.Linq;
using VeloxDB.ClientApp.Modes;
using VeloxDB.Server;

namespace VeloxDB.ClientApp.Commands;

[Command("delete", "Deletes a log from the persistence configuration. Main log cannot be deleted.",
	DirectModeName = "delete-log", ProgramMode = ProgramMode.Both)]
internal sealed class DeleteLogCommand : BindableCommand
{
	[Param("name", "Log name.", ShortName = "n", IsMandatory = true)]
	public string Name { get; set; }

	public override bool IsModeValid(Mode mode)
	{
		return mode is PersistenceConfigMode;
	}

	protected override bool OnExecute(Program program)
	{
		if (!PersistenceConfigMode.ProvidePersistenceConfigMode(program, out PersistenceConfigMode mode))
			return false;

		if (mode.PersistenceConfig.LogDescriptors.Count == 1)
		{
			ConsoleHelper.ShowError("Persisten descriptor has to have at least one log.");
			return false;
		}

		int index = mode.PersistenceConfig.LogDescriptors.FindIndex(x => x.Name == Name);
		if (index == -1)
		{
			ConsoleHelper.ShowError("Given log could not be found.");
			return false;
		}

		mode.PersistenceConfig.LogDescriptors.RemoveAt(index);
		mode.PersistenceModified();

		if (program.ProgramMode == ProgramMode.Direct)
			mode.ApplyToDatabase();

		return true;
	}
}
