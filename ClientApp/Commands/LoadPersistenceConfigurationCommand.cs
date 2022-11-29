using System;
using Velox.ClientApp.Modes;

namespace Velox.ClientApp.Commands;

[Command("load", "Loads the persistence configuration for editing.")]
internal sealed class LoadPersistenceConfigurationCommand : Command
{
	[Param("file", "Persistence configuration file to load.", ShortName = "f")]
	public string FileName { get; set; }

	public override bool IsModeValid(Mode mode)
	{
		return mode is PersistenceConfigMode;
	}

	protected override bool OnExecute(Program program)
	{
		PersistenceConfigMode mode = (PersistenceConfigMode)program.Mode;
		return mode.LoadFromFile(FileName);
	}
}
