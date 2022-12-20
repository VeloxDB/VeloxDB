using System;
using VeloxDB.Client;
using VeloxDB.ClientApp.Modes;
using VeloxDB.Networking;
using VeloxDB.Protocol;
using VeloxDB.Server;

namespace VeloxDB.ClientApp.Commands;

[Command("get-assemblies", "Retreives user assemblies from the database and stores them at the given directory.", ProgramMode = ProgramMode.Both)]
internal sealed class DownloadUserAssembliesCommand : BindableCommand
{
	[Param("dir", "Path to a directory where to store user assemblies.", ShortName = "d", IsMandatory = true)]
	public string Directory { get; set; }

	public override bool IsModeValid(Mode mode)
	{
		return mode is InitialMode;
	}

	protected override bool OnExecute(Program program)
	{
		if (!CheckClusterBinding(program))
			return false;

		try
		{
			if (!System.IO.Directory.Exists(Directory))
			{
				ConsoleHelper.ShowError("Directory does not exist.");
				return false;
			}
		}
		catch (Exception e)
		{
			ConsoleHelper.ShowError("Failed to access directory.", e);
			return false;
		}

		InitialMode mode = (InitialMode)program.Mode;
		UserAssembliesState assemblyState = null;

		IDatabaseAdministration databaseAdministration = ConnectionFactory.Get<IDatabaseAdministration>(mode.GetDatabaseConnectionString());
		try
		{
			try
			{
				assemblyState = databaseAdministration.GetAssemblyState(false).GetResult();
			}
			catch (AggregateException e)
			{
				throw e.InnerException;
			}
		}
		catch (Exception e) when (e is CommunicationException || e is ObjectDisposedException || e is ArgumentException ||
			e is TimeoutException || e is DbAPIErrorException || e is DatabaseException)
		{
			ConsoleHelper.ShowError(e.Message);
			return false;
		}

		try
		{
			foreach (UserAssembly assembly in assemblyState.Assemblies)
			{
				Console.WriteLine("Writing {0}...", assembly.Name);
				string fileName = Path.Combine(Directory, assembly.Name);
				File.WriteAllBytes(fileName, assembly.Binary);
			}
		}
		catch (Exception e)
		{
			ConsoleHelper.ShowError(null, e);
			return false;
		}

		return true;
	}
}
