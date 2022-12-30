using System;
using VeloxDB.Client;
using VeloxDB.ClientApp.Modes;
using VeloxDB.Networking;
using VeloxDB.Protocol;
using VeloxDB.Server;

namespace VeloxDB.ClientApp.Commands;

[Command("update-assemblies", "Updates user assemblies from the given directory.", ProgramMode = ProgramMode.Both)]
internal sealed class UpdateUserAssembliesCommand : BindableCommand
{
	[Param("dir", "Path to a directory from where to update assemblies. This command replaces user assemblies in the database with the " +
		"assemblies from the given directory.", ShortName = "d", IsMandatory = true)]
	public string Directory { get; set; }

	[Param("no-confirm", "When specified (without value) executes the command without asking the user for the confirmation.",
		ProgramMode = ProgramMode.Direct)]
	public bool NoConfirmation { get; set; }

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

		List<string> errors = new List<string>();
		AssemblyUpdate update = AssemblyUpdate.CreateUpdate(assemblyState, Directory, errors);

		if (errors.Count > 0)
		{
			errors.ForEach(x => ConsoleHelper.ShowError(x));
			return false;
		}

		ShowUpdate(assemblyState, update);
		if (program.ProgramMode == ProgramMode.Interactive || !NoConfirmation)
		{
			if (!ReadLine.IsRedirectedOrAlternate && !ConsoleHelper.Confirmation("Do you want to proceed (Y/N)?"))
				return true;
		}

		try
		{
			try
			{
				databaseAdministration.UpdateUserAssemblies(update, assemblyState.AssemblyVersionGuid).Wait();
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

		return true;
	}

	private void ShowUpdate(UserAssembliesState state, AssemblyUpdate update)
	{
		if (update.Inserted.Count == 0 && update.Updated.Count == 0 && update.Deleted.Count == 0)
		{
			Console.WriteLine("No modifications were detected.");
			return;
		}

		Table table = new Table(new Table.ColumnDesc[] {
			new Table.ColumnDesc(), new Table.ColumnDesc()
		});

		for (int i = 0; i < update.Inserted.Count; i++)
		{
			table.AddRow(new RichTextItem[] {
				new RichTextItem(new TextItem() { Text = update.Inserted[i].Name, Color = Colors.AssemblyInsertedColor }),
				new RichTextItem("Inserted")
			});
		}

		for (int i = 0; i < update.Updated.Count; i++)
		{
			table.AddRow(new RichTextItem[] {
				new RichTextItem(new TextItem() { Text = update.Updated[i].Name, Color = Colors.AssemblyUpdatedColor}),
				new RichTextItem("Updated")
			});
		}

		for (int i = 0; i < update.Deleted.Count; i++)
		{
			string name = state.Assemblies.Find(x => x.Id == update.Deleted[i]).Name;
			table.AddRow(new RichTextItem[] {
				new RichTextItem(new TextItem() { Text = name, Color = Colors.AssemblyDeletedColor}),
				new RichTextItem("Deleted")
			});
		}

		table.Show();
	}
}
