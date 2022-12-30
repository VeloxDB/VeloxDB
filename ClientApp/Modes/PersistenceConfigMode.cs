using System;
using System.Text.Json.Serialization;
using System.Text.Json;
using System.Xml.Linq;
using VeloxDB.Client;
using VeloxDB.Config;
using VeloxDB.Networking;
using VeloxDB.Protocol;
using VeloxDB.Server;
using System.IO;
using VeloxDB.Common;

namespace VeloxDB.ClientApp.Modes;

internal sealed class PersistenceConfigMode : Mode
{
	static readonly JsonSerializerOptions SerializerOptions = new JsonSerializerOptions()
	{
		WriteIndented = true,
		AllowTrailingCommas = true,
		PropertyNameCaseInsensitive = true,
		ReadCommentHandling = JsonCommentHandling.Skip,
		Converters ={
			new JsonStringEnumConverter(),
		}
	};

	InitialMode initialMode;

	bool hasUnsavedChanges;
	PersistenceDescriptor persistenceConfig;

	public PersistenceConfigMode(InitialMode initialMode)
	{
		this.initialMode = initialMode;
	}

	public override string Title => "persistence";
	public override Mode Parent => initialMode;

	public PersistenceDescriptor PersistenceConfig => persistenceConfig;
	public bool HasUnsavedChanges => hasUnsavedChanges;

	public override bool Confirmation()
	{
		if (hasUnsavedChanges && !ConsoleHelper.Confirmation("All modifications to the current persistence configuration will be lost. Conitnue (Y/N)?"))
			return false;

		return true;
	}

	public bool SaveToFile(string fileName)
	{
		try
		{
			File.WriteAllText(fileName, AsJson());
			hasUnsavedChanges = false;
		}
		catch (Exception e)
		{
			ConsoleHelper.ShowError(e.Message);
			return false;
		}

		return true;
	}

	public bool LoadFromFile(string fileName)
	{
		if (!Confirmation())
			return false;

		try
		{
			using FileStream fileStream = File.OpenRead(fileName);
			persistenceConfig = FromJson(fileStream);
			hasUnsavedChanges = false;
		}
		catch (Exception e)
		{
			ConsoleHelper.ShowError(e.Message);
			return false;
		}

		return true;
	}

	public bool PullFromDatabase()
	{
		IDatabaseAdministration databaseAdministration = ConnectionFactory.Get<IDatabaseAdministration>(initialMode.GetDatabaseConnectionString());
		try
		{
			try
			{
				persistenceConfig = databaseAdministration.GetPersistenceConfiguration().GetResult();
				if (persistenceConfig == null)
					persistenceConfig = new PersistenceDescriptor(new List<LogDescriptor>());
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

		hasUnsavedChanges = false;
		return true;
	}

	public bool ApplyToDatabase()
	{
		IDatabaseAdministration databaseAdministration = ConnectionFactory.Get<IDatabaseAdministration>(initialMode.GetDatabaseConnectionString());
		try
		{
			try
			{
				databaseAdministration.UpdatePersistenceConfiguration(persistenceConfig).Wait();
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

		hasUnsavedChanges = false;
		return true;
	}

	public void PersistenceModified()
	{
		hasUnsavedChanges = true;
	}

	private string AsJson()
	{
		return JsonSerializer.Serialize(persistenceConfig, SerializerOptions);
	}

	private PersistenceDescriptor FromJson(Stream stream)
	{
		return JsonSerializer.Deserialize<PersistenceDescriptor>(stream, SerializerOptions);
	}

	public static bool ProvidePersistenceConfigMode(Program program, string fileName, out PersistenceConfigMode mode)
	{
		if (program.ProgramMode == ProgramMode.Direct)
		{
			mode = new PersistenceConfigMode((InitialMode)program.Mode);
			program.EnterMode(mode);
			if (!mode.LoadFromFile(fileName))
				return false;
		}
		else
		{
			mode = (PersistenceConfigMode)program.Mode;
		}

		return true;
	}

	public static bool ProvidePersistenceConfigMode(Program program, out PersistenceConfigMode mode)
	{
		if (program.ProgramMode == ProgramMode.Direct)
		{
			mode = new PersistenceConfigMode((InitialMode)program.Mode);
			program.EnterMode(mode);
			if (!mode.PullFromDatabase())
				return false;
		}
		else
		{
			mode = (PersistenceConfigMode)program.Mode;
		}

		return true;
	}
}
