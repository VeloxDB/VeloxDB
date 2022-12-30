using System;
using VeloxDB.Config;

namespace VeloxDB.ClientApp.Modes;

internal sealed class ClusterConfigMode : Mode
{
	InitialMode initialMode;

	bool hasUnsavedChanges;
	ClusterConfiguration clusterConfig;

	public ClusterConfigMode(InitialMode initialMode)
	{
		this.initialMode = initialMode;
		clusterConfig = initialMode.ClusterConfig == null ? ClusterConfiguration.Create() : CloneClusterConfig(initialMode.ClusterConfig);
	}

	public override string Title => "cluster";
	public override Mode Parent => initialMode;

	public ClusterConfiguration ClusterConfig => clusterConfig;
	public bool HasUnsavedChanges => hasUnsavedChanges;

	public override bool Confirmation()
	{
		if (hasUnsavedChanges && !ConsoleHelper.Confirmation("All unsaved changed to the currently edited cluster configuration will be lost. Conitnue (Y/N)?"))
			return false;

		return true;
	}

	public void CreateNew()
	{
		if (!Confirmation())
			return;

		clusterConfig = ClusterConfiguration.Create();
		hasUnsavedChanges = false;
	}

	public void SetConfiguration(ClusterConfiguration clusterConfig)
	{
		hasUnsavedChanges = false;
		clusterConfig = CloneClusterConfig(clusterConfig);
	}

	public bool LoadFromFile(string fileName)
	{
		if (!Confirmation())
			return false;

		try
		{
			var errors = ClusterConfiguration.TryLoad(fileName, out ClusterConfiguration temp);
			if (errors.Count > 0)
			{
				ConsoleHelper.ShowErrors(errors);
				return false;
			}

			clusterConfig = temp;
			hasUnsavedChanges = false;
		}
		catch (Exception e)
		{
			ConsoleHelper.ShowError(e.Message);
			return false;
		}

		return true;
	}

	public bool SaveToFile(string fileName)
	{
		if (fileName == null)
		{
			fileName = initialMode.ClusterConfigFileName;
			if (fileName == null)
			{
				Console.WriteLine("Cluster configureation file has not been specified.");
				return false;
			}
		}

		if (clusterConfig.Cluster == null)
		{
			Console.WriteLine("Current cluster configuration is empty.");
			return false;
		}

		var errors = clusterConfig.Validate();
		if (errors.Count > 0)
		{
			Console.ForegroundColor = Colors.StatusWarnColor;
			Console.WriteLine("Following errors were detected in the cluster configuration:");
			foreach (var error in errors)
			{
				Console.WriteLine(error);
			}

			Console.ResetColor();
			if (!ConsoleHelper.Confirmation("Do you want to conitinue with the save operation (Y/N)?"))
				return false;
		}

		try
		{
			File.WriteAllText(fileName, clusterConfig.AsJson());
			hasUnsavedChanges = false;
		}
		catch (Exception e)
		{
			ConsoleHelper.ShowError(e.Message);
			return false;
		}

		return true;
	}

	public void ClusterModified()
	{
		hasUnsavedChanges = true;
		clusterConfig = CloneClusterConfig(clusterConfig);	// So that the name map is updated
	}

	private static ClusterConfiguration CloneClusterConfig(ClusterConfiguration clusterConfig)
	{
		ClusterConfiguration.TryLoadFromString(clusterConfig.AsJson(), out ClusterConfiguration temp);
		return temp;
	}

	public static bool ProvideClusterConfigMode(Program program, out ClusterConfigMode mode)
	{
		if (program.ProgramMode == ProgramMode.Direct)
		{
			mode = new ClusterConfigMode((InitialMode)program.Mode);
			program.EnterMode(mode);
		}
		else
		{
			mode = (ClusterConfigMode)program.Mode;
		}

		return true;
	}
}
