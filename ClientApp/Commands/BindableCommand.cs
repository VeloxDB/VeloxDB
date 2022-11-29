using System;
using Velox.Client;
using Velox.ClientApp.Modes;
using Velox.Common;
using Velox.Config;
using Velox.Networking;
using Velox.Protocol;
using Velox.Server;

namespace Velox.ClientApp.Commands;

internal abstract class BindableCommand : Command
{
	[Param("bind", "Addresses of one or more nodes in a cluster. It should be specified in the format adress:[port]. " +
		"If port is ommited, default port is used. If neither this parameter nor the --fbind parameter is provided, bind is attempted agains " +
		"a node on the local machine on the default port.", ShortName = "n", ProgramMode = ProgramMode.Direct)]
	public string[] NodeEndpoints { get; set; }

	[Param("fbind", "Path to a file containing cluster configuration.", ProgramMode = ProgramMode.Direct)]
	public string BindFile{ get; set; }

	protected override bool OnPreExecute(Program program)
	{
		if (program.ProgramMode == ProgramMode.Interactive)
			return true;

		return TryBind(program);
	}

	private bool TryBind(Program program)
	{
		if (BindFile != null)
		{
			return BindFromFile(program, BindFile);
		}
		else
		{
			return BindFromNode(program, NodeEndpoints);
		}
	}

	public static bool BindFromFile(Program program, string fileName)
	{
		var errors = ClusterConfiguration.TryLoad(fileName, out ClusterConfiguration clusterConfig);
		if (errors.Count > 0)
		{
			ConsoleHelper.ShowErrors(errors);
			return false;
		}

		InitialMode mode = (InitialMode)program.Mode;
		mode.SetClusterConfiguration(fileName, clusterConfig);
		return true;
	}

	public static bool BindFromNode(Program program, string[] nodeEndpoints)
	{
		string[] addresses;
		if (nodeEndpoints == null)
		{
			addresses = new string[] { "localhost:" + ClusterConfiguration.DefaultAdministrationPort.ToString() };
		}
		else
		{
			addresses = new string[nodeEndpoints.Length];
			for (int i = 0; i < nodeEndpoints.Length; i++)
			{
				if (!nodeEndpoints[i].Contains(":"))
					addresses[i] = nodeEndpoints[i] + ":" + ClusterConfiguration.DefaultAdministrationPort.ToString();
				else
					addresses[i] = nodeEndpoints[i];
			}
		}

		ConnectionStringParams cp = new ConnectionStringParams();
		addresses.ForEach(x => cp.AddAddress(x));
		cp.ServiceName = AdminAPIServiceNames.NodeAdministration;
		cp.RetryTimeout = Program.ConnectionRetryTimeout;
		cp.OpenTimeout = Program.ConnectionOpenTimeout;

		FileData fileData = null;

		INodeAdministration nodeAdministration = ConnectionFactory.Get<INodeAdministration>(cp.GenerateConnectionString());
		try
		{
			try
			{
				fileData = nodeAdministration.GetClusterConfigFile().Result;
			}
			catch (AggregateException e)
			{
				throw e.InnerException;
			}
		}
		catch (Exception e) when (e is CommunicationException || e is ObjectDisposedException || e is ArgumentException ||
			e is TimeoutException || e is DbAPIErrorException)
		{
			ConsoleHelper.ShowError("Failed to bind to cluster.", e);
			return false;
		}

		using MemoryStream ms = new MemoryStream(fileData.Data);
		var errors = ClusterConfiguration.TryLoad(ms, out ClusterConfiguration clusterConfig);
		if (errors.Count > 0)
		{
			ConsoleHelper.ShowErrors(errors);
			return false;
		}

		InitialMode mode = (InitialMode)program.Mode;
		mode.SetClusterConfiguration(null, clusterConfig);
		return true;
	}
}
