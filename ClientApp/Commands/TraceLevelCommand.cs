using System;
using VeloxDB.Client;
using VeloxDB.ClientApp.Modes;
using VeloxDB.Common;
using VeloxDB.Config;
using VeloxDB.Server;

namespace VeloxDB.ClientApp.Commands;

[Command("trace-level", "Sets the trace level of a given node.", ProgramMode = ProgramMode.Both)]
internal sealed class TraceLevelCommand : BindableCommand
{
	[Param("node", "Name of the node.", ShortName = "n", IsMandatory = true)]
	public string Node { get; set; }

	[Param("level", "Trace level to set for a given node.", ShortName = "l", IsMandatory = true)]
	public TraceLevel Level { get; set; }

	public override bool IsModeValid(Mode mode)
	{
		return mode is InitialMode;
	}

	protected override bool OnExecute(Program program)
	{
		ClusterConfiguration clusterConfig = ((InitialMode)program.Mode).ClusterConfig;
		if (clusterConfig.Cluster == null || !clusterConfig.TryGetElementByName(Node, out ReplicationElement element) || element is not ReplicationNode)
		{
			Console.WriteLine("Node with a given name was not found.");
			return true;
		}

		ReplicationNode node = (ReplicationNode)element;

		ConnectionStringParams cp = new ConnectionStringParams();
		cp.AddAddress(node.AdministrationAdress.ToString());
		cp.ServiceName = AdminAPIServiceNames.NodeAdministration;
		cp.RetryTimeout = Program.ConnectionRetryTimeout;
		cp.OpenTimeout = Program.ConnectionOpenTimeout;
		cp.PoolSize = 1;

		INodeAdministration nodeAdministration = ConnectionFactory.Get<INodeAdministration>(cp.GenerateConnectionString());

		try
		{
			nodeAdministration.SetTraceLevel(Level).Wait();
			return true;
		}
		catch (Exception e)
		{
			ConsoleHelper.ShowError(null, e);
			return false;
		}
	}
}
