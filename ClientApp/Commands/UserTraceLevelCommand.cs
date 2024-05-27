using System;
using VeloxDB.Client;
using VeloxDB.ClientApp.Modes;
using VeloxDB.Common;
using VeloxDB.Config;
using VeloxDB.Server;

namespace VeloxDB.ClientApp.Commands;

[Command("user-trace-level", "Sets the user trace level of a given node.", ProgramMode = ProgramMode.Both)]
internal sealed class UserTraceLevelCommand : BindableCommand
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

		ConnectionStringParams cp = program.CreateConnectionStringParams();
		cp.AddAddress(node.AdministrationAddress.ToString());
		cp.ServiceName = AdminAPIServiceNames.NodeAdministration;

		INodeAdministration nodeAdministration = ConnectionFactory.Get<INodeAdministration>(cp.GenerateConnectionString());

		try
		{
			nodeAdministration.SetUserTraceLevel(Level).Wait();
			return true;
		}
		catch (Exception e)
		{
			ConsoleHelper.ShowError(null, e);
			return false;
		}
	}
}
