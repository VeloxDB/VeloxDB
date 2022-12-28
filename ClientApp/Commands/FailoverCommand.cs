using System;
using VeloxDB.Client;
using VeloxDB.ClientApp.Modes;
using VeloxDB.Config;
using VeloxDB.Networking;
using VeloxDB.Protocol;
using VeloxDB.Server;

namespace VeloxDB.ClientApp.Commands;

[Command("failover", "Initiates a failover operation in an HA cluster, transfering the primary role from one node to another.",
	ProgramMode = ProgramMode.Both)]
internal sealed class FailoverCommand : BindableCommand
{
	[Param("name", "Name of the HA cluster on which a failover operation should be executed.", ShortName = "n", IsMandatory = true)]
	public string Name { get; set; }

	public override bool IsModeValid(Mode mode)
	{
		return mode is InitialMode;
	}

	protected override bool OnExecute(Program program)
	{
		if (!CheckClusterBinding(program))
			return false;

		InitialMode initMode = (InitialMode)program.Mode;

		if (!initMode.ClusterConfig.TryGetElementByName(Name, out ReplicationElement element) || element.Type != ElementType.LocalWrite)
		{
			ConsoleHelper.ShowError("HA Cluster with a given name could not be found.");
			return false;
		}

		LocalWriteCluster lwCluster = (LocalWriteCluster)element;

		ConnectionStringParams cp = new ConnectionStringParams();

		if (lwCluster.First != null)
			cp.AddAddress(lwCluster.First.AdministrationAdress.ToString());

		if (lwCluster.Second != null)
			cp.AddAddress(lwCluster.Second.AdministrationAdress.ToString());

		cp.ServiceName = AdminAPIServiceNames.LocalWriteClusterAdministration;
		cp.RetryTimeout = Program.ConnectionRetryTimeout;
		cp.OpenTimeout = Program.ConnectionOpenTimeout;
		cp.PoolSize = 1;

		ILocalWriteClusterAdministration clusterAdministration = ConnectionFactory.Get<ILocalWriteClusterAdministration>(cp.GenerateConnectionString());

		try
		{
			try
			{
				clusterAdministration.Failover().Wait();
			}
			catch (AggregateException e)
			{
				throw e.InnerException;
			}
		}
		catch (Exception e) when (e is CommunicationException || e is ObjectDisposedException || e is ArgumentException ||
			e is TimeoutException || e is DbAPIErrorException || e is DatabaseException)
		{
			ConsoleHelper.ShowError("Error occured.", e);
			return false;
		}

		return true;
	}
}
