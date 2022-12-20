using System;
using VeloxDB.Client;
using VeloxDB.ClientApp.Modes;
using VeloxDB.Config;
using VeloxDB.Networking;
using VeloxDB.Protocol;
using VeloxDB.Server;

namespace VeloxDB.ClientApp.Commands;

[Command("standby", "Instructs a given write HA cluster to become the standby site.", ProgramMode = ProgramMode.Both)]
internal sealed class StandbySiteCommand : BindableCommand
{
	[Param("name", "Name of the HA cluster to become the primary site.", ShortName = "n")]
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

		if (element.Parent.Type != ElementType.GlobalWrite || !element.IsMember)
		{
			ConsoleHelper.ShowError("Given HA Cluster is not a write cluster in a global cluster.");
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
				clusterAdministration.BecomeStandbySite().Wait();
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
