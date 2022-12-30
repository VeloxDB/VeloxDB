using System;
using System.Xml.Linq;
using VeloxDB.Client;
using VeloxDB.ClientApp.Modes;
using VeloxDB.Config;
using VeloxDB.Networking;
using VeloxDB.Protocol;
using VeloxDB.Server;

namespace VeloxDB.ClientApp.Commands;

[Command("primary", "Instructs a given write HA cluster to become the primary site.", ProgramMode = ProgramMode.Both)]
internal sealed class PrimarySiteCommand : BindableCommand
{
	[Param("name", "Name of the HA cluster to become the primary site.", ShortName = "n", IsMandatory = true)]
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

		GlobalWriteCluster gwCluster = (GlobalWriteCluster)element.Parent;
		LocalWriteCluster lwCluster1 = (LocalWriteCluster)element;
		ReplicationElement otherElement = object.ReferenceEquals(gwCluster.First, lwCluster1) ? gwCluster.Second : gwCluster.First;

		if (IsOtherSitePrimary(otherElement))
		{
			if (!ConsoleHelper.Confirmation("It apperas that the other site is currently designated as the primary site. Proceeding with " +
					"this action might lead to a split brain scenarion. Do you want to continue (Y/N)?"))
			{
				return false;
			}
		}

		ConnectionStringParams cp = new ConnectionStringParams();

		if (lwCluster1.First != null)
			cp.AddAddress(lwCluster1.First.AdministrationAdress.ToString());

		if (lwCluster1.Second != null)
			cp.AddAddress(lwCluster1.Second.AdministrationAdress.ToString());

		cp.ServiceName = AdminAPIServiceNames.LocalWriteClusterAdministration;
		cp.RetryTimeout = Program.ConnectionRetryTimeout;
		cp.OpenTimeout = Program.ConnectionOpenTimeout;
		cp.PoolSize = 1;

		ILocalWriteClusterAdministration clusterAdministration = ConnectionFactory.Get<ILocalWriteClusterAdministration>(cp.GenerateConnectionString());

		try
		{
			try
			{
				clusterAdministration.BecomePrimarySite().Wait();
			}
			catch (AggregateException e)
			{
				throw e.InnerException;
			}
		}
		catch (Exception e) when (e is CommunicationException || e is ObjectDisposedException || e is ArgumentException ||
			e is TimeoutException || e is DbAPIErrorException)
		{
			ConsoleHelper.ShowError("Error occured.", e);
			return false;
		}

		return true;
	}

	private bool IsOtherSitePrimary(ReplicationElement otherElement)
	{
		if (otherElement.Type == ElementType.Node)
		{
			NodeState otherState = GetNodeState((ReplicationNode)otherElement);
			ReplicaState gwState = ClusterStatusLive.GetReplicaState(otherState, ReplicaType.GlobalWrite);
			if (gwState != null && gwState.IsPrimary)
				return true;
		}
		else
		{
			LocalWriteCluster lwCluster = (LocalWriteCluster)otherElement;
			NodeState otherState1 = GetNodeState(lwCluster.First);
			NodeState otherState2 = GetNodeState(lwCluster.Second);
			ReplicaState gwState1 = otherState1 == null ? null : ClusterStatusLive.GetReplicaState(otherState1, ReplicaType.GlobalWrite);
			ReplicaState gwState2 = otherState2 == null ? null : ClusterStatusLive.GetReplicaState(otherState2, ReplicaType.GlobalWrite);
			if (gwState1 != null && gwState1.IsPrimary || gwState2 != null && gwState2.IsPrimary)
				return true;
		}

		return false;
	}

	private NodeState GetNodeState(ReplicationNode node)
	{
		if (node == null)
			return null;

		ConnectionStringParams cp = new ConnectionStringParams();
		cp.AddAddress(node.AdministrationAdress.ToString());
		cp.ServiceName = AdminAPIServiceNames.NodeAdministration;
		cp.RetryTimeout = Program.ConnectionRetryTimeout;
		cp.OpenTimeout = Program.ConnectionOpenTimeout;
		cp.PoolSize = 1;

		INodeAdministration nodeAdministration = ConnectionFactory.Get<INodeAdministration>(cp.GenerateConnectionString());

		try
		{
			try
			{
				return nodeAdministration.GetState().GetResult();
			}
			catch (AggregateException e)
			{
				throw e.InnerException;
			}
		}
		catch (Exception e) when (e is CommunicationException || e is ObjectDisposedException || e is ArgumentException ||
				e is TimeoutException || e is DbAPIErrorException || e is DatabaseException)
		{
			return null;
		}
	}
}
