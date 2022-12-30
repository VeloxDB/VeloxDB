using System;

namespace VeloxDB.Storage.Replication.HighAvailability;

internal enum ElectorResponse
{
	Success = 0,
	NotApplicable = 1,
	Busy = 2,
}

internal class ElectorState
{
	bool isWitnessConnected;
	bool isElectorConnected;

	public ElectorState(bool isWitnessConnected, bool isElectorConnected)
	{
		this.isWitnessConnected = isWitnessConnected;
		this.isElectorConnected = isElectorConnected;
	}

	public bool IsWitnessConnected => isWitnessConnected;
	public bool IsElectorConnected => isElectorConnected;
}

internal interface ILeaderElector
{
	void Activate(IElectionDatabase database, object state);
	bool ConfirmState(DatabaseElectionState[] states);
	bool ConfirmTransaction(DatabaseElectionState state);
	void GiveUpLeadership();
}

internal interface IElector
{
	ElectorResponse BecomePrimary(uint term);
	ElectorResponse BecomeStandby();
	ElectorResponse TryFailover();
}

internal interface IElectorFactory
{
	ILeaderElector CreateLocalElector(string address, string path, int electionTimeout,
		WitnessConfiguration witnessConfig, string replicaAddress, bool robustHost);
	ILeaderElector CreateGlobalElector();

	WitnessConfiguration CreateSharedFolderWitnessConfiguration(string path, int remoteFileTimeout);
	WitnessConfiguration GetWitnessServerConfiguration(string address);
}
