using System;

namespace VeloxDB.Storage.Replication.HighAvailability;

internal interface IElectionDatabase
{
	string Name { get; }
	DatabaseElectionState[] TryGetElectionState();
	ElectorResponse BecomePrimary(object state, uint term);
	ElectorResponse BecomeStandby(object state);
	ElectorResponse TryManualFailover(object state);
	void ElectorStateChanged(ElectorState state);
}
