using System;
using System.ServiceModel;

namespace Velox.Storage.Replication.HighAvailability;

internal interface IRaftNode
{
	public const short ProtocolVersion = 1;

	public const short ProvideVoteId = 0;
	public const short LeaderCheckinId = 1;
	public const short ConfirmStateId = 2;
	public const short ConfirmTransactionId = 3;

	bool ProvideVote(ref uint term, DatabaseElectionState[] dbStates);
	void LeaderCheckin(ref uint term, bool isLeader);
	bool ConfirmState(uint term, DatabaseElectionState[] dbState);
	bool ConfirmTransaction(uint term, DatabaseElectionState dbState);
}
