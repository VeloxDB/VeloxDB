using System;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Storage;

#if TEST_BUILD
internal sealed partial class Transaction
{
	int commitDelay;
	int mergeInvRefsDelay;

	internal void SetCommitDelay(int commitDelay)
	{
		this.commitDelay = commitDelay;
	}

	internal void SetMergeInvRefsDelay(int mergeInvRefsDelay)
	{
		this.mergeInvRefsDelay = mergeInvRefsDelay;
	}

	internal void DelayCommit()
	{
		if (commitDelay > 0)
			Thread.Sleep(commitDelay);
	}

	internal void DelayInvRefMerge()
	{
		if (mergeInvRefsDelay > 0)
			Thread.Sleep(mergeInvRefsDelay);
	}
}
#endif
