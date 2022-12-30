using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Networking;

internal partial class MessageChunk
{
#if HUNT_CHG_LEAKS
	string allocStack;
	string poolRetreivalStack;
	string poolReturnStack;
#endif

	[Conditional("HUNT_CHG_LEAKS")]
	private void TrackCreationStack()
	{
#if HUNT_CHG_LEAKS
		allocStack = new StackTrace(true).ToString();
#endif
	}

	[Conditional("HUNT_CHG_LEAKS")]
	public void TrackPoolRetreival()
	{
#if HUNT_CHG_LEAKS
		poolRetreivalStack = new StackTrace(true).ToString();
		Thread.MemoryBarrier();
#endif
	}

	[Conditional("HUNT_CHG_LEAKS")]
	public void TrackPoolReturn()
	{
#if HUNT_CHG_LEAKS
		poolReturnStack = new StackTrace(true).ToString();
		Thread.MemoryBarrier();
#endif
	}
}
