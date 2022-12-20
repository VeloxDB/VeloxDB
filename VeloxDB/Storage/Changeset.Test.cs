using System;
using System.Collections.Generic;
using System.Diagnostics;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal partial class Changeset
{
#if HUNT_CHG_LEAKS
	List<string> allocStacks = new List<string>(4);
	List<string> freeStacks = new List<string>(4);
#endif

	[Conditional("HUNT_CHG_LEAKS")]
	private void TrackReferencingStack()
	{
#if HUNT_CHG_LEAKS
		allocStacks.Add(new StackTrace(true).ToString());
#endif
	}

	[Conditional("HUNT_CHG_LEAKS")]
	private void TrackDereferencingStack()
	{
#if HUNT_CHG_LEAKS
		freeStacks.Add(new StackTrace(true).ToString());
#endif
	}

	[Conditional("TEST_BUILD")]
	private void TrackAllocation()
	{
#if TEST_BUILD
		if (allocated != null)
		{
			lock (allocated)
			{
				allocated.Add(this);
			}
		}
#endif
	}

	[Conditional("TEST_BUILD")]
	private void TrackDeallocation()
	{
#if TEST_BUILD
		if (allocated != null)
		{
			lock (allocated)
			{
				bool b = allocated.Remove(this);
				Checker.AssertTrue(b);
			}
		}
#endif
	}

#if TEST_BUILD
	public static HashSet<Changeset> allocated;

	public static void TurnOnLeakDetection()
	{
		allocated = new HashSet<Changeset>(8, ReferenceEqualityComparer<Changeset>.Instance);
	}

	public static void TurnOffLeakDetection()
	{
		allocated = null;
	}

	public static void ValidateNoLeaks()
	{
		lock (allocated)
		{
			if (allocated.Count > 0)
			{
				throw new InvalidOperationException();
			}
		}
	}
#endif
}
