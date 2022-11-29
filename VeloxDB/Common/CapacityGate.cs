using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Velox.Common;

internal sealed class CapacityGate
{
	long maxCapacity;
	long capacity;

	public CapacityGate(long maxCapacity)
	{
		this.maxCapacity = maxCapacity;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Enter(long capacity)
	{
		if (this.capacity < maxCapacity)
		{
			Interlocked.Add(ref this.capacity, capacity);
			return;
		}

		WaitForCapacity(capacity);
		Interlocked.Add(ref this.capacity, capacity);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void WaitForCapacity(long capacity)
	{
		SpinWait.SpinUntil(() => this.capacity < maxCapacity);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Exit(long capacity)
	{
		Interlocked.Add(ref this.capacity, -capacity);
	}
}
