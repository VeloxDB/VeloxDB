using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace VeloxDB.Common;

internal sealed class ReferenceEqualityComparer<L> : IEqualityComparer<L> where L : class
{
	readonly static ReferenceEqualityComparer<L> instance;

	static ReferenceEqualityComparer()
	{
		instance = new ReferenceEqualityComparer<L>();
	}

	private ReferenceEqualityComparer()
	{
	}

	public static ReferenceEqualityComparer<L> Instance => ReferenceEqualityComparer<L>.instance;

	public bool Equals(L x, L y)
	{
		return object.ReferenceEquals(x, y);
	}

	public int GetHashCode(L obj)
	{
		return RuntimeHelpers.GetHashCode(obj);
	}
}
