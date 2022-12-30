using System;

namespace VeloxDB.Common;

internal static class EmptyArray<T>
{
	static readonly T[] instance = new T[0];
	public static T[] Instance => instance;

	public static T[] Create(int length)
	{
		if (length == 0)
			return Instance;

		return new T[length];
	}
}
