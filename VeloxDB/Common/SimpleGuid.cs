using System;
using System.Runtime.CompilerServices;

namespace Velox.Common;

internal struct SimpleGuid : IEquatable<SimpleGuid>
{
	public const int Size = 16;
	public static readonly SimpleGuid Zero = new SimpleGuid(0, 0);

	long low;
	long high;

	public SimpleGuid(long low, long high)
	{
		this.low = low;
		this.high = high;
	}

	public long Low { get => low; set => low = value; }
	public long Hight { get => high; set => high = value; }

	public bool IsZero => Zero.Equals(this);

	public static SimpleGuid NewValue()
	{
		Guid guid = Guid.NewGuid();
		return FromGuid(guid);
	}

	public unsafe static SimpleGuid FromGuid(Guid guid)
	{
		long* pguid = (long*)&guid;
		long v1 = pguid[0];
		long v2 = pguid[1];
		return new SimpleGuid() { low = v1, high = v2 };
	}

	public unsafe Guid ToGuid()
	{
		Guid result = new Guid();
		long* pguid = (long*)&result;

		pguid[0] = low;
		pguid[1] = high;

		return result;
	}

	public static explicit operator Guid(SimpleGuid simple) => simple.ToGuid();
	public static explicit operator SimpleGuid(Guid guid) => SimpleGuid.FromGuid(guid);

	public override string ToString()
	{
		return $"({low}, {high})";
	}

	public bool Equals(SimpleGuid other)
	{
		return low == other.low && high == other.high;
	}

	public override int GetHashCode()
	{
		return (int)HashUtils.GetHash128((ulong)low, (ulong)high, HashUtils.PrimeMultiplier64);
	}
}
