using System;
using System.Runtime.CompilerServices;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal static class IdHelper
{
	public const int CounterBitCount = 51;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static short GetClassId(long id)
	{
		return (short)((int)((ulong)id >> CounterBitCount) + ClassDescriptor.MinId);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static long MakeId(short classId, long id)
	{
		return id | (long)((ulong)(classId - ClassDescriptor.MinId) << CounterBitCount);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static long MakeId(this ClassDescriptor cd, long id)
	{
		return id | (long)((ulong)(cd.Id - ClassDescriptor.MinId) << CounterBitCount);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static ClassDescriptor GetClass(DataModelDescriptor model, long id)
	{
		short classId = GetClassId(id);
		return model.GetClass(classId);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool TryGetClassIndex(DataModelDescriptor model, long id, out int index)
	{
		short classId = GetClassId(id);
		ClassDescriptor cd = model.GetClass(classId);
		if (cd == null)
		{
			index = ushort.MaxValue;
			return false;
		}

		index = cd.Index;
		return true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool TryGetClassIndex(DataModelDescriptor model, long id, out ushort index)
	{
		short classId = GetClassId(id);
		ClassDescriptor cd = model.GetClass(classId);
		if (cd == null)
		{
			index = ushort.MaxValue;
			return false;
		}

		index = (ushort)cd.Index;
		return true;
	}
}
