using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Networking;

namespace VeloxDB.Protocol;

internal enum ReadObjectResult : int
{
	Ready = 0,
	Deserialize = 1,
	CreateEmpty = 2,
}

internal sealed class DeserializerContext
{
	[ThreadStatic]
	static DeserializerContext instance;

	LongDictionary<object> pooledIdObjMap;
	LongDictionary<object> idObjMap;

	int lastReadId;
	bool doesNotSupportGraphs;

	public DeserializerContext()
	{
		idObjMap = pooledIdObjMap = new LongDictionary<object>(SerializerContext.PooledCapacity);
		doesNotSupportGraphs = false;
	}

	public static DeserializerContext GetInstance(bool supportGraphs)
	{
		DeserializerContext inst = instance;
		if (inst == null)
		{
			inst = new DeserializerContext();
			instance = inst;
		}

		inst.doesNotSupportGraphs = !supportGraphs;
		return inst;
	}

	public void Reset()
	{
		if (!doesNotSupportGraphs)
		{
			idObjMap = pooledIdObjMap;
			idObjMap.Clear();
		}

		doesNotSupportGraphs = false;
		lastReadId = 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ReadObjectResult TryReadInstanceTyped<T>(MessageReader reader, out T tobj, int depth) where T : class
	{
		ReadObjectResult result = TryReadInstance(reader, out object obj, depth);
		tobj = obj as T;
		if (obj != null && tobj == null)
			throw new DbAPIProtocolException();

		return result;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ReadObjectResult TryReadInstance(MessageReader reader, out object tobj, int depth)
	{
		if (depth > SerializerContext.AbsoluteMaxGraphDepth)
			throw new DbAPIObjectGraphDepthLimitExceededException();

		if (doesNotSupportGraphs)
		{
			tobj = null;
			return (ReadObjectResult)reader.ReadByte();
		}

		return TryReadGraphInstance(reader, out tobj);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private ReadObjectResult TryReadGraphInstance(MessageReader reader, out object obj)
	{
		lastReadId = reader.ReadInt();
		if (lastReadId == 0)
		{
			obj = null;
			return ReadObjectResult.Ready;
		}

		if (lastReadId > 0)
		{
			if (!idObjMap.TryGetValue(lastReadId, out obj))
			{
				if (idObjMap.Count == SerializerContext.MaxObjectCount)
					throw new DbAPIObjectCountLimitExceededException();
			}

			return ReadObjectResult.Deserialize;
		}
		else
		{
			if (idObjMap.TryGetValue(-lastReadId, out obj))
			{
				return ReadObjectResult.Ready;
			}
			else
			{
				if (idObjMap.Count == SerializerContext.MaxObjectCount)
					throw new DbAPIObjectCountLimitExceededException();

				return ReadObjectResult.CreateEmpty;
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AddInstance(object obj)
	{
		if (doesNotSupportGraphs)
			return;

		AddGraphInstance(obj);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void DeserializeQueued(MessageReader reader, ProtocolDeserializeDelegate[] deserializerTable)
	{
		if (doesNotSupportGraphs)
			return;

		DeserializeQueuedGraph(reader, deserializerTable);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void DeserializeQueuedGraph(MessageReader reader, ProtocolDeserializeDelegate[] deserializerTable)
	{
		while (reader.ReadBool())
		{
			Methods.DeserializePolymorphType(reader, this, deserializerTable, 0);
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void AddGraphInstance(object obj)
	{
		idObjMap.Add(Math.Abs(lastReadId), obj);
		if (idObjMap.Count > SerializerContext.PooledCapacity && object.ReferenceEquals(idObjMap, pooledIdObjMap))
			pooledIdObjMap = new LongDictionary<object>(SerializerContext.PooledCapacity);
	}
}
