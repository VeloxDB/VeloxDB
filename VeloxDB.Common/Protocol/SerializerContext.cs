using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Networking;

namespace VeloxDB.Protocol;

internal sealed class SerializerContext
{
	public const int PooledCapacity = 512;
	public const int MaxObjectCount = 1024 * 1024 * 64;
	public const int MaxGraphDepth = 100;
	public const int AbsoluteMaxGraphDepth = 200;

	[ThreadStatic]
	static SerializerContext instance;

	SerializerManager serializerManager;

	FastDictionary<object, int> pooledObjIdMap;
	FastDictionary<object, int> objIdMap;

	ArrayQueue<object> pooledQueue;
	ArrayQueue<object> queue;

	bool doesNotSupportGraphs;
	public int currId;

	public SerializerContext()
	{
		currId = 1;
		pooledObjIdMap = objIdMap = new FastDictionary<object, int>(PooledCapacity, ReferenceEqualityComparer<object>.Instance);
		pooledQueue = queue = new ArrayQueue<object>(PooledCapacity);
		doesNotSupportGraphs = false;
	}

	public static SerializerContext Instance
	{
		get
		{
			SerializerContext t = instance;
			if (t != null)
				return t;

			t = new SerializerContext();
			instance = t;
			return t;
		}
	}

	public void Init(SerializerManager serializerManager, bool supportGraphs)
	{
		this.serializerManager = serializerManager;
		this.doesNotSupportGraphs = !supportGraphs;
	}

	public void Reset()
	{
		serializerManager = null;
		currId = 1;
		if (doesNotSupportGraphs)
			return;

		objIdMap = pooledObjIdMap;
		objIdMap.Clear();
		queue = pooledQueue;
		queue.Clear();
	}

	public SerializerManager SerializerManager => serializerManager;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool TryWriteInstance(MessageWriter writer, object obj, int depth, bool isClass)
	{
		if (doesNotSupportGraphs)
		{
			if (depth > MaxGraphDepth)
				throw new DbAPIObjectGraphDepthLimitExceededException();

			if (obj == null)
			{
				writer.WriteByte((byte)ReadObjectResult.Ready);
				return true;
			}
			else
			{
				writer.WriteByte((byte)ReadObjectResult.Deserialize);
				return false;
			}
		}

		return TryWriteGraphInstance(writer, obj, depth, isClass);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool TryWriteGraphInstance(MessageWriter writer, object obj, int depth, bool isClass)
	{
		if (depth > AbsoluteMaxGraphDepth)
			throw new DbAPIObjectGraphDepthLimitExceededException();

		if (obj == null)
		{
			writer.WriteInt(0);
			return true;
		}

		if (objIdMap.TryGetValue(obj, out int v))
		{
			Unpack(v, out int id, out byte isSerialized, out byte isQueued);
			if (isSerialized == 1)
			{
				writer.WriteInt(-id);
				return true;
			}
			else
			{
				Checker.AssertTrue(isClass);
				if (depth <= MaxGraphDepth)
				{
					objIdMap[obj] = Pack(id, 1, isQueued);
					writer.WriteInt(id);
					return false;
				}
				else
				{
					if (isQueued == 0)
					{
						AddObjectToQueue(obj);
						objIdMap[obj] = Pack(id, 0, 1);
					}

					writer.WriteInt(-id);
					return true;
				}
			}
		}
		else
		{
			if (objIdMap.Count >= MaxObjectCount)
				throw new DbAPIObjectCountLimitExceededException();

			int id = currId++;
			if (depth <= MaxGraphDepth || !isClass)
			{
				writer.WriteInt(id);
				AddObjectToMap(obj, Pack(id, 1, 0));
				return false;
			}
			else
			{
				writer.WriteInt(-id);
				AddObjectToMap(obj, Pack(id, 0, 1));
				AddObjectToQueue(obj);
				return true;
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SerializeQueued(MessageWriter writer)
	{
		if (doesNotSupportGraphs)
			return;

		SerializedQueuedGraph(writer);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void SerializedQueuedGraph(MessageWriter writer)
	{
		while (queue.Count != 0)
		{
			object obj = queue.Dequeue();
			int v = objIdMap[obj];
			Unpack(v, out int id, out byte isSerialized, out byte isQueued);
			Checker.AssertTrue(isQueued == 1);
			objIdMap[obj] = Pack(id, 0, 0);
			writer.WriteBool(true);
			Methods.SerializePolymorphType(writer, obj, this, 0);
		}

		writer.WriteBool(false);
	}

	private void AddObjectToMap(object obj, int v)
	{
		objIdMap.Add(obj, v);
		if (objIdMap.Count > PooledCapacity && object.ReferenceEquals(objIdMap, pooledObjIdMap))
			pooledObjIdMap = new FastDictionary<object, int>(PooledCapacity, ReferenceEqualityComparer<object>.Instance);
	}

	private void AddObjectToQueue(object obj)
	{
		queue.Enqueue(obj);
		if (queue.Count > PooledCapacity && object.ReferenceEquals(queue, pooledQueue))
			pooledQueue = new ArrayQueue<object>(PooledCapacity);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Unpack(int v, out int id, out byte isSerialized, out byte isQueued)
	{
		id = v & 0x3fffffff;
		isSerialized = (byte)((uint)v >> 31);
		isQueued = (byte)(((uint)v >> 30) & 0x01);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static int Pack(int id, byte isSerialized, byte isQueued)
	{
		return (int)((uint)id | ((uint)isSerialized << 31) | ((uint)isQueued << 30));
	}
}
