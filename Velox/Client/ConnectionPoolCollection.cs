using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using Velox.Common;
using Velox.Networking;

namespace Velox.Client;

internal static class ConnectionPoolCollection
{
	const int byRefCapacity = 8;
	const int initChunkPoolSize = 1024 * 128;

	static readonly object sync = new object();

	static ConnectionPool[] byRefPool = EmptyArray<ConnectionPool>.Instance;
	static Dictionary<string, ConnectionPool> pools = new Dictionary<string, ConnectionPool>(1);

	static MessageChunkPool messageChunkPool = new MessageChunkPool(initChunkPoolSize);

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static ConnectionPool GetPool(string connectionString)
	{
		ConnectionPool[] temp = byRefPool;
		for (int i = 0; i < temp.Length; i++)
		{
			if (object.ReferenceEquals(connectionString, temp[i].ConnectionString))
				return temp[i];
		}

		return LookupPool(connectionString);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private static ConnectionPool LookupPool(string connectionString)
	{
		if (pools.TryGetValue(connectionString, out ConnectionPool pool))
			return pool;

		lock (sync)
		{
			if (pools.TryGetValue(connectionString, out pool))
				return pool;

			pool = new ConnectionPool(messageChunkPool, connectionString);
			messageChunkPool.ResizeIfLarger(pool.ConnParams.BufferPoolSize);
			Dictionary<string, ConnectionPool> temp = new Dictionary<string, ConnectionPool>(pools);
			temp.Add(connectionString, pool);
			Thread.MemoryBarrier();
			pools = temp;

			if (byRefPool.Length < byRefCapacity)
			{
				ConnectionPool[] tempb = new ConnectionPool[byRefPool.Length + 1];
				Array.Copy(byRefPool, tempb, byRefPool.Length);
				tempb[tempb.Length - 1] = pool;
				Thread.MemoryBarrier();
				byRefPool = tempb;
			}
		}

		return pool;
	}

	public static void Reset()
	{
		lock (sync)
		{
			foreach (var pool in pools.Values)
			{
				pool.Clear();
			}
		}
	}
}
