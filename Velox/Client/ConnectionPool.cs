using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Velox.Common;
using Velox.Networking;
using Velox.Protocol;

namespace Velox.Client;

internal class ConnectionPool
{
	RWSpinLockFair fastSync;
	TaskCompletionSource connAvailableEvent;

	string connectionString;
	ConnectionStringParams connParams;

	int pendingOpenCount;
	int currIndex;
	int connectionCount;
	ConnectionEntry[] connections;
	Stopwatch timer;

	public ConnectionPool(string connectionString)
	{
		connAvailableEvent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

		this.connectionString = connectionString;

		connParams = new ConnectionStringParams(connectionString);
		connections = new ConnectionEntry[connParams.PoolSize];
		timer = Stopwatch.StartNew();
	}

	public ConnectionStringParams ConnParams => connParams;
	public string ConnectionString => connectionString;

	public void Clear()
	{
		for (int i = 0; i < connections.Length; i++)
		{
			if (connections[i].Connection != null)
				connections[i].Connection.Connection.CloseAsync();
		}
	}

	public async ValueTask<ConnectionEntry> GetConnection(bool preferNewer)
	{
		ConnectionEntry? result = null;

		// The most common case is checked only under read lock
		fastSync.EnterReadLock();
		try
		{
			if (pendingOpenCount == connections.Length - connectionCount && connectionCount > 0)
			{
				result = GetNextConnection(preferNewer);
			}
		}
		finally
		{
			fastSync.ExitReadLock();
		}

		if (result.HasValue)
			return result.Value;

		while (true)
		{
			fastSync.EnterWriteLock();
			try
			{
				if (pendingOpenCount == connections.Length - connectionCount)
				{
					if (connectionCount == 0)
					{
						Checker.AssertFalse(connAvailableEvent.Task.IsCompleted);
						TaskCompletionSource e = connAvailableEvent;
						fastSync.ExitWriteLock();
						await e.Task;
						fastSync.EnterWriteLock();
						continue;
					}
					else
					{
						return GetNextConnection(preferNewer);
					}
				}
				else
				{
					Interlocked.Increment(ref pendingOpenCount);
					break;
				}
			}
			finally
			{
				fastSync.ExitWriteLock();
			}
		}

		try
		{
			result = await CreateConnection();

			fastSync.EnterWriteLock();
			try
			{
				Interlocked.Decrement(ref pendingOpenCount);
				connections[connectionCount++] = result.Value;
				connAvailableEvent.TrySetResult();
				return result.Value;
			}
			finally
			{
				fastSync.ExitWriteLock();
			}
		}
		catch
		{
			Interlocked.Decrement(ref pendingOpenCount);
			throw;
		}
	}

	private ConnectionEntry GetNextConnection(bool preferNewer)
	{
		if (preferNewer)
		{
			int maxIndex = 0;
			for (int i = 1; i < connectionCount; i++)
			{
				if (connections[i].Connection.Timestamp > connections[maxIndex].Connection.Timestamp)
					maxIndex = i;
			}

			return connections[maxIndex];
		}

		Interlocked.Increment(ref currIndex);
		ConnectionEntry res = connections[currIndex % connectionCount];
		return res;
	}

	private async Task<ConnectionEntry> CreateTimeoutTask(int timeout, CancellationToken cancellationToken)
	{
		try
		{
			await Task.Delay(timeout, cancellationToken);
		}
		catch
		{
		}

		return new ConnectionEntry();
	}

	private async Task<ConnectionEntry> OpenConnection(ClientConnection connection)
	{
		await connection.OpenAsync();
		PooledConnection pooledConn = new PooledConnection(this, connection, timer.Elapsed);
		connection.AddClosedHandlerSafe(() => RemoveConnection(pooledConn));
		ProtocolDescriptor descriptor = await PerformConnect(pooledConn);
		return new ConnectionEntry(pooledConn, descriptor);
	}

	private async Task<ConnectionEntry> CreateConnection()
	{
		string[] addresses = connParams.Addresses;
		Task<IPEndPoint[]>[] ipTasks = new Task<IPEndPoint[]>[addresses.Length];
		for (int i = 0; i < addresses.Length; i++)
		{
			ipTasks[i] = Connection.TranslateMultiAddressAsync(addresses[i], false, false);
		}

		await Task.WhenAll(ipTasks);

		List<IPEndPoint> endpoints = new List<IPEndPoint>(connParams.Addresses.Length * 2);
		for (int i = 0; i < addresses.Length; i++)
		{
			endpoints.AddRange(ipTasks[i].Result);
		}

		CancellationTokenSource delayCancel = new CancellationTokenSource();
		Task<ConnectionEntry> delayTask = CreateTimeoutTask(connParams.OpenTimeout, delayCancel.Token);

		List<ClientConnection> connections = new List<ClientConnection>(endpoints.Count);
		List<Task<ConnectionEntry>> tasks = new List<Task<ConnectionEntry>>(endpoints.Count + 1);
		for (int i = 0; i < endpoints.Count; i++)
		{
			connections.Add(new ClientConnection(endpoints[i], connParams.BufferPoolSize,
				TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5), int.MaxValue, true, null));
			tasks.Add(OpenConnection(connections[i]));
		}

		if (delayTask != null)
			tasks.Add(delayTask);

		Exception e = null;
		while (true)
		{
			Task<ConnectionEntry> t = await Task.WhenAny(tasks.ToArray());
			if (object.ReferenceEquals(t, delayTask))
			{
				CleanupConnectionAttempts(delayCancel, connections, -1);
				throw new TimeoutException();
			}

			if (t.IsFaulted)
			{
				if (GetExceptionImportanceRating(e) < GetExceptionImportanceRating(t.Exception.InnerException))
					e = t.Exception.InnerException;
			}
			else
			{
				CleanupConnectionAttempts(delayCancel, connections, tasks.IndexOf(t));
				return t.Result;
			}

			int n = tasks.IndexOf(t);
			tasks.RemoveAt(n);
			connections.RemoveAt(n);

			if (tasks.Count == 0 || (tasks.Count == 1 && delayTask != null))
			{
				delayCancel.Cancel();
				throw e;
			}
		}
	}

	private static void CleanupConnectionAttempts(CancellationTokenSource delayCancel, List<ClientConnection> connections, int otherThan)
	{
		delayCancel.Cancel();
		for (int i = 0; i < connections.Count; i++)
		{
			if (i != otherThan)
				connections[i].CloseAsync();
		}
	}

	private async Task<ProtocolDescriptor> PerformConnect(PooledConnection connection)
	{
		TaskCompletionSource<ProtocolDescriptor> tcp = new TaskCompletionSource<ProtocolDescriptor>();
		connection.SendRequest0(writer =>
		{
			writer.WriteUShort(SerializerManager.FormatVersion);
			writer.WriteByte((byte)RequestType.Connect);
			writer.WriteString(connParams.ServiceName);
		},
		(connection, state, error, reader) =>
		{
			if (error != null)
			{
				tcp.SetException(error);
			}
			else
			{
				try
				{
					ResponseType responseType = (ResponseType)reader.ReadByte();
					if (responseType == ResponseType.Response)
					{
						tcp.SetResult(ProtocolDescriptor.Deserialize(reader));
					}
					else if (responseType == ResponseType.APIUnavailable)
					{
						tcp.SetException(new DbAPIUnavailableException());
					}
					else if (responseType == ResponseType.ProtocolError)
					{
						tcp.SetException(new DbAPIProtocolException());
					}
					else
					{
						throw new DbAPIProtocolException();
					}
				}
				catch (Exception e)
				{
					tcp.SetException(e);
				}
			}
		},
		null);

		try
		{
			return await tcp.Task;
		}
		catch (AggregateException e)
		{
			connection.Invalidate();
			throw e.InnerException;
		}
	}

	public void RemoveConnection(PooledConnection connection)
	{
		fastSync.ExitWriteLock();
		try
		{
			int rem = 0;
			for (int i = 0; i < connectionCount; i++)
			{
				if (object.ReferenceEquals(connections[i].Connection, connection))
				{
					rem++;
				}
				else
				{
					connections[i - rem] = connections[i];
				}
			}

			if (rem > 0)
			{
				connections[connectionCount - 1] = new ConnectionEntry();
				connectionCount--;
				if (connectionCount == 0)
				{
					Checker.AssertTrue(connAvailableEvent.Task.IsCompleted);
					connAvailableEvent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
				}
			}
		}
		finally
		{
			fastSync.ExitWriteLock();
		}
	}

	private int GetExceptionImportanceRating(Exception e)
	{
		if (e == null)
			return 0;

		if (e is CommunicationException || e is ObjectDisposedException)
			return 1;

		if (e is TimeoutException)
			return 2;

		if (e is DbAPIUnavailableException)
			return 4;

		return 3;
	}

	public struct ConnectionEntry
	{
		public PooledConnection Connection { get; private set; }
		public ProtocolDescriptor Descriptor { get; private set; }

		public ConnectionEntry(PooledConnection connection, ProtocolDescriptor descriptor)
		{
			this.Connection = connection;
			this.Descriptor = descriptor;
		}
	}
}
