﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Networking;

internal sealed class Host
{
	const int concurrentAcceptCount = 1;

	readonly object sync = new object();

	IPEndPoint[] endpoints;
	Socket[] listenSockets;
	SocketAsyncEventArgs[][] acceptArgs;

	HashSet<ServerConnection> connections;
	HandleMessageDelegate messageHandler;

	TimeSpan inactivityInterval;
	TimeSpan inactivityTimeout;

	bool stopped;

	MessageChunkPool chunkPool;
	int maxQueuedChunkCount;
	bool groupSmallMessages;

	int maxOpenConnCount;

	JobWorkers<Action> priorityWorkers;

#if HUNT_CHG_LEAKS
	static HashSet<IPEndPoint> activeEndpoints = new HashSet<IPEndPoint>(ReferenceEqualityComparer<IPEndPoint>.Instance);
#endif

	public Host(int backlogSize, int maxOpenConnCount, IPEndPoint endpoint, MessageChunkPool chunkPool, TimeSpan inactivityInterval,
		TimeSpan inactivityTimeout, int maxQueuedChunkCount, bool groupSmallMessages, HandleMessageDelegate messageHandler,
		JobWorkers<Action> priorityWorkers = null) :
		this(backlogSize, maxOpenConnCount, new IPEndPoint[] { endpoint }, chunkPool, inactivityInterval, inactivityTimeout,
			maxQueuedChunkCount, groupSmallMessages, messageHandler, priorityWorkers)
	{
	}

	public Host(int backlogSize, int maxOpenConnCount, IPEndPoint[] endpoints, MessageChunkPool chunkPool, TimeSpan inactivityInterval,
		TimeSpan inactivityTimeout, int maxQueuedChunkCount, bool groupSmallMessages, HandleMessageDelegate messageHandler,
		JobWorkers<Action> priorityWorkers = null)
	{
		this.endpoints = endpoints;
		this.inactivityInterval = inactivityInterval;
		this.inactivityTimeout = inactivityTimeout;
		this.messageHandler = messageHandler;
		this.maxOpenConnCount = maxOpenConnCount;
		this.maxQueuedChunkCount = maxQueuedChunkCount;
		this.groupSmallMessages = groupSmallMessages;
		this.chunkPool = chunkPool;
		this.priorityWorkers = priorityWorkers;

		acceptArgs = new SocketAsyncEventArgs[endpoints.Length][];
		for (int i = 0; i < endpoints.Length; i++)
		{
			Tracing.Debug("Host started on {0}.", endpoints[i]);
			acceptArgs[i] = new SocketAsyncEventArgs[concurrentAcceptCount];
		}

		connections = new HashSet<ServerConnection>(32, ReferenceEqualityComparer<ServerConnection>.Instance);

		listenSockets = new Socket[endpoints.Length];
		for (int i = 0; i < endpoints.Length; i++)
		{
			listenSockets[i] = new Socket(endpoints[i].AddressFamily, SocketType.Stream, ProtocolType.Tcp);
			listenSockets[i].ExclusiveAddressUse = true;

			try
			{
				listenSockets[i].Bind(endpoints[i]);
				listenSockets[i].Listen(backlogSize);
				TTTrace.Write((long)listenSockets[i].Handle, endpoints[i].Port, new StackTrace(true).ToString());

#if HUNT_CHG_LEAKS
				activeEndpoints.Add(endpoints[i]);
#endif
			}
			catch (SocketException e)
			{
				listenSockets[i].Close();
				listenSockets[i] = null;

				Stop();
				if (NativeSocket.IsAddressAlreadyInUseError(e.ErrorCode))
					throw new AddressAlreadyInUseException("Address is already in use.", e);
				else
					throw new CommunicationException("Error occurred while creating the listening socket.", e);
			}

			for (int j = 0; j < concurrentAcceptCount; j++)
			{
				acceptArgs[i][j] = new SocketAsyncEventArgs();
				acceptArgs[i][j].Completed += AcceptCompleted;

				if (!listenSockets[i].AcceptAsync(acceptArgs[i][j]))
				{
					ThreadPool.UnsafeQueueUserWorkItem(x =>
					{
						var t = (Tuple<Socket, SocketAsyncEventArgs>)x;
						AcceptCompleted(t.Item1, t.Item2);
					}, new Tuple<Socket, SocketAsyncEventArgs>(listenSockets[i], acceptArgs[i][j]));
				}
			}
		}
	}

	private void AcceptCompleted(object sender, SocketAsyncEventArgs e)
	{
		Socket socket = null;

		lock (sync)
		{
			if (stopped)
			{
				if (e.AcceptSocket != null)
					e.AcceptSocket.Close();

				e.Dispose();
				return;
			}

			if (e.SocketError != SocketError.Success)
			{
				if (e.AcceptSocket != null)
					e.AcceptSocket.Close();
			}
			else
			{
				socket = e.AcceptSocket;
			}
		}

		Socket listenSocket = (Socket)sender;
		Checker.AssertTrue(listenSockets.Any(x => object.ReferenceEquals(x, listenSocket)));
		e.AcceptSocket = null;

		try
		{
			if (!listenSocket.AcceptAsync(e))
				ThreadPool.UnsafeQueueUserWorkItem(x => AcceptCompleted(listenSocket, e), null);
		}
		catch (ObjectDisposedException)
		{
			e.Dispose();
		}

		if (socket == null)
			return;

		lock (sync)
		{
			if (stopped || connections.Count >= maxOpenConnCount)
			{
				socket.Close();
			}
			else
			{
				ServerConnection conn = new ServerConnection(this, socket, chunkPool,
					inactivityInterval, inactivityTimeout, maxQueuedChunkCount, groupSmallMessages, messageHandler, priorityWorkers);
				connections.Add(conn);
			}
		}
	}

	internal void ConnectionDisconnected(ServerConnection conn)
	{
		lock (sync)
		{
			if (!stopped)
				connections.Remove(conn);
		}
	}

	public void Stop()
	{
		lock (sync)
		{
			if (stopped)
				return;

			stopped = true;

			for (int i = 0; i < listenSockets.Length; i++)
			{
				if (listenSockets[i] != null)
				{
					Tracing.Debug("Host stopped on {0}.", endpoints[i]);
					TTTrace.Write((long)listenSockets[i].Handle, endpoints[i].Port, new StackTrace(true).ToString());
					listenSockets[i].Close();

#if HUNT_CHG_LEAKS
					bool b = activeEndpoints.Remove(endpoints[i]);
					Checker.AssertTrue(b);
#endif
				}
			}
		}

		foreach (Connection connection in connections)
		{
			connection.Dispose();
		}

		connections.Clear();
	}
}
