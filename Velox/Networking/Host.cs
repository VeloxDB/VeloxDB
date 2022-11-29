using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Velox.Common;

namespace Velox.Networking;

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

	int bufferPoolSize;
	int maxQueuedChunkCount;
	bool groupSmallMessages;

	int maxOpenConnCount;

	public Host(int backlogSize, int maxOpenConnCount, IPEndPoint endpoint, int bufferPoolSize, TimeSpan inactivityInterval,
		TimeSpan inactivityTimeout, int maxQueuedChunkCount, bool groupSmallMessages, HandleMessageDelegate messageHandler) :
		this(backlogSize, maxOpenConnCount, new IPEndPoint[] { endpoint }, bufferPoolSize, inactivityInterval, inactivityTimeout,
			maxQueuedChunkCount, groupSmallMessages, messageHandler)
	{
	}

	public Host(int backlogSize, int maxOpenConnCount, IPEndPoint[] endpoints, int bufferPoolSize, TimeSpan inactivityInterval,
		TimeSpan inactivityTimeout, int maxQueuedChunkCount,bool groupSmallMessages, HandleMessageDelegate messageHandler)
	{
		this.endpoints = endpoints;
		this.inactivityInterval = inactivityInterval;
		this.inactivityTimeout = inactivityTimeout;
		this.bufferPoolSize = bufferPoolSize;
		this.messageHandler = messageHandler;
		this.maxOpenConnCount = maxOpenConnCount;
		this.maxQueuedChunkCount = maxQueuedChunkCount;
		this.groupSmallMessages = groupSmallMessages;

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
			}
			catch (SocketException e)
			{
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
					ThreadPool.UnsafeQueueUserWorkItem(x => AcceptCompleted(listenSockets[i], (SocketAsyncEventArgs)x), acceptArgs[i]);
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
				ServerConnection conn = new ServerConnection(this, socket, bufferPoolSize,
					inactivityInterval, inactivityTimeout, maxQueuedChunkCount, groupSmallMessages, messageHandler);
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
				Tracing.Debug("Host stopped on {0}.", endpoints[i]);
				listenSockets[i]?.Close();
			}
		}

		foreach (Connection connection in connections)
		{
			connection.Dispose();
		}

		connections.Clear();
	}
}
