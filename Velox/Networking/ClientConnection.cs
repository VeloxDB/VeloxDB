using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Velox.Networking;

internal sealed partial class ClientConnection : Connection
{
	IPEndPoint endpoint;
	bool failedToOpen;

	public unsafe ClientConnection(IPEndPoint endpoint, int bufferPoolSize, TimeSpan inactivityInterval,
		TimeSpan inactivityTimeout, int maxQueuedChunkCount, bool groupSmallMessages, HandleMessageDelegate messageHandler) :
		base(bufferPoolSize, inactivityInterval, inactivityTimeout, maxQueuedChunkCount, groupSmallMessages, messageHandler)
	{
		this.endpoint = endpoint;
	}

	protected override long BaseMsgId => 1;

	public bool FailedToOpen => failedToOpen;

	protected override bool IsResponseMessage(long msgId)
	{
		return msgId > 0;
	}

	public void Open()
	{
		try
		{
			OpenAsync().Wait();
		}
		catch (AggregateException e)
		{
			throw e.InnerException;
		}
	}

	public async Task OpenAsync()
	{
		sync.EnterWriteLock();

		try
		{
			if (state == ConnectionState.Closed)
				throw CreateClosedException();

			if (state != ConnectionState.Created)
				throw new InvalidOperationException("Connection has already been opened.");
		}
		finally
		{
			sync.ExitWriteLock();
		}

		await ConnectAsync();
	}

	private Task ConnectAsync()
	{
		socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
		socket.SendTimeout = Timeout.Infinite;
		if (inactivityTimeout != TimeSpan.MaxValue)
			NativeSocket.TurnOnKeepAlive(socket.Handle, inactivityInterval, inactivityTimeout);

		socket.NoDelay = true;
		socket.ReceiveBufferSize = MessageChunk.LargeBufferSize * 2;
		socket.SendBufferSize = MessageChunk.LargeBufferSize * 2;

		TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

		SocketAsyncEventArgs e = new SocketAsyncEventArgs();
		e.RemoteEndPoint = endpoint;
		e.Completed += ConnectCompleted;
		e.UserToken = tcs;

		try
		{
			if (!socket.ConnectAsync(e))
			{
				// Request might finish synchronously
				ProcessConnect(e);
			}
		}
		catch (Exception ex)
		{
			tcs.SetException(new CommunicationObjectAbortedException(AbortedPhase.OpenAttempt, "Failed to open connection.", ex));
		}

		return tcs.Task;
	}

	private void ConnectCompleted(object sender, SocketAsyncEventArgs e)
	{
		ProcessConnect(e);
	}

	private void ProcessConnect(SocketAsyncEventArgs e)
	{
		TaskCompletionSource<bool> tcs = (TaskCompletionSource<bool>)e.UserToken;

		sync.EnterWriteLock();

		try
		{
			if (state == ConnectionState.Closed)
			{
				tcs.SetException(new ObjectDisposedException("Connection has been closed.", new SocketException((int)e.SocketError)));
				return;
			}

			if (e.SocketError != SocketError.Success)
			{
				state = ConnectionState.Closed;
				failedToOpen = true;
				tcs.SetException(new CommunicationObjectAbortedException(AbortedPhase.OpenAttempt, "Failed to open connection.",
					new SocketException((int)e.SocketError)));

				return;
			}

			state = ConnectionState.Opened;
			tcs.SetResult(true);

			// We immediately start receiving data on this connection.
			StartReceivingAsync();
		}
		finally
		{
			e.Dispose();
			sync.ExitWriteLock();
		}
	}
}
