using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using VeloxDB.Common;

namespace VeloxDB.Networking;

internal sealed partial class ClientConnection : Connection
{
	IPEndPoint endpoint;
	SslClientAuthenticationOptions sslOptions;
	bool failedToOpen;

	public unsafe ClientConnection(IPEndPoint endpoint, MessageChunkPool chunkPool, TimeSpan inactivityInterval,
		TimeSpan inactivityTimeout, int maxQueuedChunkCount, bool groupSmallMessages, HandleMessageDelegate messageHandler,
		JobWorkers<Action> priorityWorkers = null, SslClientAuthenticationOptions sslOptions = null) :
		base(chunkPool, inactivityInterval, inactivityTimeout, maxQueuedChunkCount, groupSmallMessages, messageHandler,
		     priorityWorkers)
	{
		this.endpoint = endpoint;
		this.sslOptions = sslOptions;
	}

	public bool FailedToOpen => failedToOpen;

	public override ulong MessageIdBit => 0x0000000000000000;

	protected override bool IsResponseMessage(ulong msgId)
	{
		Checker.AssertFalse(msgId == 0);
		return (msgId & 0x8000000000000000) == 0x0000000000000000;
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

	private class SocketAsyncEventArgsEx : SocketAsyncEventArgs
	{
		public Exception Exception { get; set; }
	}

	private Task ConnectAsync()
	{
		socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
		socket.SendTimeout = Timeout.Infinite;
		if (inactivityTimeout != TimeSpan.MaxValue)
			NativeSocket.TurnOnKeepAlive(socket.Handle, inactivityInterval, inactivityTimeout);

		socket.NoDelay = false;
		socket.ReceiveBufferSize = tcpBufferSize;
		socket.SendBufferSize = tcpBufferSize;

		TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

		SocketAsyncEventArgs e = new SocketAsyncEventArgsEx();
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

				Exception exc = ((SocketAsyncEventArgsEx)e).Exception;
				if (exc == null)
				{
					tcs.SetException(new CommunicationObjectAbortedException(AbortedPhase.OpenAttempt, "Failed to open connection.",
									 new SocketException((int)e.SocketError)));
				}else
				{
					tcs.SetException(new AuthenticationFailedException(inner:exc));
				}

				return;
			}

			if(stream == null)
				CreateStream();

			if (sslOptions != null)
			{
				SslStream sslStream = (SslStream)stream;
				if (!sslStream.IsAuthenticated)
				{
					sslStream.AuthenticateAsClientAsync(sslOptions).ContinueWith((t)=>
					{
						if (t.Exception != null)
						{
							((SocketAsyncEventArgsEx)e).Exception = t.Exception.InnerException;
							e.SocketError = SocketError.ConnectionRefused;
						}
						ProcessConnect(e);
					});
					return;
				}
			}

			state = ConnectionState.Opened;
			tcs.SetResult(true);

			// We immediately start receiving data on this connection.
			StartReceiving();
		}
		finally
		{
			e.Dispose();
			sync.ExitWriteLock();
		}
	}

	private void CreateStream()
	{
		Checker.AssertNotNull(socket);
		Stream stream = new NetworkStream(socket);
		if(sslOptions != null)
		{
			stream = new SslStream(stream);
		}

		this.stream = stream;
	}

}
