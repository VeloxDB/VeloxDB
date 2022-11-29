using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Velox.Common;

namespace Velox.Networking;

internal delegate void MessageWriterDelegate(MessageWriter writer);
internal delegate void HandleMessageDelegate(Connection connection, long requestId, MessageReader reader);
internal delegate void HandleResponseDelegate(Connection connection, object state, Exception error, MessageReader reader);
internal delegate void SerializerDelegate(MessageWriter writer);
internal delegate void SerializerDelegate<T1>(MessageWriter writer, T1 value1);
internal delegate void SerializerDelegate<T1, T2>(MessageWriter writer, T1 value1, T2 value2);
internal delegate void SerializerDelegate<T1, T2, T3>(MessageWriter writer, T1 value1, T2 value2, T3 value3);
internal delegate void SerializerDelegate<T1, T2, T3, T4>(MessageWriter writer, T1 value1, T2 value2, T3 value3, T4 value4);
internal delegate void SerializerDelegate<T1, T2, T3, T4, T5>(MessageWriter writer, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5);
internal delegate void SerializerDelegate<T1, T2, T3, T4, T5, T6>(MessageWriter writer, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6);
internal delegate void SerializerDelegate<T1, T2, T3, T4, T5, T6, T7>(MessageWriter writer, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7);
internal delegate void SerializerDelegate<T1, T2, T3, T4, T5, T6, T7, T8>(MessageWriter writer, T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8);
internal delegate T DeserializerDelegate<T>(MessageReader reader);

internal enum ConnectionState
{
	Created = 1,
	Opened = 2,
	Closed = 3
}

internal abstract class Connection
{
	public const int AwaiterPoolCount = 128;
	public const int MaxRequestArguments = 8;

	protected RWSpinLock sync;

	protected Socket socket;

	// This should be internal AND protected (or just protected, but not possible in C# because ConnectionState is internal)
	internal ConnectionState state;

	protected TimeSpan inactivityInterval;
	protected TimeSpan inactivityTimeout;

	readonly object receiveSync = new object();
	bool isAsyncReceiveStarted;
	volatile bool isReceiving;

	volatile bool closeRequested;
	HandleMessageDelegate messageHandler;

	long messageId;

	IncompleteMessageCollection incompleteMessages;
	ItemPool<MessageChunk> largeChunkPool;
	ItemPool<MessageChunk> smallChunkPool;
	ProcessChunkDelegate socketSender;

	MessageReaderCallback readerCallback;
	PendingRequests pendingRequests;

	WaitCallback receiveExecutor;

	SendWorker sendWorker;

	object tag;

	public event Action Closed;
	public event Action<HandleResponseDelegate> ResponseProcessed;

	internal unsafe Connection(int bufferPoolSize, TimeSpan inactivityInterval, TimeSpan inactivityTimeout,
		int maxQueuedChunkCount, bool groupSmallMessages, HandleMessageDelegate messageHandler)
	{
		this.inactivityInterval = inactivityInterval;
		this.inactivityTimeout = inactivityTimeout;
		this.state = ConnectionState.Created;
		this.messageHandler = messageHandler;

		socketSender = SendChunk;
		isReceiving = true;

		messageId = BaseMsgId;

		sync = new RWSpinLock();

		int largeChunkCount = Math.Max(4, (int)(bufferPoolSize * 0.8) / MessageChunk.LargeBufferSize);
		int smallChunkCount = Math.Max(16, (int)(bufferPoolSize * 0.2) / MessageChunk.LargeBufferSize);

		smallChunkPool = new ItemPool<MessageChunk>(smallChunkCount,
			new MessageChunkFactory(ReceiveCompleted, MessageChunk.SmallBufferSize));

		largeChunkPool = new ItemPool<MessageChunk>(largeChunkCount,
			new MessageChunkFactory(ReceiveCompleted, MessageChunk.LargeBufferSize));

		if (groupSmallMessages)
			sendWorker = new SendWorker(this);

		incompleteMessages = new IncompleteMessageCollection(smallChunkPool,
			largeChunkPool, maxQueuedChunkCount, StopReceiving, ContinueReceiving);

		readerCallback = new MessageReaderCallback(ProvideNextMessageChunk);
		receiveExecutor = s => ReceiveCompleted(socket, (SocketAsyncEventArgs)s);

		pendingRequests = new PendingRequests(this);
	}

	public Connection(Socket socket, int bufferPoolSize, TimeSpan inactivityInterval,
		TimeSpan inactivityTimeout, int maxQueuedChunkCount, bool groupSmallMessages, HandleMessageDelegate messageHandler) :
		this(bufferPoolSize, inactivityInterval, inactivityTimeout, maxQueuedChunkCount, groupSmallMessages, messageHandler)
	{
		this.socket = socket;
		state = ConnectionState.Opened;
	}

	public ConnectionState State => state;

	protected abstract long BaseMsgId { get; }
	public object Tag { get => tag; set => tag = value; }

	public bool TrySetTagIfNull(object tag)
	{
		return Interlocked.CompareExchange(ref this.tag, tag, null) == null;
	}

	protected abstract bool IsResponseMessage(long msgId);

	public void SendMessage(SerializerDelegate serializer)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, currMsgId, null, null);
	}

	public void SendMessage<T1>(SerializerDelegate<T1> serializer, T1 value1)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, currMsgId, null, null);
	}

	public void SendMessage<T1, T2>(SerializerDelegate<T1, T2> serializer, T1 value1, T2 value2)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, currMsgId, null, null);
	}

	public void SendMessage<T1, T2, T3>(SerializerDelegate<T1, T2, T3> serializer, T1 value1, T2 value2, T3 value3)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, value3, currMsgId, null, null);
	}

	public void SendMessage<T1, T2, T3, T4>(SerializerDelegate<T1, T2, T3, T4> serializer, T1 value1, T2 value2, T3 value3, T4 value4)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, value3, value4, currMsgId, null, null);
	}

	public void SendMessage<T1, T2, T3, T4, T5>(SerializerDelegate<T1, T2, T3, T4, T5> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, currMsgId, null, null);
	}

	public void SendMessage<T1, T2, T3, T4, T5, T6>(SerializerDelegate<T1, T2, T3, T4, T5, T6> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, value6, currMsgId, null, null);
	}

	public void SendMessage<T1, T2, T3, T4, T5, T6, T7>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, value6, value7, currMsgId, null, null);
	}

	public void SendMessage<T1, T2, T3, T4, T5, T6, T7, T8>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7, T8> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, value6, value7, value8, currMsgId, null, null);
	}

	public void SendRequest(SerializerDelegate serializer, HandleResponseDelegate responseCallback, object responseState)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, currMsgId, responseCallback, responseState);
	}

	public void SendRequest<T1>(SerializerDelegate<T1> serializer, T1 value1, HandleResponseDelegate responseCallback, object responseState)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, currMsgId, responseCallback, responseState);
	}

	public void SendRequest<T1, T2>(SerializerDelegate<T1, T2> serializer, T1 value1, T2 value2,
		HandleResponseDelegate responseCallback, object responseState)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, currMsgId, responseCallback, responseState);
	}

	public void SendRequest<T1, T2, T3>(SerializerDelegate<T1, T2, T3> serializer, T1 value1, T2 value2, T3 value3,
		HandleResponseDelegate responseCallback, object responseState)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, value3, currMsgId, responseCallback, responseState);
	}

	public void SendRequest<T1, T2, T3, T4>(SerializerDelegate<T1, T2, T3, T4> serializer, T1 value1, T2 value2, T3 value3, T4 value4,
		HandleResponseDelegate responseCallback, object responseState)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, value3, value4, currMsgId, responseCallback, responseState);
	}

	public void SendRequest<T1, T2, T3, T4, T5>(SerializerDelegate<T1, T2, T3, T4, T5> serializer, T1 value1, T2 value2,
		T3 value3, T4 value4, T5 value5, HandleResponseDelegate responseCallback, object responseState)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, currMsgId, responseCallback, responseState);
	}

	public void SendRequest<T1, T2, T3, T4, T5, T6>(SerializerDelegate<T1, T2, T3, T4, T5, T6> serializer, T1 value1, T2 value2,
		T3 value3, T4 value4, T5 value5, T6 value6, HandleResponseDelegate responseCallback, object responseState)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, value6, currMsgId, responseCallback, responseState);
	}

	public void SendRequest<T1, T2, T3, T4, T5, T6, T7>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7> serializer, T1 value1, T2 value2,
		T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, HandleResponseDelegate responseCallback, object responseState)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, value6, value7, currMsgId, responseCallback, responseState);
	}

	public void SendRequest<T1, T2, T3, T4, T5, T6, T7, T8>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7, T8> serializer, T1 value1,
		T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8,
		HandleResponseDelegate responseCallback, object responseState)
	{
		long currMsgId = Interlocked.Increment(ref messageId);
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, value6, value7, value8, currMsgId, responseCallback, responseState);
	}

	public void SendResponse(SerializerDelegate serializer, long requestId)
	{
		try
		{
			SendMessageInternal(serializer, requestId, null, null);
		}
		catch (Exception e) when (e is ObjectDisposedException || e is CommunicationObjectAbortedException || e is CommunicationException)
		{
		}
	}

	public void SendResponse<T1>(SerializerDelegate<T1> serializer, T1 value1, long requestId)
	{
		try
		{
			SendMessageInternal(serializer, value1, requestId, null, null);
		}
		catch (Exception e) when (e is ObjectDisposedException || e is CommunicationObjectAbortedException || e is CommunicationException)
		{
		}
	}

	public void SendResponse<T1, T2>(SerializerDelegate<T1, T2> serializer, T1 value1, T2 value2, long requestId)
	{
		try
		{
			SendMessageInternal(serializer, value1, value2, requestId, null, null);
		}
		catch (Exception e) when (e is ObjectDisposedException || e is CommunicationObjectAbortedException || e is CommunicationException)
		{
		}
	}

	public void SendResponse<T1, T2, T3>(SerializerDelegate<T1, T2, T3> serializer, T1 value1, T2 value2, T3 value3, long requestId)
	{
		try
		{
			SendMessageInternal(serializer, value1, value2, value3, requestId, null, null);
		}
		catch (Exception e) when (e is ObjectDisposedException || e is CommunicationObjectAbortedException || e is CommunicationException)
		{
		}
	}

	public void SendResponse<T1, T2, T3, T4>(SerializerDelegate<T1, T2, T3, T4> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, long requestId)
	{
		try
		{
			SendMessageInternal(serializer, value1, value2, value3, value4, requestId, null, null);
		}
		catch (Exception e) when (e is ObjectDisposedException || e is CommunicationObjectAbortedException || e is CommunicationException)
		{
		}
	}

	public void SendResponse<T1, T2, T3, T4, T5>(SerializerDelegate<T1, T2, T3, T4, T5> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, long requestId)
	{
		try
		{
			SendMessageInternal(serializer, value1, value2, value3, value4, value5, requestId, null, null);
		}
		catch (Exception e) when (e is ObjectDisposedException || e is CommunicationObjectAbortedException || e is CommunicationException)
		{
		}
	}

	private unsafe void SendMessageInternal(SerializerDelegate serializer, long msgId,
		HandleResponseDelegate responseCallback, object responseState)
	{
		sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = smallChunkPool.Get();
			MessageWriter writer = chunk.Writer;

			try
			{
				if (responseCallback != null)
					pendingRequests.Add(msgId, new PendingRequests.PendingRequest(responseCallback, responseState));

				writer.Init(chunk, chunk.PBuffer, chunk.BufferSize, socketSender, msgId);
				serializer.Invoke(writer);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(msgId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
		}
		finally
		{
			sync.ExitReadLock();
		}
	}

	private unsafe void SendMessageInternal<T1>(SerializerDelegate<T1> serializer, T1 value1, long msgId,
		HandleResponseDelegate responseCallback, object responseState)
	{
		sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = smallChunkPool.Get();
			MessageWriter writer = chunk.Writer;

			try
			{
				if (responseCallback != null)
					pendingRequests.Add(msgId, new PendingRequests.PendingRequest(responseCallback, responseState));

				writer.Init(chunk, chunk.PBuffer, chunk.BufferSize, socketSender, msgId);
				serializer.Invoke(writer, value1);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(msgId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
		}
		finally
		{
			sync.ExitReadLock();
		}
	}

	private unsafe void SendMessageInternal<T1, T2>(SerializerDelegate<T1, T2> serializer, T1 value1, T2 value2,
		long msgId, HandleResponseDelegate responseCallback, object responseState)
	{
		sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = smallChunkPool.Get();
			MessageWriter writer = chunk.Writer;

			try
			{
				if (responseCallback != null)
					pendingRequests.Add(msgId, new PendingRequests.PendingRequest(responseCallback, responseState));

				writer.Init(chunk, chunk.PBuffer, chunk.BufferSize, socketSender, msgId);
				serializer.Invoke(writer, value1, value2);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(msgId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
		}
		finally
		{
			sync.ExitReadLock();
		}
	}

	private unsafe void SendMessageInternal<T1, T2, T3>(SerializerDelegate<T1, T2, T3> serializer, T1 value1, T2 value2, T3 value3,
		long msgId, HandleResponseDelegate responseCallback, object responseState)
	{
		sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = smallChunkPool.Get();
			MessageWriter writer = chunk.Writer;

			try
			{
				if (responseCallback != null)
					pendingRequests.Add(msgId, new PendingRequests.PendingRequest(responseCallback, responseState));

				writer.Init(chunk, chunk.PBuffer, chunk.BufferSize, socketSender, msgId);
				serializer.Invoke(writer, value1, value2, value3);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(msgId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
		}
		finally
		{
			sync.ExitReadLock();
		}
	}

	private unsafe void SendMessageInternal<T1, T2, T3, T4>(SerializerDelegate<T1, T2, T3, T4> serializer, T1 value1, T2 value2,
		T3 value3, T4 value4, long msgId, HandleResponseDelegate responseCallback, object responseState)
	{
		sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = smallChunkPool.Get();
			MessageWriter writer = chunk.Writer;

			try
			{
				if (responseCallback != null)
					pendingRequests.Add(msgId, new PendingRequests.PendingRequest(responseCallback, responseState));

				writer.Init(chunk, chunk.PBuffer, chunk.BufferSize, socketSender, msgId);
				serializer.Invoke(writer, value1, value2, value3, value4);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(msgId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
		}
		finally
		{
			sync.ExitReadLock();
		}
	}

	private unsafe void SendMessageInternal<T1, T2, T3, T4, T5>(SerializerDelegate<T1, T2, T3, T4, T5> serializer, T1 value1,
		T2 value2, T3 value3, T4 value4, T5 value5, long msgId, HandleResponseDelegate responseCallback, object responseState)
	{
		sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = smallChunkPool.Get();
			MessageWriter writer = chunk.Writer;

			try
			{
				if (responseCallback != null)
					pendingRequests.Add(msgId, new PendingRequests.PendingRequest(responseCallback, responseState));

				writer.Init(chunk, chunk.PBuffer, chunk.BufferSize, socketSender, msgId);
				serializer.Invoke(writer, value1, value2, value3, value4, value5);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(msgId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
		}
		finally
		{
			sync.ExitReadLock();
		}
	}

	private unsafe void SendMessageInternal<T1, T2, T3, T4, T5, T6>(SerializerDelegate<T1, T2, T3, T4, T5, T6> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6,
		long msgId, HandleResponseDelegate responseCallback, object responseState)
	{
		sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = smallChunkPool.Get();
			MessageWriter writer = chunk.Writer;

			try
			{
				if (responseCallback != null)
					pendingRequests.Add(msgId, new PendingRequests.PendingRequest(responseCallback, responseState));

				writer.Init(chunk, chunk.PBuffer, chunk.BufferSize, socketSender, msgId);
				serializer.Invoke(writer, value1, value2, value3, value4, value5, value6);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(msgId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
		}
		finally
		{
			sync.ExitReadLock();
		}
	}

	private unsafe void SendMessageInternal<T1, T2, T3, T4, T5, T6, T7>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7,
		long msgId, HandleResponseDelegate responseCallback, object responseState)
	{
		sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = smallChunkPool.Get();
			MessageWriter writer = chunk.Writer;

			try
			{
				if (responseCallback != null)
					pendingRequests.Add(msgId, new PendingRequests.PendingRequest(responseCallback, responseState));

				writer.Init(chunk, chunk.PBuffer, chunk.BufferSize, socketSender, msgId);
				serializer.Invoke(writer, value1, value2, value3, value4, value5, value6, value7);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(msgId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
		}
		finally
		{
			sync.ExitReadLock();
		}
	}

	private unsafe void SendMessageInternal<T1, T2, T3, T4, T5, T6, T7, T8>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7, T8> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8,
		long msgId, HandleResponseDelegate responseCallback, object responseState)
	{
		sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = smallChunkPool.Get();
			MessageWriter writer = chunk.Writer;

			try
			{
				if (responseCallback != null)
					pendingRequests.Add(msgId, new PendingRequests.PendingRequest(responseCallback, responseState));

				writer.Init(chunk, chunk.PBuffer, chunk.BufferSize, socketSender, msgId);
				serializer.Invoke(writer, value1, value2, value3, value4, value5, value6, value7, value8);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(msgId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
		}
		finally
		{
			sync.ExitReadLock();
		}
	}

	private unsafe void SendChunk(int size, ref object state, ref byte* buffer, ref int capacity)
	{
		MessageChunk chunk = (MessageChunk)state;
		chunk.UpdateSize();

		if (sendWorker != null && chunk.IsTheOnlyOne && chunk.ChunkSize <= MessageChunk.SmallBufferSize)
		{
			sendWorker.Enqueue(chunk);
		}
		else
		{
			try
			{
				lock (socket)
				{
					int n = socket.Send(new ReadOnlySpan<byte>(chunk.PBuffer, size));
					if (n != size)
						throw new InvalidOperationException(); // Should never happen with blocking sockets
				}
			}
			catch (Exception e)
			{
				chunk.ReturnToPool(smallChunkPool, largeChunkPool);
				if (e is SocketException)
					throw new CommunicationException("Failed to send data over a socket.", e);

				throw;
			}
		}

		if (chunk.IsLast)
		{
			chunk.ReturnToPool(smallChunkPool, largeChunkPool);
		}
		else if (chunk.IsFirst)
		{
			MessageChunk largeChunk = largeChunkPool.Get();
			chunk.SwapWriters(largeChunk);
			smallChunkPool.Put(chunk);
			state = largeChunk;
			buffer = largeChunk.PBuffer;
			capacity = largeChunk.BufferSize;
		}
	}

	private void ContinueReceiving()
	{
		isReceiving = true;
		StartReceivingAsync();
	}

	private void StopReceiving()
	{
		isReceiving = false;
	}

	protected void StartReceivingAsync()
	{
		if (!isReceiving)
			return;

		lock (receiveSync)
		{
			if (isAsyncReceiveStarted)
				return;

			MessageChunk chunk = largeChunkPool.Get();
			isAsyncReceiveStarted = true;
			if (socket.ReceiveAsync(chunk.Args))
				return;

			ThreadPool.UnsafeQueueUserWorkItem(receiveExecutor, chunk.Args);
		}
	}

	private unsafe void ReceiveCompleted(object sender, SocketAsyncEventArgs args)
	{
		MessageChunk chunk = (MessageChunk)args.UserToken;

		sync.EnterReadLock();

		try
		{
			try
			{
				if (state == ConnectionState.Closed)
				{
					args.Dispose();
					return;
				}

				while (true)
				{
					if (args.SocketError != SocketError.Success || args.BytesTransferred == 0)
						throw new SocketException((int)args.SocketError);

					int received = args.BytesTransferred + args.Offset;
					byte* p = chunk.PBuffer;
					while (true)
					{
						if (received < sizeof(int))
						{
							PrepareAfterPartialReceive(args, chunk, p, received);
							break;
						}

						int chunkSize = *(int*)p;
						if (received < chunkSize)
						{
							PrepareAfterPartialReceive(args, chunk, p, received);
							break;
						}

						ExtractChunk(out MessageChunk extracted, ref received, ref p);
						PreProcessMessage(extracted);
					}

					if (socket.ReceiveAsync(args))
						return;
				}
			}
			finally
			{
				sync.ExitReadLock();
			}
		}
		catch (CorruptMessageException)
		{
			Task.Run(() => CloseAsyncInternal());
			return;
		}
		catch (Exception e) when (e is SocketException || e is UnsupportedHeaderException)
		{
			if (chunk.IsFirst)
				chunk.ReturnToPool(smallChunkPool, largeChunkPool);

			Task.Run(() => CloseAsyncInternal());
			return;
		}
	}

	private unsafe void ExtractChunk(out MessageChunk extracted, ref int received, ref byte* p)
	{
		int size = *(int*)p;
		if (size <= MessageChunk.SmallBufferSize)
		{
			extracted = smallChunkPool.Get();
		}
		else
		{
			extracted = largeChunkPool.Get();
		}

		Utils.CopyMemory(p, extracted.PBuffer, size);
		p += size;
		received -= size;
		extracted.UpdateSize();
		extracted.ReadHeader();
	}

	private unsafe void PrepareAfterPartialReceive(SocketAsyncEventArgs args, MessageChunk chunk, byte* p, int received)
	{
		if (p != chunk.PBuffer && received > 0)
			Utils.CopyMemory(p, chunk.PBuffer, received);

		args.SetBuffer(received, MessageChunk.LargeBufferSize - received);
	}

	private void PreProcessMessage(MessageChunk chunk)
	{
		PendingRequests.PendingRequest pendingRequest = new PendingRequests.PendingRequest();
		ChunkAwaiter nextChunkAwaiter = incompleteMessages.ChunkReceived(chunk, out bool chunkConsumed);

		if (chunkConsumed)
			return;

		if (IsResponseMessage(chunk.MessageId))
		{
			if (chunk.IsFirst && !pendingRequests.TryRemove(chunk.MessageId, out pendingRequest))
				throw new CorruptMessageException();
		}
		else
		{
			if (messageHandler == null)
				throw new CorruptMessageException();
		}

		if (chunk.IsFirst)
			ThreadPool.UnsafeQueueUserWorkItem(x => ProcessMessage(chunk, nextChunkAwaiter, pendingRequest), null);
	}

	private unsafe void ProcessMessage(MessageChunk chunk, ChunkAwaiter nextChunkAwaiter, PendingRequests.PendingRequest pendingRequest)
	{
		chunk.Awaiter = nextChunkAwaiter;
		MessageReader reader = chunk.Reader;

		reader.Init(chunk, chunk.PBuffer, chunk.HeaderSize, chunk.ChunkSize, chunk.BufferSize, readerCallback);

		try
		{
			if (pendingRequest.Callback != null)
			{
				try
				{
					pendingRequest.Callback(this, pendingRequest.State, null, reader);
				}
				finally
				{
					ResponseProcessed?.Invoke(pendingRequest.Callback);
				}
			}
			else
			{
				messageHandler(this, chunk.MessageId, reader);
			}

			MessageChunk lastChunk = (MessageChunk)reader.State;
			Checker.AssertTrue(object.ReferenceEquals(lastChunk.Reader, reader));
			lastChunk.ReturnToPool(smallChunkPool, largeChunkPool);
		}
		catch (Exception e) when (e is ObjectDisposedException || e is SocketException ||
			e is CommunicationObjectAbortedException || e is CorruptMessageException || e is ChunkTimeoutException)
		{
			Task.Run(() => CloseAsyncInternal());
		}
	}

	private unsafe void ProvideNextMessageChunk(object state, out object newState, out byte* newBuffer, out int newOffset, out int newCapacity)
	{
		MessageChunk currChunk = (MessageChunk)state;

		sync.EnterReadLock();

		try
		{
			if (this.state == ConnectionState.Closed)
				throw CreateClosedException();

			if (currChunk.IsLast)
				throw new CorruptMessageException();

			incompleteMessages.WaitNextChunk(currChunk, out MessageChunk nextChunk, out ChunkAwaiter nextChunkAwaiter);

			nextChunk.SwapReaders(currChunk);
			currChunk.ReturnToPool(smallChunkPool, largeChunkPool);

			nextChunk.Awaiter = nextChunkAwaiter;
			newState = nextChunk;
			newBuffer = nextChunk.PBuffer;
			newOffset = nextChunk.HeaderSize;
			newCapacity = nextChunk.ChunkSize;
		}
		finally
		{
			sync.ExitReadLock();
		}
	}

	protected Exception CreateClosedException()
	{
		if (closeRequested)
			return new ObjectDisposedException(this.GetType().Name);

		return new CommunicationObjectAbortedException(AbortedPhase.Communication);
	}

	public void AddClosedHandlerSafe(Action handler)
	{
		sync.EnterWriteLock();

		try
		{
			if (state != ConnectionState.Closed)
			{
				Closed += handler;
				return;
			}
		}
		finally
		{
			sync.ExitWriteLock();
		}

		handler.Invoke();
	}

	public void Close()
	{
		CloseAsync().Wait();
	}

	public Task CloseAsync()
	{
		closeRequested = true;
		return CloseAsyncInternal();
	}

	private Task CloseAsyncInternal()
	{
		TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();

		incompleteMessages.AbortWaiters();

		sync.EnterWriteLock();

		try
		{
			if (state == ConnectionState.Closed)
			{
				tcs.SetResult(true);
				return tcs.Task;
			}

			if (state != ConnectionState.Opened)
			{
				tcs.SetException(new ConnectionNotOpenException());
				return tcs.Task;
			}

			state = ConnectionState.Closed;
		}
		finally
		{
			sync.ExitWriteLock();
		}

		pendingRequests.Close(this, CreateClosedException());
		incompleteMessages.Dispose();
		smallChunkPool.Close();
		largeChunkPool.Close();

		SocketAsyncEventArgs e = new SocketAsyncEventArgs();
		e.UserToken = tcs;
		e.Completed += DisconnectCompleted;

		try
		{
			if (!socket.DisconnectAsync(e))
			{
				DisconnectCompleted(socket, e);
			}
		}
		catch (Exception ex)
		{
			tcs.SetException(ex);
		}

		sendWorker?.Dispose();

		return tcs.Task;
	}

	protected virtual void OnDisconnected()
	{
	}

	private void DisconnectCompleted(object sender, SocketAsyncEventArgs e)
	{
		((TaskCompletionSource<bool>)e.UserToken).SetResult(true);
		sync.EnterWriteLock();

		try
		{
			socket.Close();
			OnDisconnected();
		}
		finally
		{
			e.Dispose();
			sync.ExitWriteLock();
		}

		Task.Run(() => Closed?.Invoke());
	}

	public void Dispose()
	{
		Close();
	}

	public static IPEndPoint TranslateAddress(string addressAndPort, bool isAnyAddress, bool preferIPV6)
	{
		return TranslateMultiAddress(addressAndPort, isAnyAddress, preferIPV6)[0];
	}

	public static IPEndPoint[] TranslateMultiAddress(string addressAndPort, bool isAnyAddress, bool preferIPV6)
	{
		return TranslateMultiAddressAsync(addressAndPort, isAnyAddress, preferIPV6).Result;
	}

	public static async Task<IPEndPoint[]> TranslateMultiAddressAsync(string addressAndPort, bool isAnyAddress, bool preferIPV6)
	{
		Checker.AssertNotNullOrWhitespace(addressAndPort);

		string address = null;
		if (!addressAndPort.Contains(":"))
			throw new ArgumentException("Invalid address.");

		string[] parts = addressAndPort.Split(':');
		if (parts.Length != 2)
			throw new ArgumentException("Invalid address.");

		address = parts[0];
		if (!ushort.TryParse(parts[1], out ushort port))
			throw new ArgumentException("Invalid address.");

		if (isAnyAddress || address.Equals("0.0.0.0"))
			return new IPEndPoint[] { new IPEndPoint(IPAddress.Any, port) };

		IPEndPoint[] endpoints = null;
		AddressFamily faimly = preferIPV6 ? AddressFamily.InterNetworkV6 : AddressFamily.InterNetwork;

		try
		{
			IPAddress[] addresses = await Dns.GetHostAddressesAsync(address);
			endpoints = addresses.Where(x => x.AddressFamily == faimly).Select(x => new IPEndPoint(x, port)).ToArray();
		}
		catch (ArgumentOutOfRangeException)
		{
			throw new ArgumentException("Service address is greater than 255 characters in length.");
		}
		catch (ArgumentException)
		{
			throw new ArgumentException("Invalid IP address.");
		}
		catch (SocketException e)
		{
			throw new CommunicationException(e);
		}

		if (endpoints.Length == 0)
			throw new ArgumentException("Failed to obtain IPv4 address for a given host name.");

		return endpoints;
	}

	private sealed class PendingRequests
	{
		readonly object sync = new object();
		Connection owner;
		Dictionary<long, PendingRequest> map;

		public PendingRequests(Connection owner)
		{
			this.owner = owner;
			map = new Dictionary<long, PendingRequest>(8);
		}

		public void Add(long msgId, PendingRequest request)
		{
			lock (sync)
			{
				map.Add(msgId, request);
			}
		}

		public bool TryRemove(long msgId, out PendingRequest request)
		{
			lock (sync)
			{
				return map.Remove(msgId, out request);
			}
		}

		public void Close(Connection conn, Exception e)
		{
			lock (sync)
			{
				foreach (KeyValuePair<long, PendingRequest> pendingRequest in map)
				{
					ThreadPool.UnsafeQueueUserWorkItem(x => pendingRequest.Value.Callback(conn, pendingRequest.Value.State, e, null), null);
				}

				map.Clear();
				map = null;
			}
		}

		public struct PendingRequest
		{
			public HandleResponseDelegate Callback { get; private set; }
			public object State { get; private set; }

			public PendingRequest(HandleResponseDelegate callback, object state)
			{
				this.Callback = callback;
				this.State = state;
			}
		}
	}

	private unsafe sealed class SendWorker
	{
		readonly object sync = new object();
		ManualResetEventSlim freeSignal;
		SemaphoreSlim readySignal;

		Connection owner;

		Thread thread;

		int offset;
		byte* fillBuffer;
		byte* sendBuffer;

		bool disposed;

		public SendWorker(Connection owner)
		{
			this.owner = owner;

			freeSignal = new ManualResetEventSlim(true);
			readySignal = new SemaphoreSlim(0);

			fillBuffer = (byte*)NativeAllocator.Allocate(MessageChunk.LargeBufferSize);
			sendBuffer = (byte*)NativeAllocator.Allocate(MessageChunk.LargeBufferSize);

			thread = new Thread(Sender);
			thread.IsBackground = true;
			thread.Name = "SocketSender";
			thread.Start();
		}

		public void Enqueue(MessageChunk chunk)
		{
			Checker.AssertFalse(disposed);  // Since we are comming through a connection lock
			Checker.AssertTrue(chunk.ChunkSize < MessageChunk.SmallBufferSize);

			while (true)
			{
				freeSignal.Wait();
				Monitor.Enter(sync);

				if (disposed)
				{
					Monitor.Exit(sync);
					return;
				}

				if (MessageChunk.LargeBufferSize - offset >= MessageChunk.SmallBufferSize)
					break;

				Monitor.Exit(sync);
			}

			bool fireReady = false;
			try
			{
				Utils.CopyMemory(chunk.PBuffer, fillBuffer + offset, chunk.ChunkSize);
				fireReady = offset == 0;
				offset += chunk.ChunkSize;

				if (MessageChunk.LargeBufferSize - offset < MessageChunk.SmallBufferSize)
					freeSignal.Reset();
			}
			finally
			{
				Monitor.Exit(sync);
			}

			if (fireReady)
				readySignal.Release();
		}

		private void Sender()
		{
			while (true)
			{
				int sendSize;
				readySignal.Wait();
				lock (sync)
				{
					if (disposed)
						return;

					bool full = MessageChunk.LargeBufferSize - offset < MessageChunk.SmallBufferSize;

					Checker.AssertFalse(offset == 0);
					Utils.Exchange(ref fillBuffer, ref sendBuffer);
					sendSize = offset;
					offset = 0;

					if (full)
						freeSignal.Set();
				}

				Socket socket = owner.socket;
				lock (socket)
				{
					try
					{
						int n = socket.Send(new ReadOnlySpan<byte>(sendBuffer, sendSize));
						if (n != sendSize)
							throw new InvalidOperationException(); // Should never happen with blocking sockets
					}
					catch
					{
						Task.Run(() => owner.CloseAsync());
						return;
					}
				}
			}
		}

		public void Dispose()
		{
			lock (sync)
			{
				if (disposed)
					return;

				disposed = true;
			}

			readySignal.Release();
			freeSignal.Set();

			NativeAllocator.Free((IntPtr)fillBuffer);
			NativeAllocator.Free((IntPtr)sendBuffer);

			thread.Join();
		}
	}
}
