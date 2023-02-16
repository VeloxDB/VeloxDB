using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using VeloxDB.Common;

namespace VeloxDB.Networking;

internal delegate void MessageWriterDelegate(MessageWriter writer);
internal delegate void HandleMessageDelegate(Connection connection, ulong requestId, MessageReader reader);
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
	public const int receiveGroupSize = 8;

	protected const int tcpBufferSize = 1024 * 1024 * 2;

	[ThreadStatic]
	static MessageReader readerInstance;

	[ThreadStatic]
	static MessageWriter writerInstance;

	protected MultiSpinRWLock sync;

	protected Socket socket;

	// This should be internal AND protected (or just protected, but not possible in C# because ConnectionState is internal)
	internal ConnectionState state;

	protected TimeSpan inactivityInterval;
	protected TimeSpan inactivityTimeout;

	volatile bool closeRequested;
	HandleMessageDelegate messageHandler;

	IncompleteMessageCollection incompleteMessages;
	MessageChunkPool chunkPool;

	ProcessChunkDelegate socketSenderDelegate;
	GroupingSender groupingSender;

	MessageReaderCallback readerCallback;
	PendingRequests pendingRequests;

	object tag;

	Action<ChunkRange> chunkDelegator;
	Action<byte[]> managedMemDelegator;
	Action<MessageChunk> processor;

	JobWorkers<Action> priorityWorkers;

	public event Action Closed;
	public event Action<HandleResponseDelegate> ResponseProcessed;

	internal unsafe Connection(MessageChunkPool chunkPool, TimeSpan inactivityInterval, TimeSpan inactivityTimeout,
		int maxQueuedChunkCount, bool groupSmallMessages, HandleMessageDelegate messageHandler, JobWorkers<Action> priorityWorkers)
	{
		this.inactivityInterval = inactivityInterval;
		this.inactivityTimeout = inactivityTimeout;
		this.state = ConnectionState.Created;
		this.messageHandler = messageHandler;
		this.chunkPool = chunkPool;
		this.priorityWorkers = priorityWorkers;

		socketSenderDelegate = SendChunk;
		chunkDelegator = DelegateGroupWorkItemsFromChunk;
		managedMemDelegator = DelegateGroupWorkItemsFromManagedMemory;
		processor = ProcessMessage;

		sync = new MultiSpinRWLock();

		incompleteMessages = new IncompleteMessageCollection(chunkPool, maxQueuedChunkCount, StopReceiving, ContinueReceiving);

		readerCallback = new MessageReaderCallback(ProvideNextMessageChunk);

		pendingRequests = new PendingRequests(this);
	}

	public Connection(Socket socket, MessageChunkPool chunkPool, TimeSpan inactivityInterval,
		TimeSpan inactivityTimeout, int maxQueuedChunkCount, bool groupSmallMessages, HandleMessageDelegate messageHandler,
		JobWorkers<Action> priorityWorkers) :
		this(chunkPool, inactivityInterval, inactivityTimeout, maxQueuedChunkCount, groupSmallMessages, messageHandler, priorityWorkers)
	{
		this.socket = socket;
		state = ConnectionState.Opened;
	}

	~Connection()
	{
		sync.Dispose();
	}

	public ConnectionState State => state;

	public abstract ulong MessageIdBit { get; }

	public object Tag { get => tag; set => tag = value; }

	private MessageReader ReaderInstance
	{
		get
		{
			MessageReader reader = readerInstance;
			if (reader == null)
				readerInstance = reader = new MessageReader();

			return reader;
		}
	}

	private MessageWriter WriterInstance
	{
		get
		{
			MessageWriter writer = writerInstance;
			if (writer == null)
				writerInstance = writer = new MessageWriter();

			return writer;
		}
	}

	public bool TrySetTagIfNull(object tag)
	{
		return Interlocked.CompareExchange(ref this.tag, tag, null) == null;
	}

	protected abstract bool IsResponseMessage(ulong msgId);

	public void SendMessage(SerializerDelegate serializer)
	{
		SendMessageInternal(serializer, 0, null, null);
	}

	public void SendMessage<T1>(SerializerDelegate<T1> serializer, T1 value1)
	{
		SendMessageInternal(serializer, value1, 0, null, null);
	}

	public void SendMessage<T1, T2>(SerializerDelegate<T1, T2> serializer, T1 value1, T2 value2)
	{
		SendMessageInternal(serializer, value1, value2, 0, null, null);
	}

	public void SendMessage<T1, T2, T3>(SerializerDelegate<T1, T2, T3> serializer, T1 value1, T2 value2, T3 value3)
	{
		SendMessageInternal(serializer, value1, value2, value3, 0, null, null);
	}

	public void SendMessage<T1, T2, T3, T4>(SerializerDelegate<T1, T2, T3, T4> serializer, T1 value1, T2 value2, T3 value3, T4 value4)
	{
		SendMessageInternal(serializer, value1, value2, value3, value4, 0, null, null);
	}

	public void SendMessage<T1, T2, T3, T4, T5>(SerializerDelegate<T1, T2, T3, T4, T5> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5)
	{
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, 0, null, null);
	}

	public void SendMessage<T1, T2, T3, T4, T5, T6>(SerializerDelegate<T1, T2, T3, T4, T5, T6> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6)
	{
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, value6, 0, null, null);
	}

	public void SendMessage<T1, T2, T3, T4, T5, T6, T7>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7)
	{
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, value6, value7, 0, null, null);
	}

	public void SendMessage<T1, T2, T3, T4, T5, T6, T7, T8>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7, T8> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8)
	{
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, value6, value7, value8, 0, null, null);
	}

	public void SendRequest(SerializerDelegate serializer, HandleResponseDelegate responseCallback, object responseState)
	{
		SendMessageInternal(serializer, 0, responseCallback, responseState);
	}

	public void SendRequest<T1>(SerializerDelegate<T1> serializer, T1 value1, HandleResponseDelegate responseCallback, object responseState)
	{
		SendMessageInternal(serializer, value1, 0, responseCallback, responseState);
	}

	public void SendRequest<T1, T2>(SerializerDelegate<T1, T2> serializer, T1 value1, T2 value2,
		HandleResponseDelegate responseCallback, object responseState)
	{
		SendMessageInternal(serializer, value1, value2, 0, responseCallback, responseState);
	}

	public void SendRequest<T1, T2, T3>(SerializerDelegate<T1, T2, T3> serializer, T1 value1, T2 value2, T3 value3,
		HandleResponseDelegate responseCallback, object responseState)
	{
		SendMessageInternal(serializer, value1, value2, value3, 0, responseCallback, responseState);
	}

	public void SendRequest<T1, T2, T3, T4>(SerializerDelegate<T1, T2, T3, T4> serializer, T1 value1, T2 value2, T3 value3, T4 value4,
		HandleResponseDelegate responseCallback, object responseState)
	{
		SendMessageInternal(serializer, value1, value2, value3, value4, 0, responseCallback, responseState);
	}

	public void SendRequest<T1, T2, T3, T4, T5>(SerializerDelegate<T1, T2, T3, T4, T5> serializer, T1 value1, T2 value2,
		T3 value3, T4 value4, T5 value5, HandleResponseDelegate responseCallback, object responseState)
	{
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, 0, responseCallback, responseState);
	}

	public void SendRequest<T1, T2, T3, T4, T5, T6>(SerializerDelegate<T1, T2, T3, T4, T5, T6> serializer, T1 value1, T2 value2,
		T3 value3, T4 value4, T5 value5, T6 value6, HandleResponseDelegate responseCallback, object responseState)
	{
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, value6, 0, responseCallback, responseState);
	}

	public void SendRequest<T1, T2, T3, T4, T5, T6, T7>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7> serializer, T1 value1, T2 value2,
		T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, HandleResponseDelegate responseCallback, object responseState)
	{
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, value6, value7, 0, responseCallback, responseState);
	}

	public void SendRequest<T1, T2, T3, T4, T5, T6, T7, T8>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7, T8> serializer, T1 value1,
		T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8,
		HandleResponseDelegate responseCallback, object responseState)
	{
		SendMessageInternal(serializer, value1, value2, value3, value4, value5, value6, value7, value8, 0, responseCallback, responseState);
	}

	public void SendResponse(SerializerDelegate serializer, ulong requestId)
	{
		try
		{
			SendMessageInternal(serializer, requestId, null, null);
		}
		catch (Exception e) when (e is ObjectDisposedException || e is CommunicationObjectAbortedException || e is CommunicationException)
		{
		}
	}

	public void SendResponse<T1>(SerializerDelegate<T1> serializer, T1 value1, ulong requestId)
	{
		try
		{
			SendMessageInternal(serializer, value1, requestId, null, null);
		}
		catch (Exception e) when (e is ObjectDisposedException || e is CommunicationObjectAbortedException || e is CommunicationException)
		{
		}
	}

	public void SendResponse<T1, T2>(SerializerDelegate<T1, T2> serializer, T1 value1, T2 value2, ulong requestId)
	{
		try
		{
			SendMessageInternal(serializer, value1, value2, requestId, null, null);
		}
		catch (Exception e) when (e is ObjectDisposedException || e is CommunicationObjectAbortedException || e is CommunicationException)
		{
		}
	}

	public void SendResponse<T1, T2, T3>(SerializerDelegate<T1, T2, T3> serializer, T1 value1, T2 value2, T3 value3, ulong requestId)
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
		T1 value1, T2 value2, T3 value3, T4 value4, ulong requestId)
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
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, ulong requestId)
	{
		try
		{
			SendMessageInternal(serializer, value1, value2, value3, value4, value5, requestId, null, null);
		}
		catch (Exception e) when (e is ObjectDisposedException || e is CommunicationObjectAbortedException || e is CommunicationException)
		{
		}
	}

	private unsafe void SendMessageInternal(SerializerDelegate serializer, ulong messageId,
		HandleResponseDelegate responseCallback, object responseState)
	{
		int lockHandle = sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = chunkPool.GetSmall();
			MessageWriter writer = WriterInstance;

			try
			{
				if (messageId != 0)
				{
					Checker.AssertTrue(responseCallback == null);
				}
				else
				{
					messageId = pendingRequests.Add(new PendingRequests.PendingRequest(responseCallback, responseState), lockHandle);
				}

				writer.Init(chunk, socketSenderDelegate, messageId);
				serializer.Invoke(writer);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(messageId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
			finally
			{
				writer.Reset(chunkPool);
			}
		}
		finally
		{
			sync.ExitReadLock(lockHandle);
		}
	}

	private unsafe void SendMessageInternal<T1>(SerializerDelegate<T1> serializer, T1 value1, ulong messageId,
		HandleResponseDelegate responseCallback, object responseState)
	{
		int lockHandle = sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = chunkPool.GetSmall();
			MessageWriter writer = WriterInstance;

			try
			{
				if (messageId != 0)
				{
					Checker.AssertTrue(responseCallback == null);
				}
				else
				{
					messageId = pendingRequests.Add(new PendingRequests.PendingRequest(responseCallback, responseState), lockHandle);
				}

				writer.Init(chunk, socketSenderDelegate, messageId);
				serializer.Invoke(writer, value1);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(messageId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
			finally
			{
				writer.Reset(chunkPool);
			}
		}
		finally
		{
			sync.ExitReadLock(lockHandle);
		}
	}

	private unsafe void SendMessageInternal<T1, T2>(SerializerDelegate<T1, T2> serializer, T1 value1, T2 value2,
		ulong messageId, HandleResponseDelegate responseCallback, object responseState)
	{
		int lockHandle = sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = chunkPool.GetSmall();
			MessageWriter writer = WriterInstance;

			try
			{
				if (messageId != 0)
				{
					Checker.AssertTrue(responseCallback == null);
				}
				else
				{
					messageId = pendingRequests.Add(new PendingRequests.PendingRequest(responseCallback, responseState), lockHandle);
				}

				writer.Init(chunk, socketSenderDelegate, messageId);
				serializer.Invoke(writer, value1, value2);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(messageId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
			finally
			{
				writer.Reset(chunkPool);
			}
		}
		finally
		{
			sync.ExitReadLock(lockHandle);
		}
	}

	private unsafe void SendMessageInternal<T1, T2, T3>(SerializerDelegate<T1, T2, T3> serializer, T1 value1, T2 value2, T3 value3,
		ulong messageId, HandleResponseDelegate responseCallback, object responseState)
	{
		int lockHandle = sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = chunkPool.GetSmall();
			MessageWriter writer = WriterInstance;

			try
			{
				if (messageId != 0)
				{
					Checker.AssertTrue(responseCallback == null);
				}
				else
				{
					messageId = pendingRequests.Add(new PendingRequests.PendingRequest(responseCallback, responseState), lockHandle);
				}

				writer.Init(chunk, socketSenderDelegate, messageId);
				serializer.Invoke(writer, value1, value2, value3);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(messageId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
			finally
			{
				writer.Reset(chunkPool);
			}
		}
		finally
		{
			sync.ExitReadLock(lockHandle);
		}
	}

	private unsafe void SendMessageInternal<T1, T2, T3, T4>(SerializerDelegate<T1, T2, T3, T4> serializer, T1 value1, T2 value2,
		T3 value3, T4 value4, ulong messageId, HandleResponseDelegate responseCallback, object responseState)
	{
		int lockHandle = sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = chunkPool.GetSmall();
			MessageWriter writer = WriterInstance;

			try
			{
				if (messageId != 0)
				{
					Checker.AssertTrue(responseCallback == null);
				}
				else
				{
					messageId = pendingRequests.Add(new PendingRequests.PendingRequest(responseCallback, responseState), lockHandle);
				}

				writer.Init(chunk, socketSenderDelegate, messageId);
				serializer.Invoke(writer, value1, value2, value3, value4);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(messageId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
			finally
			{
				writer.Reset(chunkPool);
			}
		}
		finally
		{
			sync.ExitReadLock(lockHandle);
		}
	}

	private unsafe void SendMessageInternal<T1, T2, T3, T4, T5>(SerializerDelegate<T1, T2, T3, T4, T5> serializer, T1 value1,
		T2 value2, T3 value3, T4 value4, T5 value5, ulong messageId, HandleResponseDelegate responseCallback, object responseState)
	{
		int lockHandle = sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = chunkPool.GetSmall();
			MessageWriter writer = WriterInstance;

			try
			{
				if (messageId != 0)
				{
					Checker.AssertTrue(responseCallback == null);
				}
				else
				{
					messageId = pendingRequests.Add(new PendingRequests.PendingRequest(responseCallback, responseState), lockHandle);
				}

				writer.Init(chunk, socketSenderDelegate, messageId);
				serializer.Invoke(writer, value1, value2, value3, value4, value5);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(messageId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
			finally
			{
				writer.Reset(chunkPool);
			}
		}
		finally
		{
			sync.ExitReadLock(lockHandle);
		}
	}

	private unsafe void SendMessageInternal<T1, T2, T3, T4, T5, T6>(SerializerDelegate<T1, T2, T3, T4, T5, T6> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6,
		ulong messageId, HandleResponseDelegate responseCallback, object responseState)
	{
		int lockHandle = sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = chunkPool.GetSmall();
			MessageWriter writer = WriterInstance;

			try
			{
				if (messageId != 0)
				{
					Checker.AssertTrue(responseCallback == null);
				}
				else
				{
					messageId = pendingRequests.Add(new PendingRequests.PendingRequest(responseCallback, responseState), lockHandle);
				}

				writer.Init(chunk, socketSenderDelegate, messageId);
				serializer.Invoke(writer, value1, value2, value3, value4, value5, value6);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(messageId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
			finally
			{
				writer.Reset(chunkPool);
			}
		}
		finally
		{
			sync.ExitReadLock(lockHandle);
		}
	}

	private unsafe void SendMessageInternal<T1, T2, T3, T4, T5, T6, T7>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7,
		ulong messageId, HandleResponseDelegate responseCallback, object responseState)
	{
		int lockHandle = sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = chunkPool.GetSmall();
			MessageWriter writer = WriterInstance;

			try
			{
				if (messageId != 0)
				{
					Checker.AssertTrue(responseCallback == null);
				}
				else
				{
					messageId = pendingRequests.Add(new PendingRequests.PendingRequest(responseCallback, responseState), lockHandle);
				}

				writer.Init(chunk, socketSenderDelegate, messageId);
				serializer.Invoke(writer, value1, value2, value3, value4, value5, value6, value7);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(messageId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
			finally
			{
				writer.Reset(chunkPool);
			}
		}
		finally
		{
			sync.ExitReadLock(lockHandle);
		}
	}

	private unsafe void SendMessageInternal<T1, T2, T3, T4, T5, T6, T7, T8>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7, T8> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8,
		ulong messageId, HandleResponseDelegate responseCallback, object responseState)
	{
		int lockHandle = sync.EnterReadLock();

		try
		{
			if (state < ConnectionState.Opened)
				throw new ConnectionNotOpenException();

			if (state > ConnectionState.Opened)
				throw CreateClosedException();

			MessageChunk chunk = chunkPool.GetSmall();
			MessageWriter writer = WriterInstance;

			try
			{
				if (messageId != 0)
				{
					Checker.AssertTrue(responseCallback == null);
				}
				else
				{
					messageId = pendingRequests.Add(new PendingRequests.PendingRequest(responseCallback, responseState), lockHandle);
				}

				writer.Init(chunk, socketSenderDelegate, messageId);
				serializer.Invoke(writer, value1, value2, value3, value4, value5, value6, value7, value8);
				writer.EmptyBuffer(true);
			}
			catch (Exception e)
			{
				if (responseCallback != null)
					pendingRequests.TryRemove(messageId, out _);

				if (e is SocketException)
				{
					Task.Run(() => CloseAsyncInternal());
					throw new CommunicationException("Error occurred while sending data through a socket.", e);
				}

				throw;
			}
			finally
			{
				writer.Reset(chunkPool);
			}
		}
		finally
		{
			sync.ExitReadLock(lockHandle);
		}
	}

	private void StopReceiving()
	{
		// Currently we do not support stop receiving
	}

	private void ContinueReceiving()
	{
		// Currently we do not support stop receiving
	}


	private unsafe bool SendChunk(MessageChunk chunk, out MessageChunk nextChunk)
	{
		if (chunk.IsFirst && !chunk.IsLast && chunk.BufferSize == MessageChunk.SmallBufferSize)
		{
			// Promote to large chunk to avoid splitting the request
			nextChunk = chunkPool.GetLarge();
			Utils.CopyMemory(chunk.PBuffer, nextChunk.PBuffer, chunk.ChunkSize);
			chunkPool.Put(chunk);
			return false;
		}

		nextChunk = null;
		if (groupingSender != null && chunk.IsTheOnlyOne && chunk.ChunkSize <= MessageChunk.SmallBufferSize)
		{
			groupingSender.Send((IntPtr)chunk.PBuffer, chunk.ChunkSize);
			chunkPool.Put(chunk);
		}
		else
		{
			try
			{
				socket.Send(new ReadOnlySpan<byte>(chunk.PBuffer, chunk.ChunkSize));

				if (chunk.IsLast)
					chunkPool.Put(chunk);
				else
					nextChunk = chunk;
			}
			catch (Exception e)
			{
				chunkPool.Put(chunk);
				if (e is SocketException)
					throw new CommunicationObjectAbortedException(AbortedPhase.Communication, "Failed to send data over a socket.", e);

				throw;
			}
		}

		return true;
	}

	protected void StartReceiving()
	{
		Thread t = new Thread(ReceiveWorker);
		if (priorityWorkers != null)
		{
			t.Priority = ThreadPriority.AboveNormal;
		}
		else
		{
			groupingSender = new GroupingSender(socket, priorityWorkers != null);    // At this point we are guaranteed to have a socket
		}

		t.IsBackground = true;
		t.Start();
	}

	private unsafe void ReceiveWorker()
	{
		int currPool = 0;   // We take chunks from all pools (from all CPUs to avoid starving a single pool)
		MessageChunk receiveChunk = chunkPool.GetLarge(currPool);

		try
		{
			int totalReceived = 0;
			while (true)
			{
				int n;
				try
				{
					n = socket.Receive(new Span<byte>((byte*)receiveChunk.PBuffer + totalReceived,
						MessageChunk.LargeBufferSize - totalReceived));
				}
				catch (Exception e) when (e is ObjectDisposedException || e is SocketException)
				{
					n = 0;
				}

				if (n == 0)
				{
					Task.Run(() => CloseAsyncInternal());
					break;
				}

				totalReceived += n;

				int handle = sync.EnterReadLock();

				try
				{
					if (state == ConnectionState.Closed)
						break;

					DelegateWorkItems(ref currPool, ref receiveChunk, ref totalReceived);
				}
				catch (Exception e) when (e is CorruptMessageException || e is UnsupportedHeaderException || e is ObjectDisposedException)
				{
					Task.Run(() => CloseAsyncInternal());
					break;
				}
				finally
				{
					sync.ExitReadLock(handle);
				}
			}
		}
		finally
		{
			if (receiveChunk != null)
				chunkPool.Put(receiveChunk);
		}
	}

	private unsafe void DelegateWorkItems(ref int currPool, ref MessageChunk receiveChunk, ref int totalReceived)
	{
		// Management of receiveChunk life gets tricky here. This method either claims the chunk for further processing
		// or return back the same chunk it got. If it claims the chunk, it must guarantee proper cleanup (returning to the pool)
		// both in case of successful processing and in case of exception at any stage during processing. General rule of thumb
		// is that if we encounter an exception during processing (closed connection, corrupted message...) we asynchronously
		// close the connection and release used chunks.

		if (totalReceived < 4)
			return;

		if (totalReceived < receiveChunk.ChunkSize)
			return;

		if (totalReceived == receiveChunk.ChunkSize && totalReceived > MessageChunk.SmallBufferSize)
		{
			incompleteMessages.PreChunkReceived(receiveChunk.Header);

			MessageChunk temp = receiveChunk;
			receiveChunk = null;    // This means that the chunk was consumed for the outside caller (even in case of exception)

			PreProcessMessage(temp, DelegationType.Global);
			currPool = (currPool + 1) % ProcessorNumber.CoreCount;
			receiveChunk = chunkPool.GetLarge(currPool);
			totalReceived = 0;
			return;
		}

		if (priorityWorkers != null)
		{
			DelegatePriorityWork(receiveChunk, ref totalReceived);
		}
		else if (totalReceived <= MessageChunk.SmallBufferSize * 2)
		{
			DelegateUsingManagedMemory(receiveChunk, ref totalReceived);
		}
		else
		{
			DelegateUsingChunk(ref currPool, ref receiveChunk, ref totalReceived);
		}
	}

	private unsafe bool TryPeekMessage(byte* buffer, int size, out int chunkSize)
	{
		if (size < 4)
		{
			chunkSize = 0;
			return false;
		}

		chunkSize = *(int*)buffer;
		if (chunkSize > MessageChunk.LargeBufferSize)
			throw new CorruptMessageException();

		if (chunkSize <= size)
		{
			incompleteMessages.PreChunkReceived((MessageChunkHeader*)buffer);
			return true;
		}
		else
		{
			return false;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private unsafe void DelegateManagedRange(byte* buffer, int size)
	{
		byte[] data = GC.AllocateUninitializedArray<byte>(size);
		fixed (byte* bp = data)
		{
			Utils.CopyMemory(buffer, bp, size);
			ThreadPool.UnsafeQueueUserWorkItem(managedMemDelegator, data, false);
		}
	}

	private unsafe void DelegateUsingManagedMemory(MessageChunk receiveChunk, ref int totalReceived)
	{
		byte* buffer = receiveChunk.PBuffer;

		int groupSize = 0;
		int itemCount = 0;
		int offset = 0;

		while (TryPeekMessage(buffer + offset, totalReceived - offset, out int chunkSize))
		{
			itemCount++;
			offset += chunkSize;
			groupSize += chunkSize;

			if (itemCount == receiveGroupSize)
			{
				DelegateManagedRange(buffer + offset - groupSize, groupSize);
				itemCount = 0;
				groupSize = 0;
			}
		}

		if (itemCount > 0)
			DelegateManagedRange(buffer + offset - groupSize, groupSize);

		Checker.AssertTrue(offset <= totalReceived);
		if (offset < totalReceived)
			Utils.CopyMemory(buffer + offset, buffer, totalReceived - offset);

		totalReceived -= offset;
	}

	private unsafe void DelegatePriorityWork(MessageChunk receiveChunk, ref int totalReceived)
	{
		byte* buffer = receiveChunk.PBuffer;
		while (TryPeekMessage(buffer, totalReceived, out _))
		{
			MessageChunk extracted = ExtractChunk(ref totalReceived, ref buffer);

			try
			{
				PreProcessMessage(extracted, DelegationType.Global);
			}
			catch
			{
				chunkPool.Put(extracted);
				throw;
			}
		}

		if (totalReceived > 0)
			Utils.CopyMemory(buffer, receiveChunk.PBuffer, totalReceived);
	}

	private unsafe void DelegateUsingChunk(ref int currPool, ref MessageChunk receiveChunk, ref int totalReceived)
	{
		MessageChunk chunk = receiveChunk;
		receiveChunk = null;    // Chunk is consumed for the outside caller (even in case of exception)

		chunk.SetupAutomaticCleanup(int.MaxValue);   // This will prevent cleanup until we are done here

		byte* buffer = chunk.PBuffer;

		int groupCount = 0;

		try
		{
			int groupSize = 0;
			int itemCount = 0;
			int offset = 0;

			while (TryPeekMessage(buffer + offset, totalReceived - offset, out int chunkSize))
			{
				itemCount++;
				offset += chunkSize;
				groupSize += chunkSize;

				if (itemCount == receiveGroupSize)
				{
					ThreadPool.UnsafeQueueUserWorkItem(chunkDelegator, new ChunkRange(chunk, offset - groupSize, groupSize), false);
					groupCount++;
					itemCount = 0;
					groupSize = 0;
				}
			}

			if (itemCount > 0)
			{
				ThreadPool.UnsafeQueueUserWorkItem(chunkDelegator, new ChunkRange(chunk, offset - groupSize, groupSize), false);
				groupCount++;
			}

			Checker.AssertTrue(offset <= totalReceived);

			currPool = (currPool + 1) % ProcessorNumber.CoreCount;
			receiveChunk = chunkPool.GetLarge(currPool);
			if (offset < totalReceived)
				Utils.CopyMemory(buffer + offset, receiveChunk.PBuffer, totalReceived - offset);

			totalReceived -= offset;
		}
		finally
		{
			chunk.DecRefCount(int.MaxValue - groupCount, chunkPool);
		}
	}

	private unsafe void DelegateGroupWorkItemsFromChunk(ChunkRange range)
	{
		MessageChunk chunk = range.Chunk;
		try
		{
			int size = range.Size;
			byte* buffer = chunk.PBuffer + range.Offset;

			while (size > 0)
			{
				MessageChunk extracted = ExtractChunk(ref size, ref buffer);
				PreProcessMessage(extracted, DelegationType.Local);
			}

			Checker.AssertTrue(size == 0);
		}
		finally
		{
			chunk.DecRefCount(1, chunkPool);
		}
	}

	private unsafe void DelegateGroupWorkItemsFromManagedMemory(byte[] range)
	{
		MessageChunk extracted = null;
		fixed (byte* bp = range)
		{
			byte* buffer = bp;
			int size = range.Length;
			while (size > 0)
			{
				extracted = ExtractChunk(ref size, ref buffer);
				if (size == 0)
					break;

				PreProcessMessage(extracted, DelegationType.Local);
			}

			Checker.AssertTrue(size == 0);
		}

		PreProcessMessage(extracted, DelegationType.Sync);
	}

	private unsafe MessageChunk ExtractChunk(ref int received, ref byte* p)
	{
		MessageChunk extracted;

		int size = *(int*)p;
		if (size <= MessageChunk.SmallBufferSize)
		{
			extracted = chunkPool.GetSmall();
		}
		else
		{
			extracted = chunkPool.GetLarge();
		}

		Utils.CopyMemory(p, extracted.PBuffer, size);
		p += size;
		received -= size;
		return extracted;
	}

	private void PreProcessMessage(MessageChunk chunk, DelegationType delegationType)
	{
		try
		{
			chunk.ReadHeader();
			if (chunk.IsFirst && chunk.ChunkSize > MessageChunk.LargeBufferSize)
				throw new CorruptMessageException();

			incompleteMessages.ChunkReceived(chunk, out bool chunkConsumed);
			if (chunkConsumed)
				return;
		}
		catch (CorruptMessageException)
		{
			chunkPool.Put(chunk);
			Task.Run(() => CloseAsyncInternal());
			return;
		}

		Checker.AssertTrue(chunk.IsFirst);
		if (delegationType == DelegationType.Sync)
		{
			ProcessMessage(chunk);
		}
		else if (priorityWorkers != null)
		{
			if (!priorityWorkers.TryEnqueueWork(() => ProcessMessage(chunk), -1, true))
				ThreadPool.UnsafeQueueUserWorkItem(processor, chunk, false);
		}
		else
		{
			ThreadPool.UnsafeQueueUserWorkItem(processor, chunk, delegationType == DelegationType.Local);
		}
	}

	private unsafe void ProcessMessage(MessageChunk chunk)
	{
		try
		{
			PendingRequests.PendingRequest pendingRequest = new PendingRequests.PendingRequest();
			try
			{
				if (IsResponseMessage(chunk.MessageId))
				{
					if (pendingRequests == null || !pendingRequests.TryRemove(chunk.MessageId, out pendingRequest))
						throw new CorruptMessageException();
				}
				else
				{
					if (messageHandler == null)
						throw new CorruptMessageException();
				}
			}
			catch
			{
				chunkPool.Put(chunk);
				throw;
			}

			MessageReader reader = ReaderInstance;
			reader.IsReleased = false;

			reader.Init(chunk, readerCallback);

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
					Checker.AssertFalse(chunk.MessageId == 0);
					messageHandler(this, chunk.MessageId, reader);
				}
			}
			finally
			{
				ReleaseReader(reader);
			}
		}
		catch (Exception e) when (e is ObjectDisposedException || e is SocketException ||
			e is CommunicationObjectAbortedException || e is CorruptMessageException || e is ChunkTimeoutException)
		{
			Task.Run(() => CloseAsyncInternal());
		}
	}

	public void ReleaseReader(MessageReader reader)
	{
		if (reader.IsReleased)
			return;

		MessageChunk lastChunk = reader.Chunk;
		if (lastChunk != null)
			chunkPool.Put(lastChunk);

		reader.IsReleased = true;
		reader.Reset();
	}

	private unsafe MessageChunk ProvideNextMessageChunk(MessageChunk currChunk)
	{
		int lockHandle = sync.EnterReadLock();

		try
		{
			ulong messageId = currChunk.MessageId;
			int chunkNum = currChunk.ChunkNum;

			try
			{
				if (this.state == ConnectionState.Closed)
					throw CreateClosedException();

				if (currChunk.IsLast)
					throw new CorruptMessageException();
			}
			finally
			{
				chunkPool.Put(currChunk);
			}

			return incompleteMessages.WaitNextChunk(messageId, chunkNum + 1);
		}
		finally
		{
			sync.ExitReadLock(lockHandle);
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

		groupingSender?.Close();

		pendingRequests?.Close(this, CreateClosedException());
		incompleteMessages.Dispose();

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

	public unsafe sealed class PendingRequests
	{
		MultiSpinRWLock sync;

		Connection owner;
		ConcurrentDictionary<ulong, PendingRequest>[] requests = new ConcurrentDictionary<ulong, PendingRequest>[ProcessorNumber.CoreCount];

		ulong messageIdBit;
		object idsHandle;
		ulong* currMessageIds;

		static PendingRequests()
		{
			if (ProcessorNumber.CoreCount > 8192)
				throw new NotSupportedException("Processor with more than 8192 cores is not supported.");
		}

		public PendingRequests(Connection owner)
		{
			this.owner = owner;
			this.messageIdBit = owner.MessageIdBit;

			sync = new MultiSpinRWLock();
			currMessageIds = (ulong*)CacheLineMemoryManager.Allocate(sizeof(long), out idsHandle);
			for (int i = 0; i < requests.Length; i++)
			{
				ulong* currMessageId = (ulong*)CacheLineMemoryManager.GetBuffer(currMessageIds, i);
				*currMessageId = 0;
				requests[i] = new ConcurrentDictionary<ulong, PendingRequest>(4, 1024 * 8);
			}
		}

		public ulong ReserveId(int procNum)
		{
			sync.EnterReadLock(procNum);
			try
			{
				NativeInterlocked64* currMessageId = (NativeInterlocked64*)CacheLineMemoryManager.GetBuffer(currMessageIds, procNum);
				ulong messageId = (ulong)currMessageId->Increment();
				messageId = messageIdBit | ((ulong)procNum << 50) | messageId;
				return messageId;
			}
			finally
			{
				sync.ExitReadLock(procNum);
			}
		}

		public ulong Add(PendingRequest request, int procNum)
		{
			if (request.Callback == null)
				return ReserveId(procNum);

			sync.EnterReadLock(procNum);
			try
			{
				NativeInterlocked64* currMessageId = (NativeInterlocked64*)CacheLineMemoryManager.GetBuffer(currMessageIds, procNum);
				ulong messageId = (ulong)currMessageId->Increment();
				messageId = messageIdBit | ((ulong)procNum << 50) | messageId;
				requests[procNum].TryAdd(messageId, request);
				return messageId;
			}
			finally
			{
				sync.ExitReadLock(procNum);
			}
		}

		public bool TryRemove(ulong messageId, out PendingRequest request)
		{
			if (messageId == 0)
			{
				request = new PendingRequest();
				return false;
			}

			ProcessorNumber.GetCore();
			int index = (int)((messageId >> 50) & 0x1fff);

			int handle = sync.EnterReadLock();
			try
			{
				if (requests[index] == null)
					throw new ObjectDisposedException(string.Empty, (Exception)null);

				return requests[index].Remove(messageId, out request);
			}
			finally
			{
				sync.ExitReadLock(handle);
			}
		}

		public void Close(Connection conn, Exception e)
		{
			sync.EnterWriteLock();
			try
			{
				for (int i = 0; i < requests.Length; i++)
				{
					foreach (KeyValuePair<ulong, PendingRequest> request in requests[i])
					{
						ThreadPool.UnsafeQueueUserWorkItem(x => request.Value.Callback(conn, request.Value.State, e, null), null);
					}

					requests[i].Clear();
					requests[i] = null;
				}

				CacheLineMemoryManager.Free(idsHandle);
			}
			finally
			{
				sync.ExitWriteLock();
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

	private struct ChunkRange
	{
		public MessageChunk Chunk { get; private set; }
		public int Offset { get; private set; }
		public int Size { get; private set; }

		public ChunkRange(MessageChunk chunk, int offset, int size)
		{
			this.Chunk = chunk;
			this.Offset = offset;
			this.Size = size;
		}
	}

	private enum DelegationType
	{
		Global = 1,
		Local = 2,
		Sync = 3
	}
}
