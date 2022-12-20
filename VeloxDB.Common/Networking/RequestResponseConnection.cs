using System;
using System.Threading;

namespace VeloxDB.Networking;

internal sealed class RequestResponseConnection
{
	static DeserializerDelegate<int> dummyDeserializer = (r) => 0;

	[ThreadStatic]
	static int assignedPooledConnection;

	readonly object eventPoolSync = new object();
	int eventPoolCount;
	EventWaitHandle[] eventPool;

	Connection[] connections;

	readonly object sync = new object();
	int currPoolConnection;
	Connection[] connectionPool;
	ConnectionState state;
	bool closeCalled;

	private event Action Closed;

	public RequestResponseConnection(Connection conn) :
		this(new Connection[] { conn })
	{
	}

	public RequestResponseConnection(Connection[] connections)
	{
		eventPool = new EventWaitHandle[8];
		eventPoolCount = connections.Length * 2;

		for (int i = 0; i < eventPoolCount; i++)
		{
			eventPool[i] = new EventWaitHandle(false, EventResetMode.AutoReset);
		}

		this.connections = connections;
		state = ConnectionState.Opened;

		connectionPool = new Connection[connections.Length];
		for (int i = 0; i < connections.Length; i++)
		{
			connections[i].AddClosedHandlerSafe(ConnectionClosedHandler);
			connectionPool[i] = connections[i];
		}
	}

	public ConnectionState State => state;

	public void AddClosedConnectionHandlerSafe(Action action)
	{
		lock (sync)
		{
			if (state != ConnectionState.Closed)
			{
				Closed += action;
				return;
			}
		}

		action();
	}

	public void SendMessage(SerializerDelegate serializer)
	{
		GetConnection().SendMessage(serializer);
	}

	public void SendMessage<T1>(SerializerDelegate<T1> serializer, T1 value1)
	{
		GetConnection().SendMessage(serializer, value1);
	}

	public void SendMessage<T1, T2>(SerializerDelegate<T1, T2> serializer, T1 value1, T2 value2)
	{
		GetConnection().SendMessage(serializer, value1, value2);
	}

	public void SendMessage<T1, T2, T3>(SerializerDelegate<T1, T2, T3> serializer, T1 value1, T2 value2, T3 value3)
	{
		GetConnection().SendMessage(serializer, value1, value2, value3);
	}

	public void SendMessage<T1, T2, T3, T4>(SerializerDelegate<T1, T2, T3, T4> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4)
	{
		GetConnection().SendMessage(serializer, value1, value2, value3, value4);
	}

	public void SendMessage<T1, T2, T3, T4, T5>(SerializerDelegate<T1, T2, T3, T4, T5> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5)
	{
		GetConnection().SendMessage(serializer, value1, value2, value3, value4, value5);
	}

	public void SendMessage<T1, T2, T3, T4, T5, T6>(SerializerDelegate<T1, T2, T3, T4, T5, T6> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6)
	{
		GetConnection().SendMessage(serializer, value1, value2, value3, value4, value5, value6);
	}

	public void SendMessage<T1, T2, T3, T4, T5, T6, T7>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7)
	{
		GetConnection().SendMessage(serializer, value1, value2, value3, value4, value5, value6, value7);
	}

	public void SendMessage<T1, T2, T3, T4, T5, T6, T7, T8>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7, T8> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8)
	{
		GetConnection().SendMessage(serializer, value1, value2, value3, value4, value5, value6, value7, value8);
	}

	public void SendRequest(SerializerDelegate serializer, int timeout = -1)
	{
		SendRequest<int>(serializer, dummyDeserializer, -1);
	}

	public TRes SendRequest<TRes>(SerializerDelegate serializer, DeserializerDelegate<TRes> deserializer, int timeout = -1)
	{
		ResponseData<TRes> rd = new ResponseData<TRes>(GetEvent(), deserializer);
		HandleResponseDelegate responseCallback = ResponseCallbackFactory<TRes>.Callback;
		GetConnection().SendRequest(serializer, responseCallback, rd);

		bool b = rd.TryWaitResponse(timeout, out EventWaitHandle e);
		PutEvent(e);
		if (!b)
			throw new TimeoutException();

		if (rd.Error != null)
			throw rd.Error;

		return rd.Value;
	}

	public void SendRequest<T1>(SerializerDelegate<T1> serializer, T1 value1, int timeout = -1)
	{
		SendRequest<int, T1>(serializer, value1, dummyDeserializer, -1);
	}

	public TRes SendRequest<TRes, T1>(SerializerDelegate<T1> serializer, T1 value1,
		DeserializerDelegate<TRes> deserializer, int timeout = -1)
	{
		ResponseData<TRes> rd = new ResponseData<TRes>(GetEvent(), deserializer);
		HandleResponseDelegate responseCallback = ResponseCallbackFactory<TRes>.Callback;
		GetConnection().SendRequest(serializer, value1, responseCallback, rd);

		bool b = rd.TryWaitResponse(timeout, out EventWaitHandle e);
		PutEvent(e);
		if (!b)
			throw new TimeoutException();

		if (rd.Error != null)
			throw rd.Error;

		return rd.Value;
	}

	public void SendRequest<T1, T2>(SerializerDelegate<T1, T2> serializer, T1 value1, T2 value2, int timeout = -1)
	{
		SendRequest<int, T1, T2>(serializer, value1, value2, dummyDeserializer, -1);
	}

	public TRes SendRequest<TRes, T1, T2>(SerializerDelegate<T1, T2> serializer, T1 value1, T2 value2,
		DeserializerDelegate<TRes> deserializer, int timeout = -1)
	{
		ResponseData<TRes> rd = new ResponseData<TRes>(GetEvent(), deserializer);
		HandleResponseDelegate responseCallback = ResponseCallbackFactory<TRes>.Callback;
		GetConnection().SendRequest(serializer, value1, value2, responseCallback, rd);

		bool b = rd.TryWaitResponse(timeout, out EventWaitHandle e);
		PutEvent(e);
		if (!b)
			throw new TimeoutException();

		if (rd.Error != null)
			throw rd.Error;

		return rd.Value;
	}

	public void SendRequest<T1, T2, T3>(SerializerDelegate<T1, T2, T3> serializer, T1 value1, T2 value2, T3 value3, int timeout = -1)
	{
		SendRequest<int, T1, T2, T3>(serializer, value1, value2, value3, dummyDeserializer, -1);
	}

	public TRes SendRequest<TRes, T1, T2, T3>(SerializerDelegate<T1, T2, T3> serializer, T1 value1, T2 value2, T3 value3,
		DeserializerDelegate<TRes> deserializer, int timeout = -1)
	{
		ResponseData<TRes> rd = new ResponseData<TRes>(GetEvent(), deserializer);
		HandleResponseDelegate responseCallback = ResponseCallbackFactory<TRes>.Callback;
		GetConnection().SendRequest(serializer, value1, value2, value3, responseCallback, rd);

		bool b = rd.TryWaitResponse(timeout, out EventWaitHandle e);
		PutEvent(e);
		if (!b)
			throw new TimeoutException();

		if (rd.Error != null)
			throw rd.Error;

		return rd.Value;
	}

	public void SendRequest<T1, T2, T3, T4>(SerializerDelegate<T1, T2, T3, T4> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, int timeout = -1)
	{
		SendRequest<int, T1, T2, T3, T4>(serializer, value1, value2, value3, value4, dummyDeserializer, -1);
	}

	public TRes SendRequest<TRes, T1, T2, T3, T4>(SerializerDelegate<T1, T2, T3, T4> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, DeserializerDelegate<TRes> deserializer, int timeout = -1)
	{
		ResponseData<TRes> rd = new ResponseData<TRes>(GetEvent(), deserializer);
		HandleResponseDelegate responseCallback = ResponseCallbackFactory<TRes>.Callback;
		GetConnection().SendRequest(serializer, value1, value2, value3, value4, responseCallback, rd);

		bool b = rd.TryWaitResponse(timeout, out EventWaitHandle e);
		PutEvent(e);
		if (!b)
			throw new TimeoutException();

		if (rd.Error != null)
			throw rd.Error;

		return rd.Value;
	}

	public void SendRequest<T1, T2, T3, T4, T5>(SerializerDelegate<T1, T2, T3, T4, T5> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, int timeout = -1)
	{
		SendRequest<int, T1, T2, T3, T4, T5>(serializer, value1, value2, value3, value4, value5, dummyDeserializer, -1);
	}

	public TRes SendRequest<TRes, T1, T2, T3, T4, T5>(SerializerDelegate<T1, T2, T3, T4, T5> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5,
		DeserializerDelegate<TRes> deserializer, int timeout = -1)
	{
		ResponseData<TRes> rd = new ResponseData<TRes>(GetEvent(), deserializer);
		HandleResponseDelegate responseCallback = ResponseCallbackFactory<TRes>.Callback;
		GetConnection().SendRequest(serializer, value1, value2, value3, value4, value5, responseCallback, rd);

		bool b = rd.TryWaitResponse(timeout, out EventWaitHandle e);
		PutEvent(e);
		if (!b)
			throw new TimeoutException();

		if (rd.Error != null)
			throw rd.Error;

		return rd.Value;
	}

	public void SendRequest<T1, T2, T3, T4, T5, T6>(SerializerDelegate<T1, T2, T3, T4, T5, T6> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, int timeout = -1)
	{
		SendRequest<int, T1, T2, T3, T4, T5, T6>(serializer, value1, value2, value3, value4, value5, value6, dummyDeserializer, -1);
	}

	public TRes SendRequest<TRes, T1, T2, T3, T4, T5, T6>(SerializerDelegate<T1, T2, T3, T4, T5, T6> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, DeserializerDelegate<TRes> deserializer, int timeout = -1)
	{
		ResponseData<TRes> rd = new ResponseData<TRes>(GetEvent(), deserializer);
		HandleResponseDelegate responseCallback = ResponseCallbackFactory<TRes>.Callback;
		GetConnection().SendRequest(serializer, value1, value2, value3, value4, value5, value6, responseCallback, rd);

		bool b = rd.TryWaitResponse(timeout, out EventWaitHandle e);
		PutEvent(e);
		if (!b)
			throw new TimeoutException();

		if (rd.Error != null)
			throw rd.Error;

		return rd.Value;
	}

	public void SendRequest<T1, T2, T3, T4, T5, T6, T7>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, int timeout = -1)
	{
		SendRequest<int, T1, T2, T3, T4, T5, T6, T7>(serializer, value1, value2, value3, value4, value5, value6, value7, dummyDeserializer, -1);
	}

	public TRes SendRequest<TRes, T1, T2, T3, T4, T5, T6, T7>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7,
		DeserializerDelegate<TRes> deserializer, int timeout = -1)
	{
		ResponseData<TRes> rd = new ResponseData<TRes>(GetEvent(), deserializer);
		HandleResponseDelegate responseCallback = ResponseCallbackFactory<TRes>.Callback;
		GetConnection().SendRequest(serializer, value1, value2, value3, value4, value5, value6, value7, responseCallback, rd);

		bool b = rd.TryWaitResponse(timeout, out EventWaitHandle e);
		PutEvent(e);
		if (!b)
			throw new TimeoutException();

		if (rd.Error != null)
			throw rd.Error;

		return rd.Value;
	}

	public void SendRequest<T1, T2, T3, T4, T5, T6, T7, T8>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7, T8> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8, int timeout = -1)
	{
		SendRequest<int, T1, T2, T3, T4, T5, T6, T7, T8>(serializer, value1, value2, value3, value4, value5, value6, value7, value8, dummyDeserializer, -1);
	}

	public TRes SendRequest<TRes, T1, T2, T3, T4, T5, T6, T7, T8>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7, T8> serializer,
		T1 value1, T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8,
		DeserializerDelegate<TRes> deserializer, int timeout = -1)
	{
		ResponseData<TRes> rd = new ResponseData<TRes>(GetEvent(), deserializer);
		HandleResponseDelegate responseCallback = ResponseCallbackFactory<TRes>.Callback;
		GetConnection().SendRequest(serializer, value1, value2, value3, value4, value5, value6, value7, value8, responseCallback, rd);

		bool b = rd.TryWaitResponse(timeout, out EventWaitHandle e);
		PutEvent(e);
		if (!b)
			throw new TimeoutException();

		if (rd.Error != null)
			throw rd.Error;

		return rd.Value;
	}

	private Connection GetConnection()
	{
		int assigned = assignedPooledConnection;
		if (assigned == 0)
			assigned = assignedPooledConnection = Interlocked.Increment(ref currPoolConnection);

		return connectionPool[assigned % connectionPool.Length];
	}

	private EventWaitHandle GetEvent()
	{
		lock (eventPoolSync)
		{
			if (eventPoolCount > 0)
				return eventPool[--eventPoolCount];
		}

		return new EventWaitHandle(false, EventResetMode.AutoReset);
	}

	private void PutEvent(EventWaitHandle e)
	{
		lock (eventPoolSync)
		{
			if (eventPool.Length == eventPoolCount)
				Array.Resize(ref eventPool, eventPool.Length * 2);

			eventPool[eventPoolCount++] = e;
		}
	}

	private void ConnectionClosedHandler()
	{
		lock (sync)
		{
			Dispose();

			if (closeCalled)
				return;

			closeCalled = true;
		}

		Closed?.Invoke();
	}

	public void Close()
	{
		Dispose();
	}

	public void Dispose()
	{
		lock (sync)
		{
			if (state == ConnectionState.Closed)
				return;

			state = ConnectionState.Closed;

			for (int i = 0; i < connections.Length; i++)
			{
				connections[i].Close();
			}
		}
	}

	private sealed class ResponseData<T>
	{
		public T Value { get; private set; }
		public EventWaitHandle Event { get; private set; }
		public DeserializerDelegate<T> Deserializer { get; private set; }
		public Exception Error { get; private set; }

		public ResponseData(EventWaitHandle e, DeserializerDelegate<T> deserializer)
		{
			this.Event = e;
			this.Deserializer = deserializer;
		}

		public void SetError(Exception error)
		{
			lock (this)
			{
				if (Event == null)
					return;

				this.Error = error;
				Event.Set();
			}
		}

		public void SetResult(T value)
		{
			lock (this)
			{
				if (Event == null)
					return;

				this.Value = value;
				Event.Set();
			}
		}

		public void SetEmptyResult()
		{
			lock (this)
			{
				if (Event == null)
					return;

				Event.Set();
			}
		}

		public bool TryWaitResponse(int timeout, out EventWaitHandle e)
		{
			e = Event;

			if (!Event.WaitOne(timeout))
				return false;

			lock (this)
			{
				if (Event != null)
					Event = null;
			}

			return true;
		}
	}

	private static class ResponseCallbackFactory<TRes>
	{
		public static readonly HandleResponseDelegate Callback = new HandleResponseDelegate(HandleResponseCallback);

		private static void HandleResponseCallback(Connection connection, object state, Exception error, MessageReader reader)
		{
			ResponseData<TRes> rd = (ResponseData<TRes>)state;

			if (error != null)
			{
				rd.SetError(error);
			}
			else
			{
				if (rd.Deserializer != null)
				{
					try
					{
						rd.SetResult(rd.Deserializer(reader));
					}
					catch (Exception e)
					{
						rd.SetError(e);
					}
				}
				else
				{
					rd.SetEmptyResult();
				}
			}
		}
	}
}
