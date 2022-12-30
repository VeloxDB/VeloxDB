using System;
using VeloxDB.Common;
using VeloxDB.Networking;

namespace VeloxDB.Client;

internal unsafe sealed class PooledConnection
{
	private enum State
	{
		Open,
		Closing,
		Closed
	}

	MultiSpinRWLock sync;
	object countersHandle;
	long* pendingCounters;

	ConnectionPool owner;
	State state;
	ClientConnection connection;
	TimeSpan timestamp;

	public PooledConnection(ConnectionPool owner, ClientConnection connection, TimeSpan timestamp)
	{
		this.owner = owner;
		this.connection = connection;
		this.timestamp = timestamp;
		this.state = State.Open;

		sync = new MultiSpinRWLock();
		pendingCounters = (long*)CacheLineMemoryManager.Allocate(sizeof(long), out countersHandle);

		this.connection.ResponseProcessed += ResponseProcessed;
	}

	~PooledConnection()
	{
		sync.Dispose();
		CacheLineMemoryManager.Free(countersHandle);
	}

	public ConnectionPool Owner => owner;
	public ClientConnection Connection => connection;
	public TimeSpan Timestamp => timestamp;
	public object Tag => Connection.Tag;

	public bool TrySetTagIfNull(object tag)
	{
		return Connection.TrySetTagIfNull(tag);
	}

	public void Invalidate()
	{
		sync.EnterWriteLock();
		try
		{
			if (state == State.Closing || state == State.Closed)
				return;

			owner.RemoveConnection(this);
			if (PendingCount() == 0)
			{
				state = State.Closed;
				connection.CloseAsync();
			}
			else
			{
				state = State.Closing;
			}
		}
		finally
		{
			sync.ExitWriteLock();
		}
	}

	private long PendingCount()
	{
		long s = 0;
		for (int i = 0; i < ProcessorNumber.CoreCount; i++)
		{
			s += *(long*)CacheLineMemoryManager.GetBuffer(pendingCounters, i);
		}

		return s;
	}

	private bool TryStartRequest()
	{
		int handle = sync.EnterReadLock();
		try
		{
			if (state != State.Open)
				return false;

			NativeInterlocked64* p = (NativeInterlocked64*)CacheLineMemoryManager.GetBuffer(pendingCounters, handle);
			p->Increment();
			return true;
		}
		finally
		{
			sync.ExitReadLock(handle);
		}
	}

	private void EndRequest()
	{
		int handle = sync.EnterReadLock();
		try
		{
			NativeInterlocked64* p = (NativeInterlocked64*)CacheLineMemoryManager.GetBuffer(pendingCounters, handle);
			p->Decrement();
		}
		finally
		{
			sync.ExitReadLock(handle);
		}

		if (state == State.Closing)
		{
			sync.EnterWriteLock();
			try
			{
				if (state == State.Closing)
				{
					if (PendingCount() == 0)
					{
						state = State.Closed;
						connection.CloseAsync();
					}
				}
			}
			finally
			{
				sync.ExitWriteLock();
			}
		}
	}

	private void ResponseProcessed(HandleResponseDelegate callback)
	{
		EndRequest();
	}

	public bool SendRequest0(SerializerDelegate serializer, HandleResponseDelegate responseCallback, object responseState)
	{
		if (!TryStartRequest())
			return false;

		connection.SendRequest(serializer, responseCallback, responseState);
		return true;
	}

	public bool SendRequest1<T1>(SerializerDelegate<T1> serializer, T1 value1,
		HandleResponseDelegate responseCallback, object responseState)
	{
		if (!TryStartRequest())
			return false;

		connection.SendRequest(serializer, value1, responseCallback, responseState);
		return true;
	}

	public bool SendRequest2<T1, T2>(SerializerDelegate<T1, T2> serializer, T1 value1, T2 value2,
		HandleResponseDelegate responseCallback, object responseState)
	{
		if (!TryStartRequest())
			return false;

		connection.SendRequest(serializer, value1, value2, responseCallback, responseState);
		return true;
	}

	public bool SendRequest3<T1, T2, T3>(SerializerDelegate<T1, T2, T3> serializer, T1 value1, T2 value2, T3 value3,
		HandleResponseDelegate responseCallback, object responseState)
	{
		if (!TryStartRequest())
			return false;

		connection.SendRequest(serializer, value1, value2, value3, responseCallback, responseState);
		return true;
	}

	public bool SendRequest4<T1, T2, T3, T4>(SerializerDelegate<T1, T2, T3, T4> serializer, T1 value1, T2 value2, T3 value3, T4 value4,
		HandleResponseDelegate responseCallback, object responseState)
	{
		if (!TryStartRequest())
			return false;

		connection.SendRequest(serializer, value1, value2, value3, value4, responseCallback, responseState);
		return true;
	}

	public bool SendRequest5<T1, T2, T3, T4, T5>(SerializerDelegate<T1, T2, T3, T4, T5> serializer, T1 value1, T2 value2,
		T3 value3, T4 value4, T5 value5, HandleResponseDelegate responseCallback, object responseState)
	{
		if (!TryStartRequest())
			return false;

		connection.SendRequest(serializer, value1, value2, value3, value4, value5, responseCallback, responseState);
		return true;
	}

	public bool SendRequest6<T1, T2, T3, T4, T5, T6>(SerializerDelegate<T1, T2, T3, T4, T5, T6> serializer, T1 value1, T2 value2,
		T3 value3, T4 value4, T5 value5, T6 value6, HandleResponseDelegate responseCallback, object responseState)
	{
		if (!TryStartRequest())
			return false;

		connection.SendRequest(serializer, value1, value2, value3, value4, value5, value6, responseCallback, responseState);
		return true;
	}

	public bool SendRequest7<T1, T2, T3, T4, T5, T6, T7>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7> serializer, T1 value1, T2 value2,
		T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, HandleResponseDelegate responseCallback, object responseState)
	{
		if (!TryStartRequest())
			return false;

		connection.SendRequest(serializer, value1, value2, value3, value4, value5, value6, value7, responseCallback, responseState);
		return true;
	}

	public bool SendRequest8<T1, T2, T3, T4, T5, T6, T7, T8>(SerializerDelegate<T1, T2, T3, T4, T5, T6, T7, T8> serializer, T1 value1,
		T2 value2, T3 value3, T4 value4, T5 value5, T6 value6, T7 value7, T8 value8,
		HandleResponseDelegate responseCallback, object responseState)
	{
		if (!TryStartRequest())
			return false;

		connection.SendRequest(serializer, value1, value2, value3, value4, value5, value6, value7, value8, responseCallback, responseState);
		return true;
	}
}
