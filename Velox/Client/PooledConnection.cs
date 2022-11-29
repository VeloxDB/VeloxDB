using System;
using System.Threading;
using Velox.Common;
using Velox.Networking;

namespace Velox.Client;

internal sealed class PooledConnection
{
	private enum State
	{
		Open,
		Closing,
		Closed
	}

	readonly object sync = new object();
	ConnectionPool owner;
	int pendingReqCount;
	State state;
	ClientConnection connection;
	TimeSpan timestamp;

	public PooledConnection(ConnectionPool owner, ClientConnection connection, TimeSpan timestamp)
	{
		this.owner = owner;
		this.connection = connection;
		this.timestamp = timestamp;
		this.state = State.Open;

		this.connection.ResponseProcessed += ResponseProcessed;
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
		lock (sync)
		{
			if (state == State.Closing || state == State.Closed)
				return;

			owner.RemoveConnection(this);
			if (pendingReqCount == 0)
			{
				state = State.Closed;
				connection.CloseAsync();
			}
			else
			{
				state = State.Closing;
			}
		}
	}

	private bool TryStartRequest()
	{
		lock (sync)
		{
			if (state != State.Open)
				return false;

			pendingReqCount++;
			return true;
		}
	}

	private void EndRequest()
	{
		lock (sync)
		{
			pendingReqCount--;
			Checker.AssertFalse(pendingReqCount < 0);
			if (state != State.Open)
			{
				Checker.AssertTrue(state != State.Closed);
				if (pendingReqCount == 0)
				{
					state = State.Closed;
					connection.CloseAsync();
				}
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
