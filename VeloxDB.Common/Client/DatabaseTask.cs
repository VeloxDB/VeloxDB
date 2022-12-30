using System;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using VeloxDB.Common;
using VeloxDB.Networking;
using VeloxDB.Protocol;
using static VeloxDB.Client.ConnectionFactory;
using static VeloxDB.Client.ConnectionPool;

namespace VeloxDB.Client;

/// <summary>
/// Represents an asynchonous result of a database API operation of a given return type.
/// Use this class to create asynchronous database operations.
/// </summary>
public abstract class DatabaseTask<T> : DatabaseTask
{
	T result;

	internal DatabaseTask(string connString, int operationId) :
		base(connString, operationId)
	{
	}

	internal void SetResult(T result)
	{
		Action t;

		lock (this)
		{
			Checker.AssertFalse(isCompleted);
			this.result = result;
			Volatile.Write(ref isCompleted, true);
			t = awaitContinuation;

			if (hasWaiter)
				Monitor.Pulse(this);
		}

		t?.Invoke();
	}

	/// <summary>
	/// Used by the await mechanism (C# comiler). Do not use this method directly.
	/// </summary>
	public new T GetResult()
	{
		base.GetResult();
		return result;
	}

	/// <summary>
	/// Used by the await mechanism (C# comiler). Do not use this method directly.
	/// </summary>
	public new DatabaseTask<T> GetAwaiter()
	{
		return this;
	}
}

/// <summary>
/// Represents an asynchonous result of a database API operation without a return type. Use this class to create asynchronous.
/// Use this class to create asynchronous database operations.
/// </summary>
public abstract class DatabaseTask : INotifyCompletion
{
	const int initRetryInterval = 0;
	const int maxRetryInterval = 200;

	internal static readonly MethodInfo[] SendRequestMethods;
	internal static readonly Type[] SerializerDelegateGenericTypes;

	internal static readonly MethodInfo ExecuteMethod = typeof(DatabaseTask).GetMethods(
		BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly).
		Where(x => x.Name.Equals(nameof(Execute)) && x.IsGenericMethod).First();

	internal static readonly MethodInfo ProcessResponseErrorMethod = typeof(DatabaseTask).GetMethod(nameof(ProcessResponseError),
		BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Instance);

	internal static readonly MethodInfo SetErrorMethod = typeof(DatabaseTask).GetMethod(nameof(SetErrorResult),
		BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Instance);

	internal static readonly MethodInfo WaitMethod = typeof(DatabaseTask).GetMethod(nameof(Wait),
		BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Instance);

	internal static readonly ConstructorInfo ConstructorMethod = typeof(DatabaseTask).GetConstructor(
		BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly, new Type[] { typeof(string), typeof(int) });

	internal static readonly MethodInfo GetConnectionMethod = typeof(DatabaseTask).
		GetProperty(nameof(Connection), BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly).GetMethod;

	internal static readonly MethodInfo GetDeserializerTableMethod = typeof(DatabaseTask).GetProperty(nameof(DeserializerTable),
		BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Instance).GetGetMethod(true);

	internal static readonly MethodInfo GetSerializerManagerMethod = typeof(DatabaseTask).GetProperty(nameof(SerializerManager),
		BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Instance).GetGetMethod(true);

	internal static readonly MethodInfo GetSerializerMethod = typeof(DatabaseTask).GetProperty(nameof(Serializer),
		BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Instance).GetGetMethod(true);

	internal static readonly MethodInfo GetDeserializerMethod = typeof(DatabaseTask).GetProperty(nameof(Deserializer),
		BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Instance).GetGetMethod(true);

	internal static readonly MethodInfo ThrowMismatchMethod = typeof(DatabaseTask).GetMethod(nameof(ThrowMismatch),
		BindingFlags.DeclaredOnly | BindingFlags.NonPublic | BindingFlags.Static);

	internal static Action<ConnectionEntry, Exception, object> GetConnectionContinuation =
		(c, e, p) => ((DatabaseTask)p).PostGetConnection(c, e);

	string connString;
	Type interfaceType;
	OperationData operationData;
	int operationId;
	long startTickCount;
	int retryInterval;

	internal bool isCompleted;
	internal Action awaitContinuation;
	internal bool hasWaiter;
	Exception error;

	static DatabaseTask()
	{
		SendRequestMethods = new MethodInfo[9];
		for (int i = 0; i < SendRequestMethods.Length; i++)
		{
			SendRequestMethods[i] = typeof(PooledConnection).
				GetMethod("SendRequest" + i.ToString(), BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);
		}

		SerializerDelegateGenericTypes = new Type[9];
		SerializerDelegateGenericTypes[1] = typeof(SerializerDelegate<>);
		SerializerDelegateGenericTypes[2] = typeof(SerializerDelegate<,>);
		SerializerDelegateGenericTypes[3] = typeof(SerializerDelegate<,,>);
		SerializerDelegateGenericTypes[4] = typeof(SerializerDelegate<,,,>);
		SerializerDelegateGenericTypes[5] = typeof(SerializerDelegate<,,,,>);
		SerializerDelegateGenericTypes[6] = typeof(SerializerDelegate<,,,,,>);
		SerializerDelegateGenericTypes[7] = typeof(SerializerDelegate<,,,,,,>);
		SerializerDelegateGenericTypes[8] = typeof(SerializerDelegate<,,,,,,,>);
	}

	internal DatabaseTask(string connString, int operationId)
	{
		this.connString = connString;
		this.operationId = operationId;
		this.retryInterval = initRetryInterval;
	}

	internal PooledConnection Connection => operationData.Connection;
	internal Delegate[] DeserializerTable => operationData.DeserializerTable;
	internal SerializerManager SerializerManager => operationData.SerializerManager;
	internal Delegate Serializer => operationData.Serializer;
	internal Delegate Deserializer => operationData.Deserializer;

	/// <summary>
	/// Used by the await mechanism (C# comiler). Do not use this property directly.
	/// </summary>
	public bool IsCompleted => isCompleted;

	internal abstract bool TryExecute();
	internal abstract DbAPIOperationType GetOperationType();

	/// <summary>
	/// Used by the await mechanism (C# comiler). Do not use this method directly.
	/// </summary>
	public void OnCompleted(Action awaitContinuation)
	{
		bool immediate = false;
		lock (this)
		{
			immediate = isCompleted;
			if (!isCompleted)
				this.awaitContinuation = awaitContinuation;
		}

		if (immediate)
			awaitContinuation();
	}

	internal void SetResult()
	{
		Action t;
		lock (this)
		{
			Checker.AssertFalse(isCompleted);
			Volatile.Write(ref isCompleted, true);
			t = awaitContinuation;

			if (hasWaiter)
				Monitor.Pulse(this);
		}

		t?.Invoke();
	}

	internal void SetErrorResult(Exception e)
	{
		Action t;
		lock (this)
		{
			Checker.AssertFalse(isCompleted);
			error = e;
			Volatile.Write(ref isCompleted, true);
			t = awaitContinuation;

			if (hasWaiter)
				Monitor.Pulse(this);
		}

		t?.Invoke();
	}

	/// <summary>
	/// Used by the await mechanism (C# comiler). Do not use this method directly.
	/// </summary>
	public void GetResult()
	{
		Wait();
	}

	/// <summary>
	/// Used by the await mechanism (C# comiler). Do not use this method directly.
	/// </summary>
	public DatabaseTask GetAwaiter()
	{
		return this;
	}

	internal void Wait()
	{
		if (!isCompleted)
		{
			lock (this)
			{
				if (!isCompleted)
				{
					hasWaiter = true;
					Monitor.Wait(this);
					Checker.AssertTrue(isCompleted);
				}
			}
		}

		if (error != null)
			throw error;
	}

	internal void Execute<T>()
	{
		try
		{
			interfaceType = typeof(T);
			startTickCount = Environment.TickCount64;
			ConnectionPoolCollection.GetPool(connString).GetConnection(false, GetConnectionContinuation, this);
		}
		catch (Exception e)
		{
			SetErrorResult(e);
		}

		if (error != null)
			throw error;
	}

	private bool IsRetryPossible()
	{
		int retryTimeout = ConnectionPoolCollection.GetPool(connString).ConnParams.RetryTimeout;
		return Environment.TickCount64 - startTickCount <= retryTimeout || retryTimeout == -1;
	}

	private void PostGetConnection(ConnectionEntry connection, Exception e)
	{
		if (e != null)
		{
			if (e is DbAPIUnavailableException)
			{
				int retryTimeout = ConnectionPoolCollection.GetPool(connString).ConnParams.RetryTimeout;
				if (Environment.TickCount64 - startTickCount <= retryTimeout || retryTimeout == -1)
				{
					Task.Delay(GetNextRetryInterval()).ContinueWith(t =>
					{
						ConnectionPoolCollection.GetPool(connString).GetConnection(true, GetConnectionContinuation, this);
					});

					return;
				}
				else
				{
					SetErrorResult(e);
					return;
				}
			}
			else
			{
				SetErrorResult(e);
				return;
			}
		}

		try
		{
			operationData = ConnectionFactory.GetOperationData(connection, connString, interfaceType, operationId);
			if (TryExecute())
				return;
		}
		catch (Exception ex)
		{
			SetErrorResult(ex);
			return;
		}
		finally
		{
			SerializerContext.Instance.Reset();
		}

		if (!IsRetryPossible())
		{
			SetErrorResult(new DbAPIUnavailableException());
			return;
		}

		ConnectionPoolCollection.GetPool(connString).GetConnection(true, GetConnectionContinuation, this);
	}

	private async ValueTask ObtainOperationDataAsync(ConnectionPool pool, bool preferNewer)
	{
		while (true)
		{
			try
			{
				TaskCompletionSource<ConnectionEntry> tcs = new TaskCompletionSource<ConnectionEntry>();
				pool.GetConnection(preferNewer, (c, e, p) =>
				{
					if (e != null)
					{
						tcs.SetException(e);
					}
					else
					{
						tcs.SetResult(c);
					}
				}, this);

				ConnectionEntry connection = await tcs.Task;

				operationData = ConnectionFactory.GetOperationData(connection,
					connString, interfaceType, operationId);
				return;
			}
			catch (DbAPIUnavailableException)
			{
				int retryTimeout = pool.ConnParams.RetryTimeout;
				if (Environment.TickCount64 - startTickCount <= retryTimeout || retryTimeout == -1)
					await Task.Delay(GetNextRetryInterval());
				else
					throw;
			}
		}
	}

	internal static void ThrowMismatch()
	{
		throw new DbAPIMismatchException();
	}

	internal void ProcessResponseError(MessageReader reader, Exception commError, out Exception e, out bool retryScheduled)
	{
		if (commError != null)
		{
			e = commError;
		}
		else
		{
			ResponseType responseType = (ResponseType)reader.ReadByte();

			if (responseType == ResponseType.Response)
			{
				e = null;
				retryScheduled = false;
				return;
			}

			if (responseType == ResponseType.APIUnavailable)
			{
				e = new DbAPIUnavailableException();
			}
			else if (responseType == ResponseType.ProtocolError)
			{
				e = new DbAPIProtocolException();
			}
			else
			{
				DeserializerContext context = new DeserializerContext();
				object obj = Methods.DeserializePolymorphType(reader, context, DeserializerTable, 0);
				e = obj as Exception;
			}
		}

		if (e == null)
		{
			e = new DbAPIProtocolException();
			retryScheduled = false;
			return;
		}

		bool shouldRetry = ExceptionRetryable(e, out bool closeConnection);
		if (closeConnection)
			Connection.Invalidate();


		ConnectionPool pool = ConnectionPoolCollection.GetPool(connString);

		if (!shouldRetry || !IsRetryPossible())
		{
			retryScheduled = false;
			return;
		}

		Exception outerError = e;
		Task.Delay(GetNextRetryInterval()).ContinueWith(async (t) =>
		{
			ConnectionPool pool = ConnectionPoolCollection.GetPool(connString);

			Exception err = null;
			while (IsRetryPossible())
			{
				try
				{
					await ObtainOperationDataAsync(pool, closeConnection);
				}
				catch (Exception e)
				{
					err = e;
					break;
				}

				if (TryExecute())
				{
					err = null;
					break;
				}
				else
				{
					err = outerError;
				}
			}

			if (err != null)
				SetErrorResult(err);
		});

		e = null;
		retryScheduled = true;
	}

	private int GetNextRetryInterval()
	{
		int t = retryInterval;
		if (retryInterval == 0)
		{
			retryInterval = 1;
		}
		else
		{
			retryInterval = Math.Min(maxRetryInterval, retryInterval * 2);
		}

		return t;
	}

	private bool ExceptionRetryable(Exception e, out bool closeConnection)
	{
		if (e.GetType() == typeof(CommunicationException) || (e is CommunicationObjectAbortedException) || (e is ObjectDisposedException))
		{
			closeConnection = true;
			if (e is CommunicationObjectAbortedException)
			{
				// If we fail to open a connection, this is idicated with CommunicationObjectAbortedException
				return (e as CommunicationObjectAbortedException).AbortedPhase != AbortedPhase.OpenAttempt &&
					GetOperationType() == DbAPIOperationType.Read;
			}

			return GetOperationType() == DbAPIOperationType.Read;
		}

		if (e is ChunkTimeoutException)
		{
			closeConnection = true;
			return true;
		}

		if (e is DbAPIUnavailableException)
		{
			closeConnection = true;
			return true;
		}

		DatabaseException dbe = e as DatabaseException;
		if (dbe == null)
		{
			closeConnection = false;
			return false;
		}

		closeConnection = dbe.Detail.ErrorType == DatabaseErrorType.TransactionNotAllowed ||
			dbe.Detail.ErrorType == DatabaseErrorType.NotApplicable;

		return dbe.Detail.IsRetryable();
	}
}
