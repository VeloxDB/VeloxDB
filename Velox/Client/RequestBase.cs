using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Velox.Networking;
using Velox.Protocol;
using static Velox.Client.ConnectionFactory;

namespace Velox.Client;

internal abstract class RequestBase
{
	const int initRetryInterval = 0;
	const int maxRetryInterval = 200;

	public static readonly MethodInfo[] SendRequestMethods;
	public static readonly Type[] SerializerDelegateGenericTypes;

	public static readonly MethodInfo ExecuteMethod = typeof(RequestBase).GetMethods(
		BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly).
		Where(x => x.Name.Equals(nameof(Execute)) && x.IsGenericMethod).First();

	public static readonly MethodInfo ProcessResponseErrorMethod = typeof(RequestBase).GetMethod(nameof(ProcessResponseError),
		BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance);

	public static readonly ConstructorInfo ConstructorMethod = typeof(RequestBase).GetConstructor(
		BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly, new Type[] { typeof(string), typeof(int), typeof(object) });

	public static readonly MethodInfo WaitTaskMethod = typeof(RequestBase).GetMethods(BindingFlags.Public | BindingFlags.Static).
		Where(x => x.Name.Equals(nameof(WaitTask)) && x.IsGenericMethod).First();

	public static readonly MethodInfo GetTCSMethod = typeof(RequestBase).
		GetProperty(nameof(TaskCompletionSource), BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly).GetMethod;

	public static readonly MethodInfo GetConnectionMethod = typeof(RequestBase).
		GetProperty(nameof(Connection), BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly).GetMethod;

	public static readonly MethodInfo GetDeserializerTableMethod = typeof(RequestBase).GetProperty(nameof(DeserializerTable),
		BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance).GetGetMethod();

	public static readonly MethodInfo GetSerializerManagerMethod = typeof(RequestBase).GetProperty(nameof(SerializerManager),
		BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance).GetGetMethod();

	public static readonly MethodInfo GetSerializerMethod = typeof(RequestBase).GetProperty(nameof(Serializer),
		BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance).GetGetMethod();

	public static readonly MethodInfo GetDeserializerMethod = typeof(RequestBase).GetProperty(nameof(Deserializer),
		BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance).GetGetMethod();

	public static readonly MethodInfo ThrowMismatchMethod = typeof(RequestBase).GetMethod(nameof(ThrowMismatch),
		BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Static);

	string connString;
	Type interfaceType;
	OperationData operationData;
	object taskCompletionSource;
	int operationId;
	long startTickCount;
	int retryInterval;

	static RequestBase()
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

	public RequestBase(string connString, int operationId, object taskCompletionSource)
	{
		this.connString = connString;
		this.operationId = operationId;
		this.taskCompletionSource = taskCompletionSource;
		this.retryInterval = initRetryInterval;
	}

	public object TaskCompletionSource => taskCompletionSource;
	public PooledConnection Connection => operationData.Connection;
	public Delegate[] DeserializerTable => operationData.DeserializerTable;
	public SerializerManager SerializerManager => operationData.SerializerManager;
	public Delegate Serializer => operationData.Serializer;
	public Delegate Deserializer => operationData.Deserializer;

	public abstract bool TryExecute();
	public abstract void SetErrorResult(Exception e);
	public abstract DbAPIOperationType GetOperationType();

	public async void Execute<T>()
	{
		try
		{
			await ExecuteInternal<T>();
		}
		catch (Exception e)
		{
			SetErrorResult(e);
		}
	}

	private async ValueTask ExecuteInternal<T>()
	{
		interfaceType = typeof(T);

		ConnectionPool pool = ConnectionPoolCollection.GetPool(connString);
		startTickCount = Environment.TickCount64;
		int retryTimeout = pool.ConnParams.RetryTimeout;

		bool isFirst = true;
		while (isFirst || IsRetryPossible(retryTimeout))
		{
			await ObtainOperationData(pool, !isFirst);

			try
			{
				if (TryExecute())
					return;
			}
			finally
			{
				SerializerContext.Instance.Reset();
			}

			isFirst = false;
		}

		throw new DbAPIUnavailableException();
	}

	private bool IsRetryPossible(int retryTimeout)
	{
		return Environment.TickCount64 - startTickCount <= retryTimeout || retryTimeout == -1;
	}

	private async ValueTask ObtainOperationData(ConnectionPool pool, bool preferNewer)
	{
		while (true)
		{
			try
			{
				operationData = await ConnectionFactory.GetOperationData(pool,
					connString, interfaceType, operationId, preferNewer);
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

	public static void ThrowMismatch()
	{
		throw new DbAPIMismatchException();
	}

	public void ProcessResponseError(MessageReader reader, Exception commError, out Exception e, out bool retryScheduled)
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
		int retryTimeout = pool.ConnParams.RetryTimeout;

		if (!shouldRetry || !IsRetryPossible(retryTimeout))
		{
			retryScheduled = false;
			return;
		}

		Exception outerError = e;
		Task.Delay(GetNextRetryInterval()).ContinueWith(async (t) =>
		{
			ConnectionPool pool = ConnectionPoolCollection.GetPool(connString);

			Exception err = null;
			while (IsRetryPossible(retryTimeout))
			{
				try
				{
					await ObtainOperationData(pool, closeConnection);
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

	public static void WaitTask<K>(Task<K> task)
	{
		try
		{
			task.Wait();
		}
		catch (AggregateException e)
		{
			throw e.InnerException;
		}
	}
}
