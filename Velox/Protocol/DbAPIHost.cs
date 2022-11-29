using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Reflection.Emit;
using Velox.Common;
using Velox.Networking;
using Velox.Protocol;

namespace Velox.Server;

internal delegate void APIRequestCallback(APIRequest request);
internal delegate void ParametrizedAPIRequestCallback(ParametrizedAPIRequest request);

internal delegate IPendingRequest InvokeOperationDelegate(object implementation, Connection connection, long requestId,
	MessageReader reader, ProtocolDeserializeDelegate[] deserializerTable, SerializerManager serializerManager);

internal delegate IPendingRequest ParametrizedInvokeOperationDelegate(object param, object implementation,
	Connection connection, long requestId, MessageReader reader, ProtocolDeserializeDelegate[] deserializerTable,
	SerializerManager serializerManager);

internal sealed class DbAPIHost
{
	RWSpinLock sync = new RWSpinLock();

	int backlogSize;
	int maxOpenConnCount;
	int bufferPoolSize;
	TimeSpan inactivityInterval;
	TimeSpan inactivityTimeout;
	int maxQueuedChunkCount;

	Host host;

	Dictionary<string, HostedService> services;

	static DbAPIHost()
	{
		AssemblyName aName = new AssemblyName("__dynprotmodule");
		AssemblyBuilder ab = AssemblyBuilder.DefineDynamicAssembly(aName, AssemblyBuilderAccess.RunAndCollect);
		ModuleBuilder moduleBuilder = ab.DefineDynamicModule(aName.Name);
	}

	public DbAPIHost(int backlogSize, int maxOpenConnCount, IPEndPoint endpoint, int bufferPoolSize,
		TimeSpan inactivityInterval, TimeSpan inactivityTimeout, int maxQueuedChunkCount) :
		this(backlogSize, maxOpenConnCount, new IPEndPoint[] { endpoint }, bufferPoolSize, inactivityInterval, inactivityTimeout, maxQueuedChunkCount)
	{
	}

	public DbAPIHost(int backlogSize, int maxOpenConnCount, IPEndPoint[] endpoints, int bufferPoolSize,
		TimeSpan inactivityInterval, TimeSpan inactivityTimeout, int maxQueuedChunkCount)
	{
		this.backlogSize = backlogSize;
		this.maxOpenConnCount = maxOpenConnCount;
		this.bufferPoolSize = bufferPoolSize;
		this.inactivityInterval = inactivityInterval;
		this.inactivityTimeout = inactivityTimeout;
		this.maxQueuedChunkCount = maxQueuedChunkCount;

		services = new Dictionary<string, HostedService>(2, StringComparer.OrdinalIgnoreCase);
		host = new Host(backlogSize, maxOpenConnCount, endpoints, bufferPoolSize,
			inactivityInterval, inactivityTimeout, maxQueuedChunkCount, true, MessageHandler);
	}

	public void Stop()
	{
		host.Stop();
	}

	public void StopService(string serviceName)
	{
		sync.EnterWriteLock();
		try
		{
			if (!services.TryGetValue(serviceName, out HostedService service))
				throw new ArgumentException("Given service has not been found.");

			if (service.IsStopped)
				return;

			service.Stop();
		}
		finally
		{
			sync.ExitWriteLock();
		}
	}

	public void StartService(string serviceName)
	{
		sync.EnterWriteLock();
		try
		{
			if (!services.TryGetValue(serviceName, out HostedService service))
				throw new ArgumentException("Given service has not been found.");

			if (!service.IsStopped)
				return;

			service.Start();
		}
		finally
		{
			sync.ExitWriteLock();
		}
	}

	public static void PrepareSerialization(Type requiredParamType, Type[] types,
		out SerializerManager serializerManager, out DeserializerManager deserializerManager, out ProtocolDiscoveryContext discoveryContext)
	{
		deserializerManager = new DeserializerManager();
		serializerManager = new SerializerManager(deserializerManager.ModuleBuilder);
		discoveryContext = new ProtocolDiscoveryContext();

		for (int i = 0; i < types.Length; i++)
		{
			Type type = types[i];

			ProtocolInterfaceDescriptor interfaceDesc = discoveryContext.GetInterfaceDescriptor(type,
				requiredParamType != null ? 1 : 0, out var inTypes, out var outTypes);

			serializerManager.GetInterfaceSerializers(interfaceDesc, outTypes, ProtocolInterfaceDirection.Response, 0);
			deserializerManager.GetInterfaceDeserializers(interfaceDesc, inTypes,
				ProtocolInterfaceDirection.Request, requiredParamType != null ? 1 : 0);
		}
	}

	public static void ValidateAPIs(Type requiredParamType, Type[] apiTypes)
	{
		if (!requiredParamType.IsClass)
			throw new ArgumentException("Required parameter type must be a class.");

		HashSet<string> names = new HashSet<string>(8, StringComparer.OrdinalIgnoreCase);

		for (int i = 0; i < apiTypes.Length; i++)
		{
			Type type = apiTypes[i];

			string name = type.FullName;
			DbAPIAttribute dba = type.GetCustomAttribute<DbAPIAttribute>();
			if (dba.Name != null)
				name = dba.Name;

			if (type.IsInterface || type.IsAbstract)
				throw DbAPIDefinitionException.CreateAbstractAPI(name);

			if (names.Contains(name))
				throw DbAPIDefinitionException.CreateAPINameDuplicate(name);

			names.Add(name);

			MethodInfo[] methods = type.GetMethods(BindingFlags.Public | BindingFlags.FlattenHierarchy | BindingFlags.Instance).
				Where(x => x.IsDefined(typeof(DbAPIOperationAttribute))).ToArray();

			HashSet<string> operNames = new HashSet<string>(methods.Length, StringComparer.OrdinalIgnoreCase);

			for (int j = 0; j < methods.Length; j++)
			{
				string operName = methods[j].Name;
				DbAPIOperationAttribute dboa = methods[j].GetCustomAttribute<DbAPIOperationAttribute>();
				if (dboa.Name != null)
					operName = dboa.Name;

				if (operNames.Contains(operName))
					throw DbAPIDefinitionException.CreateAPINameDuplicate(operName);

				operNames.Add(operName);

				if (!IsRequiredParamValid(methods[j], requiredParamType))
					throw DbAPIDefinitionException.CreateOperationRequiredParamsMissing(operName, name);
			}
		}
	}

	public void HostService(string serviceName, object[] apis, APIRequestCallback requestCallback, Guid versionGuid,
		SerializerManager serializerManager = null, DeserializerManager deserializerManager = null,
		ProtocolDiscoveryContext discoveryContext = null, bool isInitiallyStopped = false)
	{
		HostServiceInternal(serviceName, null, apis, requestCallback, versionGuid, serializerManager, deserializerManager,
			discoveryContext, isInitiallyStopped);
	}

	public void HostService(string serviceName, Type requiredParamType, object[] apis, ParametrizedAPIRequestCallback requestCallback,
		Guid versionGuid, SerializerManager serializerManager = null, DeserializerManager deserializerManager = null,
		ProtocolDiscoveryContext discoveryContext = null, bool isInitiallyStopped = false)
	{
		HostServiceInternal(serviceName, requiredParamType, apis, requestCallback,
			versionGuid, serializerManager, deserializerManager, discoveryContext, isInitiallyStopped);
	}

	internal void HostServiceInternal(string serviceName, Type requiredParamType, object[] apis, Delegate requestCallback,
		Guid versionGuid, SerializerManager serializerManager = null, DeserializerManager deserializerManager = null,
		ProtocolDiscoveryContext discoveryContext = null, bool isInitiallyStopped = false)
	{
		sync.EnterWriteLock();

		try
		{
			Type[] types = Array.ConvertAll(apis, api => api.GetType());

			if (serializerManager == null)
				PrepareSerialization(requiredParamType, types, out serializerManager, out deserializerManager, out discoveryContext);

			ProtocolDescriptor protocolDescriptor = new ProtocolDescriptor(versionGuid, discoveryContext.Interfaces);

			services.TryGetValue(serviceName, out HostedService oldService);

			HostedService service = new HostedService(serviceName, protocolDescriptor, serializerManager, deserializerManager);
			services[serviceName] = service;

			if ((oldService != null && oldService.IsStopped) || isInitiallyStopped)
				service.Stop();

			if (oldService != null)
				oldService.Stop();

			for (int i = 0; i < apis.Length; i++)
			{
				HostAPI(service, discoveryContext.GetInterfaceDescriptor(apis[i].GetType()), apis[i], requiredParamType, requestCallback);
			}
		}
		finally
		{
			sync.ExitWriteLock();
		}
	}

	private void HostAPI(HostedService service, ProtocolInterfaceDescriptor interfaceDescriptor,
		object implementation, Type requiredParamType, Delegate requestCallback)
	{
		Type type = implementation.GetType();

		string name = type.FullName;
		DbAPIAttribute dba = type.GetCustomAttribute<DbAPIAttribute>();
		if (dba.Name != null)
			name = dba.Name;

		MethodInfo[] methods = type.GetMethods(BindingFlags.Public | BindingFlags.FlattenHierarchy | BindingFlags.Instance).
			Where(x => x.IsDefined(typeof(DbAPIOperationAttribute))).ToArray();

		HashSet<string> nameSet = new HashSet<string>(methods.Length, StringComparer.OrdinalIgnoreCase);
		for (int i = 0; i < methods.Length; i++)
		{
			string operName = methods[i].Name;
			DbAPIOperationAttribute dboa = methods[i].GetCustomAttribute<DbAPIOperationAttribute>();
			if (dboa.Name != null)
				operName = dboa.Name;

			if (nameSet.Contains(operName))
				throw DbAPIDefinitionException.CreateAPINameDuplicate(operName);

			nameSet.Add(operName);

			if (!IsRequiredParamValid(methods[i], requiredParamType))
				throw DbAPIDefinitionException.CreateOperationRequiredParamsMissing(operName, name);
		}

		Delegate[] methodSerializers = service.SerializerManager.GetInterfaceSerializers(
			interfaceDescriptor, null, ProtocolInterfaceDirection.Response, 0);

		MethodInfo[] methodDeserializers = service.DeserializerManager.GetInterfaceDeserializers(
			interfaceDescriptor, null, ProtocolInterfaceDirection.Request, requiredParamType != null ? 1 : 0);

		Checker.AssertTrue(methodSerializers.Length == methodDeserializers.Length);

		string guid = Guid.NewGuid().ToString("N");
		TypeBuilder tb = service.DeserializerManager.ModuleBuilder.DefineType("__" + guid,
			TypeAttributes.Class | TypeAttributes.Public, typeof(object));

		FieldBuilder[] serializerFields = new FieldBuilder[methodSerializers.Length];
		for (int i = 0; i < methodSerializers.Length; i++)
		{
			serializerFields[i] = tb.DefineField("serializer" + i.ToString(),
				methodSerializers[i].GetType(), FieldAttributes.Private | FieldAttributes.Static);
		}

		Checker.AssertTrue(interfaceDescriptor.Operations.Length == methods.Length);

		Dictionary<string, MethodBuilder> invokeMethods =
			new Dictionary<string, MethodBuilder>(methods.Length, StringComparer.OrdinalIgnoreCase);

		for (int i = 0; i < methods.Length; i++)
		{
			string operName = methods[i].Name;
			DbAPIOperationAttribute dboa = methods[i].GetCustomAttribute<DbAPIOperationAttribute>();
			if (dboa.Name != null)
				name = dboa.Name;

			invokeMethods[operName] = CreateInvokeMethod(tb, methods[i], serializerFields[i], methodDeserializers[i], requiredParamType,
				(dboa.ObjectGraphSupport & DbAPIObjectGraphSupportType.Response) != 0);
		}

		Type finalType = tb.CreateType();

		Delegate[] invokeDelegates = new Delegate[interfaceDescriptor.Operations.Select(x => x.Id).Max() + 1];
		foreach (var kv in invokeMethods)
		{
			MethodInfo mi = finalType.GetMethod(kv.Value.Name, BindingFlags.Public | BindingFlags.Static);
			int id = interfaceDescriptor.GetOperationByName(kv.Key).Id;
			invokeDelegates[id] = Delegate.CreateDelegate(requiredParamType != null ?
				typeof(ParametrizedInvokeOperationDelegate) : typeof(InvokeOperationDelegate), mi);
		}

		for (int i = 0; i < methodSerializers.Length; i++)
		{
			FieldInfo fi = finalType.GetField(serializerFields[i].Name, BindingFlags.NonPublic | BindingFlags.Static);
			fi.SetValue(null, methodSerializers[i]);
		}

		InterfaceEntry entry = new InterfaceEntry(implementation, invokeDelegates, interfaceDescriptor);
		if (requestCallback is APIRequestCallback)
		{
			entry.RequestCallback = (APIRequestCallback)requestCallback;
		}
		else
		{
			entry.ParamRequestCallback = (ParametrizedAPIRequestCallback)requestCallback;
		}

		service.AddInterface(interfaceDescriptor.Id, name, entry);
	}

	private static bool IsRequiredParamValid(MethodInfo method, Type requiredParamType)
	{
		if (requiredParamType == null)
			return true;

		ParameterInfo[] ps = method.GetParameters();
		if (ps.Length == 0 && requiredParamType != null)
			return false;

		return ps[0].ParameterType == requiredParamType;
	}

	private void MessageHandler(Connection connection, long requestId, MessageReader reader)
	{
		try
		{
			ushort formatVersion = reader.ReadUShort();
			if (formatVersion != SerializerManager.FormatVersion)
			{
				connection.CloseAsync();
				return;
			}

			RequestType requestType = (RequestType)reader.ReadByte();
			if (requestType == RequestType.Operation)
			{
				HandleOperationRequest(connection, requestId, reader);
			}
			else if (requestType == RequestType.Connect)
			{
				HandleConnectRequest(connection, requestId, reader);
			}
			else
			{
				throw new DbAPIProtocolException();
			}
		}
		catch (CorruptMessageException)
		{
			connection.CloseAsync();
		}
		catch (CommunicationException)
		{
			connection.CloseAsync();
		}
		catch (ObjectDisposedException)
		{
			connection.CloseAsync();
		}
		catch (Exception e) when (ProtocolOperationDescriptor.IsBuiltInExceptionType(e.GetType()))
		{
			SendErrorResponse(connection, requestId, e);
		}
	}

	private void HandleConnectRequest(Connection connection, long requestId, MessageReader reader)
	{
		sync.EnterWriteLock();
		try
		{
			string serviceName = reader.ReadString();
			if (!services.TryGetValue(serviceName, out HostedService service) || service.IsStopped)
				throw new DbAPIUnavailableException();

			if (connection.Tag != null)
				throw new DbAPIProtocolException();

			SerializerContext context = SerializerContext.Instance;
			context.Init(service.SerializerManager, true);

			try
			{
				connection.Tag = service;

				connection.SendResponse((writer, service) =>
				{
					writer.WriteByte((byte)ResponseType.Response);
					service.ProtocolDescriptor.Serialize(writer);
				}, service, requestId);
			}
			finally
			{
				context.Reset();
			}
		}
		finally
		{
			sync.ExitWriteLock();
		}
	}

	private static object DeserializeWithException(MessageReader reader,
		DeserializerContext context, ProtocolDeserializeDelegate[] deserializerTable, int depth)
	{
		throw new DbAPIMismatchException();
	}

	private void HandleOperationRequest(Connection connection, long requestId, MessageReader reader)
	{
		sync.EnterReadLock();

		try
		{
			HostedService service = (HostedService)connection.Tag;

			if (service == null)
				throw new DbAPIProtocolException();

			if (service.IsStopped)
				throw new DbAPIUnavailableException();

			ushort interfaceId = reader.ReadUShort();
			ushort operationId = reader.ReadUShort();

			if (!service.TryGetInterface(interfaceId, out InterfaceEntry interfaceEntry))
				throw new DbAPIProtocolException();

			if (!interfaceEntry.TryGetInvoker(operationId, out Delegate operationInvoker))
				throw new DbAPIProtocolException();

			ProtocolOperationDescriptor opDesc = interfaceEntry.InterfaceDesc.Operations[operationId];

			try
			{
				sync.ExitReadLock();
				try
				{
					if (interfaceEntry.RequestCallback != null)
					{
						interfaceEntry.RequestCallback(new APIRequest((InvokeOperationDelegate)operationInvoker,
							interfaceEntry.Implementation, connection, requestId, reader,
							service.DeserializerTable, service.SerializerManager, opDesc.OperationType));
					}
					else
					{
						interfaceEntry.ParamRequestCallback(new ParametrizedAPIRequest(
							(ParametrizedInvokeOperationDelegate)operationInvoker, interfaceEntry.Implementation,
							connection, requestId, reader, service.DeserializerTable,
							service.SerializerManager, opDesc.OperationType));
					}
				}
				finally
				{
					sync.EnterReadLock();
				}
			}
			catch (Exception e) when (e is not CriticalDatabaseException)
			{
				SendErrorResponse(connection, requestId, e);
			}
		}
		catch (Exception e) when (ProtocolOperationDescriptor.IsBuiltInExceptionType(e.GetType()))
		{
			SendErrorResponse(connection, requestId, e);
		}
		finally
		{
			sync.ExitReadLock();
		}
	}

	internal static void SendErrorResponse(Connection connection, long requestId, Exception e)
	{
		ResponseType? type = (e as DbAPIErrorException)?.ResponseCode;

		if (type.HasValue && type.Value != ResponseType.Error)
		{
			try
			{
				connection.SendResponse((writer, ex) =>
				{
					writer.WriteByte((byte)type.Value);
				}, e, requestId);
			}
			catch (ObjectDisposedException)
			{
			}
			catch (CommunicationException)
			{
			}

			return;
		}

		if (connection.Tag == null)
		{
			connection.CloseAsync();
			return;
		}

		HostedService service = (HostedService)connection.Tag;
		TypeSerializerEntry entry = service.SerializerManager.GetTypeSerializer(e.GetType());
		if (entry == null)
		{
			Tracing.Error("Exception occured while executing API operation.");
			Tracing.Error(e);
			e = new DbAPIUnknownErrorException();
			entry = service.SerializerManager.GetTypeSerializer(e.GetType());
		}

		SerializerContext context = SerializerContext.Instance;
		context.Init(service.SerializerManager, true);

		try
		{
			connection.SendResponse((writer, ex) =>
			{
				writer.WriteByte((byte)ResponseType.Error);
				Methods.SerializePolymorphType(writer, ex, SerializerContext.Instance, 0);
			}, e, requestId);
		}
		catch (ObjectDisposedException)
		{
		}
		catch (CommunicationException)
		{
		}
		finally
		{
			context.Reset();
		}
	}

	private MethodBuilder CreateInvokeMethod(TypeBuilder typeBuilder, MethodInfo methodInfo,
		FieldBuilder serField, MethodInfo methodDeserializer, Type requiredParamType, bool supportObjectGraph)
	{
		int n = requiredParamType != null ? 1 : 0;
		Type[] paramTypes = new Type[n + 6];

		if (n > 0)
			paramTypes[0] = typeof(object);

		paramTypes[n + 0] = typeof(object);
		paramTypes[n + 1] = typeof(Connection);
		paramTypes[n + 2] = typeof(long);
		paramTypes[n + 3] = typeof(MessageReader);
		paramTypes[n + 4] = typeof(ProtocolDeserializeDelegate[]);
		paramTypes[n + 5] = typeof(SerializerManager);

		MethodBuilder method = typeBuilder.DefineMethod("__" + Guid.NewGuid().ToString("N"),
			MethodAttributes.Static | MethodAttributes.Public, typeof(IPendingRequest), paramTypes);

		ILGenerator il = method.GetILGenerator();

		// Call deserializer method
		il.Emit(OpCodes.Ldarg, n + 3);
		il.Emit(OpCodes.Ldarg, n + 4);

		ParameterInfo[] parameters = methodInfo.GetParameters();
		LocalBuilder[] paramVars = new LocalBuilder[parameters.Length - n];
		for (int i = n; i < parameters.Length; i++)
		{
			paramVars[i - n] = il.DeclareLocal(parameters[i].ParameterType);
			il.Emit(OpCodes.Ldloca, paramVars[i - n]);
		}

		il.Emit(OpCodes.Call, methodDeserializer);

		// Call implementation
		il.Emit(OpCodes.Ldarg, n + 0);

		if (n > 0)
		{
			il.Emit(OpCodes.Ldarg, 0);
			il.Emit(OpCodes.Castclass, requiredParamType);
		}

		for (int i = 0; i < paramVars.Length; i++)
		{
			il.Emit(OpCodes.Ldloc, paramVars[i]);
		}

		il.Emit(OpCodes.Call, methodInfo);

		// Create and return pending request
		if (methodInfo.ReturnType != typeof(void))
		{
			// Store result
			LocalBuilder responseVar = il.DeclareLocal(methodInfo.ReturnType);
			il.Emit(OpCodes.Stloc, responseVar);

			Type reqType = typeof(PendingRequest<>).MakeGenericType(methodInfo.ReturnType);
			ConstructorInfo ctor = reqType.GetConstructors().First();

			il.Emit(OpCodes.Ldarg, n + 1);
			il.Emit(OpCodes.Ldarg, n + 2);
			il.Emit(OpCodes.Ldloc, responseVar);
			il.Emit(OpCodes.Ldsfld, serField);
			il.Emit(OpCodes.Ldarg, n + 5);
			il.Emit(OpCodes.Ldc_I4, supportObjectGraph ? 1 : 0);
			il.Emit(OpCodes.Newobj, ctor);
		}
		else
		{
			il.Emit(OpCodes.Ldarg, n + 1);
			il.Emit(OpCodes.Ldarg, n + 2);
			il.Emit(OpCodes.Ldsfld, serField);
			il.Emit(OpCodes.Ldarg, n + 5);
			il.Emit(OpCodes.Ldc_I4, supportObjectGraph ? 1 : 0);
			il.Emit(OpCodes.Newobj, PendingRequest.CtorInfo);
		}

		il.Emit(OpCodes.Ret);

		return method;
	}

	private sealed class HostedService
	{
		string name;
		ProtocolDescriptor protocolDescriptor;

		SerializerManager serializerManager;
		DeserializerManager deserializerManager;

		HashSet<string> apiNames;

		InterfaceEntry[] hostedInterfaces;
		ProtocolDeserializeDelegate[] deserializerTable;

		bool isStopped;

		public HostedService(string name, ProtocolDescriptor protocolDescriptor,
			SerializerManager serializerManager, DeserializerManager deserializerManager)
		{
			this.name = name;
			this.protocolDescriptor = protocolDescriptor;
			this.serializerManager = serializerManager;
			this.deserializerManager = deserializerManager;

			apiNames = new HashSet<string>(protocolDescriptor.Interfaces.Length, StringComparer.OrdinalIgnoreCase);
			hostedInterfaces = new InterfaceEntry[protocolDescriptor.Interfaces.Select(x => x.Id).DefaultIfEmpty().Max() + 1];

			deserializerTable = deserializerManager.GetDeserializerTable().Select(x => (ProtocolDeserializeDelegate)x).ToArray();
		}

		public string Name => name;
		public bool IsStopped => isStopped;

		public SerializerManager SerializerManager => serializerManager;
		public DeserializerManager DeserializerManager => deserializerManager;
		public ProtocolDescriptor ProtocolDescriptor => protocolDescriptor;
		public ProtocolDeserializeDelegate[] DeserializerTable => deserializerTable;

		public void AddInterface(int id, string name, InterfaceEntry entry)
		{
			if (apiNames.Contains(name))
				throw DbAPIDefinitionException.CreateAPINameDuplicate(name);

			apiNames.Add(name);
			hostedInterfaces[id] = entry;
		}

		public bool TryGetInterface(int id, out InterfaceEntry entry)
		{
			entry = null;
			if (id < 0 || id >= hostedInterfaces.Length)
				return false;

			entry = hostedInterfaces[id];
			return true;
		}

		public void Stop()
		{
			isStopped = true;
		}

		public void Start()
		{
			isStopped = false;
		}
	}

	private sealed class InterfaceEntry
	{
		public ProtocolInterfaceDescriptor InterfaceDesc { get; set; }
		public object Implementation { get; set; }
		public Delegate[] OperationInvokers { get; set; }
		public APIRequestCallback RequestCallback { get; set; }
		public ParametrizedAPIRequestCallback ParamRequestCallback { get; set; }

		public InterfaceEntry(object implementation, Delegate[] operationInvokers,
			ProtocolInterfaceDescriptor interfaceDesc)
		{
			this.Implementation = implementation;
			this.OperationInvokers = operationInvokers;
			this.InterfaceDesc = interfaceDesc;
		}

		public bool TryGetInvoker(int operationId, out Delegate invoker)
		{
			invoker = null;
			if (operationId < 0 || operationId >= OperationInvokers.Length)
				return false;

			invoker = OperationInvokers[operationId];
			return true;
		}
	}
}

internal interface IPendingRequest
{
	void SendResponse(Exception e);
}

internal sealed class PendingRequest : IPendingRequest
{
	public static readonly ConstructorInfo CtorInfo = typeof(PendingRequest).GetConstructor(
		BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly,
		new Type[] { typeof(Connection), typeof(long), typeof(SerializerDelegate), typeof(SerializerManager), typeof(bool) });

	Connection connection;
	long requestId;
	SerializerManager serializerManager;
	SerializerDelegate serializer;
	bool supportObjectGraph;

	public PendingRequest(Connection connection, long requestId, SerializerDelegate serializer,
		SerializerManager serializerManager, bool supportObjectGraph)
	{
		this.connection = connection;
		this.requestId = requestId;
		this.serializer = serializer;
		this.serializerManager = serializerManager;
		this.supportObjectGraph = supportObjectGraph;
	}

	public void SendResponse(Exception e)
	{
		if (e != null)
		{
			DbAPIHost.SendErrorResponse(connection, requestId, e);
		}
		else
		{
			SerializerContext context = SerializerContext.Instance;
			try
			{
				context.Init(serializerManager, supportObjectGraph);
				connection.SendResponse(serializer, requestId);
			}
			catch (ObjectDisposedException)
			{
			}
			catch (CommunicationException)
			{
			}
			catch (DbAPIErrorException ex)
			{
				DbAPIHost.SendErrorResponse(connection, requestId, ex);
			}
			finally
			{
				context.Reset();
			}
		}
	}
}

internal sealed class PendingRequest<T> : IPendingRequest
{
	Connection connection;
	long requestId;
	SerializerManager serializerManager;
	SerializerDelegate<T> serializer;
	T result;
	bool supportObjectGraph;

	public PendingRequest(Connection connection, long requestId, T result,
		SerializerDelegate<T> serializer, SerializerManager serializerManager, bool supportObjectGraph)
	{
		this.connection = connection;
		this.requestId = requestId;
		this.result = result;
		this.serializer = serializer;
		this.serializerManager = serializerManager;
		this.supportObjectGraph = supportObjectGraph;
	}

	public void SendResponse(Exception e)
	{
		if (e != null)
		{
			DbAPIHost.SendErrorResponse(connection, requestId, e);
		}
		else
		{
			SerializerContext context = SerializerContext.Instance;

			try
			{
				context.Init(serializerManager, supportObjectGraph);
				connection.SendResponse(serializer, result, requestId);
			}
			catch (ObjectDisposedException)
			{
			}
			catch (CommunicationException)
			{
			}
			catch (DbAPIErrorException ex)
			{
				DbAPIHost.SendErrorResponse(connection, requestId, ex);
			}
			finally
			{
				context.Reset();
			}
		}
	}
}

internal struct APIRequest
{
	object implementation;
	Connection connection;
	long requestId;
	MessageReader reader;
	ProtocolDeserializeDelegate[] deserializerTable;
	InvokeOperationDelegate operationInvoker;
	SerializerManager serializerManager;
	DbAPIOperationType operationType;

	internal APIRequest(InvokeOperationDelegate operationInvoker, object implementation, Connection connection,
		long requestId, MessageReader reader, ProtocolDeserializeDelegate[] deserializerTable, SerializerManager serializerManager,
		DbAPIOperationType operationType)
	{
		this.operationInvoker = operationInvoker;
		this.implementation = implementation;
		this.connection = connection;
		this.requestId = requestId;
		this.reader = reader;
		this.deserializerTable = deserializerTable;
		this.serializerManager = serializerManager;
		this.operationType = operationType;
	}

	public DbAPIOperationType OperationType => operationType;

	public IPendingRequest Execute()
	{
		return operationInvoker(implementation, connection, requestId, reader, deserializerTable, serializerManager);
	}
}

internal struct ParametrizedAPIRequest
{
	object implementation;
	Connection connection;
	long requestId;
	MessageReader reader;
	ProtocolDeserializeDelegate[] deserializerTable;
	ParametrizedInvokeOperationDelegate operationInvoker;
	SerializerManager serializerManager;
	DbAPIOperationType operationType;

	internal ParametrizedAPIRequest(ParametrizedInvokeOperationDelegate operationInvoker, object implementation, Connection connection,
		long requestId, MessageReader reader, ProtocolDeserializeDelegate[] deserializerTable, SerializerManager serializerManager,
		DbAPIOperationType operationType)
	{
		this.operationInvoker = operationInvoker;
		this.implementation = implementation;
		this.connection = connection;
		this.requestId = requestId;
		this.reader = reader;
		this.deserializerTable = deserializerTable;
		this.serializerManager = serializerManager;
		this.operationType = operationType;
	}

	public DbAPIOperationType OperationType => operationType;

	public IPendingRequest Execute(object param)
	{
		return operationInvoker(param, implementation, connection, requestId, reader, deserializerTable, serializerManager);
	}
}
