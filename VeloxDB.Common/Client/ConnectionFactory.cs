using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using VeloxDB.Common;
using VeloxDB.Networking;
using VeloxDB.Protocol;
using static VeloxDB.Client.ConnectionPool;

namespace VeloxDB.Client;

internal delegate ConnectionBase ConnectionFactoryDelegate(string connectionString);
internal delegate void OperationDeserializerDelegate<T>(MessageReader reader, ProtocolDeserializeDelegate[] deserializerTable, out T value);
internal delegate void OperationDeserializerDelegate(MessageReader reader, ProtocolDeserializeDelegate[] deserializerTable);

/// <summary>
/// Creates VeloxDB connection.
/// </summary>
public static class ConnectionFactory
{
	static readonly object sync = new object();

	static ModuleBuilder moduleBuilder;

	static Dictionary<Type, InterfaceEntry> interfaces;
	static Dictionary<Guid, ServerEntry> servers;

	static HashSet<Assembly> classAssemblies = new HashSet<Assembly>(ReferenceEqualityComparer<Assembly>.Instance);

	static ConnectionFactory()
	{
		Reset();

		AssemblyName aName = new AssemblyName("__connfactmodule");
		AssemblyBuilder ab = AssemblyBuilder.DefineDynamicAssembly(aName, AssemblyBuilderAccess.RunAndCollect);
		moduleBuilder = ab.DefineDynamicModule(aName.Name);
	}

	internal static void Reset()
	{
		interfaces = new Dictionary<Type, InterfaceEntry>(2);
		servers = new Dictionary<Guid, ServerEntry>(2);
		classAssemblies = new HashSet<Assembly>(ReferenceEqualityComparer<Assembly>.Instance);
		ConnectionPoolCollection.Reset();
	}

	/// <summary>
	/// Creates VeloxDB connection using supplied connection string.
	/// Use <see cref="VeloxDB.Client.ConnectionStringParams"/> to create connection string.
	/// </summary>
	/// <typeparam name="T">Interface od database API to connect to.</typeparam>
	/// <param name="connectionString">Connection string.</param>
	/// <returns>Proxy object implementing T.</returns>
	/// <exception cref="ArgumentException">T is not an interface.</exception>
	/// <exception cref="InvalidConnectionStringException">If supplied connection string is not valid</exception>
	public static T Get<T>(string connectionString) where T : class
	{
		return Get<T>(connectionString, null);
	}

	/// <summary>
	/// Creates VeloxDB connection using supplied connection string.
	/// Use <see cref="VeloxDB.Client.ConnectionStringParams"/> to create connection string.
	/// </summary>
	/// <typeparam name="T">Interface od database API to connect to.</typeparam>
	/// <param name="connectionString">Connection string.</param>
	/// <param name="assemblyProvider">Assembly provider that can be used to provide the protocol serializer
	/// with additional assemblies containing protocl classes.</param>
	/// <returns>Proxy object implementing T.</returns>
	/// <exception cref="ArgumentException">T is not an interface.</exception>
	/// <exception cref="InvalidConnectionStringException">If supplied connection string is not valid</exception>
	public static T Get<T>(string connectionString, IAssemblyProvider assemblyProvider) where T : class
	{
		Type type = typeof(T);

		if (!type.IsInterface)
			throw new ArgumentException("Provided type is not an interface.");

		if (!interfaces.TryGetValue(type, out InterfaceEntry interfaceEntry))
		{
			lock (sync)
			{
				if (!interfaces.TryGetValue(type, out interfaceEntry))
				{
					interfaceEntry = CreateInterfaceEntry(type);
					Dictionary<Type, InterfaceEntry> temp = new Dictionary<Type, InterfaceEntry>(interfaces);
					temp.Add(type, interfaceEntry);
					Thread.MemoryBarrier();
					interfaces = temp;
				}
			}
		}

		ConnectionBase conn = interfaceEntry.GetConnection(connectionString);
		if (assemblyProvider != null && !conn.AssemblyProviderRegistered)
		{
			lock (classAssemblies)
			{
				classAssemblies.UnionWith(assemblyProvider.GetAssemblies());
			}

			conn.AssemblyProviderRegistered = true;
		}

		return (T)(object)conn;
	}

	internal static OperationData GetOperationData(ConnectionEntry connectionEntry, string connectionString, Type interfaceType, int operationId)
	{
		ServerEntry serverEntry = (ServerEntry)connectionEntry.Connection.Tag;
		if (serverEntry == null)
		{
			lock (sync)
			{
				serverEntry = (ServerEntry)connectionEntry.Connection.Tag;
				if (serverEntry == null)
				{
					if (!servers.TryGetValue(connectionEntry.Descriptor.Guid, out serverEntry))
					{
						serverEntry = new ServerEntry(connectionEntry.Descriptor);
						servers.Add(connectionEntry.Descriptor.Guid, serverEntry);
					}

					connectionEntry.Connection.TrySetTagIfNull(serverEntry);
				}
			}
		}

		ServerInterfaceEntry serverInterfaceEntry = serverEntry.GetOrCreateInterfaceEntry(interfaceType);

		Delegate serializer = serverInterfaceEntry.Serializers[operationId];
		Delegate deserializer = serverInterfaceEntry.Deserializers[operationId];
		if (serializer == null || deserializer == null)
			throw new DbAPIMismatchException(serverInterfaceEntry.OperationMismatchReason[operationId]);

		return new OperationData()
		{
			Connection = connectionEntry.Connection,
			Deserializer = deserializer,
			Serializer = serializer,
			SerializerManager = serverEntry.SerializerManager,
			DeserializerTable = serverEntry.DeserializerTable
		};
	}

	private static InterfaceEntry CreateInterfaceEntry(Type type)
	{
		ValidateInterface(type);

		string guid = Guid.NewGuid().ToString("N");
		TypeBuilder tb = moduleBuilder.DefineType("__" + guid,
			TypeAttributes.Class | TypeAttributes.Public, typeof(ConnectionBase));
		tb.AddInterfaceImplementation(type);

		MethodInfo[] methods = type.GetMethods(BindingFlags.Public | BindingFlags.FlattenHierarchy | BindingFlags.Instance);

		FieldBuilder[] deserializerFields = new FieldBuilder[methods.Length];
		for (int i = 0; i < methods.Length; i++)
		{
			deserializerFields[i] = tb.DefineField("deserializer" + i.ToString(),
				typeof(HandleResponseDelegate), FieldAttributes.Assembly | FieldAttributes.Static);
		}

		for (int i = 0; i < methods.Length; i++)
		{
			CreateInterfaceMethod(tb, type, methods[i], i, deserializerFields[i]);
		}

		MethodInfo[] deserializerMethods = new MethodInfo[methods.Length];
		for (int i = 0; i < methods.Length; i++)
		{
			deserializerMethods[i] = CreateDeserializerDelegate(tb, methods[i]);
		}

		CreateConstructor(tb);

		Type finalType = tb.CreateType();
		ConstructorInfo ctor = finalType.GetConstructor(new Type[] { typeof(string) });

		for (int i = 0; i < methods.Length; i++)
		{
			FieldInfo fi = finalType.GetField(deserializerFields[i].Name, BindingFlags.NonPublic | BindingFlags.Static);
			deserializerMethods[i] = finalType.GetMethod(deserializerMethods[i].Name, BindingFlags.NonPublic | BindingFlags.Static);
			fi.SetValue(null, (HandleResponseDelegate)Delegate.CreateDelegate(typeof(HandleResponseDelegate), deserializerMethods[i]));
		}

		DynamicMethod m = new DynamicMethod("__" + Guid.NewGuid().ToString("N"), typeof(ConnectionBase), new Type[] { typeof(string) }, moduleBuilder);
		ILGenerator il = m.GetILGenerator();

		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Newobj, ctor);
		il.Emit(OpCodes.Ret);

		ConnectionFactoryDelegate factDel = (ConnectionFactoryDelegate)m.CreateDelegate(typeof(ConnectionFactoryDelegate));
		return new InterfaceEntry(factDel);
	}

	private static ConstructorBuilder CreateConstructor(TypeBuilder tb)
	{
		ConstructorBuilder cb = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, new Type[] { typeof(string) });

		ILGenerator il = cb.GetILGenerator();
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Call, ConnectionBase.ConstructorMethod);
		il.Emit(OpCodes.Ret);

		return cb;
	}

	private static MethodBuilder CreateDeserializerDelegate(TypeBuilder tb, MethodInfo methodInfo)
	{
		MethodBuilder method = tb.DefineMethod("__" + Guid.NewGuid().ToString("N"), MethodAttributes.Private | MethodAttributes.Static,
			null, new Type[] { typeof(Connection), typeof(object), typeof(Exception), typeof(MessageReader) });

		ILGenerator il = method.GetILGenerator();

		// try
		Label tryCatchLabel = il.DefineLabel();
		il.BeginExceptionBlock();

		Type reqType = GetDatabaseTaskSourceType(methodInfo);
		LocalBuilder reqVar = il.DeclareLocal(reqType);

		// RequestBase
		il.Emit(OpCodes.Ldarg, 1);
		il.Emit(OpCodes.Castclass, reqType);
		il.Emit(OpCodes.Stloc, reqVar);

		// Process exception with retry (if possible)
		LocalBuilder errVar = il.DeclareLocal(typeof(Exception));
		LocalBuilder retryScheduledVar = il.DeclareLocal(typeof(bool));

		il.Emit(OpCodes.Ldloc, reqVar);
		il.Emit(OpCodes.Ldarg, 3);
		il.Emit(OpCodes.Ldarg, 2);
		il.Emit(OpCodes.Ldloca, errVar);
		il.Emit(OpCodes.Ldloca, retryScheduledVar);
		il.Emit(OpCodes.Call, DatabaseTask.ProcessResponseErrorMethod);

		// If we have an error store it in the database task and return
		Label skipErrorLabel = il.DefineLabel();
		il.Emit(OpCodes.Ldloc, errVar);
		il.Emit(OpCodes.Ldnull);
		il.Emit(OpCodes.Cgt_Un);
		il.Emit(OpCodes.Brfalse, skipErrorLabel);

		il.Emit(OpCodes.Ldloc, reqVar);
		il.Emit(OpCodes.Ldloc, errVar);
		il.Emit(OpCodes.Call, DatabaseTask.SetErrorMethod);
		il.Emit(OpCodes.Leave, tryCatchLabel);

		il.MarkLabel(skipErrorLabel);

		// If the retry is scheduled we just return
		Label skipRetryScheduledLabel = il.DefineLabel();
		il.Emit(OpCodes.Ldloc, retryScheduledVar);
		il.Emit(OpCodes.Brfalse, skipRetryScheduledLabel);
		il.Emit(OpCodes.Leave, tryCatchLabel);

		il.MarkLabel(skipRetryScheduledLabel);

		// No error, deserialize response (if response type is not void/Task)
		if (methodInfo.ReturnType != typeof(void) && methodInfo.ReturnType != typeof(DatabaseTask))
		{
			Type resultType = reqType.GetGenericArguments()[0];
			LocalBuilder resVar = il.DeclareLocal(resultType);

			Type deserializerType = typeof(OperationDeserializerDelegate<>).MakeGenericType(resultType);

			// Load deserializer delegate to call
			il.Emit(OpCodes.Ldloc, reqVar);
			il.Emit(OpCodes.Call, DatabaseTask.GetDeserializerMethod);
			il.Emit(OpCodes.Castclass, deserializerType);

			// Deserializer expects reader, deserializer table and out parameter where to store result
			il.Emit(OpCodes.Ldarg_3);
			il.Emit(OpCodes.Ldloc, reqVar);
			il.Emit(OpCodes.Call, DatabaseTask.GetDeserializerTableMethod);
			il.Emit(OpCodes.Ldloca, resVar);

			MethodInfo invokeMethod = deserializerType.GetMethod("Invoke");
			il.Emit(OpCodes.Callvirt, invokeMethod);

			// Set tcs result
			il.Emit(OpCodes.Ldloc, reqVar);
			il.Emit(OpCodes.Ldloc, resVar);
			il.Emit(OpCodes.Call, reqType.GetMethod(nameof(DatabaseTask<object>.SetResult),
				BindingFlags.NonPublic | BindingFlags.DeclaredOnly | BindingFlags.Instance));
		}
		else
		{
			// Just signal tcs as completed
			il.Emit(OpCodes.Ldloc, reqVar);
			il.Emit(OpCodes.Call, reqType.GetMethod(nameof(DatabaseTask.SetResult),
				BindingFlags.NonPublic | BindingFlags.DeclaredOnly | BindingFlags.Instance));
		}

		il.Emit(OpCodes.Leave, tryCatchLabel);

		// catch
		il.BeginCatchBlock(typeof(Exception));

		//Store exception in a variable
		LocalBuilder excVar = il.DeclareLocal(typeof(Exception));
		il.Emit(OpCodes.Stloc, excVar);

		// Set database task exception
		il.Emit(OpCodes.Ldloc, reqVar);
		il.Emit(OpCodes.Ldloc, excVar);
		il.Emit(OpCodes.Call, DatabaseTask.SetErrorMethod);

		// if exception is not a DbAPIErrorException rethrow it to the lower network level
		Label skipRethrowLabel = il.DefineLabel();
		il.Emit(OpCodes.Ldloc, excVar);
		il.Emit(OpCodes.Isinst, typeof(DbAPIErrorException));
		il.Emit(OpCodes.Ldnull);
		il.Emit(OpCodes.Ceq);
		il.Emit(OpCodes.Brfalse, skipRethrowLabel);
		il.Emit(OpCodes.Rethrow);
		il.MarkLabel(skipRethrowLabel);

		il.Emit(OpCodes.Leave, tryCatchLabel);
		il.EndExceptionBlock();

		il.MarkLabel(tryCatchLabel);
		il.Emit(OpCodes.Ret);

		return method;
	}

	private static void ValidateInterface(Type type)
	{
		if (type.GetProperties().Length > 0)
			throw DbAPIDefinitionException.CreateAPIPropertyDefinition(ProtocolClassDescriptor.GetClassName(type));

		if (type.GetEvents().Length > 0)
			throw DbAPIDefinitionException.CreateAPIEventDefinition(ProtocolClassDescriptor.GetClassName(type));
	}

	private static Type GetDatabaseTaskSourceType(MethodInfo methodInfo)
	{
		if (methodInfo.ReturnType.IsGenericType && methodInfo.ReturnType.GetGenericTypeDefinition() == typeof(DatabaseTask<>) ||
			methodInfo.ReturnType == typeof(DatabaseTask))
		{
			return methodInfo.ReturnType;
		}
		else
		{
			if (methodInfo.ReturnType == typeof(void))
				return typeof(DatabaseTask);
			else
				return typeof(DatabaseTask<>).MakeGenericType(new Type[] { methodInfo.ReturnType });
		}
	}

	private static void CreateInterfaceMethod(TypeBuilder typeBuilder, Type interfaceType, MethodInfo methodInfo,
		int operationId, FieldInfo deserializerField)
	{
		ParameterInfo[] pis = methodInfo.GetParameters();
		Type[] paramTypes = new Type[pis.Length];
		for (int i = 0; i < pis.Length; i++)
		{
			paramTypes[i] = pis[i].ParameterType;
		}

		Type reqType = CreateRequestType(methodInfo, deserializerField, pis, paramTypes, out ConstructorInfo reqCtor);

		MethodBuilder method = typeBuilder.DefineMethod(methodInfo.Name,
			MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig, methodInfo.ReturnType, paramTypes);

		ILGenerator il = method.GetILGenerator();

		// Create request object
		LocalBuilder reqVar = il.DeclareLocal(reqType);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldfld, ConnectionBase.ConnStrField);
		il.Emit(OpCodes.Ldc_I4, operationId);
		for (int i = 0; i < paramTypes.Length; i++)
		{
			il.Emit(OpCodes.Ldarg, i + 1);
		}

		il.Emit(OpCodes.Newobj, reqCtor);
		il.Emit(OpCodes.Stloc, reqVar);

		// Call request.Execute
		MethodInfo execMethod = DatabaseTask.ExecuteMethod.MakeGenericMethod(new Type[] { interfaceType });
		il.Emit(OpCodes.Ldloc, reqVar);
		il.Emit(OpCodes.Call, execMethod);

		// If result is DatabaseTask return the request object, else wait for the result and return it
		if ((methodInfo.ReturnType.IsGenericType && methodInfo.ReturnType.GetGenericTypeDefinition() == typeof(DatabaseTask<>)) ||
			methodInfo.ReturnType == typeof(DatabaseTask))
		{
			il.Emit(OpCodes.Ldloc, reqVar);
			il.Emit(OpCodes.Ret);
		}
		else
		{
			// Wait for the database task to finish
			il.Emit(OpCodes.Ldloc, reqVar);
			il.Emit(OpCodes.Call, DatabaseTask.WaitMethod);

			// Read result (if any) and return it
			if (methodInfo.ReturnType != typeof(void))
			{
				il.Emit(OpCodes.Ldloc, reqVar);
				il.Emit(OpCodes.Call, reqType.BaseType.GetMethod(nameof(DatabaseTask<int>.GetResult)));
			}

			il.Emit(OpCodes.Ret);
		}
	}

	private static Type CreateRequestType(MethodInfo methodInfo, FieldInfo deserializerField,
		ParameterInfo[] parameters, Type[] paramTypes, out ConstructorInfo ctor)
	{
		Type baseReqType = GetDatabaseTaskSourceType(methodInfo);

		TypeBuilder reqTypeBuilder = moduleBuilder.DefineType("__" + Guid.NewGuid().ToString("N"),
			TypeAttributes.Class | TypeAttributes.Public, baseReqType);

		Type[] ctorParams = new Type[2 + parameters.Length];
		ctorParams[0] = typeof(string);
		ctorParams[1] = typeof(int);

		FieldInfo[] paramFields = new FieldInfo[paramTypes.Length];
		for (int i = 0; i < paramTypes.Length; i++)
		{
			paramFields[i] = reqTypeBuilder.DefineField(parameters[i].Name, parameters[i].ParameterType, FieldAttributes.Private);
			ctorParams[i + 2] = parameters[i].ParameterType;
		}

		ConstructorBuilder ctorBuilder = reqTypeBuilder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, ctorParams);

		ILGenerator il = ctorBuilder.GetILGenerator();
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Ldarg_2);

		ConstructorInfo ctorInfo;
		if (baseReqType == typeof(DatabaseTask))
		{
			ctorInfo = DatabaseTask.ConstructorMethod;
		}
		else
		{
			ctorInfo = baseReqType.GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly,
				new Type[] { typeof(string), typeof(int) });
		}

		il.Emit(OpCodes.Call, ctorInfo);

		for (int i = 0; i < paramTypes.Length; i++)
		{
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldarg, i + 3);
			il.Emit(OpCodes.Stfld, paramFields[i]);
		}

		il.Emit(OpCodes.Ret);

		CreateRequestExecuteMethod(reqTypeBuilder, methodInfo, paramTypes, paramFields, deserializerField);
		CreateRequestOperationTypeMethod(reqTypeBuilder, methodInfo);

		Type reqType = reqTypeBuilder.CreateType();
		ctor = reqType.GetConstructor(ctorParams);
		return reqType;
	}

	private static void CreateRequestExecuteMethod(TypeBuilder typeBuilder, MethodInfo methodInfo, Type[] paramTypes,
		FieldInfo[] paramFields, FieldInfo deserializerField)
	{
		MethodBuilder method = typeBuilder.DefineMethod(nameof(DatabaseTask.TryExecute),
			MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig, typeof(bool), EmptyArray<Type>.Instance);

		ILGenerator il = method.GetILGenerator();

		DbAPIOperationAttribute dboa = methodInfo.GetCustomAttribute<DbAPIOperationAttribute>();
		DbAPIObjectGraphSupportType objectGraphSupport = dboa == null ? DbAPIObjectGraphSupportType.Both : dboa.ObjectGraphSupport;

		// Get connection and store it in a variable
		LocalBuilder connVar = il.DeclareLocal(typeof(PooledConnection));
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, DatabaseTask.GetConnectionMethod);
		il.Emit(OpCodes.Stloc, connVar);

		// Get serializer manager and store it in a variable
		LocalBuilder serMgrVar = il.DeclareLocal(typeof(SerializerManager));
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, DatabaseTask.GetSerializerManagerMethod);
		il.Emit(OpCodes.Stloc, serMgrVar);

		// Get serializer and store it in a variable (if there are input parameters in the method)
		Type serializerType = paramTypes.Length == 0 ? typeof(SerializerDelegate) :
			DatabaseTask.SerializerDelegateGenericTypes[paramTypes.Length].MakeGenericType(paramTypes);
		LocalBuilder serVar = il.DeclareLocal(serializerType);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, DatabaseTask.GetSerializerMethod);
		il.Emit(OpCodes.Castclass, serializerType);
		il.Emit(OpCodes.Stloc, serVar);

		// If we do not find the serializer, throw DbAPIMismatchException
		Label skipNoSerializerLabel = il.DefineLabel();
		il.Emit(OpCodes.Ldloc, serVar);
		il.Emit(OpCodes.Brtrue, skipNoSerializerLabel);
		il.Emit(OpCodes.Call, DatabaseTask.ThrowMismatchMethod);
		il.MarkLabel(skipNoSerializerLabel);

		// Call SerializerContext.Init
		LocalBuilder contextVar = il.DeclareLocal(typeof(SerializerContext));
		il.Emit(OpCodes.Call, Methods.SerializerContextGetMethod);
		il.Emit(OpCodes.Stloc, contextVar);

		il.Emit(OpCodes.Ldloc, contextVar);
		il.Emit(OpCodes.Ldloc, serMgrVar);
		il.Emit(OpCodes.Ldc_I4, (objectGraphSupport & DbAPIObjectGraphSupportType.Request) != 0 ? 1 : 0);
		il.Emit(OpCodes.Call, Methods.SerializerContextInitMethod);

		// Call connection.SendRequestN(serializerField, args..., deserializer, request)
		il.Emit(OpCodes.Ldloc, connVar);
		il.Emit(OpCodes.Ldloc, serVar);

		// Load serialized parameters
		for (int i = 0; i < paramFields.Length; i++)
		{
			il.Emit(OpCodes.Ldarg_0);
			il.Emit(OpCodes.Ldfld, paramFields[i]);
		}

		il.Emit(OpCodes.Ldsfld, deserializerField);

		il.Emit(OpCodes.Ldarg_0);   // this pointer is the request object

		MethodInfo sendMethod = paramFields.Length == 0 ? DatabaseTask.SendRequestMethods[0] :
			DatabaseTask.SendRequestMethods[paramFields.Length].MakeGenericMethod(paramTypes);
		il.Emit(OpCodes.Call, sendMethod);

		LocalBuilder resVar = il.DeclareLocal(typeof(bool));
		il.Emit(OpCodes.Stloc, resVar);

		il.Emit(OpCodes.Ldloc, resVar);
		il.Emit(OpCodes.Ret);
	}

	private static void CreateRequestOperationTypeMethod(TypeBuilder typeBuilder, MethodInfo methodInfo)
	{
		MethodBuilder method = typeBuilder.DefineMethod(nameof(DatabaseTask.GetOperationType),
			MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
			typeof(DbAPIOperationType), EmptyArray<Type>.Instance);

		ILGenerator il = method.GetILGenerator();

		DbAPIOperationAttribute dboa = methodInfo.GetCustomAttribute<DbAPIOperationAttribute>();
		DbAPIOperationType operationType = dboa == null ? DbAPIOperationType.ReadWrite : dboa.OperationType;

		il.Emit(OpCodes.Ldc_I4, (int)operationType);
		il.Emit(OpCodes.Ret);
	}

	private sealed class InterfaceEntry
	{
		const int byRefLimit = 8;

		readonly object sync = new object();
		ConnectionBase[] connectionPoolByRef;
		public ConnectionFactoryDelegate factory;

		public InterfaceEntry(ConnectionFactoryDelegate factory)
		{
			this.factory = factory;
			connectionPoolByRef = EmptyArray<ConnectionBase>.Instance;
		}

		public ConnectionFactoryDelegate Factory => factory;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public ConnectionBase GetConnection(string connectionString)
		{
			ConnectionBase[] temp = connectionPoolByRef;
			for (int i = 0; i < temp.Length; i++)
			{
				if (object.ReferenceEquals(connectionString, temp[i].ConnectionString))
					return temp[i];
			}

			ConnectionBase conn = GetConnectionSynced(connectionString);
			if (conn != null)
				return conn;

			return Factory(connectionString);
		}

		[MethodImpl(MethodImplOptions.NoInlining)]
		private ConnectionBase GetConnectionSynced(string connectionString)
		{
			lock (sync)
			{
				for (int i = 0; i < connectionPoolByRef.Length; i++)
				{
					if (object.ReferenceEquals(connectionString, connectionPoolByRef[i].ConnectionString))
						return connectionPoolByRef[i];
				}

				if (connectionPoolByRef.Length < byRefLimit)
				{
					ConnectionBase[] temp = new ConnectionBase[connectionPoolByRef.Length + 1];
					for (int i = 0; i < connectionPoolByRef.Length; i++)
					{
						temp[i] = connectionPoolByRef[i];
					}

					ConnectionBase conn = Factory(connectionString);
					temp[temp.Length - 1] = conn;
					Thread.MemoryBarrier();

					connectionPoolByRef = temp;
					return conn;
				}
			}

			return null;
		}
	}

	private sealed class ServerInterfaceEntry
	{
		Delegate[] serializers;
		Delegate[] deserializers;
		string[] operationMismatchReason;

		public ServerInterfaceEntry(Delegate[] serializers, Delegate[] deserializers, string[] operationMismatchReason)
		{
			this.serializers = serializers;
			this.deserializers = deserializers;
			this.operationMismatchReason = operationMismatchReason;
		}

		public Delegate[] Serializers => serializers;
		public Delegate[] Deserializers => deserializers;
		public string[] OperationMismatchReason => operationMismatchReason;
	}

	private sealed class ServerEntry
	{
		readonly object sync = new object();

		ProtocolDescriptor protocolDescriptor;
		Dictionary<ProtocolTypeDescriptor, bool> matchedTypes;

		Delegate[] deserializerTable;
		Dictionary<Type, ServerInterfaceEntry> interfaces;

		ProtocolDiscoveryContext discoveryContext;
		SerializerManager serializerManager;
		DeserializerManager deserializerManager;

		public ServerEntry(ProtocolDescriptor protocolDescriptor)
		{
			this.protocolDescriptor = protocolDescriptor;
			this.protocolDescriptor.PrepareMaps();

			deserializerManager = new DeserializerManager();
			serializerManager = new SerializerManager(deserializerManager.ModuleBuilder);
			discoveryContext = new ProtocolDiscoveryContext();
			deserializerTable = EmptyArray<ProtocolDeserializeDelegate>.Instance;
			interfaces = new Dictionary<Type, ServerInterfaceEntry>(0);
			matchedTypes = new Dictionary<ProtocolTypeDescriptor, bool>(128, ReferenceEqualityComparer<ProtocolTypeDescriptor>.Instance);
		}

		public Delegate[] DeserializerTable => deserializerTable;
		public object Sync => sync;
		public SerializerManager SerializerManager => serializerManager;

		public ServerInterfaceEntry GetOrCreateInterfaceEntry(Type type)
		{
			if (interfaces.TryGetValue(type, out ServerInterfaceEntry entry))
				return entry;

			lock (sync)
			{
				if (interfaces.TryGetValue(type, out entry))
					return entry;

				entry = CreateInterfaceEntry(type, new ClassAssemblyProvider());
				Dictionary<Type, ServerInterfaceEntry> temp = new Dictionary<Type, ServerInterfaceEntry>(interfaces);
				temp.Add(type, entry);
				Thread.MemoryBarrier();

				interfaces = temp;
				return entry;
			}
		}

		private ServerInterfaceEntry CreateInterfaceEntry(Type type, IAssemblyProvider assemblyProvider)
		{
			ProtocolInterfaceDescriptor clientInterfaceDesc =
				discoveryContext.GetInterfaceDescriptor(type, 0, out var inTypes, out var outTypes, assemblyProvider);

			ProtocolInterfaceDescriptor serverInterfaceDesc = protocolDescriptor.GetInterface(clientInterfaceDesc.Name);
			if (serverInterfaceDesc == null)
				throw new DbAPINotFoundException(clientInterfaceDesc.Name);

			Delegate[] serializers = new Delegate[clientInterfaceDesc.Operations.Length];
			Delegate[] deserializers = new Delegate[clientInterfaceDesc.Operations.Length];
			string[] operationMismatchReason = new string[clientInterfaceDesc.Operations.Length];

			serverInterfaceDesc.TargetType = clientInterfaceDesc.TargetType;
			for (int i = 0; i < clientInterfaceDesc.Operations.Length; i++)
			{
				ProtocolOperationDescriptor clientOpDesc = clientInterfaceDesc.Operations[i];
				ProtocolOperationDescriptor serverOpDesc = serverInterfaceDesc.GetOperationByName(clientOpDesc.Name);
				if (serverOpDesc != null)
				{					
					if(clientOpDesc.IsMatch(serverOpDesc, matchedTypes, out string mismatchReason))
						serverOpDesc.TargetMethod = clientOpDesc.TargetMethod;
					else
						operationMismatchReason[i] =  $"The operation {clientOpDesc.Name} exists in server interface {serverInterfaceDesc.Name} but there is a mismatch: {mismatchReason}";
				}
				else
				{
					operationMismatchReason[i] = $"The operation {clientOpDesc.Name} not found in server interface {serverInterfaceDesc.Name}.";
				}

			}

			MapServerTypes(serverInterfaceDesc, inTypes, true);
			MapServerTypes(serverInterfaceDesc, outTypes, false);

			Delegate[] serverSerializers = serializerManager.GetInterfaceSerializers(serverInterfaceDesc,
				serverInterfaceDesc.InTypes, ProtocolInterfaceDirection.Request, 0);

			MethodInfo[] serverDeserializers = deserializerManager.GetInterfaceDeserializers(serverInterfaceDesc,
				serverInterfaceDesc.OutTypes, ProtocolInterfaceDirection.Response, 0);

			for (int i = 0; i < serverInterfaceDesc.Operations.Length; i++)
			{
				ProtocolOperationDescriptor serverOpDesc = serverInterfaceDesc.Operations[i];
				ProtocolOperationDescriptor clientOpDesc = clientInterfaceDesc.GetOperationByName(serverOpDesc.Name);
				if (clientOpDesc != null)
				{
					if (serverDeserializers[i] != null && serverSerializers[i] != null)
					{
						serializers[clientOpDesc.Id] = serverSerializers[i];
						deserializers[clientOpDesc.Id] = CreateDeserializerDelegate(serverDeserializers[i]);
					}
				}
			}

			deserializerTable = deserializerManager.GetDeserializerTable();
			return new ServerInterfaceEntry(serializers, deserializers, operationMismatchReason);
		}

		private Delegate CreateDeserializerDelegate(MethodInfo methodInfo)
		{
			Type outType = typeof(void);
			ParameterInfo[] pis = methodInfo.GetParameters();
			if (pis.Length > 2)
				outType = pis[2].ParameterType.GetElementType();

			if (outType == typeof(void))
			{
				return Delegate.CreateDelegate(typeof(OperationDeserializerDelegate), methodInfo);
			}
			else
			{
				Type delegateType = typeof(OperationDeserializerDelegate<>).MakeGenericType(new Type[] { outType });
				return Delegate.CreateDelegate(delegateType, methodInfo);
			}
		}

		private void MapServerTypes(ProtocolInterfaceDescriptor serverInterfaceDesc, ProtocolTypeDescriptor[] clientTypes, bool isInput)
		{
			for (int i = 0; i < clientTypes.Length; i++)
			{
				ProtocolTypeDescriptor clientTypeDesc = clientTypes[i];
				ProtocolTypeDescriptor serverTypeDesc = serverInterfaceDesc.GetType(clientTypeDesc.Name, isInput);
				if (serverTypeDesc != null)
					serverTypeDesc.TargetType = clientTypeDesc.TargetType;
			}
		}

		public void AddInterface(Type type, ServerInterfaceEntry entry)
		{
			Dictionary<Type, ServerInterfaceEntry> temp = new Dictionary<Type, ServerInterfaceEntry>(interfaces);
			temp.Add(type, entry);
			Thread.MemoryBarrier();
			interfaces = temp;
		}
	}

	internal struct OperationData
	{
		public PooledConnection Connection { get; set; }
		public SerializerManager SerializerManager { get; set; }
		public Delegate Serializer { get; set; }
		public Delegate Deserializer { get; set; }
		public Delegate[] DeserializerTable { get; set; }
	}

	private sealed class ClassAssemblyProvider : IAssemblyProvider
	{
		public IEnumerable<Assembly> GetAssemblies()
		{
			lock (classAssemblies)
			{
				return new List<Assembly>(classAssemblies);
			}
		}
	}
}
