using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Security;
using System.Reflection;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using VeloxDB.Common;
using VeloxDB.Config;
using VeloxDB.Descriptor;
using VeloxDB.Networking;
using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;
using VeloxDB.Storage;
using VeloxDB.Storage.Replication;
using VeloxDB.Storage.Replication.HighAvailability;

namespace VeloxDB.Server;
internal sealed class Server : IDisposable
{
	private const int AdminBufferPoolSize = 128 * 1024;
	private const int AdminBacklogSize = 1;
	private const int AdminMaxOpenConnCount = 20;
	private const int AdminMaxQueuedChunkCount = 16;
	static readonly TimeSpan AdminInactivityInterval = TimeSpan.FromSeconds(1);
	static readonly TimeSpan AdminInactivityTimeout = TimeSpan.FromSeconds(2);

	readonly Configuration configuration;
	private readonly ClusterConfiguration? clusterConfiguration;
	readonly ManualResetEvent terminateEvent;
	readonly NodeAdministration nodeAdministration;
	readonly bool blocking;
	readonly Tracing.Source? trace;
	readonly string? updateAsmDir;
	readonly string? persistenceDir;
	DbAPIHost? adminHost;
	DbAPIHost? execHost;
	SslServerAuthenticationOptions? sslOptions;
	SslClientOptionsFactory? sslClientOptionsFactory;

	StorageEngine? engine;
	DatabaseAdministration? databaseAdministration;
	ObjectModelContextPool? ctxPool;
	ILeaderElector? localWriteElector;
	ILeaderElector? globalWriteElector;
	DatabaseInfo? lastReplInfo;
	HostedModel? hostedModel;

	public Server(Configuration configuration, ClusterConfiguration? clusterConfiguration = null, bool blocking = true,
				  Tracing.Source? trace = null, string? updateAssembliesDir = null, string? persistenceDir = null)
	{
		this.configuration = configuration;
		this.clusterConfiguration = clusterConfiguration;
		this.blocking = blocking;
		this.trace = trace;
		this.updateAsmDir = updateAssembliesDir;
		this.persistenceDir = persistenceDir;


		terminateEvent = new ManualResetEvent(false);
		nodeAdministration = new NodeAdministration(configuration.Replication?.ThisNodeName,
			configuration.Replication?.ClusterConfigFile, clusterConfiguration, trace);
	}

	public bool Run()
	{
		LogVersion();
		if (!LoadClusterConfiguration())
			return false;

		if (!InitSSL())
			return false;

		if (!InitDatabaseEngine())
		{
			return false;
		}

		if (updateAsmDir != null && !UpdateAssembliesFromDirectory(updateAsmDir))
		{
			return false;
		}

		if (persistenceDir != null && !InitPersistance(persistenceDir))
		{
			return false;
		}

		CreateHosts();
		HostNodeAdministrationInterface();
		HostLocalWriteClusterAdministrationInterface();
		HostDatabaseAdministrationInterface();
		engine.SubscribeToStateChanges(OnEngineStateChange);
		Tracing.Info("Server successfully started.");

		if (blocking)
		{
			Console.TreatControlCAsInput = false;
			Console.CancelKeyPress += (sender, events) => Terminate(events);
			terminateEvent.WaitOne();
			Tracing.Info("Server shutting down...");
		}

		return true;
	}

	private static void LogVersion()
	{
		Version? version = Assembly.GetExecutingAssembly().GetName().Version;
		Checker.AssertNotNull(version);

		Tracing.Info("Starting VeloxDB {0} ({2}), PID: {1}", version.ToString(), Environment.ProcessId, BuildPlatform);
	}

	private static string BuildPlatform =>
	#if X86_64
		"X64";
	#elif ARM_64
		"ARM64";
	#else
		"Unknown";
	#endif

	private bool InitPersistance(string persistenceDir)
	{
		Checker.AssertNotNull(configuration.Replication);
		Checker.AssertNotNull(engine, databaseAdministration);

		if (!configuration.Replication.IsStandalone)
		{
			Tracing.Error("--init-persistence can't be used with replication.");
			return false;
		}

		try
		{
			databaseAdministration.InitPersistence(persistenceDir);
			return true;
		}
		catch (DatabaseException dbe)
		{
			Tracing.Error(dbe.Message);
			return false;
		}
	}

	private bool UpdateAssembliesFromDirectory(string updateAsmDir)
	{
		Checker.AssertNotNull(configuration.Replication);
		Checker.AssertNotNull(engine, databaseAdministration);

		if (!configuration.Replication.IsStandalone)
		{
			Tracing.Error("--update-assemblies can't be used with replication.");
			return false;
		}

		if (!Directory.Exists(updateAsmDir))
		{
			Tracing.Error("Directory {0} doesn't exist.", updateAsmDir);
			return false;
		}

		try
		{
			List<string> errors = databaseAdministration.UpdateAssembliesFromDirectory(updateAsmDir);
			foreach (string error in errors)
				Tracing.Error(error);
			return errors.Count == 0;
		}
		catch (DatabaseException dbe)
		{
			Tracing.Error(dbe.Message);
			return false;
		}
	}

	public void Terminate(ConsoleCancelEventArgs e)
	{
		e.Cancel = true;
		terminateEvent.Set();
	}

	private bool LoadClusterConfiguration()
	{
		Checker.AssertNotNull(configuration.Replication);

		IReadOnlyCollection<string> errors;
		if (clusterConfiguration != null)
		{
			List<string> errorList = new List<string>();
			configuration.TryLoadClusterConfig(clusterConfiguration, errorList);
			errors = errorList;
		}
		else
		{
			errors = configuration.TryLoadClusterConfig();
		}

		foreach (string error in errors)
		{
			Tracing.Error(error);
		}

		return errors.Count == 0;
	}

	[MemberNotNullWhen(true, nameof(engine))]
	private bool InitDatabaseEngine()
	{
		Checker.AssertNotNull(configuration.Database, configuration.Replication);

		ReplicationSettings? replication;

		CreateElectors();
		replication = configuration.Replication.ToRepplicationSettings(sslOptions, sslClientOptionsFactory);

		try
		{
			engine = new StorageEngine(configuration.Database.SystemDatabasePath, replication, localWriteElector, globalWriteElector, trace);
		}
		catch (SharingViolationException)
		{
			Tracing.Error("One or more of database files are in use. Is vlxdbsrv already running?");
			return false;
		}

		ctxPool = new ObjectModelContextPool(engine);
		databaseAdministration = new DatabaseAdministration(engine);
		return true;
	}

	private void OnEngineStateChange(DatabaseInfo info)
	{
		Checker.AssertNotNull(adminHost, execHost);
		nodeAdministration.OnStateChanged(info);

		if (LocalWritePrimaryChanged(info))
			if (info.IsLocalWritePrimary)
				adminHost.StartService(AdminAPIServiceNames.LocalWriteClusterAdministration);
			else
				adminHost.StopService(AdminAPIServiceNames.LocalWriteClusterAdministration);

		if (AssembliesChanged(info))
		{
			HostExecutionInterface(info.Assemblies, info.ModelVersionGuid, info.AssembliesVersionGuid, info.ModelDescriptor, info.IsWriteMaster,
								   (AssemblyData)(info.CustomObject));
		}

		if (WriteMasterChanged(info))
		{
			if (info.IsWriteMaster)
			{
				adminHost.StartService(AdminAPIServiceNames.DatabaseAdministration);
				execHost.StartService(string.Empty);
			}
			else
			{
				adminHost.StopService(AdminAPIServiceNames.DatabaseAdministration);
				execHost.StopService(string.Empty);
			}
		}

		lastReplInfo = info;
	}

	private bool LocalWritePrimaryChanged(DatabaseInfo info)
	{
		if (lastReplInfo == null)
			return info.IsLocalWritePrimary;

		return info.IsLocalWritePrimary != lastReplInfo.IsLocalWritePrimary;
	}

	private bool WriteMasterChanged(DatabaseInfo info)
	{
		if (lastReplInfo == null)
			return info.IsWriteMaster;

		return info.IsWriteMaster != lastReplInfo.IsWriteMaster;
	}
	private bool AssembliesChanged(DatabaseInfo info)
	{
		if (lastReplInfo == null)
			return true;

		return !info.AssembliesVersionGuid.Equals(lastReplInfo.AssembliesVersionGuid);
	}

	private void HostExecutionInterface(Storage.ModelUpdate.UserAssembly[] userAssemblies, SimpleGuid modelVersionGuid,
										SimpleGuid assemblyVersionGuid, DataModelDescriptor modelDesc, bool isPrimary,
										AssemblyData? assemblyData)
	{
		LoadedAssemblies loadedAssemblies;
		SerializerManager? serializerManager;
		DeserializerManager? deserializerManager;
		ProtocolDiscoveryContext? discoveryContext;

		if (assemblyData == null)
		{
			loadedAssemblies = new LoadedAssemblies(userAssemblies);
			serializerManager = null;
			deserializerManager = null;
			discoveryContext = null;
		}
		else
		{
			loadedAssemblies = assemblyData.Loaded;
			serializerManager = assemblyData.SerializerManager;
			deserializerManager = assemblyData.DeserializerManager;
			discoveryContext = assemblyData.DiscoveryContext;
		}

		Checker.AssertNotNull(execHost, engine, ctxPool);
		object[] apis = CreateExecutionApis(loadedAssemblies.Loaded);

		LogAPIs(loadedAssemblies.Loaded.Length, apis.Length);

		HostedModel? oldHostedModel = hostedModel;
		hostedModel = new HostedModel(engine, ctxPool, modelVersionGuid, assemblyVersionGuid, modelDesc, loadedAssemblies);

		if (oldHostedModel != null)
			oldHostedModel.Assemblies.Unload();

		execHost.HostService(string.Empty, typeof(ObjectModel), apis, hostedModel.ExecutionRequestCallback,
			engine.AssemblyVersionGuid.ToGuid(), new AssemblyProvider(loadedAssemblies.Loaded),
			serializerManager, deserializerManager, discoveryContext, !isPrimary);
	}

	private static void LogAPIs(int loadedAssemblies, int loadedApis)
	{
		if (loadedAssemblies != 0)
		{
			if (loadedApis == 0)
			{
				Tracing.Warning("No APIs were found to host in the {0} loaded assemblies.", loadedAssemblies);
			}
			else
			{
				Tracing.Info("Hosting {0} APIs.", loadedApis);
			}
		}
	}

	private object[] CreateExecutionApis(Assembly[] assemblies)
	{
		Checker.AssertNotNull(engine);
		List<object> result = new List<object>();

		foreach (Type classType in AssemblyUtils.GetDBApiTypes(assemblies))
		{
			object? instance;

			try
			{
				instance = Activator.CreateInstance(classType);
			}
			catch (Exception e)
			{
				engine.Trace.Error(e, "Exception encountered while trying to host {0}.", classType.FullName);
				continue;
			}

			Checker.AssertNotNull(instance);
			result.Add(instance);
		}

		return result.ToArray();
	}

	private void HostLocalWriteClusterAdministrationInterface()
	{
		Checker.AssertNotNull(adminHost, engine);

		LocalWriteClusterAdministration localWriteClusterAdministration =
			new LocalWriteClusterAdministration((IElector)localWriteElector!, (IElector)globalWriteElector!, engine);
		adminHost.HostService(AdminAPIServiceNames.LocalWriteClusterAdministration, new object[] { localWriteClusterAdministration },
							  (r) => r.Execute().SendResponse(null), Guid.NewGuid(), null, isInitiallyStopped: true);
	}

	private void HostDatabaseAdministrationInterface()
	{
		Checker.AssertNotNull(adminHost, databaseAdministration);
		adminHost.HostService(AdminAPIServiceNames.DatabaseAdministration, new object[] { databaseAdministration },
							  (r) => r.Execute().SendResponse(null), Guid.NewGuid(), null, isInitiallyStopped: true);
	}

	private void CreateElectors()
	{
		localWriteElector = null;
		globalWriteElector = null;

		Assembly repAssembly = IReplicatorFactory.FindReplicatorAssembly();
		if (repAssembly != null)
		{
			Type? elecFactType = repAssembly.GetTypes().FirstOrDefault(x => typeof(IElectorFactory).IsAssignableFrom(x));
			if (elecFactType == null)
				throw new CriticalDatabaseException($"Replicator could not be found in {repAssembly.FullName}.dll file.");

			IElectorFactory elecFact = (IElectorFactory)Activator.CreateInstance(elecFactType)!;
			localWriteElector = CreateLocalWriteElector(elecFact);
			globalWriteElector = CreateGlobalWriteElector(elecFact);
		}
	}

	private ILeaderElector? CreateGlobalWriteElector(IElectorFactory elecFactory)
	{
		Checker.AssertNotNull(configuration.Replication);

		if (!configuration.Replication.HasGlobalWriteWitness())
			return null;

		return elecFactory.CreateGlobalElector();
	}

	private ILeaderElector? CreateLocalWriteElector(IElectorFactory elecFactory)
	{
		Checker.AssertNotNull(configuration.Replication);
		Checker.AssertNotNull(configuration.Replication.ThisNodeName, configuration.Replication.ClusterConfig);

		string thisNodeName = configuration.Replication.ThisNodeName;

		LocalWriteCluster? localWriteCluster;
		if (!configuration.Replication.TryGetLocalWriteCluster(out localWriteCluster))
			return null;

		Checker.AssertNotNull(localWriteCluster.Witness, localWriteCluster.ElectionTimeout, configuration.Database);

		LocalWriteNode node = localWriteCluster.GetNode(thisNodeName);
		LocalWriteNode replica = localWriteCluster.GetReplica(thisNodeName);

		Checker.AssertNotNull(node.ElectorAddress, replica.ElectorAddress);

		SslClientAuthenticationOptions? sslReplicaOptions = null;
		if (sslClientOptionsFactory != null)
			sslReplicaOptions = sslClientOptionsFactory.CreateSslOptions(replica.ElectorAddress.Address);

		return elecFactory.CreateLocalElector(node.ElectorAddress.ToString(),
							   configuration.Database.SystemDatabasePath,
							   (int)(localWriteCluster.ElectionTimeout * 1000 + 0.5),
							   localWriteCluster.Witness.AsWitnessConfig(elecFactory, sslClientOptionsFactory),
							   replica.ElectorAddress.ToString(),
							   false,
							   sslReplicaOptions,
							   sslOptions);
	}

	private void CreateHosts()
	{
		Checker.AssertNotNull(configuration.Replication);
		Checker.AssertNotNull(configuration.Replication.ThisNode);

		ReplicationNode node = configuration.Replication.ThisNode;
		Checker.AssertNotNull(node.AdministrationAddress);

		List<IPEndPoint> endpoints = new List<IPEndPoint>(2) { node.AdministrationAddress.ToIPEndPoint() };
		if (!IPAddress.IsLoopback(endpoints[0].Address) && !IPAddress.Any.Equals(endpoints[0].Address))
			endpoints.Add(new IPEndPoint(IPAddress.Loopback, endpoints[0].Port));

		adminHost = new DbAPIHost(AdminBacklogSize, AdminMaxOpenConnCount, endpoints.ToArray(),
								  AdminBufferPoolSize, AdminInactivityInterval, AdminInactivityTimeout,
								  AdminMaxQueuedChunkCount, sslOptions);
		foreach (IPEndPoint endpoint in endpoints)
		{
			Tracing.Info("Administration endpoint hosted on {0}.", endpoint);
		}

		Checker.AssertNotNull(configuration.ExecutionEndpoint);
		HostEndpointConfiguration executionEndpoint = configuration.ExecutionEndpoint;

		Checker.AssertNotNull(executionEndpoint.InactivityInterval, executionEndpoint.BacklogSize,
							  executionEndpoint.BufferPoolSize, executionEndpoint.InactivityTimeout);
		Checker.AssertNotNull(executionEndpoint.MaxQueuedChunkCount);

		endpoints = new List<IPEndPoint>(2) { node.ExecutionAddress.ToIPEndPoint() };
		if (!IPAddress.IsLoopback(endpoints[0].Address) && !IPAddress.Any.Equals(endpoints[0].Address))
			endpoints.Add(new IPEndPoint(IPAddress.Loopback, endpoints[0].Port));

		foreach (IPEndPoint endpoint in endpoints)
		{
			Tracing.Info("Execution endpoint hosted on {0}.", endpoint);
		}

		execHost = new DbAPIHost((int)executionEndpoint.BacklogSize, (int)executionEndpoint.BufferPoolSize,
									  endpoints.ToArray(), (int)executionEndpoint.BufferPoolSize,
									  TimeSpan.FromSeconds((double)executionEndpoint.InactivityInterval),
									  TimeSpan.FromSeconds((double)executionEndpoint.InactivityTimeout),
									  (int)executionEndpoint.MaxQueuedChunkCount, sslOptions);
	}

	private bool InitSSL()
	{
		Checker.AssertNotNull(configuration.SSLConfiguration);

		if (!configuration.SSLConfiguration.Enabled)
		{
			sslOptions = null;
			return true;
		}

		X509Certificate2? serverCert = LoadCertificate(configuration.SSLConfiguration.CertificateKeyPath,
													   nameof(SSLConfiguration.CertificateKeyPath),
													   configuration.SSLConfiguration.Password);

		if (serverCert == null)
			return false;

		SslStreamCertificateContext context = SslStreamCertificateContext.Create(serverCert, null);
		sslOptions = new()
		{
			ServerCertificate = serverCert,
			ServerCertificateContext = context
		};

		string[] certificates = Array.Empty<string>();
		if (!string.IsNullOrEmpty(configuration.SSLConfiguration.CertificateStorePath))
			certificates = Directory.GetFiles(configuration.SSLConfiguration.CertificateStorePath);

		sslClientOptionsFactory = SslClientOptionsFactory.Create(certificates, true, configuration.SSLConfiguration.CACertificatePath);

		return true;
	}

	private static X509Certificate2? LoadCertificate(string? path, string keyName, string? password = null, bool onlyPublicKey = false)
	{
		if (path == null)
		{
			Tracing.Error("SSLConfiguration.CertificateKeyPath is not configured. Please either disable SSL or provide the path to the server key.");
			return null;
		}

		if (!File.Exists(path))
		{
			Tracing.Error($"The certificate '{path}' specified at {keyName} could not be found.");
			return null;
		}

		try
		{
			return SSLUtils.LoadCertificate(path, password, onlyPublicKey);
		}
		catch (CryptographicException e)
		{
			Tracing.Error(e.Message);
			return null;
		}
	}

	private void HostNodeAdministrationInterface()
	{
		Checker.AssertNotNull(adminHost);
		adminHost.HostService(AdminAPIServiceNames.NodeAdministration, new object[] { nodeAdministration },
			(r) => r.Execute().SendResponse(null), Guid.NewGuid(), null);
	}

	public void Dispose()
	{
		Tracing.Info("Disposing server.");
		adminHost?.Stop();
		execHost?.Stop();
		engine?.Dispose();
		((IDisposable?)localWriteElector)?.Dispose();
	}

	private class HostedModel
	{
		StorageEngine engine;
		ObjectModelContextPool ctxPool;
		ObjectModelData objectModelData;
		Action<object, DatabaseException> asyncCallback;

		public LoadedAssemblies Assemblies { get; private set; }
		public SimpleGuid ModelVersionGuid { get; private set; }
		public SimpleGuid AssemblyVersionGuid { get; private set; }

		public HostedModel(StorageEngine engine, ObjectModelContextPool contextPool, SimpleGuid modelVersionGuid,
						   SimpleGuid assemblyVersionGuid, DataModelDescriptor modelDesc, LoadedAssemblies assemblies)
		{
			this.engine = engine;
			this.ctxPool = contextPool;
			this.ModelVersionGuid = modelVersionGuid;
			this.AssemblyVersionGuid = assemblyVersionGuid;
			objectModelData = new ObjectModelData(modelDesc, assemblies.Loaded);
			this.Assemblies = assemblies;

			asyncCallback = (s, e) => ((IPendingRequest)s).SendResponse(e);
		}

		public void ExecutionRequestCallback(ParametrizedAPIRequest request)
		{
			Checker.AssertNotNull(engine);
			ObjectModel objectModel = new ObjectModel(objectModelData, ctxPool, (TransactionType)request.OperationType);
			try
			{
				IPendingRequest pendingRequest = request.Execute(objectModel);
				if (request.OperationType == DbAPIOperationType.Read)
				{
					objectModel.Dispose();
					pendingRequest.SendResponse(null);
				}
				else
				{
					objectModel.CommitAsyncAndDispose(asyncCallback, pendingRequest);
				}

				if (objectModel.StoredException != null)
					throw objectModel.StoredException;
			}
			catch
			{
				if (objectModel.StoredException != null)
				{
					throw objectModel.StoredException;
				}
				else
				{
					objectModel.Dispose();
					throw;
				}
			}
		}
	}
}
