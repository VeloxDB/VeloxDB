using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.ObjectInterface;
using VeloxDB.Storage;
using VeloxDB.Storage.ModelUpdate;

namespace VeloxDB.Embedded;


/// <summary>
/// Provides an embedded instance of the VeloxDB database, allowing for local database operations
/// within an application.
/// </summary>
/// <remarks>
/// The <see cref="VeloxDBEmbedded"/> class initializes and manages an embedded VeloxDB instance.
/// It allows the creation or opening of a database at a specified file system path and provides
/// methods for starting transactions and managing database resources.
/// </remarks>
public sealed class VeloxDBEmbedded : IDisposable
{
	private const string InitLogDir = "user/log";
	private const string InitSnapshotDir = "user/snapshot";
	private const string SystemDir = "system";

	readonly StorageEngine engine;
	readonly ObjectModelContextPool ctxPool;
	readonly ObjectModelData objectModelData;


	/// <summary>
	/// Initializes a new instance of the <see cref="VeloxDBEmbedded"/> class.
	/// </summary>
	/// <param name="dataPath">The file system path where the database data will be stored.</param>
	/// <param name="assemblies">
	///   An optional array of assemblies to scan for database classes. If not provided,
	///   all assemblies currently loaded will be scanned automatically.
	/// </param>
	/// <param name="allowModelUpdate">Specifies whether updates to the data model are allowed. Defaults to false.</param>
	/// <remarks>
	/// When created in embedded mode, VeloxDB either creates or opens a database at the specified dataPath.
	/// If the database does not exist, a new database is created using the provided model.
	/// If the database exists, the provided model is checked against the existing one.
	/// If they do not match, depending on the value of allowModelUpdate, the process will either
	/// fail with a <see cref="DatabaseException"/> or update the model in the database.
	/// </remarks>
	public VeloxDBEmbedded(string dataPath, Assembly[] assemblies = null, bool allowModelUpdate = false)
	{
		CheckInteractive();
		assemblies ??= ScanAssemblies();

		CreateDirectories(dataPath);

		engine = new StorageEngine(Path.Combine(dataPath, SystemDir), null, null, null, null);
		ctxPool = new ObjectModelContextPool(engine);

		InitPersistence(dataPath);
		objectModelData = UpdateAssemblies(assemblies, allowModelUpdate);
	}

	/// <summary>
	/// Begins a new transaction.
	/// </summary>
	/// <param name="type">The type of transaction to begin. Defaults to <see cref="TransactionType.ReadWrite"/>.</param>
	/// <returns>A new <see cref="VeloxDBTransaction"/> object representing the transaction.</returns>
	public VeloxDBTransaction BeginTransaction(TransactionType type = TransactionType.ReadWrite)
	{
		var om = new ObjectModel(objectModelData, ctxPool, (Storage.TransactionType)type);
		return new VeloxDBTransaction(om);
	}

	/// <summary>
	/// Disposes of the resources used by the <see cref="VeloxDBEmbedded"/> instance.
	/// </summary>
	public void Dispose()
	{
		engine.Dispose();
	}

	private static Assembly[] ScanAssemblies()
	{
		List<Assembly> result = new List<Assembly>();

		foreach (Assembly assembly in AppDomain.CurrentDomain.GetAssemblies())
		{
			foreach (Type classType in assembly.GetTypes())
			{
				if (classType.IsDefined(typeof(DatabaseClassAttribute)))
				{
					result.Add(assembly);
					break;
				}
			}
		}

		return result.ToArray();
	}

	private void CheckInteractive()
	{
		if (Assembly.GetEntryAssembly().GetName().Name == "Microsoft.DotNet.Interactive.App")
		{
			Throw.UsingErrorType(DatabaseErrorType.DotNetInteractiveNotSupported);
		}
	}

	private static void CreateDirectories(string dataPath)
	{
		if (!Directory.Exists(dataPath))
			Directory.CreateDirectory(dataPath);

		string systemPath = Path.Combine(dataPath, SystemDir);
		if (!Directory.Exists(systemPath))
			Directory.CreateDirectory(systemPath);
	}

	private void InitPersistence(string persistenceDir)
	{
		string logDir = Path.Combine(persistenceDir, InitLogDir);
		string snapshotDir = Path.Combine(persistenceDir, InitSnapshotDir);

		PersistenceDescriptor descriptor = engine.GetPersistenceConfiguration();
		if (descriptor == null || descriptor.LogDescriptors == null || descriptor.LogDescriptors.Length == 0)
		{
			Tracing.Info($"Initializing persistence to {persistenceDir}.");
			Directory.CreateDirectory(logDir);
			Directory.CreateDirectory(snapshotDir);

			LogDescriptor logDescriptor = new LogDescriptor("main", false, logDir, snapshotDir, 16 * 1024 * 1024);
			engine.UpdatePersistenceConfiguration([logDescriptor]);
		}
		else
		{
			Tracing.Info($"Persistence already initialized.");
		}
	}

	private ObjectModelData UpdateAssemblies(Assembly[] assemblies, bool allowUpdate)
	{
		UserAssembly[] fromEngineArr = engine.GetUserAssemblies(out var modelVersionGuid, out var assemblyVersionGuid, out var modelDescriptor);
		Dictionary<string, UserAssembly> fromEngine = fromEngineArr.ToDictionary(asm => asm.Name);

		bool emptyDatabase = fromEngine.Count == 0;

		List<UserAssembly> fromUser = new List<UserAssembly>();

		foreach (Assembly assembly in assemblies)
		{
			string filename = Path.GetFileName(assembly.Location).ToLower();
			fromUser.Add(new UserAssembly(0, filename, File.ReadAllBytes(assembly.Location)));
		}

		AssemblyUpdate update = GetUpdate(fromUser, fromEngine, assemblyVersionGuid);

		if (update.Inserted.Count > 0 || update.Updated.Count > 0 || update.Deleted.Count > 0)
		{
			DataModelDescriptor old = modelDescriptor;
			modelDescriptor = CreateModelDescriptor(modelDescriptor, assemblies);

			var hasModelChanged = HasModelChanged(old, modelDescriptor);

			if (hasModelChanged && !allowUpdate && !emptyDatabase)
			{
				Throw.UsingErrorType(DatabaseErrorType.DatabaseModelMismatch);
			}

			if (hasModelChanged)
				engine.UpdateUserAssemblies(update, modelDescriptor, null, out _, out _);
		}

		return new ObjectModelData(modelDescriptor, assemblies);
	}

	private bool HasModelChanged(DataModelDescriptor oldModel, DataModelDescriptor newModel)
	{
		DataModelUpdate modelUpdate = new DataModelUpdate(engine.UserDatabase, oldModel, newModel, false);
		return !modelUpdate.IsEmpty;
	}

	private static DataModelDescriptor CreateModelDescriptor(DataModelDescriptor dataModelDescriptor, Assembly[] loaded)
	{
		ObjectModelSettings objectModelSettings = new ObjectModelSettings();

		foreach (Assembly assembly in loaded)
		{
			objectModelSettings.AddAssembly(assembly);
		}

		DataModelDescriptor descriptor = objectModelSettings.CreateModel(dataModelDescriptor);
		return descriptor;
	}

	private AssemblyUpdate GetUpdate(List<UserAssembly> fromUser, Dictionary<string, UserAssembly> fromEngine, SimpleGuid previous)
	{
		List<UserAssembly> inserted = new List<UserAssembly>();
		List<UserAssembly> updated = new List<UserAssembly>();
		Dictionary<string, UserAssembly> deleted = new Dictionary<string, UserAssembly>(fromEngine);

		foreach (UserAssembly userAssembly in fromUser)
		{
			if (fromEngine.TryGetValue(userAssembly.Name, out var engineAssembly))
			{
				deleted.Remove(userAssembly.Name);
				if (!Utils.ByteArrayEqual(userAssembly.Binary, engineAssembly.Binary))
				{
					updated.Add(new UserAssembly(engineAssembly.Id, userAssembly.Name, userAssembly.Binary));
				}
			}
			else
			{
				inserted.Add(userAssembly);
			}
		}

		List<long> deletedIds = deleted.Select(kv => kv.Value.Id).ToList();

		return new AssemblyUpdate(inserted, updated, deletedIds, previous);
	}
}
