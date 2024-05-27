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

public delegate void DatabaseOperationDelegate(ObjectModel om);
public enum TransactionType
{
	Read = 0,
	ReadWrite = 1
}
public sealed class VeloxDBEmbedded : IDisposable
{
	private const string InitLogDir = "user/log";
	private const string InitSnapshotDir = "user/snapshot";
	private const string SystemDir = "system";

	StorageEngine engine;
	ObjectModelContextPool ctxPool;
	ObjectModelData objectModelData;

	// Treba dodati i opcije za logovanje, automatsko skeniranje assembly-ja, il validacija
	public VeloxDBEmbedded(string dataPath, Assembly[] assemblies, bool preventModelChange)
	{
		if (!Directory.Exists(dataPath))
			Directory.CreateDirectory(dataPath);

		string systemPath = Path.Combine(dataPath, SystemDir);
		if (!Directory.Exists(systemPath))
			Directory.CreateDirectory(systemPath);

		engine = new StorageEngine(systemPath, null, null, null, null);
		ctxPool = new ObjectModelContextPool(engine);

		InitPersistence(dataPath);

		UpdateAssemblies(assemblies, preventModelChange);
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

	private void UpdateAssemblies(Assembly[] assemblies, bool preventModelChange)
	{
		UserAssembly[] fromEngineArr = engine.GetUserAssemblies(out var modelVersionGuid, out var assemblyVersionGuid, out var modelDescriptor);
		Dictionary<string, UserAssembly> fromEngine = fromEngineArr.ToDictionary(asm => asm.Name);

		List<UserAssembly> fromUser = new List<UserAssembly>();

		foreach(Assembly assembly in assemblies)
		{
			string filename = Path.GetFileName(assembly.Location).ToLower();
			fromUser.Add(new UserAssembly(0, filename, File.ReadAllBytes(assembly.Location)));
		}

		AssemblyUpdate update = GetUpdate(fromUser, fromEngine, assemblyVersionGuid);

		// Ovo ne poredi model, nego assembly-je
		if(update.Inserted.Count > 0 || update.Updated.Count > 0 || update.Deleted.Count > 0)
		{
			modelDescriptor = CreateModelDescriptor(modelDescriptor, assemblies);
			engine.UpdateUserAssemblies(update, modelDescriptor, null, out _, out _);
		}

		 objectModelData = new ObjectModelData(modelDescriptor, assemblies);
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

		foreach(UserAssembly userAssembly in fromUser)
		{
			if(fromEngine.TryGetValue(userAssembly.Name, out var engineAssembly))
			{
				deleted.Remove(userAssembly.Name);
				if(!Utils.ByteArrayEqual(userAssembly.Binary, engineAssembly.Binary))
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

	public VeloxDBTransaction BeginTransaction(TransactionType type = TransactionType.ReadWrite)
	{
		var om = new ObjectModel(objectModelData, ctxPool, (Storage.TransactionType)type);
		return new VeloxDBTransaction(om);
	}

	public void Dispose()
	{
		engine.Dispose();
	}
}

public sealed class VeloxDBTransaction : IDisposable
{
	public ObjectModel ObjectModel { get; private set; }

	internal VeloxDBTransaction(ObjectModel om)
	{
		this.ObjectModel = om;
	}

	public void Commit()
	{
		ObjectModel.CommitAndDispose();
	}

	public void Dispose()
	{
		ObjectModel.Dispose();
	}

	public void Rollback()
	{
		ObjectModel.Rollback();
	}
}