using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.ObjectInterface;
using VeloxDB.Protocol;
using VeloxDB.Storage;
using Engine = VeloxDB.Storage.ModelUpdate;

namespace VeloxDB.Server;
[DbAPI(Name = AdminAPIServiceNames.DatabaseAdministration)]
public sealed class DatabaseAdministration
{
	private const string InitLogDir = "user/log";
	private const string InitSnapshotDir = "user/snapshot";

	private static readonly Regex asmFilenameRegex = new Regex("^[a-z,A-Z,0-9,\\., ,_]+\\.dll$", RegexOptions.IgnoreCase);
	private static readonly Regex logNameRegex = new Regex("^[a-z,A-Z,0-9,\\., ,_,\\-,:]+$", RegexOptions.IgnoreCase);
	private static readonly Regex dirnameRegex = new Regex("^([a-z,A-Z,0-9,\\., ,_,\\-,/,\\\\,:]+|\\$\\{NodeName\\})*$", RegexOptions.IgnoreCase);

	object sync;
	StorageEngine engine;

	internal DatabaseAdministration(StorageEngine engine)
	{
		this.engine = engine;
		sync = new object();
	}

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	public UserAssembliesState GetAssemblyState(bool hashOnly = true)
	{
		SimpleGuid modelVersionGuid;
		SimpleGuid asmVerSimGuid;
		Guid asmVerGuid;
		Engine.UserAssembly[] assemblies = engine.GetUserAssemblies(out modelVersionGuid, out asmVerSimGuid, out _);

		asmVerGuid = (Guid)asmVerSimGuid;

		List<UserAssembly> resAssemblies = new List<UserAssembly>();

		foreach (Engine.UserAssembly assembly in assemblies)
		{
			byte[] hash = new byte[20];

			int written;
			bool success = SHA1.TryHashData(assembly.Binary, hash, out written);

			Checker.AssertTrue(success && written == hash.Length);

			byte[]? binary = hashOnly ? null : assembly.Binary;

			UserAssembly resAssembly = new UserAssembly(assembly.Id, assembly.Name, hash, binary);
			resAssemblies.Add(resAssembly);
		}

		return new UserAssembliesState(resAssemblies, asmVerGuid);
	}

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	public PersistenceDescriptor GetPersistenceConfiguration()
	{
		return Convert(engine.GetPersistenceConfiguration());
	}

	[DbAPIOperation]
	public void UpdatePersistenceConfiguration(PersistenceDescriptor persistenceDescriptor)
	{
		lock (sync)
		{
			Check(persistenceDescriptor);
			engine.UpdatePersistenceConfiguration(Convert(persistenceDescriptor));
		}
	}

	[DbAPIOperation]
	public void UpdateUserAssemblies(AssemblyUpdate assemblyUpdate, Guid assemblyVersionGuid)
	{
		lock (sync)
		{
			Check(assemblyUpdate);
			Engine.AssemblyUpdate engineUpdate = ToEngineAssemblyUpdate(assemblyUpdate, assemblyVersionGuid);

			SimpleGuid modelVersionId;
			SimpleGuid prevAsmSimGuid;

			DataModelDescriptor dataModelDescriptor;

			Engine.UserAssembly[] current = engine.GetUserAssemblies(out modelVersionId, out prevAsmSimGuid, out dataModelDescriptor);
			Guid prevAsmGuid = (Guid)prevAsmSimGuid;

			if (assemblyVersionGuid != prevAsmGuid)
				throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.InvalidAssemblyVersionGuid));

			Engine.UserAssembly[] allAssemblies = ApplyUpdate(current, engineUpdate);

			ValidateIL(allAssemblies);

			LoadedAssemblies loaded = new LoadedAssemblies(allAssemblies);
			Type[] apis = AssemblyUtils.GetDBApiTypes(loaded.Loaded).ToArray();
			ValidateAPIs(apis);

			SerializerManager serializerManager;
			DeserializerManager deserializerManager;
			ProtocolDiscoveryContext discoveryContext;
			PrepareSerialization(apis, new AssemblyProvider(loaded.Assemblies), out serializerManager, out deserializerManager, out discoveryContext);

			DataModelDescriptor descriptor = CreateModelDescriptor(dataModelDescriptor, loaded.Loaded);

			SimpleGuid modelVersionGuid;
			SimpleGuid asmVersionSimGuid;
			engine.UpdateUserAssemblies(engineUpdate, descriptor, new AssemblyData(loaded, serializerManager, deserializerManager, discoveryContext),
										out modelVersionGuid, out asmVersionSimGuid);
		}
	}

	private static void PrepareSerialization(Type[] apis, IAssemblyProvider assemblyProvider, out SerializerManager serializerManager,
		out DeserializerManager deserializerManager, out ProtocolDiscoveryContext discoveryContext)
	{
		serializerManager = null!;
		deserializerManager = null!;
		discoveryContext = null!;

		try
		{
			DbAPIHost.PrepareSerialization(typeof(ObjectModel), apis, assemblyProvider,
				out serializerManager, out deserializerManager, out discoveryContext);
		}
		catch (DbAPIDefinitionException e)
		{
			Throw.DbAPIDefinitionException(e);
		}
	}

	internal void InitPersistence(string persistenceDir)
	{
		string logDir = Path.Combine(persistenceDir, InitLogDir);
		string snapshotDir = Path.Combine(persistenceDir, InitSnapshotDir);

		PersistenceDescriptor descriptor = GetPersistenceConfiguration();
		if (descriptor.LogDescriptors.Count == 0)
		{
			Tracing.Info($"Initializing persistence to {persistenceDir}.");
			Directory.CreateDirectory(logDir);
			Directory.CreateDirectory(snapshotDir);

			LogDescriptor logDescriptor = new LogDescriptor("main", false, logDir, snapshotDir, 16 * 1024 * 1024);
			UpdatePersistenceConfiguration(new PersistenceDescriptor(new List<LogDescriptor>() { logDescriptor }));
		}
		else
		{
			Tracing.Info($"Persistence already initialized.");
		}
	}

	internal List<string> UpdateAssembliesFromDirectory(string updateAsmDir)
	{
		Tracing.Info("Looking for changes in {0}", updateAsmDir);

		List<string> errors = new List<string>();
		UserAssembliesState state = GetAssemblyState(false);
		AssemblyUpdate update = AssemblyUpdate.CreateUpdate(state, updateAsmDir, errors);

		if (errors.Count == 0 && AssembliesChanged(update))
			UpdateUserAssemblies(update, state.AssemblyVersionGuid);

		return errors;
	}

	private static bool AssembliesChanged(AssemblyUpdate update)
	{
		return update.Inserted.Count != 0 || update.Updated.Count != 0 || update.Deleted.Count != 0;
	}

	private void ValidateAPIs(Type[] apis)
	{
		try
		{
			DbAPIHost.ValidateAPIs(typeof(ObjectModel), apis);
		}catch(DbAPIDefinitionException e)
		{
			Throw.DbAPIDefinitionException(e);
		}

		ValidateConstructors(apis);
	}

	private void ValidateConstructors(Type[] types)
	{
		foreach (Type type in types)
		{
			try
			{
				var instance = Activator.CreateInstance(type);
			}
			catch (Exception e)
			{
				Throw.FailedToCreateInstance(type.FullName, e.ToString());
			}
		}
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

	private static void ValidateIL(IReadOnlyCollection<Engine.UserAssembly> allAssemblies)
	{
		ILValidation.Result result = ILValidation.Validate(allAssemblies);
		if (!result.Success)
			Throw.UsingDetail(result.Detail);
	}

	private void Check([NotNull] PersistenceDescriptor? persistenceDescriptor)
	{
		CheckNullArgument(persistenceDescriptor, nameof(persistenceDescriptor));

		CheckNull(persistenceDescriptor.LogDescriptors, nameof(persistenceDescriptor), nameof(PersistenceDescriptor), nameof(PersistenceDescriptor.LogDescriptors));

		foreach (LogDescriptor? ld in persistenceDescriptor.LogDescriptors)
		{
			CheckNull(ld, nameof(persistenceDescriptor), nameof(PersistenceDescriptor), nameof(PersistenceDescriptor.LogDescriptors));
			CheckNull(ld.Directory, nameof(persistenceDescriptor), nameof(LogDescriptor), nameof(LogDescriptor.Directory));
			CheckNull(ld.SnapshotDirectory, nameof(persistenceDescriptor), nameof(LogDescriptor), nameof(LogDescriptor.SnapshotDirectory));
			CheckNull(ld.Name, nameof(persistenceDescriptor), nameof(LogDescriptor), nameof(LogDescriptor.Name));

			CheckLogName(ld.Name);
			CheckDirName(ld.Directory);
			CheckDirName(ld.SnapshotDirectory);
		}
	}

	private void Check([NotNull] AssemblyUpdate? assemblyUpdate)
	{
		CheckNullArgument(assemblyUpdate, nameof(assemblyUpdate));
		CheckNull(assemblyUpdate.Inserted, nameof(assemblyUpdate), nameof(AssemblyUpdate), nameof(AssemblyUpdate.Inserted));
		CheckNull(assemblyUpdate.Deleted, nameof(assemblyUpdate), nameof(AssemblyUpdate), nameof(AssemblyUpdate.Deleted));
		CheckNull(assemblyUpdate.Updated, nameof(assemblyUpdate), nameof(AssemblyUpdate), nameof(AssemblyUpdate.Updated));

		Check(assemblyUpdate.Inserted, nameof(AssemblyUpdate), nameof(AssemblyUpdate.Inserted));
		Check(assemblyUpdate.Updated, nameof(AssemblyUpdate), nameof(AssemblyUpdate.Updated));
	}

	private void Check(List<UserAssembly> userAssemblies, string argName, string collectionName)
	{
		foreach (UserAssembly ua in userAssemblies)
		{
			if (ua == null)
				Throw.InvalidArgument(argName, $"{collectionName} contains null");

			CheckNull(ua.Binary, argName, nameof(UserAssembly), nameof(UserAssembly.Binary));
			CheckNull(ua.Name, argName, nameof(UserAssembly), nameof(UserAssembly.Name));

			CheckAsmFilename(ua.Name);
		}
	}

	private void CheckAsmFilename(string name)
	{
		if (!asmFilenameRegex.Match(name).Success)
			Throw.InvalidAssemblyFilename(name);
	}

	private void CheckLogName(string name)
	{
		if (!logNameRegex.Match(name).Success)
			Throw.InvalidLogName(name);
	}

	private void CheckDirName(string directory)
	{
		if (!dirnameRegex.Match(directory).Success || directory == "." || directory == "..")
			Throw.InvalidDirectoryName(directory);
	}

	private void CheckNull(object? obj, string argName, string className, string propName)
	{
		if (obj == null)
			Throw.InvalidArgument(argName, $"{className}.{propName} cannot be null.");
	}

	private void CheckNullArgument([NotNull] object? obj, string name)
	{
		if (obj == null)
			Throw.NullArgument(name);
	}

	private Descriptor.LogDescriptor[] Convert(PersistenceDescriptor persistenceDescriptor)
	{
		Descriptor.LogDescriptor[] resDescriptors = new Descriptor.LogDescriptor[persistenceDescriptor.LogDescriptors.Count];

		for (int i = 0; i < resDescriptors.Length; i++)
		{
			resDescriptors[i] = Convert(persistenceDescriptor.LogDescriptors[i]);
		}

		return resDescriptors;
	}

	private Descriptor.LogDescriptor Convert(LogDescriptor d)
	{
		return new Descriptor.LogDescriptor(d.Name, d.IsPackedFormat, d.Directory, d.SnapshotDirectory, d.MaxSize);
	}

	private PersistenceDescriptor Convert(Descriptor.PersistenceDescriptor pd)
	{
		if (pd == null)
			return new PersistenceDescriptor(new List<LogDescriptor>());

		List<LogDescriptor> resDescriptors = new List<LogDescriptor>(pd.LogDescriptors.Length);

		foreach (Descriptor.LogDescriptor logDescriptor in pd.LogDescriptors.Skip(1))	// Skip master
		{
			resDescriptors.Add(Convert(logDescriptor));
		}

		return new PersistenceDescriptor(resDescriptors);
	}

	private LogDescriptor Convert(Descriptor.LogDescriptor d)
	{
		return new LogDescriptor(d.Name, d.IsPackedFormat, d.Directory, d.SnapshotDirectory, d.MaxSize);
	}

	private Engine.AssemblyUpdate ToEngineAssemblyUpdate(AssemblyUpdate assemblyUpdate, Guid prevAsmVerGuid)
	{
		List<Engine.UserAssembly> inserted = Convert(assemblyUpdate.Inserted);
		List<Engine.UserAssembly> updated = Convert(assemblyUpdate.Updated);
		List<long> deleted = new List<long>(assemblyUpdate.Deleted);
		return new Engine.AssemblyUpdate(inserted, updated, deleted, (SimpleGuid)prevAsmVerGuid);
	}

	private Engine.UserAssembly Convert(UserAssembly ua)
	{
		return new Engine.UserAssembly(ua.Id, ua.Name, ua.Binary);
	}

	private List<Engine.UserAssembly> Convert(List<UserAssembly> ua)
	{
		List<Engine.UserAssembly> result = new List<Engine.UserAssembly>(ua.Count);
		result.AddRange(ua.Select(Convert));
		return result;
	}

	private static Engine.UserAssembly[] ApplyUpdate(Engine.UserAssembly[] current, Engine.AssemblyUpdate assemblyUpdate)
	{
		Dictionary<long, Engine.UserAssembly> updatedAssemblies = current.ToDictionary(ua => ua.Id);
		HashSet<string> knownNames = new HashSet<string>(current.Select(ua => ua.Name));

		foreach (long deleted in assemblyUpdate.Deleted)
		{
			if (!updatedAssemblies.ContainsKey(deleted))
				Throw.UnknownUserAssembly(deleted, "");

			updatedAssemblies.Remove(deleted);
		}

		long tempId = -1;
		foreach (Engine.UserAssembly inserted in assemblyUpdate.Inserted)
		{
			if (knownNames.Contains(inserted.Name))
				Throw.AssemblyNameAlreadyExists(inserted.Name);

			updatedAssemblies.Add(tempId, inserted);
			knownNames.Add(inserted.Name);
			tempId--;
		}

		foreach (Engine.UserAssembly updated in assemblyUpdate.Updated)
		{
			if (!updatedAssemblies.ContainsKey(updated.Id))
				Throw.UnknownUserAssembly(updated.Id, updated.Name);

			updatedAssemblies[updated.Id] = updated;
		}

		return updatedAssemblies.Values.ToArray();
	}

}
