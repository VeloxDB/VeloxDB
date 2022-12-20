using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using VeloxDB.Common;
using VeloxDB.Config;
namespace VeloxDB.Server;
internal sealed class Configuration
{
	private const string ConfigFileName = "vlxdbcfg.json";
	private const string ConfigDir = "vlxdb";
	static readonly JsonSerializerOptions SerializerOptions = new JsonSerializerOptions()
	{
		WriteIndented = true,
		AllowTrailingCommas = true,
		PropertyNameCaseInsensitive = true,
		ReadCommentHandling = JsonCommentHandling.Skip,
		Converters ={
			new JsonStringEnumConverter(),
			new EndpointConverter(),
		}
	};

	public int Version { get; set; } = 1;
	public HostEndpointConfiguration? ExecutionEndpoint { get; set; }
	public DatabaseConfiguration? Database { get; set; }
	public LoggingConfiguration? Logging { get; set; }

	public ReplicationConfiguration? Replication { get; set; }

	public string AsJson()
	{
		return JsonSerializer.Serialize<Configuration>(this, SerializerOptions);
	}

	public void Evaluate()
	{
		Checker.AssertNotNull(Database, Logging, Replication);
		Checker.AssertNotNull(Database.SystemDatabasePath, Logging.Path, obj3: Replication.ClusterConfigFile);

		Database.SystemDatabasePath = TryEvaluatePath(Database.SystemDatabasePath);
		Logging.Path = TryEvaluatePath(Logging.Path);
		Replication.ClusterConfigFile = TryEvaluatePath(Replication.ClusterConfigFile);

		if (Replication.ClusterConfig != null)
			Replication.ClusterConfig.FillDefaults();
	}

	private string TryEvaluatePath(string path)
	{
		List<string> errors;
		string result = PathTemplate.TryEvaluate(path, out errors, Replication?.ThisNodeName);

		if(errors.Count > 0)
		{
			throw new ConfigurationException(string.Join(",", errors));
		}

		return result;
	}

	public void Override(Configuration newConfig)
	{
		Checker.AssertNotNull(ExecutionEndpoint, Database, Logging, Replication);

		if (newConfig.ExecutionEndpoint != null)
			ExecutionEndpoint.Override(newConfig.ExecutionEndpoint);

		if (newConfig.Database != null)
			Database.Override(newConfig.Database);

		if (newConfig.Logging != null)
			Logging.Override(newConfig.Logging);

		if (newConfig.Replication != null)
			Replication.Override(newConfig.Replication);
	}

	public IReadOnlyCollection<string> TryLoadClusterConfig()
	{
		Checker.AssertNotNull(Replication);

		List<string> errors = new List<string>();

		if (Replication.ClusterConfigFile == null)
		{
			errors.Add("ClusterConfigFile is not set in replication configuration");
			return errors;
		}

		ClusterConfiguration? cluster;

		errors.AddRange(ClusterConfiguration.TryLoad(Replication.ClusterConfigFile, out cluster));

		TryLoadClusterConfig(cluster, errors);

		return errors;
	}

	internal void TryLoadClusterConfig(ClusterConfiguration? cluster, List<string> errors)
	{
		if (errors.Count == 0)
		{
			Checker.AssertNotNull(cluster);
			ValidateCluster(cluster, errors);
		}

		if (errors.Count == 0)
		{
			Checker.AssertNotNull(Replication);
			Checker.AssertNotNull(cluster);
			Replication.ClusterConfig = cluster;
		}
	}

	internal List<string> ValidateCluster()
	{
		Checker.AssertNotNull(Replication);
		Checker.AssertNotNull(Replication.ClusterConfig);

		Evaluate();
		List<string> errors = new List<string>();
		errors.AddRange(Replication.ClusterConfig.Validate());
		ValidateCluster(Replication.ClusterConfig, errors);
		return errors;
	}

	private void ValidateCluster(ClusterConfiguration cluster, List<string> errors)
	{
		Checker.AssertNotNull(Replication);

		string? thisNodeName = Replication.ThisNodeName;
		if (thisNodeName != null)
		{
			ReplicationElement? element;
			if (cluster.TryGetElementByName(thisNodeName, out element))
			{
				if (!(element is ReplicationNode))
				{
					errors.Add($"It appears that in replication configuration Name {thisNodeName}, references {element.Type}. Only {nameof(ElementType.LocalWriteNode)} or {nameof(ElementType.Node)} are allowed.");
				}
			}
			else
			{
				errors.Add($"Node name specified in replication configuration {thisNodeName} does not exist in cluster configuration.");
			}
		}
		else
		{
			errors.Add("ThisNodeName is not set in replication configuration.");
		}
	}

	internal static Configuration CreateDefault()
	{

		return new Configuration()
		{
			ExecutionEndpoint = new HostEndpointConfiguration
			{
				BacklogSize = 20,
				MaxOpenConnCount = 10,
				BufferPoolSize = 1024 * 128,
				InactivityTimeout = 1.0f,
				InactivityInterval = 2.0f,
				MaxQueuedChunkCount = 64
			},

			Database = new DatabaseConfiguration()
			{
				SystemDatabasePath = "${LocalApplicationData}/vlxdb/data",
			},
			Logging = new LoggingConfiguration()
			{
				Path = "${LocalApplicationData}/vlxdb/log",
				Level = LoggingLevel.Info
			},
			Replication = new ReplicationConfiguration()
			{
				ClusterConfigFile = "${Base}/config.cluster.json",
				ThisNodeName = null,
				PrimaryWorkerCount = 4,
				StandbyWorkerCount = 0,
				UseSeparateConnectionPerWorker = true,
			}
		};
	}

	internal static Configuration? LoadFromString(string json)
	{
		return JsonSerializer.Deserialize<Configuration>(json, SerializerOptions);
	}

	public static Configuration Load(IEnumerable<string> filenames)
	{
		Configuration result = CreateDefault();

		foreach (string filename in filenames)
		{
			if (!File.Exists(filename))
				continue;

			using FileStream fileStream = File.OpenRead(filename);

			Configuration? newConfig;
			try
			{
				newConfig = JsonSerializer.Deserialize<Configuration>(fileStream, SerializerOptions);
			}
			catch (JsonException e)
			{
				throw new ConfigurationException($"Error loading {filename}. {e.Message}", e);
			}

			if (newConfig == null)
				continue;

			result.Override(newConfig);
		}

		result.Evaluate();
		return result;
	}

	[MemberNotNull(nameof(ExecutionEndpoint), nameof(Database), nameof(Logging))]
	public static Configuration Load()
	{
		string[] filenames = GetFilenames();
		return Load(filenames);
	}

	[MemberNotNull(nameof(ExecutionEndpoint), nameof(Database), nameof(Logging))]
	public static Configuration Load(string overrideConfig)
	{
		if (!File.Exists(overrideConfig))
			throw new ConfigurationException($"Config File {overrideConfig} not found");

		IEnumerable<string> filenames = GetFilenames().Append<string>(overrideConfig);
		return Load(filenames);
	}

	private static string[] GetFilenames()
	{
		string[] directories = new string[]{
			AppDomain.CurrentDomain.BaseDirectory,
			GetGlobalConfigDirectory(),
			Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), ConfigDir)
		};

		string[] filenames = new string[directories.Length];

		for (int i = 0; i < directories.Length; i++)
		{
			filenames[i] = Path.Combine(directories[i], ConfigFileName);
		}

		return filenames;
	}

	private static string GetGlobalConfigDirectory()
	{
		if (OperatingSystem.IsWindows())
		{
			return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), ConfigDir);
		}
		else if (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
		{
			return Path.Combine("/etc", ConfigDir);
		}

		throw Utils.OSNotSupportedException();
	}

}

internal sealed class ConfigurationException : Exception
{
	public ConfigurationException()
	{
	}

	public ConfigurationException(string? message) : base(message)
	{
	}

	public ConfigurationException(string? message, Exception? innerException) : base(message, innerException)
	{
	}
}

internal sealed class HostEndpointConfiguration
{
	public int? BacklogSize { get; set; }
	public int? MaxOpenConnCount { get; set; }
	public int? BufferPoolSize { get; set; }
	public float? InactivityInterval { get; set; }
	public float? InactivityTimeout { get; set; }
	public int? MaxQueuedChunkCount { get; set; }

	public void Override(HostEndpointConfiguration newEndPoint)
	{
		HostEndpointConfiguration newHostEndpoint = (HostEndpointConfiguration)newEndPoint;

		if (newHostEndpoint.BacklogSize != null)
			BacklogSize = newHostEndpoint.BacklogSize;

		if (newHostEndpoint.MaxOpenConnCount != null)
			MaxOpenConnCount = newHostEndpoint.MaxOpenConnCount;

		if (newHostEndpoint.BufferPoolSize != null)
			BufferPoolSize = newHostEndpoint.BufferPoolSize;

		if (newHostEndpoint.InactivityInterval != null)
			InactivityInterval = newHostEndpoint.InactivityInterval;

		if (newHostEndpoint.InactivityTimeout != null)
			InactivityTimeout = newHostEndpoint.InactivityTimeout;

		if (newHostEndpoint.MaxQueuedChunkCount != null)
			MaxQueuedChunkCount = newHostEndpoint.MaxQueuedChunkCount;
	}
}

internal sealed class DatabaseConfiguration
{
	public string? SystemDatabasePath { get; set; }

	public void Override(DatabaseConfiguration newDatabase)
	{
		if (newDatabase.SystemDatabasePath != null)
			SystemDatabasePath = newDatabase.SystemDatabasePath;
	}
}

internal enum LoggingLevel
{
	None = 0,
	Error = 1,
	Warning = 2,
	Info = 3,
	Debug = 4,
	Verbose = 5,
}

internal sealed class LoggingConfiguration
{
	public string? Path { get; set; }
	public LoggingLevel? Level { get; set; }
	public string? UserPath { get; set; }
	public LoggingLevel? UserLevel { get; set; }

	public void Override(LoggingConfiguration newTracing)
	{
		if (newTracing.Path != null)
			Path = newTracing.Path;

		if (newTracing.Level != null)
			Level = newTracing.Level;

		if (newTracing.UserPath != null)
			UserPath = newTracing.UserPath;

		if (newTracing.UserLevel != null)
			UserLevel = newTracing.UserLevel;
	}
}
