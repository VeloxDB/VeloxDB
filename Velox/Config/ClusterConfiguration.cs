using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Velox.Common;

namespace Velox.Config;

internal class ClusterConfiguration
{
	static readonly JsonSerializerOptions SerializerOptions = new JsonSerializerOptions()
	{
		WriteIndented = true,
		AllowTrailingCommas = true,
		PropertyNameCaseInsensitive = true,
		ReadCommentHandling = JsonCommentHandling.Skip,
		Converters ={
			new JsonStringEnumConverter(),
			new ReplicationElementConverter(),
			new WitnessConverter(),
			new EndpointConverter(),
		}
	};

	public const ushort DefaultReplicationPort = 7570;
	public const string DefaultReplicationPortString = "7570";
	public const ushort DefaultElectorPort = 7571;
	public const string DefaultElectorPortString = "7571";
	public const float DefaultElectionTimeout = 2.0f;
	public const string DefaultElectionTimeoutString = "2";
	public const float DefaultRemoteFileTimeout = 2.0f;
	public const string DefaultRemoteFileTimeoutString = "2";
	public const ushort DefaultAdministrationPort = 7569;
	public const string DefaultAdministrationPortString = "7569";
	public const ushort DefaultExecutionPort = 7568;
	public const string DefaultExecutionPortString = "7568";
	Dictionary<string, ReplicationElement> elementsByName;

	public int Version { get; set; } = 1;
	public ReplicationElement Cluster { get; set; }

	public ClusterConfiguration()
	{
	}

	public static ClusterConfiguration Create()
	{
		return new ClusterConfiguration() { elementsByName = new Dictionary<string, ReplicationElement>(StringComparer.InvariantCultureIgnoreCase) };
	}

	public static IReadOnlyCollection<string> TryLoad(string filename, out ClusterConfiguration clusterConfig)
	{
		if (!File.Exists(filename))
		{
			clusterConfig = null;
			return new string[] { $"File {filename} does not exist." };
		}

		using FileStream fileStream = File.OpenRead(filename);
		return TryLoad(fileStream, out clusterConfig);
	}

	public static IReadOnlyCollection<string> TryLoad(Stream stream, out ClusterConfiguration clusterConfig)
	{
		List<string> errors = new List<string>();
		clusterConfig = null;

		try
		{
			clusterConfig = JsonSerializer.Deserialize<ClusterConfiguration>(stream, SerializerOptions);
		}
		catch (JsonException e)
		{
			FileStream fs = stream as FileStream;
			if (fs != null)
			{
				errors.Add($"Error loading {fs.Name}. {e.Message}");
			}
			else
			{
				errors.Add($"Error loading cluster configuration. {e.Message}");
			}
		}

		if (errors.Count == 0)
		{
			Checker.AssertNotNull(clusterConfig);
			clusterConfig.FillDefaults();

			errors.AddRange(clusterConfig.Validate());
		}

		return errors;
	}

	internal void FillDefaults()
	{
		if (Cluster != null)
			Cluster.FillDefaults();
	}

	internal IReadOnlyCollection<string> Validate()
	{
		ValidationHelper helper = new ValidationHelper();

		if (Cluster == null)
		{
			helper.AddError("Cluster field in cluster configuration is not set, you must define a cluster.");
		}
		else
		{
			Cluster.Validate(helper);
		}

		if (helper.Errors.Count == 0)
		{
			elementsByName = helper.CreateElementDictionary();
		}

		return helper.Errors;
	}

	public bool TryGetElementByName(string name, [NotNullWhen(true)] out ReplicationElement element)
	{
		Checker.AssertNotNull(elementsByName);
		return elementsByName.TryGetValue(name, out element);
	}

	public string AsJson()
	{
		return JsonSerializer.Serialize(this, SerializerOptions);
	}

	internal static IReadOnlyCollection<string> TryLoadFromString(string json, out ClusterConfiguration clusterConfig)
	{
		clusterConfig = null;
		try
		{
			clusterConfig = JsonSerializer.Deserialize<ClusterConfiguration>(json, SerializerOptions);
		}
		catch (JsonException e)
		{
			return new string[] { $"Error loading json. {e.Message}" };
		}

		Checker.AssertNotNull(clusterConfig);

		clusterConfig.FillDefaults();
		return clusterConfig.Validate();
	}
}

internal sealed class ValidationHelper
{
	private record ElementInfo(ReplicationElement Element, string Path);

	readonly List<string> errors;
	readonly List<string> elements;

	readonly Dictionary<string, ElementInfo> names;

	string currentPath;

	public ValidationHelper()
	{
		errors = new List<string>();
		elements = new List<string>();
		names = new Dictionary<string, ElementInfo>();
	}

	public IReadOnlyCollection<string> Errors => errors;

	public string GetCurrentPath()
	{
		if (currentPath == null)
			currentPath = string.Join(".", elements);

		return currentPath;
	}

	public void Enter(ReplicationElement element)
	{
		currentPath = null;
		elements.Add(element.NodeIdentifier);
	}

	public void Leave()
	{
		currentPath = null;
		elements.RemoveAt(elements.Count - 1);
	}

	public bool TryAddElementByName(ReplicationElement element, [NotNullWhen(false)] out string collisionPath)
	{
		Checker.AssertNotNull(element.Name);

		collisionPath = null;
		ElementInfo elementInfo;

		if (!names.TryGetValue(element.Name, out elementInfo))
		{
			names.Add(element.Name, new ElementInfo(element, GetCurrentPath()));
			return true;
		}

		collisionPath = elementInfo.Path;
		return false;
	}

	public void AddError(string error)
	{
		if (elements.Count == 0)
			errors.Add(error);
		else
			errors.Add($"{GetCurrentPath()}: {error}");
	}

	public bool NameExists(string name)
	{
		return names.ContainsKey(name);
	}

	public ReplicationElement GetElementByName(string name)
	{
		ElementInfo elementInfo;
		if (!names.TryGetValue(name, out elementInfo))
		{
			throw new ArgumentException($"Given name {name} is unknown.");
		}

		return elementInfo.Element;
	}

	public Dictionary<string, ReplicationElement> CreateElementDictionary()
	{
		Dictionary<string, ReplicationElement> result = new Dictionary<string, ReplicationElement>(names.Count, StringComparer.InvariantCultureIgnoreCase);

		foreach (KeyValuePair<string, ElementInfo> pair in names)
		{
			result.Add(pair.Key, pair.Value.Element);
		}
		return result;
	}
}

internal enum ElementType
{
	LocalWrite,
	GlobalWrite,
	Node,
	LocalWriteNode,
}

internal abstract class ReplicationElement
{
	private static readonly Regex nameRegex = new Regex("^[a-z,A-Z,0-9,\\., ,_,\\-,:]+$", RegexOptions.IgnoreCase);

	[JsonPropertyOrder(-1)]
	public string Name { get; set; }

	[JsonPropertyOrder(-1)]
	public abstract ElementType Type { get; }

	[JsonIgnore]
	public ReplicationElement Parent { get; set; }

	[JsonIgnore]
	public bool IsMember { get; set; }
	public bool NameEqual(string other)
	{
		Checker.AssertNotNull(Name);
		return string.Compare(Name, other, true, CultureInfo.InvariantCulture) == 0;
	}

	internal virtual void FillDefaults()
	{

	}

	internal void Validate(ValidationHelper helper)
	{
		helper.Enter(this);

		if (Name == null)
		{
			helper.AddError($"Node {helper.GetCurrentPath()} doesn't have name set.");
			if (!nameRegex.Match(Name).Success)
				helper.AddError($"Node {helper.GetCurrentPath()} has an invalid name (invalid characters detected).");
		}
		else
		{
			string collision;
			if (!helper.TryAddElementByName(this, out collision))
			{
				helper.AddError($"Name is not unique. {collision} has the same name.");
			}
		}

		OnValidate(helper);

		helper.Leave();
	}

	protected abstract void OnValidate(ValidationHelper helper);
	public virtual string[] GetPrimaryAdresses()
	{
		throw new NotSupportedException("This element does not provide primary addresses");
	}
	internal string NodeIdentifier => $"{Type}({Name ?? ""})";
}


internal abstract class ReplicationNode : ReplicationElement
{
	public Endpoint ReplicationAddress { get; set; }
	public Endpoint AdministrationAdress { get; set; }
	public Endpoint ExecutionAdress { get; set; }

	internal override void FillDefaults()
	{
		base.FillDefaults();

		ReplicationAddress = SetDefault(ReplicationAddress, "0.0.0.0", ClusterConfiguration.DefaultReplicationPort);
		Checker.AssertNotNull(ReplicationAddress.Address);

		AdministrationAdress = SetDefault(AdministrationAdress, ReplicationAddress.Address, ClusterConfiguration.DefaultAdministrationPort);
		ExecutionAdress = SetDefault(ExecutionAdress, ReplicationAddress.Address, ClusterConfiguration.DefaultExecutionPort);
	}

	protected static Endpoint SetDefault(Endpoint endPoint, string address, ushort port)
	{
		Endpoint result;

		result = endPoint ?? new Endpoint();

		if (result.Address == null)
		{
			result.Address = address;
		}

		if (result.Port == null)
		{
			result.Port = port;
		}

		return result;
	}

	protected override void OnValidate(ValidationHelper helper)
	{
		if (ReplicationAddress == null || ReplicationAddress.Address == null)
		{
			helper.AddError("Address must be set.");
		}
	}

	public override string[] GetPrimaryAdresses()
	{
		Checker.AssertNotNull(ReplicationAddress);
		return new string[] { ReplicationAddress.ToString() };
	}
}

internal sealed class StandaloneNode : ReplicationNode
{
	public ReplicationElement[] Children { get; set; }

	[JsonPropertyOrder(-1)]
	public override ElementType Type => ElementType.Node;

	protected override void OnValidate(ValidationHelper helper)
	{
		base.OnValidate(helper);

		if (Children == null)
			return;

		foreach (ReplicationElement node in Children)
		{
			if (node == null)
				helper.AddError("Error loading child");
			else
				node.Validate(helper);
		}
	}

	internal override void FillDefaults()
	{
		base.FillDefaults();

		if (Children == null)
			return;

		foreach (ReplicationElement node in Children)
		{
			if (node == null)
				continue;

			node.Parent = this;
			node.IsMember = false;
			node.FillDefaults();
		}
	}
}
internal abstract class ClusterBase<T> : ReplicationElement where T : ReplicationElement
{
	public T First { get; set; }
	public T Second { get; set; }
	public ReplicationElement[] Children { get; set; }

	internal override void FillDefaults()
	{
		base.FillDefaults();

		if (First != null)
		{
			First.Parent = this;
			First.IsMember = true;
			First.FillDefaults();
		}

		if (Second != null)
		{
			Second.Parent = this;
			Second.IsMember = true;
			Second.FillDefaults();
		}

		if (Children != null)
		{
			foreach (ReplicationElement child in Children)
			{
				if (child == null)
					continue;

				child.Parent = this;
				child.IsMember = false;
				child.FillDefaults();
			}
		}
	}

	protected override void OnValidate(ValidationHelper helper)
	{
		CheckMember(First, nameof(First), helper);
		CheckMember(Second, nameof(Second), helper);

		if (Children != null)
		{
			foreach (ReplicationElement child in Children)
			{
				if (child == null)
					helper.AddError("Error loading child.");
				else
					child.Validate(helper);
			}
		}

	}

	private void CheckMember(ReplicationElement child, string fieldName, ValidationHelper helper)
	{
		if (child == null)
		{
			helper.AddError($"{fieldName} member is not set. You need to specify both members of {Type} cluster.");
		}
		else
		{
			child.Validate(helper);
		}
	}

	public T GetOther(T t)
	{
		if (object.ReferenceEquals(t, First))
			return Second;
		else
			return First;
	}
}

internal sealed class GlobalWriteCluster : ClusterBase<ReplicationElement>
{
	public bool SynchronousReplication { get; set; }

	[JsonPropertyOrder(-1)]
	public override ElementType Type => ElementType.GlobalWrite;

	public GlobalWriteCluster()
	{
		SynchronousReplication = false;
	}
}


internal sealed class LocalWriteCluster : ClusterBase<LocalWriteNode>
{
	public float? ElectionTimeout { get; set; }
	public Witness Witness { get; set; }

	[JsonPropertyOrder(-1)]
	public override ElementType Type => ElementType.LocalWrite;

	internal override void FillDefaults()
	{
		base.FillDefaults();

		if (ElectionTimeout == null)
		{
			ElectionTimeout = ClusterConfiguration.DefaultElectionTimeout;
		}

		if (Witness != null)
			Witness.FillDefaults();
	}

	public LocalWriteNode GetNode(string name)
	{
		Checker.AssertNotNull(First, Second);
		if (First.NameEqual(name))
			return First;

		if (Second.NameEqual(name))
			return Second;

		throw new ArgumentException($"Name {name} is not part of this LW Cluster");
	}

	public LocalWriteNode GetReplica(string name)
	{
		Checker.AssertNotNull(First, Second);
		if (First.NameEqual(name))
			return Second;

		if (Second.NameEqual(name))
			return First;

		throw new ArgumentException($"Name {name} is not part of this LW Cluster");
	}

	protected override void OnValidate(ValidationHelper helper)
	{
		if (Witness != null)
		{
			Witness.Validate(helper);
		}
		else
			helper.AddError("Witness is not set for LW Cluster");

		base.OnValidate(helper);
	}

	public override string[] GetPrimaryAdresses()
	{
		Checker.AssertNotNull(First, Second);
		Checker.AssertNotNull(First.ReplicationAddress, Second.ReplicationAddress);
		return new string[]
		{
			First.ReplicationAddress.ToString(),
			Second.ReplicationAddress.ToString()
		};
	}
}

internal sealed class LocalWriteNode : ReplicationNode
{
	public Endpoint ElectorAddress { get; set; }

	[JsonPropertyOrder(-1)]
	public override ElementType Type => ElementType.LocalWriteNode;

	internal override void FillDefaults()
	{
		base.FillDefaults();
		Checker.AssertNotNull(ReplicationAddress);
		Checker.AssertNotNull(ReplicationAddress.Address);

		ElectorAddress = SetDefault(ElectorAddress, ReplicationAddress.Address, ClusterConfiguration.DefaultElectorPort);
	}

	protected override void OnValidate(ValidationHelper helper)
	{
	}
}

internal enum WitnessType
{
	SharedFolder, Standalone
}
internal abstract class Witness
{
	public abstract WitnessType Type { get; }
	public abstract void Validate(ValidationHelper helper);

	internal virtual void FillDefaults()
	{
	}
}

internal sealed class SharedFolderWitness : Witness
{
	public string Path { get; set; }
	public float RemoteFileTimeout { get; set; } = ClusterConfiguration.DefaultRemoteFileTimeout;

	[JsonPropertyOrder(-1)]
	public override WitnessType Type => WitnessType.SharedFolder;

	public override void Validate(ValidationHelper helper)
	{
		if (Path == null)
			helper.AddError("Path must be set.");
		else
		{
			List<string> errors;
			Path = PathTemplate.TryEvaluate(Path, out errors);

			if (errors.Count > 0)
			{
				foreach (string error in errors)
					helper.AddError(error);
			}
		}
	}
}

internal sealed class StandaloneWitness : Witness
{
	public Endpoint Address { get; set; }

	[JsonPropertyOrder(-1)]
	public override WitnessType Type => WitnessType.Standalone;

	public override void Validate(ValidationHelper helper)
	{
		if (Address == null || Address.Address == null)
			helper.AddError("Address must be set.");
	}
	internal override void FillDefaults()
	{
		base.FillDefaults();

		if (Address != null && Address.Port == null)
		{
			Address.Port = ClusterConfiguration.DefaultElectorPort;
		}
	}
}
