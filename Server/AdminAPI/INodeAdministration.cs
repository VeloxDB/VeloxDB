using System;
using Velox.Client;
using Velox.Common;
using Velox.Protocol;

namespace Velox.Server;

[DbAPI(Name = AdminAPIServiceNames.NodeAdministration)]
public interface INodeAdministration
{
	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	DatabaseTask<NodeState> GetState();

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	DatabaseTask<FileData> GetClusterConfigFile();

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	DatabaseTask SetTraceLevel(TraceLevel level);

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	DatabaseTask SetUserTraceLevel(TraceLevel level);
}

public sealed class FileData
{
	public string Name { get; set; }
	public byte[] Data { get; set; }

	public FileData()
	{
		Name = null!;
		Data = null!;
	}
}
