using System;
using VeloxDB.Client;
using VeloxDB.Common;
using VeloxDB.Protocol;

namespace VeloxDB.Server;

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
