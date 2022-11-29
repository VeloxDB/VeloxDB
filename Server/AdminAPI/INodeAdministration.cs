using System;
using Velox.Common;
using Velox.Protocol;

namespace Velox.Server;

[DbAPI(Name = AdminAPIServiceNames.NodeAdministration)]
public interface INodeAdministration
{
    [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
    Task<NodeState> GetState();

    [DbAPIOperation(OperationType = DbAPIOperationType.Read)]
    Task<FileData> GetClusterConfigFile();

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	Task SetTraceLevel(TraceLevel level);

	[DbAPIOperation(OperationType = DbAPIOperationType.Read)]
	Task SetUserTraceLevel(TraceLevel level);
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
