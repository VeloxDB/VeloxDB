using System;
using System.Net.Security;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal class ReplicaSettings
{
	public ReplicaSettings()
	{
		SendWorkerCount = 2;
		RedoWorkerCount = ProcessorNumber.CoreCount * 2;
	}

	public string Name { get; set; }
	public string HostAddress { get; set; }
	public SslServerAuthenticationOptions SSLOptions { get; set; }

	public string[] PartnerAddresses { get; set; }
	public SslClientAuthenticationOptions[] PartnerSSLOptions { get; set; }
	public int SendWorkerCount { get; set; }
	public int RedoWorkerCount { get; set; }
	public bool UseSeparateConnectionPerWorker { get; set; }
}

internal sealed class GlobalWriteReplicaSettings : ReplicaSettings
{
	bool isSyncMode;

	public GlobalWriteReplicaSettings()
	{
		isSyncMode = false;
	}

	public bool IsSyncMode { get => isSyncMode; set => isSyncMode = value; }
}
