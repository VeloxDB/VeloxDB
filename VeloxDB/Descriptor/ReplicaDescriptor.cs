using System;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal class ReplicaDescriptor
{
	readonly string name;
	readonly string hostAddress;
	readonly ReadOnlyArray<string> partnerAddresses;
	readonly int primaryWorkerCount;
	readonly int standbyWorkerCount;
	bool useSeparateConnectionPerWorker;

	public ReplicaDescriptor(ReplicaSettings settings)
	{
		this.name = settings.Name;
		this.hostAddress = settings.HostAddress;
		this.partnerAddresses = settings.PartnerAddresses == null ? null : new ReadOnlyArray<string>(settings.PartnerAddresses, true);
		this.hostAddress = settings.HostAddress;
		this.primaryWorkerCount = Math.Max(settings.SendWorkerCount, 1);
		this.standbyWorkerCount = settings.RedoWorkerCount != 0 ? settings.RedoWorkerCount : NativeProcessorInfo.LogicalCoreCount * 2;
		this.useSeparateConnectionPerWorker = settings.UseSeparateConnectionPerWorker;
	}

	public static ReplicaDescriptor Create(ReplicaSettings settings)
	{
		if (settings == null)
			return null;

		return new ReplicaDescriptor(settings);
	}

	public static ReadOnlyArray<ReplicaDescriptor> Create(ReplicaSettings[] settings)
	{
		if (settings == null)
			return ReadOnlyArray<ReplicaDescriptor>.Empty;

		ReplicaDescriptor[] res = new ReplicaDescriptor[settings.Length];
		for (int i = 0; i < res.Length; i++)
		{
			res[i] = new ReplicaDescriptor(settings[i]);
		}

		return new ReadOnlyArray<ReplicaDescriptor>(res);
	}

	public string Name => name;
	public string HostAddress => hostAddress;
	public ReadOnlyArray<string> PartnerAddresses => partnerAddresses;
	public int PrimaryWorkerCount => primaryWorkerCount;
	public int StandbyWorkerCount => standbyWorkerCount;
	public bool ReplicationEnabled => hostAddress != null || partnerAddresses != null;
	public bool UseSeparateConnectionPerWorker => useSeparateConnectionPerWorker;

	internal void Validate()
	{
		if (string.IsNullOrWhiteSpace(name))
			throw new ArgumentException("Replica name is invalid.");
	}
}

internal sealed class GlobalWriteReplicaDescriptor : ReplicaDescriptor
{
	bool syncModeAllowed;

	public GlobalWriteReplicaDescriptor(GlobalWriteReplicaSettings settings) :
		base(settings)
	{
		syncModeAllowed = settings.IsSyncMode;
	}

	public static GlobalWriteReplicaDescriptor Create(GlobalWriteReplicaSettings settings)
	{
		if (settings == null)
			return null;

		return new GlobalWriteReplicaDescriptor(settings);
	}

	public bool SyncModeAllowed => syncModeAllowed;
}
