using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage.ModelUpdate;

namespace VeloxDB.Storage.Replication;

internal sealed class ReplicationInfoPublisher
{
	readonly object sync = new object();
	DatabaseInfo info;

	private event Action<DatabaseInfo> StateChanged;

	public ReplicationInfoPublisher(ReplicationSettings replSettings)
	{
		info = new DatabaseInfo(InitialReplicas(replSettings), SimpleGuid.Zero,
			SimpleGuid.Zero, null, EmptyArray<UserAssembly>.Instance, false, false, null);
	}

	private ReplicaInfo[] InitialReplicas(ReplicationSettings replSettings)
	{
		List<ReplicaInfo> result = new List<ReplicaInfo>();

		if(replSettings == null)
			return result.ToArray();

		if(replSettings.LocalWriteReplica != null)
		{
			result.Add(new ReplicaInfo(replSettings.LocalWriteReplica.Name, ReplicaStateType.NotStarted,
				ReplicaType.LocalWrite, false, false));
		}

		if(replSettings.GlobalWriteReplica != null)
		{
			result.Add(new ReplicaInfo(replSettings.GlobalWriteReplica.Name, ReplicaStateType.NotStarted,
				ReplicaType.GlobalWrite, false, false));
		}

		if(replSettings.SourceReplica != null)
		{
			result.Add(new ReplicaInfo(replSettings.SourceReplica.Name, ReplicaStateType.NotStarted,
				ReplicaType.Source, false, false));
		}

		if (replSettings.LocalReadReplicas != null)
		{
			foreach (ReplicaSettings child in replSettings.LocalReadReplicas)
			{
				result.Add(new ReplicaInfo(child.Name, ReplicaStateType.NotStarted,
					ReplicaType.LocalRead, false, false));
			}
		}

		if (replSettings.GlobalReadReplicas != null)
		{
			foreach (ReplicaSettings child in replSettings.GlobalReadReplicas)
			{
				result.Add(new ReplicaInfo(child.Name, ReplicaStateType.NotStarted, ReplicaType.GlobalRead, false, false));
			}
		}

		return result.ToArray();
	}

	public void Subscribe(Action<DatabaseInfo> handler)
	{
		lock (sync)
		{
			StateChanged += handler;
			handler(info);
		}
	}

	public void Unsubscribe(Action<DatabaseInfo> handler)
	{
		lock (sync)
		{
			StateChanged -= handler;
		}
	}

	public void PublishLocalWriteElectorChange(bool isWitnessConnected, bool isElectorConnected)
	{
		lock (sync)
		{
			info = info.Update(isWitnessConnected,  isElectorConnected);
			PublishEvent();
		}
	}

	public void Publish(ReplicaInfo replicaInfo, int index)
	{
		if(index == -1)
			return;

		lock (sync)
		{
			if (!info.ReplicaChanged(replicaInfo, index))
			 	return;

			info = info.Update(replicaInfo, index);
			PublishEvent();
		}
	}

	public void Publish(UserAssembly[] assemblies, SimpleGuid modelVersionGuid, SimpleGuid assembliesVersionGuid,
						DataModelDescriptor modelDescriptor, object customObj)
	{
		lock (sync)
		{
			if (!info.AssembliesChanged(assembliesVersionGuid) && info.ModelDescriptor != null)
			 	return;

			info = info.Update(assemblies, modelVersionGuid, assembliesVersionGuid, modelDescriptor, customObj);
			PublishEvent();
		}
	}

	private void PublishEvent()
	{
		StateChanged?.Invoke(info);
	}
}

internal sealed class DatabaseInfo : IEquatable<DatabaseInfo>
{
	bool isChild;
	bool isGWPrimary;
	bool isLWPrimary;
	bool isWriteMaster;

	bool isWitnessConnected;
	bool isElectorConnected;

	ReplicaInfo[] replicas;
	SimpleGuid modelVersionGuid;
	SimpleGuid assembliesVersionGuid;
	DataModelDescriptor modelDescriptor;
	UserAssembly[] assemblies;
	object customObj;

	private  DatabaseInfo(DatabaseInfo old, SimpleGuid modelVersionGuid, SimpleGuid assembliesVersionGuid,
						   DataModelDescriptor modelDescriptor, UserAssembly[] assemblies, object customObj)
	{
		this.replicas = old.replicas;
		this.isGWPrimary = old.isGWPrimary;
		this.isChild = old.isChild;
		this.isLWPrimary = old.isLWPrimary;
		this.isWriteMaster = old.isWriteMaster;
		this.isWitnessConnected = old.isWitnessConnected;
		this.isElectorConnected = old.isElectorConnected;

		this.modelVersionGuid = modelVersionGuid;
		this.assembliesVersionGuid = assembliesVersionGuid;
		this.modelDescriptor = modelDescriptor;
		this.assemblies = assemblies;
		this.customObj = customObj;
	}

	public DatabaseInfo(ReplicaInfo[] replicas, SimpleGuid modelVersionGuid, SimpleGuid assembliesVersionGuid,
		DataModelDescriptor modelDescriptor, UserAssembly[] assemblies, bool isWitnessConnected,
		bool isElectorConnected, object customObj)
	{
		this.replicas = replicas;
		this.modelVersionGuid = modelVersionGuid;
		this.assembliesVersionGuid = assembliesVersionGuid;
		this.modelDescriptor = modelDescriptor;
		this.assemblies = assemblies;
		this.customObj = customObj;
		this.isWitnessConnected = isWitnessConnected;
		this.isElectorConnected = isElectorConnected;

		if(replicas == null)
			throw new ArgumentNullException();

		isWriteMaster = true;
		isLWPrimary = true;

		for(int i = 0; i < replicas.Length; i++)
		{
			if(replicas[i] == null)
				continue;

			isWriteMaster = isWriteMaster && replicas[i].IsPrimary;

			ReplicaInfo replica = replicas[i];
			switch(replica.ReplicaType)
			{
				case ReplicaType.Source:
					isChild = true;
					break;
				case ReplicaType.LocalWrite:
					isLWPrimary = replica.IsPrimary;
					break;
				case ReplicaType.GlobalRead:
					isGWPrimary = replica.IsPrimary;
					break;
			}
		}
	}

	public bool IsChild => isChild;
	public bool IsGlobalWritePrimary => isGWPrimary;
	public bool IsLocalWritePrimary => isLWPrimary;
	public bool IsWriteMaster => isWriteMaster;

	public ReplicaInfo[] Replicas => replicas;

	public SimpleGuid ModelVersionGuid => modelVersionGuid;
	public SimpleGuid AssembliesVersionGuid => assembliesVersionGuid;
	public DataModelDescriptor ModelDescriptor => modelDescriptor;
	public UserAssembly[] Assemblies => assemblies;
	public object CustomObject => customObj;

	public bool IsWitnessConnected => isWitnessConnected;
	public bool IsElectorConnected => isElectorConnected;

	public bool Equals(DatabaseInfo other)
	{
		return isChild == other.isChild && isGWPrimary == other.isGWPrimary && isLWPrimary == other.isLWPrimary;
	}

	public bool ReplicaChanged(ReplicaInfo replicaInfo, int index)
	{
		return !Replicas[index].Equals(replicaInfo);
	}

	public DatabaseInfo Update(bool isWitnessConnected, bool isElectorConnected)
	{
		return new DatabaseInfo(replicas, modelVersionGuid, assembliesVersionGuid, modelDescriptor, assemblies,
			isWitnessConnected, isElectorConnected, customObj);
	}

	public DatabaseInfo Update(ReplicaInfo replicaInfo, int index)
	{
		int count = index+1;

		if(replicas != null)
			count = Math.Max(count, replicas.Length);

		ReplicaInfo[] newReplicas = new ReplicaInfo[count];

		if(replicas != null)
			Array.Copy(replicas, newReplicas, replicas.Length);

		newReplicas[index] = replicaInfo;

		DatabaseInfo result = new DatabaseInfo(newReplicas, modelVersionGuid,
			assembliesVersionGuid, modelDescriptor, assemblies, isWitnessConnected, isElectorConnected, customObj);
		return result;
	}

	public bool AssembliesChanged(SimpleGuid assembliesVersionGuid)
	{
		return !this.assembliesVersionGuid.Equals(assembliesVersionGuid);
	}

	public DatabaseInfo Update(UserAssembly[] assemblies, SimpleGuid modelVersionGuid,
		SimpleGuid assembliesVersionGuid, DataModelDescriptor modelDescriptor, object customObj)
	{
		return new DatabaseInfo(this, modelVersionGuid, assembliesVersionGuid, modelDescriptor, assemblies, customObj);
	}
}

internal sealed class ReplicaInfo
{
	string name;
    ReplicaStateType stateType;
    ReplicaType replicaType;
    bool isPrimary;
	bool isAligned;

    public ReplicaInfo(string name, ReplicaStateType stateType, ReplicaType replicaType, bool isPrimary, bool isAligned)
	{
		this.name = name;
		this.stateType = stateType;
		this.replicaType = replicaType;
		this.isPrimary = isPrimary;
		this.isAligned = isAligned;
	}

	public string Name => name;
    public ReplicaStateType StateType => stateType;
    public ReplicaType ReplicaType => replicaType;
    public bool IsPrimary => isPrimary;
	public bool IsAligned => isAligned;

	public bool Equals(ReplicaInfo other)
	{
		return name == other.name && stateType == other.stateType && replicaType == other.replicaType &&
			isPrimary == other.isPrimary && isAligned == other.isAligned;
	}
}
