using System;

namespace VeloxDB.Server;

public class NodeState : IEquatable<NodeState>
{
    string? nodeName;
    bool isWitnessConnected;
    bool isElectorConnected;
    List<ReplicaState> replicaStates;

    public NodeState()
    {
        replicaStates = null!;
    }

    public NodeState(string? nodeName, List<ReplicaState> replicaStates, bool isWitnessConnected, bool isElectorConnected)
    {
        this.nodeName = nodeName;
        this.replicaStates = replicaStates;
        this.isWitnessConnected = isWitnessConnected;
        this.isElectorConnected = isElectorConnected;
    }

    public string? NodeName { get => nodeName; set => nodeName = value; }
    public List<ReplicaState> ReplicaStates {get => replicaStates; set => replicaStates = value;}
    public bool IsWitnessConnected { get => isWitnessConnected; set => isWitnessConnected = value; }
    public bool IsElectorConnected { get => isElectorConnected; set => isElectorConnected = value; }

    public bool Equals(NodeState? other)
    {
        if (isWitnessConnected != other!.isWitnessConnected || isElectorConnected != other.isElectorConnected)
            return false;

        if (replicaStates.Count != other.replicaStates.Count)
            return false;

        for (int i = 0; i < replicaStates.Count; i++)
        {
            if (!replicaStates[i].Equals(other.replicaStates[i]))
                return false;
        }

        return true;
    }
}

public enum ReplicaStateType
{
    NotUsed = 1,
    Disconnected = 2,
    ConnectedPendingSync = 3,
    ConnectedAsync = 4,
    ConnectedSync = 5,
}

public enum ReplicaType
{
    Source = 1,
    LocalRead = 2,
    GlobalRead = 3,
    GlobalWrite = 4,
    LocalWrite = 5
}

public class ReplicaState : IEquatable<ReplicaState>
{
    string name;
    ReplicaStateType stateType;
    ReplicaType replicaType;
    bool isPrimary;
    bool isAligned;

    public ReplicaState()
    {
        name = null!;
    }
    
    public ReplicaState(string name, ReplicaStateType stateType, ReplicaType replicaType, bool isPrimary, bool isAligned)
    {
        this.name = name;
        this.stateType = stateType;
        this.replicaType = replicaType;
        this.isPrimary = isPrimary;
        this.isAligned = isAligned;
    }

    public string Name { get => name; set => name = value;}
    public ReplicaStateType StateType { get => stateType; set => stateType = value;}
    public ReplicaType ReplicaType { get => replicaType; set => replicaType = value;}
    public bool IsPrimary { get => isPrimary; set => isPrimary = value;}
    public bool IsAligned { get => isAligned; set => isAligned = value; }

    public bool Equals(ReplicaState? other)
    {
        if(other == null)
            return false;

        return other.isPrimary == isPrimary && other.name == name && other.replicaType == replicaType &&
            other.stateType == stateType && other.isAligned == isAligned;
    }
}
