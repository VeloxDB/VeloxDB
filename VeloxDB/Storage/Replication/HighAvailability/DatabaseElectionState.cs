using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using VeloxDB.Common;
using VeloxDB.Networking;

namespace VeloxDB.Storage.Replication.HighAvailability;

internal struct DatabaseElectionState
{
	public const int StateSize = sizeof(long) + sizeof(ulong) + sizeof(uint);

	long id;
	uint term;
	ulong version;

	public DatabaseElectionState(long id, uint term, ulong version)
	{
		TTTrace.Write(id, term, version);

		this.id = id;
		this.term = term;
		this.version = version;
	}

	public long Id { get => id; set => id = value; }
	public uint Term { get => term; set => term = value; }
	public ulong Version { get => version; set => version = value; }

	public static bool IsLessThan(DatabaseElectionState[] states1, Dictionary<long, DatabaseElectionState> states2)
	{
		List<DatabaseElectionState> l2 = new List<DatabaseElectionState>(states1.Length);

		for (int i = 0; i < states1.Length; i++)
		{
			if (states2.TryGetValue(states1[i].Id, out DatabaseElectionState s2))
			{
				l2.Add(s2);
			}
		}

		return Compare(states1, l2.ToArray()) < 0;
	}

	public static DatabaseElectionState FindUser(DatabaseElectionState[] states)
	{
		for (int i = 0; i < states.Length; i++)
		{
			if (states[i].Id == 2)
			{
				return states[i];
			}
		}

		throw new InvalidOperationException();
	}

	public static int Compare(DatabaseElectionState[] states1, DatabaseElectionState[] states2)
	{
		Checker.AssertTrue(states1.Length == states2.Length);

		uint m1 = states1.Select(x => x.Term).Max();
		uint m2 = states2.Select(x => x.Term).Max();
		if (m1 < m2)
			return -1;
		else if (m1 > m2)
			return 1;

		for (int i = states1.Length - 1; i >= 0; i--)
		{
			int c = Compare(states1[i], states2[i]);
			if (c != 0)
				return c;
		}

		return 0;
	}

	private static int Compare(DatabaseElectionState s1, DatabaseElectionState s2)
	{
		if (s1.Term < s2.Term)
			return -1;

		if (s1.Term > s2.Term)
			return 1;

		if (s1.Version < s2.Version)
			return -1;

		if (s1.Version > s2.Version)
			return 1;

		return 0;
	}

	[Conditional("TTTRACE")]
	public static void TTTraceState(DatabaseElectionState[] states)
	{
		TTTrace.Write(states.Length);
		for (int i = 0; i < states.Length; i++)
		{
			TTTrace.Write(states[i].Id, states[i].Term, states[i].Version);
		}
	}

	[Conditional("TTTRACE")]
	public static void TTTraceState(Dictionary<long, DatabaseElectionState> states)
	{
		TTTrace.Write(states.Count);
		foreach (DatabaseElectionState state in states.Values)
		{
			TTTrace.Write(state.Id, state.Term, state.Version);
		}
	}

	public void Serialize(MessageWriter writer)
	{
		writer.WriteLong(id);
		writer.WriteUInt(term);
		writer.WriteULong(version);
	}

	public static void SerializeArray(MessageWriter writer, DatabaseElectionState[] states)
	{
		writer.WriteInt(states.Length);
		for (int i = 0; i < states.Length; i++)
		{
			DatabaseElectionState state = states[i];
			writer.WriteLong(state.id);
			writer.WriteUInt(state.term);
			writer.WriteULong(state.version);
		}
	}

	public static DatabaseElectionState Deserialize(MessageReader reader)
	{
		DatabaseElectionState v = new DatabaseElectionState();
		v.id = reader.ReadLong();
		v.term = reader.ReadUInt();
		v.version = reader.ReadULong();
		return v;
	}

	public static DatabaseElectionState[] DeserializeArray(MessageReader reader)
	{
		int c = reader.ReadInt();
		DatabaseElectionState[] v = new DatabaseElectionState[c];
		for (int i = 0; i < c; i++)
		{
			v[i].id = reader.ReadLong();
			v[i].term = reader.ReadUInt();
			v[i].version = reader.ReadULong();
		}

		return v;
	}

	public override string ToString()
	{
		return $"(id={id}, term={term}, version={version})";
	}
}
