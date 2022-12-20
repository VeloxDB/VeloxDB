using System;
using System.Diagnostics;
using System.Runtime.Serialization;
using VeloxDB.Common;
using VeloxDB.Networking;

namespace VeloxDB.Storage;

internal struct GlobalVersion
{
	SimpleGuid globalTerm;
	ulong version;

	public GlobalVersion(SimpleGuid globalTerm, ulong version)
	{
		this.globalTerm = globalTerm;
		this.version = version;
	}

	public ulong Version { get => version; set => version = value; }
	public SimpleGuid GlobalTerm { get => globalTerm; set => globalTerm = value; }

	public void Serialize(MessageWriter writer)
	{
		writer.WriteLong(globalTerm.Low);
		writer.WriteLong(globalTerm.Hight);
		writer.WriteULong(version);
	}

	public static GlobalVersion Deserialize(MessageReader reader)
	{
		return new GlobalVersion(new SimpleGuid(reader.ReadLong(), reader.ReadLong()), reader.ReadULong());
	}

	[Conditional("TTTRACE")]
	public static void TTTraceState(long traceId, GlobalVersion[] versions)
	{
		if (versions == null)
			return;

		TTTrace.Write(traceId, versions.Length);
		for (int i = 0; i < versions.Length; i++)
		{
			TTTrace.Write(versions[i].globalTerm.Low, versions[i].globalTerm.Hight, versions[i].version);
		}
	}

	public override string ToString()
	{
		return $"(Term = {globalTerm}, Version = {version})";
	}
}
