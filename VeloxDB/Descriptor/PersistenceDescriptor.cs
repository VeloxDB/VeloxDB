using System;
using System.Runtime.Serialization;
using System.IO;
using Velox.Common;

namespace Velox.Descriptor;

internal sealed class PersistenceDescriptor
{
	public const int MaxLogGroups = 4;
	public const int FirstUserLogIndex = 1;

	const short serializerVersion = 1;

	ReadOnlyArray<LogDescriptor> logDescriptors;

	private PersistenceDescriptor()
	{
	}

	public PersistenceDescriptor(LogDescriptor[] logDescriptors, long databaseId)
	{
		string masterPath = Path.Combine(LogDescriptor.SysDirTemplate, databaseId.ToString());

		if (logDescriptors.Length == 0 || logDescriptors[0].Name != LogDescriptor.MasterLogName)
		{
			LogDescriptor[] temp = new LogDescriptor[logDescriptors.Length + 1];
			temp[0] = new LogDescriptor(LogDescriptor.MasterLogName, false, masterPath, masterPath, LogDescriptor.SysLogDefaultMaxSize);
			Array.Copy(logDescriptors, 0, temp, 1, logDescriptors.Length);
			logDescriptors = temp;
		}

		this.logDescriptors = new ReadOnlyArray<LogDescriptor>(logDescriptors);
	}

	public PersistenceDescriptor(PersistenceSettings persistanceSettings, long databaseId)
	{
		if (persistanceSettings.MainLog.Classes != null)
			Checker.ArgumentException("Main log classes must not be specified directly and should be set to null.");

		string sysDBPath = Path.Combine(persistanceSettings.SystemDirectory, databaseId.ToString());

		LogDescriptor[] logs = new LogDescriptor[2 + persistanceSettings.SecondaryLogs.Length];
		logs[0] = new LogDescriptor(LogDescriptor.MasterLogName, false, sysDBPath, sysDBPath, LogDescriptor.SysLogDefaultMaxSize);
		logs[1] = new LogDescriptor(persistanceSettings.MainLog.Name, persistanceSettings.MainLog.IsPackedFormat,
			persistanceSettings.MainLog.Directory, persistanceSettings.MainLog.SnapshotDirectory, persistanceSettings.MainLog.MaxSize);

		for (int i = 0; i < persistanceSettings.SecondaryLogs.Length; i++)
		{
			LogSettings ls = persistanceSettings.SecondaryLogs[i];
			logs[i + 2] = new LogDescriptor(ls.Name, ls.IsPackedFormat, ls.Directory, ls.SnapshotDirectory, ls.MaxSize);
		}

		this.logDescriptors = new ReadOnlyArray<LogDescriptor>(logs);
	}

	public PersistenceDescriptor Clone()
	{
		LogDescriptor[] lds = new LogDescriptor[logDescriptors.Length];
		for (int i = 0; i < lds.Length; i++)
		{
			lds[i] = logDescriptors[i].Clone();
		}

		return new PersistenceDescriptor() { logDescriptors = new ReadOnlyArray<LogDescriptor>(lds) };
	}

	public ReadOnlyArray<LogDescriptor> LogDescriptors => logDescriptors;
	public byte CompleteLogMask => (byte)((1 << logDescriptors.Length) - 1);
	public bool HasNonMasterLogs => logDescriptors.Length > FirstUserLogIndex;

	public static byte[] Serialize(PersistenceDescriptor model)
	{
		using (MemoryStream ms = new MemoryStream())
		using (BinaryWriter w = new BinaryWriter(ms))
		{
			model.Serialize(w);
			return ms.ToArray();
		}
	}

	public void Serialize(BinaryWriter writer)
	{
		writer.Write(serializerVersion);

		writer.Write(logDescriptors.Length);
		for (int i = 0; i < logDescriptors.Length; i++)
		{
			logDescriptors[i].Serialize(writer);
		}
	}

	public static PersistenceDescriptor Deserialize(byte[] binary)
	{
		using (MemoryStream ms = new MemoryStream(binary))
		using (BinaryReader r = new BinaryReader(ms))
		{
			return Deserialize(r);
		}
	}

	public static PersistenceDescriptor Deserialize(BinaryReader reader)
	{
		short version = reader.ReadInt16();
		if (version > serializerVersion)
			throw new SerializationException("Unsupported persistence descriptor serialization format.");

		PersistenceDescriptor d = new PersistenceDescriptor();

		int logCount = reader.ReadInt32();
		LogDescriptor[] logs = new LogDescriptor[logCount];
		for (int i = 0; i < logCount; i++)
		{
			logs[i] = LogDescriptor.Deserialize(reader);
		}

		d.logDescriptors = new ReadOnlyArray<LogDescriptor>(logs);
		return d;
	}
}
