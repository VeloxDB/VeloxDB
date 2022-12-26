using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using VeloxDB.Common;
using VeloxDB.Networking;

namespace VeloxDB.Storage;

internal sealed unsafe partial class Changeset : IDisposable
{
	const ushort serializationVersion = 1;

	int refCount;

	MemoryManager memoryManager;
	LogChangeset[] logChangesets;

	bool disposed;

	public Changeset(MemoryManager memoryManager)
	{
		this.memoryManager = memoryManager;
		refCount = 1;

		TrackReferencingStack();
		TrackAllocation();
	}

	public Changeset(LogChangeset[] logChangesets)
	{
		refCount = 1;
		this.logChangesets = logChangesets;

		for (int i = 0; i < logChangesets.Length; i++)
		{
			logChangesets[i].SetOwner(this);
		}

		TrackReferencingStack();
		TrackAllocation();
	}

	~Changeset()
	{
#if DEBUG
		throw new CriticalDatabaseException();
#endif
	}

	public LogChangeset[] LogChangesets => logChangesets;
	public Changeset Next { get; set; }

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public LogChangeset GetLogChangeset(int logIndex)
	{
		for (int i = 0; i < logChangesets.Length; i++)
		{
			if (logChangesets[i].LogIndex == logIndex)
				return logChangesets[i];
		}

		return null;
	}

	public void TakeRef()
	{
		Interlocked.Increment(ref refCount);
		TrackReferencingStack();
	}

	public void TakeRef(int count)
	{
		Interlocked.Add(ref refCount, count);
		TrackReferencingStack(count);
	}

	public void ReleaseRef()
	{
		TrackDereferencingStack();

		int nrc = Interlocked.Decrement(ref refCount);

		if (nrc < 0)
			throw new CriticalDatabaseException();

		if (nrc == 0)
		{
			CleanUp();
			GC.SuppressFinalize(this);
		}
	}

	public void Dispose()
	{
		ReleaseRef();
	}

	public int GetSerializedSize()
	{
		int s = sizeof(ushort) + sizeof(byte);
		for (int i = 0; i < logChangesets.Length; i++)
		{
			s += logChangesets[i].SerializedSize;
		}

		return s;
	}

	public void Merge(Changeset ch)
	{
		for (int i = 0; i < ch.logChangesets.Length; i++)
		{
			LogChangeset lch2 = ch.logChangesets[i];
			int index1 = FindLogChangeset(lch2.LogIndex);
			if (logChangesets[index1] != null)
			{
				logChangesets[index1].Merge(lch2);
			}
			else
			{
				lch2.SetOwner(this);
				logChangesets[index1] = lch2;
				ch.logChangesets[i] = null;
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private int FindLogChangeset(int logIndex)
	{
		for (int i = 0; i < logChangesets.Length; i++)
		{
			if (logChangesets[i].LogIndex == logIndex)
				return i;
		}

		LogChangeset[] temp = new LogChangeset[logChangesets.Length + 1];
		for (int i = 0; i < logChangesets.Length; i++)
		{
			temp[i] = logChangesets[i];
		}

		logChangesets = temp;
		return temp.Length - 1;
	}

	public void Serialize(MessageWriter writer)
	{
		writer.WriteUShort(serializationVersion);
		writer.WriteByte((byte)logChangesets.Length);
		for (int i = 0; i < logChangesets.Length; i++)
		{
			logChangesets[i].Serialize(writer);
		}
	}

	public void Deserialize(MessageReader reader)
	{
		try
		{
			TryDeserialize(reader);
		}
		catch (Exception e)
		{
			Tracing.Debug("Failed to deserialize changeset, errorType={0}, message={1}.", e.GetType().FullName, e.Message);
			TTTrace.Write(e.GetType().FullName, e.Message);

			if (logChangesets != null)
			{
				for (int i = 0; i < logChangesets.Length; i++)
				{
					logChangesets[i]?.Dispose();
				}
			}

			GC.SuppressFinalize(this);
			throw;
		}
	}

	private void TryDeserialize(MessageReader reader)
	{
		ushort ver = reader.ReadUShort();
		if (ver > serializationVersion)
			Checker.NotSupportedException("Unsupported changeset format {0} encountered.", ver);

		int logCount = reader.ReadByte();
		logChangesets = new LogChangeset[logCount];
		for (int i = 0; i < logCount; i++)
		{
			logChangesets[i] = new LogChangeset(this, memoryManager);
			logChangesets[i].Deserialize(reader);
		}
	}

	private void CleanUp()
	{
		if (disposed)
			return;

		TrackDeallocation();

		for (int i = 0; i < logChangesets.Length; i++)
		{
			logChangesets[i]?.Dispose();
		}

		disposed = true;
	}
}
