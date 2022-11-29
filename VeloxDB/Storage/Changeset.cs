using System;
using System.Diagnostics;
using System.Threading;
using Velox.Common;
using Velox.Networking;

namespace Velox.Storage;

internal sealed unsafe partial class Changeset : IDisposable
{
	const ushort serializationVersion = 1;

	int refCount;

	MemoryManager memoryManager;
	LogChangeset[] logChangesets;

	bool ownsLogChangesets;

	bool disposed;

	internal Changeset(MemoryManager memoryManager)
	{
		this.memoryManager = memoryManager;

		TrackReferencingStack();
		TrackAllocation();
	}

	internal Changeset(LogChangeset[] logChangesets, bool ownsLogChangesets = true)
	{
		this.ownsLogChangesets = ownsLogChangesets;

		refCount = 1;
		this.logChangesets = logChangesets;

		for (int i = 0; i < logChangesets.Length; i++)
		{
			logChangesets[i].SetOwner(this);
		}

		TrackReferencingStack();
		TrackAllocation();
	}

	internal LogChangeset[] LogChangesets => logChangesets;

	internal void TakeRef()
	{
		Interlocked.Increment(ref refCount);
		TrackReferencingStack();
	}

	internal void ReleaseRef()
	{
		int nrc = Interlocked.Decrement(ref refCount);
		Checker.AssertTrue(nrc >= 0);

		if (nrc == 0)
		{
			CleanUp();
			System.GC.SuppressFinalize(this);
		}

		TrackDereferencingStack();
	}

	public void Dispose()
	{
		TrackDereferencingStack();

		int nrc = Interlocked.Decrement(ref refCount);
		Checker.AssertTrue(nrc >= 0);

		if (nrc > 0)
			return;

		CleanUp();
	}

	internal int GetSerializedSize()
	{
		int s = sizeof(ushort) + sizeof(byte);
		for (int i = 0; i < logChangesets.Length; i++)
		{
			s += logChangesets[i].GetSerializedSize();
		}

		return s;
	}

	internal void Serialize(MessageWriter writer)
	{
		writer.WriteUShort(serializationVersion);
		writer.WriteByte((byte)logChangesets.Length);
		for (int i = 0; i < logChangesets.Length; i++)
		{
			logChangesets[i].Serialize(writer);
		}
	}

	internal void Deserialize(MessageReader reader)
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

			System.GC.SuppressFinalize(this);
			throw;
		}
	}

	private void TryDeserialize(MessageReader reader)
	{
		ushort ver = reader.ReadUShort();
		if (ver > serializationVersion)
			Checker.NotSupportedException("Unsupported changeset format {0} encountered.", ver);

		refCount = 1;

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

		if (ownsLogChangesets)
		{
			for (int i = 0; i < logChangesets.Length; i++)
			{
				logChangesets[i].Dispose();
			}
		}

		disposed = true;
	}
}

internal sealed unsafe class LogChangeset : IDisposable
{
	Changeset owner;
	MemoryManager memoryManager;
	int logIndex;
	int bufferCount;
	byte* buffers;
	string[] strings;
	LogChangeset next;

	internal LogChangeset(Changeset owner, MemoryManager memoryManager)
	{
		this.owner = owner;
		this.memoryManager = memoryManager;
	}

	internal LogChangeset(MemoryManager memoryManager, int logIndex, int bufferCount, byte* buffers, string[] strings)
	{
		this.memoryManager = memoryManager;
		this.logIndex = logIndex;
		this.bufferCount = bufferCount;
		this.buffers = buffers;
		this.strings = strings;
	}

	~LogChangeset()
	{
		memoryManager.SafeFree(() =>
		{
			ChangesetBufferHeader* curr = (ChangesetBufferHeader*)buffers;
			while (curr != null)
			{
				ChangesetBufferHeader* next = curr->next;
				memoryManager.Free(curr->handle);
				curr = next;
			}
		});
	}

	internal int BufferCount => bufferCount;
	internal byte* Buffers => buffers;
	internal int LogIndex => logIndex;
	internal string[] Strings => strings;
	internal LogChangeset Next { get => next; set => next = value; }

	public void SetOwner(Changeset owner)
	{
		this.owner = owner;
	}

	public void Dispose()
	{
		ChangesetBufferHeader* curr = (ChangesetBufferHeader*)buffers;
		while (curr != null)
		{
			ChangesetBufferHeader* next = curr->next;
			memoryManager.Free(curr->handle);
			curr = next;
		}

		buffers = null;

		GC.SuppressFinalize(this);
	}

	internal int GetSerializedSize()
	{
		int size = sizeof(byte) + sizeof(short);

		ChangesetBufferHeader* cp = (ChangesetBufferHeader*)buffers;
		for (int i = 0; i < bufferCount; i++)
		{
			size += (int)cp->size - ChangesetBufferHeader.Size + sizeof(int);
			cp = cp->next;
		}

		return size;
	}

	internal void WriteTo(ref byte* bp)
	{
		*bp = (byte)logIndex;
		bp += 1;

		*((short*)bp) = (short)BufferCount;
		bp += 2;

		ChangesetBufferHeader* cp = (ChangesetBufferHeader*)buffers;
		for (int i = 0; i < bufferCount; i++)
		{
			int currSize = (int)cp->size;
			currSize -= ChangesetBufferHeader.Size;

			*(int*)bp = (int)currSize;
			bp += sizeof(int);

			Utils.CopyMemory((byte*)cp + ChangesetBufferHeader.Size, bp, currSize);
			bp += currSize;

			cp = (ChangesetBufferHeader*)cp->next;
		}
	}

	internal void ReadFrom(ref byte* bp)
	{
		logIndex = *bp;
		bp += 1;

		bufferCount = *(short*)bp;
		bp += 2;

		ChangesetBufferHeader* prev = null;
		for (uint i = 0; i < bufferCount; i++)
		{
			int size = *(int*)bp;
			bp += sizeof(int);

			ulong handle = memoryManager.Allocate(size + ChangesetBufferHeader.Size);
			byte* buffer = memoryManager.GetBuffer(handle);

			if (i == 0)
				buffers = buffer;

			Utils.CopyMemory(bp, buffer + ChangesetBufferHeader.Size, size);
			bp += size;

			if (prev != null)
				prev->next = (ChangesetBufferHeader*)buffer;

			prev = (ChangesetBufferHeader*)buffer;
			prev->size = size + ChangesetBufferHeader.Size; // This is total buffer size including the header
			prev->handle = handle;
		}

		prev->next = null;
	}

	internal void Serialize(MessageWriter writer)
	{
		writer.WriteByte((byte)logIndex);
		writer.WriteUShort((ushort)bufferCount);

		ChangesetBufferHeader* curr = (ChangesetBufferHeader*)buffers;
		for (int i = 0; i < bufferCount; i++)
		{
			int size = (int)curr->size;
			size -= ChangesetBufferHeader.Size;

			writer.WriteInt(size);
			writer.WriteBuffer((byte*)curr + ChangesetBufferHeader.Size, size);

			curr = (ChangesetBufferHeader*)curr->next;
		}
	}

	internal void Deserialize(MessageReader reader)
	{
		logIndex = reader.ReadByte();
		bufferCount = reader.ReadUShort();

		if (bufferCount != 0)
		{
			ChangesetBufferHeader* prev = null;
			for (uint i = 0; i < bufferCount; i++)
			{
				int size = reader.ReadInt();
				ulong handle = memoryManager.Allocate(size + ChangesetBufferHeader.Size);
				byte* buffer = memoryManager.GetBuffer(handle);

				try
				{
					reader.ReadBuffer(buffer + ChangesetBufferHeader.Size, size);
				}
				catch
				{
					memoryManager.Free(handle);
					throw;
				}

				if (i == 0)
					buffers = buffer;

				if (prev != null)
					prev->next = (ChangesetBufferHeader*)buffer;

				prev = (ChangesetBufferHeader*)buffer;
				prev->size = size + ChangesetBufferHeader.Size; // This is total buffer size including the header
				prev->handle = handle;
				prev->next = null;
			}

			prev->next = null;
		}
	}
}
