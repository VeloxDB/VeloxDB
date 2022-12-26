using System;
using System.Security.Cryptography;
using VeloxDB.Common;
using VeloxDB.Networking;

namespace VeloxDB.Storage;

internal sealed unsafe class LogChangeset : IDisposable
{
	static readonly int mergeBufferSize = MemoryManager.MaxManagedSize;

	Changeset owner;
	MemoryManager memoryManager;
	int logIndex;
	int bufferCount;
	byte* buffers;
	string[] strings;
	LogChangeset next;
	int serializedSize = -1;
	byte* lastBuffer;

	public LogChangeset(Changeset owner, MemoryManager memoryManager)
	{
		this.owner = owner;
		this.memoryManager = memoryManager;
	}

	public LogChangeset(MemoryManager memoryManager, int logIndex, int bufferCount, byte* buffers, string[] strings)
	{
		this.memoryManager = memoryManager;
		this.logIndex = logIndex;
		this.bufferCount = bufferCount;
		this.buffers = buffers;
		this.strings = strings;
		UpdateInternalState();
	}

	~LogChangeset()
	{
#if DEBUG
		throw new CriticalDatabaseException();
#else

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
#endif
	}

	public int BufferCount => bufferCount;
	public byte* Buffers => buffers;
	public int LogIndex => logIndex;
	public string[] Strings => strings;
	public LogChangeset Next { get => next; set => next = value; }

	public int SerializedSize
	{
		get
		{
			Checker.AssertFalse(serializedSize == -1);
			return serializedSize;
		}
	}

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

	private void UpdateInternalState()
	{
		serializedSize = sizeof(byte) + sizeof(short);
		lastBuffer = buffers;

		ChangesetBufferHeader* cp = (ChangesetBufferHeader*)buffers;
		for (int i = 0; i < bufferCount; i++)
		{
			serializedSize += (int)cp->size - ChangesetBufferHeader.Size + sizeof(int);
			cp = cp->next;
			if (cp != null)
				lastBuffer = (byte*)cp;
		}
	}

	public void Merge(LogChangeset ch)
	{
		// Once changeset is merged, managed string storage is no longer used.
		strings = null;

		ChangesetBufferHeader* cp = (ChangesetBufferHeader*)buffers;
		if (bufferCount == 1 && cp->bufferSize < mergeBufferSize)
		{
			ulong newHandle = memoryManager.Allocate(mergeBufferSize);
			byte* newBuffer = memoryManager.GetBuffer(newHandle);
			Utils.CopyMemory(buffers, newBuffer, cp->size);

			ChangesetBufferHeader* newCp = (ChangesetBufferHeader*)newBuffer;
			newCp->next = null;
			newCp->handle = newHandle;
			newCp->size = cp->size;
			newCp->bufferSize = mergeBufferSize;

			memoryManager.Free(cp->handle);

			buffers = newBuffer;
			lastBuffer = buffers;
		}

		cp = (ChangesetBufferHeader*)ch.buffers;
		while (cp != null)
		{
			AppendData((byte*)cp + ChangesetBufferHeader.Size, cp->size - ChangesetBufferHeader.Size);
			ChangesetBufferHeader* next = cp->next;
			ulong handle = cp->handle;
			Utils.ZeroMemory((byte*)cp, cp->bufferSize);
			memoryManager.Free(handle);
			cp = next;
		}


		ch.bufferCount = 0;
		ch.buffers = null;

#if DEBUG
		int ss = serializedSize;
		byte* lb = lastBuffer;
		UpdateInternalState();
		Checker.AssertTrue(ss == serializedSize);
		Checker.AssertTrue(lb == lastBuffer);
#endif
	}

	private void AppendData(byte* src, int size)
	{
		do
		{
			ChangesetBufferHeader* dstCp = (ChangesetBufferHeader*)lastBuffer;
			ProvideSpace(ref dstCp);
			byte* dst = lastBuffer + dstCp->size;
			int toCopy = Math.Min(size, dstCp->bufferSize - dstCp->size);
			Utils.CopyMemory(src, dst, toCopy);
			src += toCopy;
			size -= toCopy;
			dstCp->size += toCopy;
			serializedSize += toCopy;
		}
		while (size > 0);
	}

	private void ProvideSpace(ref ChangesetBufferHeader* dstCp)
	{
		if (dstCp->size < dstCp->bufferSize)
			return;

		ulong newHandle = memoryManager.Allocate(mergeBufferSize);
		byte* newBuffer = memoryManager.GetBuffer(newHandle);
		ChangesetBufferHeader* newCp = (ChangesetBufferHeader*)newBuffer;
		newCp->next = null;
		newCp->handle = newHandle;
		newCp->size = ChangesetBufferHeader.Size;
		newCp->bufferSize = mergeBufferSize;
		dstCp->next = newCp;
		serializedSize += sizeof(int);
		bufferCount++;
		lastBuffer = newBuffer;
		dstCp = newCp;
	}

	public void WriteTo(ref byte* bp)
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

			cp = cp->next;
		}
	}

	public void ReadFrom(ref byte* bp)
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
			prev->size = size + ChangesetBufferHeader.Size; // This is total data size including the header
			prev->bufferSize = prev->size;
			prev->handle = handle;
		}

		prev->next = null;

		UpdateInternalState();
	}

	public void Serialize(MessageWriter writer)
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

	public void Deserialize(MessageReader reader)
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
				prev->bufferSize = size;
				prev->handle = handle;
				prev->next = null;
			}

			prev->next = null;
		}

		UpdateInternalState();
	}
}
