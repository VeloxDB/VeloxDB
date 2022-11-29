using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Velox.Common;

namespace Velox.Storage;

internal enum ModifiedType : int
{
	Class = 0,
	InverseReference = 1,
	ObjectReadLock = 3,
	HashReadLock = 4,
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = 1)]
internal unsafe struct ModifiedBufferHeader
{
	public static readonly int Size = 48;

	public ModifiedBufferHeader* nextQueueGroup;    // Used for chaining gc items in the queue

	public ulong readVersion;
	public ulong handle;
	public ulong nextBuffer;
	public int count;
	public int bufferSize;
	public int dataOffsetLimit;
	public ModifiedType modificationType;

	public int DataSize => dataOffsetLimit - Size;
}


internal unsafe class ModifiedList
{
	const int startBufferSize = 1024;
	const int maxBufferSize = 1024 * 1024;

	MemoryManager memoryManager;

	ulong headBuffer;
	int count;

	int bufferSize;
	int offset;
	byte* currBuffer;

	public ModifiedList(MemoryManager memoryManager)
	{
		this.memoryManager = memoryManager;

		headBuffer = 0;
		currBuffer = null;

		bufferSize = startBufferSize;
		offset = startBufferSize;
	}

	public bool NotEmpty => headBuffer != 0;
	public int Count => count;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Init()
	{
		headBuffer = 0;
		currBuffer = null;

		bufferSize = startBufferSize;
		offset = startBufferSize;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ulong TakeContent()
	{
		ulong res = headBuffer;
		headBuffer = 0;
		count = 0;
		return res;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void MergeChanges(ModifiedList l)
	{
		if (headBuffer == 0 && l.headBuffer == 0)
			return;

		if (headBuffer == 0)
			ExhangeContent(l);

		if (l.headBuffer == 0)
			return;

		ulong lastChunk = headBuffer;
		ModifiedBufferHeader* head1 = (ModifiedBufferHeader*)memoryManager.GetBuffer(lastChunk);
		while (head1->nextBuffer != 0)
		{
			lastChunk = head1->nextBuffer;
			head1 = (ModifiedBufferHeader*)memoryManager.GetBuffer(lastChunk);
		}

		ModifiedBufferHeader* head2 = (ModifiedBufferHeader*)memoryManager.GetBuffer(l.headBuffer);
		if (head2->nextBuffer == 0 && head1->dataOffsetLimit + head2->DataSize <= bufferSize)
		{
			Checker.AssertTrue(head2->nextBuffer == 0);
			byte* pdst = (byte*)head1 + head1->dataOffsetLimit;
			byte* psrc = (byte*)head2 + ModifiedBufferHeader.Size;
			Utils.CopyMemory(psrc, pdst, head2->DataSize);
			head1->count += head2->count;
			head1->dataOffsetLimit += head2->DataSize;

			count += l.count;
			offset += head2->DataSize;

			l.FreeMemory();
		}
		else
		{
			head1->nextBuffer = l.headBuffer;
			count += l.count;
			bufferSize = l.bufferSize;
			offset = l.offset;
			currBuffer = l.currBuffer;

			l.Init();
		}
	}

	private void ExhangeContent(ModifiedList l)
	{
		Utils.Exchange(ref headBuffer, ref l.headBuffer);
		Utils.Exchange(ref count, ref l.count);
		Utils.Exchange(ref bufferSize, ref l.bufferSize);
		Utils.Exchange(ref offset, ref l.offset);
		Utils.Exchange(ref currBuffer, ref l.currBuffer);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* MoveToNext(ref ModifiedBufferHeader* header, byte* pitem, int size)
	{
		pitem += size;
		if (pitem - (byte*)header < header->dataOffsetLimit)
			return pitem;

		header = (ModifiedBufferHeader*)memoryManager.GetBuffer(header->nextBuffer);
		return (byte*)header + ModifiedBufferHeader.Size;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* StartIteration(out ModifiedBufferHeader* header)
	{
		header = (ModifiedBufferHeader*)memoryManager.GetBuffer(headBuffer);
		return (byte*)header + ModifiedBufferHeader.Size;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public byte* AddItem(ModifiedType type, int size)
	{
		if (offset + size > bufferSize)
			AllocateNewBuffer(type, size);

		byte* res = currBuffer + offset;
		ModifiedBufferHeader* mb = (ModifiedBufferHeader*)currBuffer;

		mb->dataOffsetLimit += size;
		mb->count++;

		offset += size;
		count++;

		return res;
	}

	public void FreeMemory()
	{
		if (headBuffer == 0)
			return;

		ulong currChunk = headBuffer;
		while (currChunk != 0)
		{
			ModifiedBufferHeader* cp = (ModifiedBufferHeader*)memoryManager.GetBuffer(currChunk);
			ulong temp = cp->nextBuffer;
			memoryManager.Free(currChunk);
			currChunk = temp;
		}

		headBuffer = 0;
		count = 0;
	}

	private void AllocateNewBuffer(ModifiedType type, int size)
	{
		ulong handle;
		ModifiedBufferHeader* head;

		if (headBuffer == 0)
		{
			handle = memoryManager.Allocate(startBufferSize);
			head = (ModifiedBufferHeader*)memoryManager.GetBuffer(handle);
			bufferSize = startBufferSize;
			headBuffer = handle;
		}
		else
		{
			if (bufferSize < maxBufferSize)
				bufferSize *= 2;

			handle = memoryManager.Allocate(bufferSize);
			head = (ModifiedBufferHeader*)memoryManager.GetBuffer(handle);
			((ModifiedBufferHeader*)currBuffer)->nextBuffer = handle;
		}

		head->nextQueueGroup = null;
		head->count = 0;
		head->handle = handle;
		head->bufferSize = bufferSize;
		head->dataOffsetLimit = ModifiedBufferHeader.Size;
		head->modificationType = type;
		head->nextBuffer = 0;

		offset = ModifiedBufferHeader.Size;
		currBuffer = (byte*)head;
	}
}
