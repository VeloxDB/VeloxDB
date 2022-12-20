using System;
using System.IO;
using System.Threading;
using VeloxDB.Common;
using static System.Math;

namespace VeloxDB.Storage.Persistence;

internal unsafe sealed class ReadThroughLogReader : IDisposable
{
	const int readChunkSize = 1024 * 1024 * 16; // Must be multiple of disk sector size
	const int maxReadAheadChunks = 3;

	NativeFile file;
	uint sectorSize;
	long fileSize;
	bool isPackedFormat;
	JobQueue<LoadedChunk> queue;
	volatile bool stopReading = false;
	Thread worker;
	bool finished = false;
	int currentOffset;
	LoadedChunk currentChunk;
	long totalReadBytes;

	public ReadThroughLogReader(string traceName, string fileName, long initPosition, uint sectorSize, bool isPackedFormat)
	{
		isPackedFormat = false;

		this.sectorSize = sectorSize;
		this.isPackedFormat = isPackedFormat;

		file = NativeFile.Create(fileName, FileMode.Open, FileAccess.Read, FileShare.None, FileFlags.Unbuffered);
		fileSize = file.Size;

		SetFilePosAligned(initPosition);
		totalReadBytes = initPosition;

		queue = new JobQueue<LoadedChunk>(maxReadAheadChunks, JobQueueMode.Normal, maxReadAheadChunks);

		worker = Utils.RunThreadWithSupressedFlow(ReadWorker, string.Format("{0}: vlx-UnbufferedLogReader", traceName));
	}

	public long FileSize => fileSize;

	public long TotalReadBytes => totalReadBytes;

	public bool Read(IntPtr p, long toRead, bool aligned)
	{
		long read;
		Read(p, toRead, aligned, out read);
		return read == toRead;
	}

	public void Read(IntPtr p, long toRead, bool aligned, out long read)
	{
		read = 0;
		if (finished)
			return;

		while (read < toRead)
		{
			ProvideChunk();

			int currReadSize = (int)Min(toRead - read, currentChunk.Size - currentOffset);
			Utils.CopyMemory((byte*)currentChunk.Data + currentOffset, (byte*)p + read, currReadSize);
			currentOffset += currReadSize;
			read += currReadSize;

			if (currentOffset < readChunkSize && read < toRead)
			{
				finished = true;
				totalReadBytes += read;
				return;
			}
		}

		totalReadBytes += read;

		if (aligned && !isPackedFormat && (currentOffset & (sectorSize - 1)) != 0)
		{
			int delta = -(currentOffset & (int)(sectorSize - 1)) + (int)sectorSize;
			currentOffset = currentOffset + delta;
			totalReadBytes += delta;
		}
	}

	private void ProvideChunk()
	{
		if (currentChunk.Data == IntPtr.Zero || currentOffset == currentChunk.Size)
		{
			if (currentChunk.Data != IntPtr.Zero)
				AlignedAllocator.Free(currentChunk.Data);

			currentOffset = 0;
			currentChunk = queue.Dequeue();
		}
	}

	private void ReadWorker()
	{
		while (!stopReading)
		{
			IntPtr p = AlignedAllocator.Allocate(readChunkSize, (int)sectorSize, false);

			long read;
			try
			{
				file.Read(p, readChunkSize, out read);
			}
			catch (NativeException)
			{
				NativeAllocator.Free(p);
				throw;
			}

			while (!queue.TryEnqueue(new LoadedChunk() { Data = p, Size = (int)read }, 1))
			{
				if (stopReading)
					break;
			}

			if (read == 0)
				break;
		}
	}

	private void SetFilePosAligned(long pos)
	{
		file.Seek(pos);
	}

	public void Dispose()
	{
		stopReading = true;
		worker.Join();

		while (true)
		{
			LoadedChunk p;
			if (!queue.TryDequeue(0, out p))
				break;

			AlignedAllocator.Free(p.Data);
		}

		if (currentChunk.Data != IntPtr.Zero)
			AlignedAllocator.Free(currentChunk.Data);

		file.Dispose();
	}

	private struct LoadedChunk
	{
		public IntPtr Data { get; set; }
		public int Size { get; set; }
	}
}
