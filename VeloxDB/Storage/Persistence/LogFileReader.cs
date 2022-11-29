using System;
using System.Collections.Generic;
using System.IO;
using Velox.Common;
using static System.Math;

namespace Velox.Storage.Persistence;

internal unsafe sealed class LogFileReader : IDisposable
{
	const int initBufferSize = 1024 * 1024;

	string fileName;
	ReadThroughLogReader reader;
	long bufferSize;
	long bufferOffset;
	IntPtr buffer;
	LogBlockFileHeader chunkHeader;
	LogFileHeader logHeader;
	long offset;
	MemoryManager memoryManager;

	public LogFileReader(string traceName, string fileName, LogFileHeader header, MemoryManager memoryManager)
	{
		TTTrace.Write(fileName, header.version);

		this.fileName = fileName;
		this.logHeader = header;
		this.memoryManager = memoryManager;

		reader = new ReadThroughLogReader(traceName, fileName, LogFileHeader.SkipSize, header.sectorSize, header.isPackedFormat);

		offset = reader.TotalReadBytes;

		bufferSize = initBufferSize;
		buffer = NativeAllocator.Allocate(initBufferSize);
	}

	public static LogFileHeader ReadHeader(string fileName)
	{
		using (NativeFile file = NativeFile.Create(fileName, FileMode.Open, FileAccess.Read, FileShare.None, FileFlags.Sequential))
		{
			LogFileHeader head = new LogFileHeader();

			file.Read(new IntPtr(&head), LogFileHeader.Size);

			if (head.version > DatabasePersister.FormatVersion)
				Checker.NotSupportedException("Unsupported log file format detected.", head.version);

			return head;
		}
	}

	public static long GetEmptyLogPosition()
	{
		return LogFileHeader.SkipSize;
	}

	public long GetLogPosition()
	{
		return offset;
	}

	public bool TryRead(List<LogItem> logItems, ulong invalidLogSeqNum)
	{
		if (reader == null)
			return false;

		if (!TryLoadChunk())
		{
			reader.Dispose();
			reader = null;
			return false;
		}

		offset = reader.TotalReadBytes;

		while (bufferOffset < chunkHeader.size - SimpleGuid.Size)
		{
			LogItem logItem = ReadLogItem();
			if (logItem.LogSeqNum >= invalidLogSeqNum)
			{
				logItem.DisposeChangesets();
				reader.Dispose();
				reader = null;
				return logItems.Count > 0;
			}

			logItems.Add(logItem);
		}

		return logItems.Count > 0;
	}

	private bool TryLoadChunk()
	{
		LogBlockFileHeader h = new LogBlockFileHeader();
		h.version = 1234;
		h.size = 123123;

		if (!reader.Read((IntPtr)(&h), LogBlockFileHeader.Size, false))
			return false;

		if (h.marker.Low != logHeader.marker.Low || h.marker.Hight != logHeader.marker.Hight)
			return false;

		chunkHeader = h;

		EnsureBufferSize();

		if (!reader.Read(buffer, chunkHeader.size, true))
			return false;

		long* lp = (long*)((byte*)buffer + (chunkHeader.size - SimpleGuid.Size));
		if (logHeader.marker.Low != lp[0] || logHeader.marker.Hight != lp[1])
			return false;

		bufferOffset = 0;

		return true;
	}

	private void EnsureBufferSize()
	{
		if (bufferSize < chunkHeader.size)
		{
			long newSize = Max(bufferSize * 2, chunkHeader.size);
			Utils.ResizeMem(ref buffer, bufferSize, newSize);
			bufferSize = newSize;
		}
	}

	private LogItem ReadLogItem()
	{
		byte* bp = (byte*)buffer + bufferOffset;
		LogItem li = LogItem.Deserialize(memoryManager, ref bp);
		bufferOffset = (long)bp - buffer.ToInt64();

		return li;
	}

	public void Dispose()
	{
		NativeAllocator.Free(buffer);

		if (reader != null)
			reader.Dispose();
	}
}
