using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using VeloxDB.Common;
using static System.Math;

namespace VeloxDB.Storage.Persistence;

internal unsafe sealed class LogFileReader : IDisposable
{
	const int smallBufferSize = 1024 * 8;
	const int mediumBufferSize = 1024 * 128;

	string fileName;
	ReadThroughLogReader reader;

	LogFileHeader logHeader;
	long offset;

	LogBufferPool smallBufferPool, mediumBuffersPool;

	public LogFileReader(string traceName, string fileName, LogFileHeader header)
	{
		TTTrace.Write(fileName, header.version);

		this.fileName = fileName;
		this.logHeader = header;

		smallBufferPool = new LogBufferPool(smallBufferSize, 2048);
		mediumBuffersPool = new LogBufferPool(mediumBufferSize, 128);

		reader = new ReadThroughLogReader(traceName, fileName, LogFileHeader.SkipSize, header.sectorSize, header.isPackedFormat);

		offset = reader.TotalReadBytes;
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

	public bool TryReadBlock(out LogBufferPool.Buffer buffer, out bool isStandard)
	{
		isStandard = true;

		LogBlockFileHeader h = new LogBlockFileHeader();
		if (!reader.Read((IntPtr)(&h), LogBlockFileHeader.Size, false))
		{
			buffer = default;
			return false;
		}

		if (h.marker.Low != logHeader.marker.Low || h.marker.Hight != logHeader.marker.Hight)
		{
			buffer = default;
			return false;
		}

		if (h.size <= smallBufferSize)
			buffer = smallBufferPool.Get(h.size);
		else if (h.size <= mediumBufferSize)
			buffer = mediumBuffersPool.Get(h.size);
		else
			buffer = new LogBufferPool.Buffer(h.size);

		if (!reader.Read(buffer.Value, h.size, true))
		{
			buffer.Dispose();
			buffer = default;
			return false;
		}

		long* lp = (long*)((byte*)buffer.Value + (h.size - SimpleGuid.Size));
		if (logHeader.marker.Low != lp[0] || logHeader.marker.Hight != lp[1])
		{
			buffer.Dispose();
			buffer = default;
			return false;
		}

		isStandard = h.type == LogBlockType.Standard;

		offset = reader.TotalReadBytes;

		return true;
	}

	public static bool TryExtractLogItems(MemoryManager memoryManager, byte* buffer, long size,
		List<LogItem> logItems, ulong invalidLogSeqNum)
	{
		long bufferOffset = 0;
		while (bufferOffset < size - SimpleGuid.Size)
		{
			byte* bp = buffer + bufferOffset;
			LogItem logItem = LogItem.Deserialize(memoryManager, ref bp);
			bufferOffset = (long)bp - (long)buffer;

			if (logItem.LogSeqNum >= invalidLogSeqNum)
			{
				logItem.DisposeChangesets();
				return logItems.Count > 0;
			}

			logItems.Add(logItem);
		}

		return logItems.Count > 0;
	}

	public void Dispose()
	{
		reader.Dispose();
		smallBufferPool.Dispose();
		mediumBuffersPool.Dispose();
	}
}
