using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using VeloxDB.Common;

namespace VeloxDB.Storage.Persistence;

internal unsafe sealed class LogFileWriter : IDisposable
{
	const long initCapacity = 1024 * 1024 * 2;
	const long maxBufferSize = 1024 * 1024 * 256;

	uint timestamp;

	string fileName;
	int logIndex;

	WriteThroughLogWriter writer;
	SimpleGuid marker;
	bool isPackedFormat;

	IntPtr buffer;
	uint additionalCapacity;
	long capacity = initCapacity;

	long maxWrittenCount;
	long writtenCount;

	public LogFileWriter(string fileName, int logIndex, uint timestamp,
		uint sectorSize, bool isPackedFormat, long limitSize) :
		this(fileName, logIndex, timestamp, GenerateNewMarker(), sectorSize, isPackedFormat, LogFileHeader.SkipSize, limitSize)
	{
	}

	public LogFileWriter(string fileName, int logIndex, uint timestamp, SimpleGuid marker,
		uint sectorSize, bool isPackedFormat, long initPosition, long limitSize)
	{
		writer = new WriteThroughLogWriter(fileName, initPosition, sectorSize, isPackedFormat);

		if (writer.SectorSize < LogFileHeader.Size)
			throw new NotSupportedException("Unsupported disk sector size. Sector size must be larger than the log file header size.");

		if (writer.SectorSize > LogFileHeader.SkipSize)
			throw new NotSupportedException("Unsupported disk sector size. Sector size must be smaller than 16K.");

		this.fileName = fileName;
		this.logIndex = logIndex;
		this.maxWrittenCount = limitSize;
		this.timestamp = timestamp;
		this.isPackedFormat = isPackedFormat;
		this.marker = marker;

		writtenCount = 0;

		additionalCapacity = LogBlockFileHeader.Size + SimpleGuid.Size + writer.SectorSize;
		buffer = AlignedAllocator.Allocate(capacity + additionalCapacity, (int)writer.SectorSize);
	}

	public bool SectorSizeMismatch => writer.SecotSizeMismatch;

	public static LogFileHeader CreateEmpty(string fileName, uint timestamp, bool hasSnapshot, long size, bool isPackedFormat)
	{
		string path = Path.GetDirectoryName(fileName);
		if (!Directory.Exists(path))
			Directory.CreateDirectory(path);

		if (File.Exists(fileName))
			File.Delete(fileName);

		using (NativeFile file = NativeFile.Create(fileName, FileMode.Create, FileAccess.Write, FileShare.None, FileFlags.Sequential))
		{
			ResizeFile(file, (long)(size * 1.3));

			LogFileHeader header = new LogFileHeader();
			header.version = DatabasePersister.FormatVersion;
			header.sectorSize = WriteThroughLogWriter.GetPhysicalSectorSize(fileName);
			header.isPackedFormat = isPackedFormat;
			header.timestamp = timestamp;
			header.hasSnapshot = hasSnapshot;
			header.marker = GenerateNewMarker();

			file.Write((IntPtr)(&header), LogFileHeader.Size);
			file.Flush();

			return header;
		}
	}

	public uint Timestamp => timestamp;
	public bool IsFull => writtenCount > maxWrittenCount;
	public bool IsPackedFormat => isPackedFormat;
	public uint SectorSize => writer.SectorSize;
	public uint AdditionalBuffSize => additionalCapacity;

	public void SetLimitSize(long limitSize)
	{
		this.maxWrittenCount = limitSize;
	}

	public void WriteItems(Transaction[] transactions, int count)
	{
		long totalByteSize = CalculateSize(transactions, count);

		if (capacity >= totalByteSize + additionalCapacity)
		{
			WriteItemsInternal(transactions, 0, count, totalByteSize);
			return;
		}
		else if (capacity < maxBufferSize)
		{
			capacity = Math.Min(maxBufferSize, Math.Max(capacity * 4, totalByteSize + additionalCapacity));
			AlignedAllocator.Free(buffer);
			buffer = AlignedAllocator.Allocate(capacity + additionalCapacity, (int)writer.SectorSize);
		}

		int i = 0;
		while (i < count)
		{
			totalByteSize = LogItem.CalculateSerializedSize(transactions[i], logIndex);
			int c = 1;

			long t;
			while (i + c < count && totalByteSize + (t = LogItem.CalculateSerializedSize(transactions[i + c], logIndex)) < capacity)
			{
				totalByteSize += t;
				c++;
			}

			WriteItemsInternal(transactions, i, c, totalByteSize);
			i += c;
		}
	}

	private void WriteItemsInternal(Transaction[] transactions, int offset, int count, long totalByteSize)
	{
		if (capacity < totalByteSize + additionalCapacity)
		{
			Checker.AssertTrue(count == 1);
			AlignedAllocator.Free(buffer);
			capacity = Math.Max(capacity * 2, totalByteSize + additionalCapacity);
			buffer = AlignedAllocator.Allocate(capacity, (int)writer.SectorSize);
		}

		LogBlockFileHeader* lp = (LogBlockFileHeader*)new IntPtr((long)buffer + writer.LastSectorWritten);
		long size = SerializeItems((byte*)lp, transactions, offset, count);
		Checker.AssertTrue(size == totalByteSize + LogBlockFileHeader.Size + SimpleGuid.Size);
		writer.Write(buffer, size);
		writtenCount += size;

		if (capacity > maxBufferSize)
		{
			AlignedAllocator.Free(buffer);
			capacity = maxBufferSize;
			buffer = AlignedAllocator.Allocate(capacity, (int)writer.SectorSize);
		}

		return;
	}

	public void MarkHasSnapshot()
	{
		OverwriteHeader(timestamp, true, marker);
	}

	public void Activate()
	{
		OverwriteHeader(timestamp, false, marker);
	}

	private long CalculateSize(Transaction[] transactions, int count)
	{
		long size = 0;
		for (int i = 0; i < count; i++)
		{
			size += LogItem.CalculateSerializedSize(transactions[i], logIndex);
		}

		return size;
	}

	private void OverwriteHeader(uint timestamp, bool hasSnapshot, SimpleGuid marker)
	{
		IntPtr p = AlignedAllocator.Allocate(writer.SectorSize, (int)writer.SectorSize);

		try
		{
			LogFileHeader header = new LogFileHeader()
			{
				version = DatabasePersister.FormatVersion,
				sectorSize = writer.SectorSize,
				isPackedFormat = isPackedFormat,
				timestamp = timestamp,
				marker = marker,
				hasSnapshot = hasSnapshot,
			};

			*(LogFileHeader*)p = header;

			long pos = writer.GetFilePosAligned();

			writer.SetFilePosAligned(0);
			writer.WriteSingleSector(p);
			writer.Flush();

			writer.SetFilePosAligned(pos);
		}
		finally
		{
			AlignedAllocator.Free(p);
		}
	}

	private static SimpleGuid GenerateNewMarker()
	{
		byte[] b = RandomNumberGenerator.GetBytes(16);
		fixed (byte* bp = b)
		{
			long* lp = (long*)bp;
			return new SimpleGuid(lp[0], lp[1]);
		}
	}

	private unsafe long SerializeItems(byte* dst, Transaction[] transactions, int offset, int count)
	{
		LogBlockFileHeader* phead = (LogBlockFileHeader*)dst;
		phead->version = DatabasePersister.FormatVersion;
		phead->marker = marker;
		phead->type = LogBlockType.Standard;

		dst += LogBlockFileHeader.Size;

		long totalSize = 0;
		for (int i = offset; i < offset + count; i++)
		{
			TransactionContext tc = transactions[i].Context;
			LogChangeset lc = tc.Changeset?.GetLogChangeset(logIndex);
			if (tc.Alignment != null || !LogItem.CanLogRunParallel(lc))
				phead->type = LogBlockType.NonParallel;

			long itemSize = LogItem.Serialize(dst, transactions[i], lc);
			totalSize += itemSize;
			dst += itemSize;
		}

		((long*)dst)[0] = marker.Low;
		((long*)dst)[1] = marker.Hight;
		totalSize += SimpleGuid.Size;

		phead->size = totalSize;

		return totalSize + LogBlockFileHeader.Size;
	}

	private static void ResizeFile(NativeFile file, long size)
	{
		file.Resize(size);
		file.Seek(0);
	}

	public void MoveFile(Func<string> moveFileAction)
	{
		uint sectorSize = writer.SectorSize;
		bool isPackedFormat = writer.IsPackedFormat;
		long position = writer.GetFilePosAligned();

		writer.Dispose();

		fileName = moveFileAction();

		writer = new WriteThroughLogWriter(fileName, position, sectorSize, isPackedFormat);
	}

	public void Dispose()
	{
		writer.Dispose();
		AlignedAllocator.Free(buffer);
	}
}

internal enum LogBlockType : int
{
	Standard = 0,
	NonParallel = 1
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = LogBlockFileHeader.Size)]
internal unsafe struct LogBlockFileHeader
{
	public const int Size = 32;

	public int version;
	public long size;
	public SimpleGuid marker;
	public LogBlockType type;
}


[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal unsafe struct LogFileHeader
{
	// Log header must fit inside single disk sector so that wi can write it atomicaly.
	public static readonly uint SkipSize = 1024 * 16;

	public static int Size = sizeof(LogFileHeader);

	public long version;
	public SimpleGuid marker;
	public uint sectorSize;
	public uint timestamp;
	public bool hasSnapshot;
	public bool isPackedFormat;
}
