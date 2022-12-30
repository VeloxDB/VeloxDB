using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using VeloxDB.Common;
using static System.Math;

namespace VeloxDB.Storage.Persistence;

internal unsafe sealed class WriteThroughLogWriter : IDisposable
{
	const uint maxBytesPerWrite = (uint)1 << 31;

	NativeFile file;

	uint sectorSize;
	uint lastSectorWritten;

	byte* lastSector;

	bool isPackedFormat;

	bool secotSizeMismatch;

	public WriteThroughLogWriter(string fileName, long initPosition, uint sectorSize, bool isPackedFormat)
	{
		this.sectorSize = GetPhysicalSectorSize(fileName);
		if (sectorSize != this.sectorSize)
			secotSizeMismatch = true;

		if (!Utils.IsPowerOf2(this.sectorSize))
			throw new CriticalDatabaseException();

		lastSector = (byte*)AlignedAllocator.Allocate((long)sectorSize, (int)sectorSize);

		if (!isPackedFormat)
		{
			if (initPosition % sectorSize != 0)
				initPosition += sectorSize - (initPosition % sectorSize);
		}
		else
		{
			// Load current sector if partial
			lastSectorWritten = (uint)(initPosition % sectorSize);
			if (lastSectorWritten != 0)
			{
				initPosition -= lastSectorWritten;
				using (NativeFile t = NativeFile.Create(fileName, FileMode.Open, FileAccess.Read, FileShare.None, FileFlags.None))
				{
					t.Seek(initPosition);
					t.Read((IntPtr)lastSector, lastSectorWritten);
				}
			}
		}

		this.isPackedFormat = isPackedFormat;

		file = NativeFile.Create(fileName, FileMode.Open, FileAccess.Write | FileAccess.Read, FileShare.None, FileFlags.Unbuffered);
		file.Seek(initPosition);
	}

	public uint LastSectorWritten => lastSectorWritten;
	public uint SectorSize => sectorSize;
	public bool IsPackedFormat => isPackedFormat;
	public bool SecotSizeMismatch => secotSizeMismatch;

	public void Flush()
	{
		file.Flush();
	}

	public long GetFilePosAligned()
	{
		return file.Position;
	}

	public void SetFilePosAligned(long pos)
	{
		file.Seek(pos);
	}

	public void WriteSingleSector(IntPtr buffer)
	{
		file.Write(buffer, sectorSize);
	}

	public void Write(IntPtr buffer, long size)
	{
		if (lastSectorWritten > 0)
			Utils.CopyMemory((byte*)lastSector, (byte*)buffer, lastSectorWritten);

		long actualToWrite = lastSectorWritten + size;
		long toWrite = actualToWrite;

		lastSectorWritten = (uint)(toWrite & (sectorSize - 1));
		if (lastSectorWritten != 0)
			toWrite = toWrite - lastSectorWritten + sectorSize;

		long totalWritten = 0;
		while (totalWritten < toWrite)
		{
			uint currToWrite = (uint)Min(maxBytesPerWrite, toWrite - totalWritten);

			file.Write(new IntPtr((long)buffer + totalWritten), currToWrite);
			totalWritten += currToWrite;
		}

		if (isPackedFormat)
		{
			if (lastSectorWritten > 0)
			{
				Utils.CopyMemory((byte*)buffer + actualToWrite - lastSectorWritten, lastSector, lastSectorWritten);
				file.Seek(-(long)sectorSize, MoveMethod.Current);
			}
		}
		else
		{
			lastSectorWritten = 0;
		}
	}

	public static uint GetPhysicalSectorSize(string fileName)
	{
		return NativeFile.GetPhysicalSectorSize(fileName);
	}

	public void Dispose()
	{
		file.Flush();
		file.Dispose();
		AlignedAllocator.Free((IntPtr)lastSector);
	}
}
