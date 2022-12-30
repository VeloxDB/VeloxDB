using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

namespace VeloxDB.Common;

internal unsafe sealed class TextFileTraceCollector : ITraceCollector
{
	const int utf8BuffSize = 128 * 1024;
	const int maxUtf16Chars = utf8BuffSize / 2;

	readonly byte[] utf8Bom = new byte[] { 0xef, 0xbb, 0xbf };

	readonly object sync = new object();

	string fileName;
	long fileSizeLimit;

	TraceLevel level;

	Encoder encoder;
	IntPtr utf8Buffer;
	IntPtr utf16Buffer;

	NativeFile file;
	long writtenSize;

	public TextFileTraceCollector(string fileName, long fileSizeLimit = -1)
	{
		this.fileName = fileName;
		this.fileSizeLimit = fileSizeLimit;
		level = TraceLevel.Verbose;

		utf8Buffer = Marshal.AllocHGlobal(utf8BuffSize);
		utf16Buffer = Marshal.AllocHGlobal(maxUtf16Chars);
		encoder = Encoding.UTF8.GetEncoder();

		CreateFile();
	}

	public unsafe void AddTrace(TraceLevel level, StringBuilder text)
	{
		lock (sync)
		{
			if (level > this.level)
				return;

			int len = Math.Min(text.Length, maxUtf16Chars - Environment.NewLine.Length);
			for (int i = 0; i < len; i++)
			{
				((char*)utf16Buffer)[i] = text[i];
			}

			int size = encoder.GetBytes((char*)utf16Buffer, len, (byte*)utf8Buffer, utf8BuffSize, true);
			for (int i = 0; i < Environment.NewLine.Length; i++)
			{
				((byte*)utf8Buffer)[size++] = (byte)Environment.NewLine[i];
			}

			WriteToFile(utf8Buffer, size);
		}
	}

	public void SetTraceLevel(TraceLevel level)
	{
		lock (sync)
		{
			this.level = level;
		}
	}

	private void RecreateFile()
	{
		file.Dispose();

		string dirName = Path.GetDirectoryName(fileName);
		string name = Path.GetFileNameWithoutExtension(fileName);
		string ext = Path.GetExtension(fileName);
		File.Move(fileName, Path.Combine(dirName, name + string.Format(".{0:yyyyMMdd_HH_mm_ss_fff}" + ext, DateTime.Now)));
		CreateFile();
	}

	private unsafe void CreateFile()
	{
		file = NativeFile.Create(fileName, FileMode.Create, FileAccess.Write, FileShare.Read, FileFlags.None);

		fixed (byte* bp = utf8Bom)
		{
			file.Write((IntPtr)bp, utf8Bom.Length);
		}

		writtenSize = 0;
	}

	private void WriteToFile(IntPtr buffer, int size)
	{
		file.Write(buffer, size);
		writtenSize += size;

		if (fileSizeLimit != -1 && writtenSize >= fileSizeLimit)
			RecreateFile();
	}

	public void Dispose()
	{
		file.Dispose();
		Marshal.FreeHGlobal(utf8Buffer);
		Marshal.FreeHGlobal(utf16Buffer);
	}
}
