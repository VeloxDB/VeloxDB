using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;

namespace Velox.Common;

internal enum MoveMethod : uint
{
	Begin = 0,
	Current = 1,
	End = 2
}

internal enum FileFlags
{
	None = 0x00,
	Unbuffered = 0x01,
	Sequential = 0x02,
}

internal class NativeException : Exception
{
	int errorCode;

	public NativeException(string message, int errorCode) : base(message)
	{
		this.errorCode = errorCode;
	}
}

internal sealed class SharingViolationException : Exception
{
	public SharingViolationException() : base("File could not be accessed because it is locked.")
	{
	}
}

internal abstract class NativeFile : IDisposable
{
	bool disposed;

	public NativeFile()
	{
	}

	~NativeFile()
	{
		Cleanup(false);
	}

	public static NativeFile Create(string fileName, FileMode mode, FileAccess access, FileShare share, FileFlags flags)
	{
		if (OperatingSystem.IsWindows())
			return new WindowsFile(fileName, mode, access, share, flags);

		if (OperatingSystem.IsLinux())
			return new LinuxFile(fileName, mode, access, share, flags);

		throw Utils.OSNotSupportedException();
	}

	public void Dispose()
	{
		Cleanup(true);
		GC.SuppressFinalize(this);
	}

	public abstract long Size { get; }
	public abstract long Position { get; }

	public abstract void Read(IntPtr buffer, long size);
	public abstract void Read(IntPtr buffer, long size, out long readSize);
	public abstract void Write(IntPtr buffer, long size);
	public abstract void Seek(long offset, MoveMethod moveMethod = MoveMethod.Begin);
	public abstract void Flush();
	public abstract void Resize(long size);

	private void Cleanup(bool isDisposing)
	{
		if (!disposed)
		{
			OnCleanup(isDisposing);
		}

		disposed = true;
	}

	protected abstract void OnCleanup(bool isDisposing);

	public static uint GetPhysicalSectorSize(string fileName)
	{
		if (OperatingSystem.IsWindows())
			return WindowsFile.GetPhysicalSectorSizeInternal(fileName);

		if (OperatingSystem.IsLinux())
			return LinuxFile.GetPhysicalSectorSizeInternal(fileName);

		throw Utils.OSNotSupportedException();
	}
}

internal sealed class WindowsFile : NativeFile
{
	[Flags]
	private enum EFileAccess : uint
	{
		GenericRead = 0x80000000,
		GenericWrite = 0x40000000,
		ReadControl = 0x00020000,
	}

	private enum ECreationDisposition : uint
	{
		CreateNew = 1,
		CreateAlways = 2,
		OpenExisting = 3,
		OpenAlways = 4,
		TruncateExisting = 5,
	}

	[Flags]
	private enum EFileShare : uint
	{
		None = 0x00000000,
		Read = 0x00000001,
		Write = 0x00000002,
		Delete = 0x00000004
	}

	[Flags]
	private enum EFileAttributes : uint
	{
		Readonly = 0x00000001,
		Hidden = 0x00000002,
		System = 0x00000004,
		Directory = 0x00000010,
		Archive = 0x00000020,
		Device = 0x00000040,
		Normal = 0x00000080,
		Temporary = 0x00000100,
		SparseFile = 0x00000200,
		ReparsePoint = 0x00000400,
		Compressed = 0x00000800,
		Offline = 0x00001000,
		NotContentIndexed = 0x00002000,
		Encrypted = 0x00004000,
		Write_Through = 0x80000000,
		Overlapped = 0x40000000,
		NoBuffering = 0x20000000,
		RandomAccess = 0x10000000,
		SequentialScan = 0x08000000,
		DeleteOnClose = 0x04000000,
		BackupSemantics = 0x02000000,
		PosixSemantics = 0x01000000,
		OpenReparsePoint = 0x00200000,
		OpenNoRecall = 0x00100000,
		FirstPipeInstance = 0x00080000
	}

	[DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
	[SuppressUnmanagedCodeSecurity]
	private static extern IntPtr CreateFile(string lpFileName, EFileAccess dwDesiredAccess,
		EFileShare dwShareMode, IntPtr lpSecurityAttributes, ECreationDisposition dwCreationDisposition,
		EFileAttributes dwFlagsAndAttributes, IntPtr hTemplateFile);

	[DllImport("kernel32.dll", SetLastError = true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern bool ReadFile(IntPtr hFile, IntPtr lpBuffer, uint nNumberOfBytesToRead,
		out uint lpNumberOfBytesRead, IntPtr lpOverlapped);

	[DllImport("kernel32.dll")]
	[SuppressUnmanagedCodeSecurity]
	private static extern bool WriteFile(IntPtr hFile, IntPtr lpBuffer, uint nNumberOfBytesToWrite,
		out uint lpNumberOfBytesWritten, IntPtr lpOverlapped);

	[DllImport("kernel32.dll")]
	[SuppressUnmanagedCodeSecurity]
	private static extern bool GetFileSizeEx(IntPtr hFile, out long lpFileSize);

	[DllImport("kernel32.dll")]
	[SuppressUnmanagedCodeSecurity]
	private static extern bool SetFilePointerEx(IntPtr hFile, long liDistanceToMove,
		IntPtr lpNewFilePointer, MoveMethod dwMoveMethod);

	[DllImport("kernel32.dll")]
	[SuppressUnmanagedCodeSecurity]
	private static extern bool FlushFileBuffers(IntPtr hFile);

	[DllImport("kernel32.dll", SetLastError = true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern bool SetEndOfFile(IntPtr hFile);

	[DllImport("kernel32.dll", ExactSpelling = true, SetLastError = true, CharSet = CharSet.Auto)]
	[SuppressUnmanagedCodeSecurity]
	private static extern bool DeviceIoControl(IntPtr hDevice, uint dwIoControlCode, IntPtr lpInBuffer,
		uint nInBufferSize, IntPtr lpOutBuffer, uint nOutBufferSize, out uint lpBytesReturned, IntPtr lpOverlapped);

	[DllImport("kernel32.dll", SetLastError = true)]
	[return: MarshalAs(UnmanagedType.Bool)]
	private static extern bool CloseHandle(IntPtr hObject);

	const int invalidHandleValue = -1;
	const int ERROR_FILE_NOT_FOUND = 2;
	const int ERROR_SHARING_VIOLATION = 32;
	const int ERROR_LOCK_VIOLATION = 33;

	const uint IOCTL_STORAGE_QUERY_PROPERTY = 0x002d1400;
	const int PropertyStandardQuery = 0;
	const int StorageAccessAlignmentProperty = 6;

	IntPtr hfile;

	public WindowsFile(string fileName, FileMode mode, FileAccess access, FileShare share, FileFlags flags)
	{
		hfile = CreateFile(fileName, Convert(access), Convert(share),
			IntPtr.Zero, Convert(mode), Convert(flags), IntPtr.Zero);

		if (hfile.ToInt64() == invalidHandleValue)
			throw CreateException();
	}

	public override long Size
	{
		get
		{
			if (!GetFileSizeEx(hfile, out long fileSize))
				throw CreateException();

			return fileSize;
		}
	}

	public unsafe override long Position
	{
		get
		{
			long pos;
			if (!SetFilePointerEx(hfile, 0, (IntPtr)(&pos), MoveMethod.Current))
				throw new Win32Exception(Marshal.GetLastWin32Error());

			return pos;
		}
	}

	public override void Read(IntPtr buffer, long size)
	{
		while (size > 0)
		{
			uint toRead = (uint)Math.Min(size, uint.MaxValue);
			if (!ReadFile(hfile, buffer, toRead, out uint readBytes, IntPtr.Zero) || readBytes != toRead)
				throw CreateException();

			size -= toRead;
		}
	}

	public override void Read(IntPtr buffer, long size, out long readSize)
	{
		readSize = 0;
		while (size > 0)
		{
			uint toRead = (uint)Math.Min(size, uint.MaxValue);
			if (!ReadFile(hfile, buffer, toRead, out uint readBytes, IntPtr.Zero))
				throw CreateException();

			readSize += readBytes;

			if (readBytes < toRead)
				return;

			size -= readBytes;
		}
	}

	public override void Write(IntPtr buffer, long size)
	{
		while (size > 0)
		{
			uint toWrite = (uint)Math.Min(size, uint.MaxValue);
			if (!WriteFile(hfile, buffer, toWrite, out uint writtenBytes, IntPtr.Zero) || writtenBytes != toWrite)
				throw CreateException();

			size -= toWrite;
		}
	}

	public override void Seek(long offset, MoveMethod moveMethod = MoveMethod.Begin)
	{
		if (!SetFilePointerEx(hfile, offset, IntPtr.Zero, moveMethod))
			throw CreateException();
	}

	public override void Flush()
	{
		if (!FlushFileBuffers(hfile))
			throw CreateException();
	}

	public override void Resize(long size)
	{
		Seek(size);
		if (!SetEndOfFile(hfile))
			throw CreateException();
	}

	protected override void OnCleanup(bool isDisposing)
	{
		CloseHandle(hfile);
	}

	private Exception CreateException()
	{
		int err = Marshal.GetLastWin32Error();
		if (err == ERROR_FILE_NOT_FOUND)
			return new FileNotFoundException();

		if (err == ERROR_LOCK_VIOLATION || err == ERROR_SHARING_VIOLATION)
			return new SharingViolationException();

		Win32Exception we = new Win32Exception(Marshal.GetLastWin32Error());
		return new NativeException(we.Message, we.ErrorCode);
	}

	private ECreationDisposition Convert(FileMode mode)
	{
		switch (mode)
		{
			case FileMode.Append:
				throw new NotSupportedException();

			case FileMode.Create:
				return ECreationDisposition.CreateAlways;

			case FileMode.CreateNew:
				return ECreationDisposition.CreateNew;

			case FileMode.Open:
				return ECreationDisposition.OpenExisting;

			case FileMode.OpenOrCreate:
				return ECreationDisposition.OpenAlways;

			case FileMode.Truncate:
				return ECreationDisposition.TruncateExisting;

			default:
				throw new ArgumentException();
		}
	}

	private EFileAccess Convert(FileAccess mode)
	{
		switch (mode)
		{
			case FileAccess.Read:
				return EFileAccess.GenericRead;

			case FileAccess.ReadWrite:
				return EFileAccess.GenericWrite | EFileAccess.GenericRead;

			case FileAccess.Write:
				return EFileAccess.GenericWrite;

			default:
				throw new ArgumentException();
		}
	}

	private EFileShare Convert(FileShare share)
	{
		switch (share)
		{
			case FileShare.Delete:
				return EFileShare.Delete;

			case FileShare.Inheritable:
				throw new NotSupportedException();

			case FileShare.None:
				return EFileShare.None;

			case FileShare.Read:
				return EFileShare.Read;

			case FileShare.ReadWrite:
				return EFileShare.Read| EFileShare.Write;

			case FileShare.Write:
				return EFileShare.Write;

			default:
				throw new ArgumentException();
		}
	}

	private EFileAttributes Convert(FileFlags flags)
	{
		if (flags == FileFlags.None)
			return EFileAttributes.Normal;

		EFileAttributes fa = (EFileAttributes)0;
		if ((flags & FileFlags.Unbuffered) != 0)
			fa |= EFileAttributes.NoBuffering;

		if ((flags & FileFlags.Sequential) != 0)
			fa |= EFileAttributes.SequentialScan;

		return fa;
	}

	private static string GetDriveName(string fileName)
	{
		DirectoryInfo di = Directory.GetParent(fileName).Root;
		return @"\\.\" + di.Name.Substring(0, di.Name.IndexOf(":") + 1);
	}

	public unsafe static uint GetPhysicalSectorSizeInternal(string fileName)
	{
		STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR ad;
		uint bytes = 0;
		STORAGE_PROPERTY_QUERY query = new STORAGE_PROPERTY_QUERY();

		IntPtr hFile = CreateFile(GetDriveName(fileName), EFileAccess.ReadControl, EFileShare.Read | EFileShare.Write,
			IntPtr.Zero, ECreationDisposition.OpenExisting, EFileAttributes.Normal, IntPtr.Zero);

		if (hFile == new IntPtr(-1))
			throw new Win32Exception(Marshal.GetLastWin32Error());

		query.QueryType = PropertyStandardQuery;
		query.PropertyId = StorageAccessAlignmentProperty;

		if (!DeviceIoControl(hFile, IOCTL_STORAGE_QUERY_PROPERTY, new IntPtr(&query),
			(uint)sizeof(STORAGE_PROPERTY_QUERY), new IntPtr(&ad),
			(uint)sizeof(STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR), out bytes, IntPtr.Zero))
		{
			return 1024 * 4;    // Some RAM Disk apps do not provide device information so we use the most common sector size
		}

		CloseHandle(hFile);

		return ad.BytesPerPhysicalSector;
	}

	[StructLayout(LayoutKind.Sequential, Pack = 1)]
	public struct STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR
	{
		public uint Version;
		public uint Size;
		public uint BytesPerCacheLine;
		public uint BytesOffsetForCacheAlignment;
		public uint BytesPerLogicalSector;
		public uint BytesPerPhysicalSector;
		public uint BytesOffsetForSectorAlignment;
	}

	[StructLayout(LayoutKind.Sequential, Pack = 1)]
	public struct STORAGE_PROPERTY_QUERY
	{
		public int PropertyId;
		public int QueryType;
		public int AdditionalParameters;
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal struct stat_x86_64
{
	public ulong st_dev;
	public long st_ino;
	public long st_nlink;
	public int st_mode;
	public int st_uid;
	public int st_gid;
	private int __pad0;
	public long st_rdev;
	public long st_size;
	public long st_blksize;
	public long st_blocks;
	public long st_atim_low;
	public long st_atim_high;
	public long st_mtim_low;
	public long st_mtim_high;
	public long st_ctim_low;
	public long st_ctim_high;

	private long __glibc_reserved1;
	private long __glibc_reserved2;
	private long __glibc_reserved3;
}

internal sealed class LinuxFile : NativeFile
{
	int hFile;

	private const string libc = "libc";

	private const int error = -1;

	//Open file mode
	private const int S_IRUSR = 256;
	private const int S_IWUSR = 128;

	//Open flags
	private const int O_RDONLY = 0;
	private const int O_WRONLY = 1;
	private const int O_RDWR = 2;
	private const int O_CREAT = 64;
	private const int O_EXCL = 128;
	private const int O_TRUNC = 512;
	private const int O_APPEND = 1024;
	private static readonly int O_DIRECT = 16384;

	//Seek flags
	private const int SEEK_SET = 0;
	private const int SEEK_CUR = 1;
	private const int SEEK_END = 2;

	//flock flags
	private const int LOCK_SH = 1;
	private const int LOCK_EX = 2;
	private const int LOCK_NB = 4;

	//FAdvise flags
	private const int POSIX_FADV_SEQUENTIAL = 2;

	//Errors
	private const int ENOENT = 2; // No such file or directory

	private static readonly int _STAT_VER_KERNEL;

	[DllImport(libc, SetLastError=true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern int open(string pathname, int flags, int mode);

	[DllImport(libc, SetLastError=true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern int __fxstat(int ver, int fd, ref stat_x86_64 statbuf);

	[DllImport(libc, SetLastError=true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern int __xstat(int ver, string fileName, ref stat_x86_64 statbuf);


	[DllImport(libc, SetLastError=true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern long lseek64(int fd, long offset, int whence);

	[DllImport(libc, SetLastError=true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern int fsync(int fd);

	[DllImport(libc, SetLastError=true, EntryPoint="read")]
	[SuppressUnmanagedCodeSecurity]
	private static extern long read64(int fd, IntPtr buf, ulong count);

	[DllImport(libc, SetLastError=true, EntryPoint="read")]
	[SuppressUnmanagedCodeSecurity]
	private static extern int read32(int fd, IntPtr buf, uint count);

	[DllImport(libc, SetLastError=true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern int ftruncate64(int fd, long length);

	[DllImport(libc, SetLastError=true, EntryPoint="write")]
	[SuppressUnmanagedCodeSecurity]
	private static extern long write64(int fd, IntPtr buf, ulong count);

	[DllImport(libc, SetLastError=true, EntryPoint="write")]
	[SuppressUnmanagedCodeSecurity]
	private static extern int write32(int fd, IntPtr buf, uint count);

	[DllImport(libc, SetLastError=true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern int close(int fd);

	[DllImport(libc, SetLastError=true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern int posix_fadvise(int fd, long offset, long len, int advice);

	[DllImport(libc, SetLastError=true)]
	[SuppressUnmanagedCodeSecurity]
	private static extern int flock(int fd, int operation);

	[DllImport(libc, SetLastError=true, EntryPoint="strerror")]
	[SuppressUnmanagedCodeSecurity]
	private static extern IntPtr _strerror(int errnum);

	private static string strerror(int errnum)
	{
		return Marshal.PtrToStringUTF8(_strerror(errnum));
	}

	private delegate long RWDelegate(int fd, IntPtr buf, ulong count);

	static readonly RWDelegate read;
	static readonly RWDelegate write;

	static LinuxFile()
	{
		_STAT_VER_KERNEL = 0;

		if (IntPtr.Size == 8)
		{
			read = read64;
			write = write64;
		}
		else if (IntPtr.Size == 4)
		{
			read = Read64To32Wrapper;
			write = Write64To32Wrapper;
		}

		if (RuntimeInformation.ProcessArchitecture == Architecture.Arm64 ||
			RuntimeInformation.ProcessArchitecture == Architecture.Arm)
		{
			O_DIRECT = 65536;
		}
	}

	public LinuxFile(string fileName, FileMode mode, FileAccess access, FileShare share, FileFlags flags)
	{
		int osFlags = FlagsFromMode(mode) | FlagsFromAccess(access) | FlagsFromFlags(flags);
		hFile = open(fileName, osFlags, S_IRUSR | S_IWUSR);

		if (hFile == error)
			throw CreateException();

		if (flags.HasFlag(FileFlags.Sequential))
		{
			posix_fadvise(hFile, 0, 0, POSIX_FADV_SEQUENTIAL);
		}

		SetShare(hFile, share);
	}

	private void SetShare(int hFile, FileShare share)
	{
		if (share == FileShare.ReadWrite)
			return;

		int lck = 0;

		switch(share)
		{
			case FileShare.None:
				lck = LOCK_EX | LOCK_NB;
				break;
			case FileShare.Read:
			case FileShare.ReadWrite:
			case FileShare.Write:
				lck = LOCK_SH | LOCK_NB;
				break;
			default:
				throw new ArgumentException();
		}

		int result = flock(hFile, lck);
		if (result == error)
			throw new SharingViolationException();
	}

	private int FlagsFromFlags(FileFlags flags)
	{
		int result = 0;

		if (flags.HasFlag(FileFlags.Unbuffered))
			result |= O_DIRECT;

		return result;
	}

	private static long Read64To32Wrapper(int fd, IntPtr buf, ulong count)
	{
		return read32(fd, buf, (uint)count);
	}

	private static long Write64To32Wrapper(int fd, IntPtr buf, ulong count)
	{
		return write32(fd, buf, (uint)count);
	}

	private static Exception CreateException()
	{
		int err = Marshal.GetLastWin32Error();
		if (err == ENOENT)
			return new FileNotFoundException();
		return new NativeException($"IOError({strerror(err)})", err);
	}

	private int FlagsFromMode(FileMode mode)
	{
		switch(mode)
		{
			case FileMode.Append:
				return O_APPEND;

			case FileMode.Create:
				return O_CREAT | O_TRUNC;

			case FileMode.CreateNew:
				return O_CREAT | O_EXCL;

			case FileMode.Open:
				return 0;

			case FileMode.OpenOrCreate:
				return O_CREAT;

			case FileMode.Truncate:
				return O_TRUNC;

			default:
				throw new ArgumentException();
		}

	}

	private int FlagsFromAccess(FileAccess access)
	{
		switch(access)
		{
			case FileAccess.Read:
				return O_RDONLY;
			case FileAccess.Write:
				return O_WRONLY;
			case FileAccess.ReadWrite:
				return O_RDWR;
			default:
				throw new ArgumentException();
		}
	}

	public override long Size
	{
		get
		{
			stat_x86_64 statbuf = new stat_x86_64();
			int result = __fxstat(_STAT_VER_KERNEL, hFile, ref statbuf);

			if (result != 0)
				throw CreateException();

			return statbuf.st_size;
		}
	}

	public override long Position
	{
		get
		{
			long result = lseek64(hFile, 0, SEEK_CUR);
			if (result == error)
				throw CreateException();

			return result;
		}
	}

	public override void Flush()
	{
		int result = fsync(hFile);
		if (result == error)
			throw CreateException();
	}

	public override void Read(IntPtr buffer, long size)
	{
		Read(buffer, size, out _);
	}

	public override void Read(IntPtr buffer, long size, out long readSize)
	{
		long result = read(hFile, buffer, (ulong)size);

		if (result == error)
			throw CreateException();

		readSize = result;
	}

	public override void Resize(long size)
	{
		int result = ftruncate64(hFile, size);
		if (result == error)
			throw CreateException();
	}

	public override void Seek(long offset, MoveMethod moveMethod = MoveMethod.Begin)
	{
		lseek64(hFile, offset, MoveMethodToWhence(moveMethod));
	}

	private int MoveMethodToWhence(MoveMethod moveMethod)
	{
		switch(moveMethod)
		{
			case MoveMethod.Begin:
				return SEEK_SET;
			case MoveMethod.Current:
				return SEEK_CUR;
			case MoveMethod.End:
				return SEEK_END;
			default:
				throw new ArgumentException();
		}
	}

	public static uint GetPhysicalSectorSizeInternal(string fileName)
	{
		stat_x86_64 stat = new stat_x86_64();
		int result = __xstat(_STAT_VER_KERNEL, fileName, ref stat);
		if (result == error)
			throw CreateException();

		uint major, minor;
		ExtractMajorMinor(stat.st_dev, out major, out minor);

		return FindSectorSize(major, minor);
	}

	private static uint FindSectorSize(uint major, uint minor)
	{
		foreach (string dir in Directory.GetDirectories("/sys/block", "*"))
		{
			string device = Path.GetFileName(dir);

			IEnumerable<string> paths = Directory.GetDirectories(dir, device + "*").Prepend(dir);

			foreach(string path in paths)
			{
				if (IsDevice(path, major, minor))
				{
					string sectorSizePath = Path.Combine(dir, "queue/physical_block_size");
					if (!File.Exists(sectorSizePath))
						continue;

					return uint.Parse(File.ReadAllText(sectorSizePath));
				}
			}
		}

		return 4096;
	}

	private static bool IsDevice(string dir, uint major, uint minor)
	{
		string dev = Path.Combine(dir, "dev");
		if (!File.Exists(dev))
			return false;

		string idText = File.ReadAllText(dev);
		string[] split = idText.Split(":", 2);
		return int.Parse(split[0]) == major && int.Parse(split[1]) == minor;
	}

	private static void ExtractMajorMinor(ulong st_dev, out uint major, out uint minor)
	{
		major = (uint)((st_dev & 0xFFF00) >> 8);
		minor = (uint)(((st_dev & 0xFFF00000) >> 12) | (st_dev & 0xFF));
	}

	public override void Write(IntPtr buffer, long size)
	{
		long result = write(hFile, buffer, (ulong)size);

		if (result == error)
			throw CreateException();
	}

	protected override void OnCleanup(bool isDisposing)
	{
		int result = close(hFile);
	}
}

