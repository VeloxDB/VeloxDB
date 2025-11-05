#ifdef _LINUX
#define _GNU_SOURCE
#endif

#include <stdint.h>
#include <stddef.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/file.h>
#include <fcntl.h>
#include <sys/stat.h>

#ifdef _LINUX
#include <sys/sysmacros.h>
#endif

#ifdef _MACOS
#include <stdio.h>
#include <sys/mount.h>
#include <IOKit/IOKitLib.h>
#include <IOKit/storage/IOMedia.h>
#include <CoreFoundation/CoreFoundation.h>
#include <IOKit/storage/IOStorageDeviceCharacteristics.h>
#endif

#if defined(__GNUC__) || defined(__clang__)
    #define EXPORT __attribute__((visibility("default")))
#else
    #define EXPORT
#endif

/* Minimal stat placeholder – only the fields we expose */
struct stat_x86_64 {
    int64_t st_size;
};

/* Enum equivalents for the C# side */
typedef enum {
    FileMode_CreateNew = 1,
    FileMode_Create = 2,
    FileMode_Open = 3,
    FileMode_OpenOrCreate = 4,
    FileMode_Truncate = 5,
    FileMode_Append = 6
} FileMode;

typedef enum {
    FileAccess_Read = 1,
    FileAccess_Write = 2,
    FileAccess_ReadWrite = 3
} FileAccess;

typedef enum {
    FileShare_None = 0,
    FileShare_Read = 1,
    FileShare_Write = 2,
    FileShare_ReadWrite = 3,
    FileShare_Delete = 4,
    FileShare_Inheritable = 16
} FileShare;

typedef enum {
    FileFlags_None       = 0,
    FileFlags_Unbuffered = 1,
    FileFlags_Sequential = 2
} FileFlags;

typedef enum {
    MoveMethod_Begin = 0,
    MoveMethod_Current = 1,
    MoveMethod_End = 2
} MoveMethod;

typedef enum {
    NativeFileError_None = 0,
    NativeFileError_CheckErrorCode = 1,
    NativeFileError_SharingViolation = 2,
    NativeFileError_UnknownEnumValue = 3,
	NativeFileError_NotImplemented = 4,
    NativeFileError_Unknown = 99
} NativeFileError;

typedef struct {
    uint32_t major;
    uint32_t minor;
} __attribute__((packed)) DeviceInfo;

int FlagsFromMode(FileMode mode)
{
    switch(mode)
    {
        case FileMode_CreateNew:
            return O_CREAT | O_EXCL;

        case FileMode_Create:
            return O_CREAT | O_TRUNC;

        case FileMode_Open:
            return 0;

        case FileMode_OpenOrCreate:
            return O_CREAT;

        case FileMode_Truncate:
            return O_TRUNC;

        case FileMode_Append:
            return O_APPEND;

        default:
            return -1;
    }
}

int FlagsFromAccess(FileAccess access)
{
    switch(access)
    {
        case FileAccess_Read:
            return O_RDONLY;

        case FileAccess_Write:
            return O_WRONLY;

        case FileAccess_ReadWrite:
            return O_RDWR;

        default:
            return -1;
    }
}

int FlagsFromFlags(FileFlags flags)
{
    int result = 0;
#if defined(_LINUX)
    if (flags & FileFlags_Unbuffered)
        result |= O_DIRECT;

#elif defined(_MACOS)
    (void)flags;
#endif

    return result;
}

void SetShare(int hFile, FileShare share, int32_t* errorCode)
{
    if (share == FileShare_ReadWrite)
        return;

    int lck = 0;

    switch(share)
    {
        case FileShare_None:
            lck = LOCK_EX | LOCK_NB;
            break;
        case FileShare_Read:
        case FileShare_ReadWrite:
        case FileShare_Write:
            lck = LOCK_SH | LOCK_NB;
            break;
        default:
            *errorCode = NativeFileError_UnknownEnumValue;
            return;
    }

    int result = flock(hFile, lck);
    if (result == -1)
    {
        *errorCode = NativeFileError_SharingViolation;
        return;
    }
}

EXPORT int NativeFile_Create(
    const char*  fileName,
    int32_t     mode,
    int32_t   access,
    int32_t    share,
    int32_t    flags,
    int32_t* errorCode)
{
    *errorCode = NativeFileError_None;

    int osFlags;
    osFlags = FlagsFromMode(mode);
    if(osFlags == -1)
    {
        *errorCode = NativeFileError_UnknownEnumValue;
        return -1;
    }

    int tempFlags = FlagsFromAccess(access);
    if(tempFlags == -1)
    {
        *errorCode = NativeFileError_UnknownEnumValue;
        return -1;
    }

    osFlags |= tempFlags;
    osFlags |= FlagsFromFlags(flags);

    int hFile = open(fileName, osFlags, S_IRUSR | S_IWUSR);

    if(hFile == -1)
    {
        *errorCode = NativeFileError_CheckErrorCode;
        return -1;
    }

#if defined(_LINUX)    
    if (flags & FileFlags_Sequential)
    {
        posix_fadvise(hFile, 0, 0, POSIX_FADV_SEQUENTIAL);
    }
#endif

#if defined(_MACOS)
    if (flags & FileFlags_Unbuffered)
    {
        fcntl(hFile, F_NOCACHE, 1);
    }    
#endif

    SetShare(hFile, share, errorCode);

    return hFile;
}

EXPORT int64_t NativeFile_GetSize(int hFile)
{
    struct stat statbuf;
    int result = fstat(hFile, &statbuf);
    if (result == -1)
        return -1;

    return statbuf.st_size;
}

EXPORT int64_t NativeFile_GetPosition(int hFile)
{
    int64_t result = lseek(hFile, 0, SEEK_CUR);
    return result;
}

EXPORT int32_t NativeFile_Flush(int hFile)
{
    return fsync(hFile);
}

EXPORT int64_t NativeFile_Read(int hFile, void* buffer, int64_t size)
{
    return read(hFile, buffer, size);
}

EXPORT int64_t NativeFile_Resize(int hFile, int64_t size)
{
    return ftruncate(hFile, size);
}

EXPORT int64_t NativeFile_Seek(int hFile, int64_t offset, MoveMethod moveMethod)
{
    return lseek(hFile, offset,
        moveMethod == MoveMethod_Begin ? SEEK_SET :
        moveMethod == MoveMethod_Current ? SEEK_CUR :
        SEEK_END);
}

EXPORT int64_t NativeFile_Write(int hFile, const void* buffer, int64_t size)
{
    return write(hFile, buffer, size);
}

EXPORT DeviceInfo NativeFile_GetDeviceInfo(const char* fileName, NativeFileError* errorCode)
{
#ifdef _LINUX
	*errorCode = NativeFileError_None;
    struct stat statbuf;
    DeviceInfo result = {0, 0};

    int statResult = stat(fileName, &statbuf);
    if (statResult == -1)
	{
		*errorCode = NativeFileError_CheckErrorCode;
        return result;
	}

    result.major = major(statbuf.st_dev);
    result.minor = minor(statbuf.st_dev);
    return result;
#else
    (void)fileName;
    DeviceInfo result = {0, 0};
	*errorCode = NativeFileError_NotImplemented;
	return result;
#endif
}

EXPORT int32_t NativeFile_GetDeviceSectorSize(const char* fileName, NativeFileError* errorCode)
{
#ifdef _LINUX
    (void)fileName;
    *errorCode = NativeFileError_NotImplemented;
    return -1;
#endif

#ifdef _MACOS
    *errorCode = NativeFileError_None;
    struct statfs sfs;
    if (statfs(fileName, &sfs) != 0) {
        *errorCode = NativeFileError_CheckErrorCode;
        return -1;
    }

    char devicePath[512];
    strncpy(devicePath, sfs.f_mntfromname, sizeof(devicePath));
    devicePath[sizeof(devicePath) - 1] = '\0';

    const char* dev_name = strrchr(devicePath, '/');
    if (!dev_name) dev_name = devicePath;
    else dev_name++;

    CFMutableDictionaryRef matchingDict = IOBSDNameMatching(kIOMainPortDefault, 0, dev_name);
    if (!matchingDict) {
        *errorCode = NativeFileError_Unknown;
        return -1;
    }

    io_iterator_t iter;
    kern_return_t kr = IOServiceGetMatchingServices(kIOMainPortDefault, matchingDict, &iter);
    if (kr != KERN_SUCCESS) {
        *errorCode = NativeFileError_Unknown;
        return -1;
    }

    io_object_t service = IOIteratorNext(iter);
    IOObjectRelease(iter);
    if (!service) {
        *errorCode = NativeFileError_Unknown;
        return -1;
    }

    CFTypeRef blockSizeRef = IORegistryEntryCreateCFProperty(service, CFSTR(kIOPropertyPhysicalBlockSizeKey), kCFAllocatorDefault, 0);

    IOObjectRelease(service);

    if (!blockSizeRef) {
        *errorCode = NativeFileError_Unknown;
        return -1;
    }

    int blockSize = 0;
    if (CFGetTypeID(blockSizeRef) == CFNumberGetTypeID()) {
        CFNumberGetValue((CFNumberRef)blockSizeRef, kCFNumberIntType, &blockSize);
    }
    CFRelease(blockSizeRef);
    return blockSize;
#endif
}

/* Cleanup */
EXPORT int NativeFile_Close(int hFile)
{
    return close(hFile);
}