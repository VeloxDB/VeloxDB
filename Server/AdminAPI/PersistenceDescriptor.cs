using System;

namespace Velox.Server;

public class PersistenceDescriptor
{
	public List<LogDescriptor> LogDescriptors {get; set;}

    public PersistenceDescriptor()
    {
        LogDescriptors = null!;
    }

    public PersistenceDescriptor(List<LogDescriptor> logDescriptors)
    {
		this.LogDescriptors = logDescriptors;
    }
}

public sealed class LogDescriptor
{
    public string Name {get; set;}
    public bool IsPackedFormat {get; set;}
    public string Directory {get; set;}
    public string SnapshotDirectory {get; set;}
    public long MaxSize {get; set;}

    public LogDescriptor()
    {
        Name = null!;
        Directory = null!;
        SnapshotDirectory = null!;
    }
    public LogDescriptor(string name, bool isPackedFormat, string directory, string snapshotDirectory, long maxSize)
    {
        this.Name = name;
        this.IsPackedFormat = isPackedFormat;
        this.Directory = directory;
        this.SnapshotDirectory = snapshotDirectory;
        this.MaxSize = maxSize;
    }
}
