using System;

namespace VeloxDB.Server;

public sealed class AssemblyUpdate
{
    private static readonly HashSet<string> dllIgnore = new HashSet<string>() { "vlxdb.dll", "vlxc.dll" };


    public List<UserAssembly> Inserted { get; set; }
    public List<UserAssembly> Updated { get; set; }
    public List<long> Deleted  { get; set; }

    public AssemblyUpdate()
    {
        Inserted = null!;
        Updated = null!;
        Deleted = null!;
    }

	public AssemblyUpdate(List<UserAssembly> inserted, List<UserAssembly> updated, List<long> deleted)
	{
		this.Inserted = inserted;
		this.Updated = updated;
		this.Deleted = deleted;
	}

    public static AssemblyUpdate CreateUpdate(UserAssembliesState state, string updateAsmDir, List<string> errors)
    {
        string[] files = Directory.GetFiles(updateAsmDir);

        Dictionary<string, UserAssembly> current = state.Assemblies.ToDictionary(a => a.Name.ToLower());

        List<UserAssembly> inserted = new List<UserAssembly>();
        List<UserAssembly> updated = new List<UserAssembly>();
        List<long> deleted = new List<long>();

        HashSet<string> processed = new HashSet<string>();

        foreach (string path in files)
        {
            string filename = Path.GetFileName(path);
            string fnLower = filename.ToLower();
            if (!IsDLL(fnLower) || dllIgnore.Contains(fnLower))
                continue;

            if (processed.Contains(fnLower))
            {
                errors.Add($"Encountered duplicate filename {filename}.");
                continue;
            }

            UserAssembly? existing;

            UserAssembly ua = ReadFromFile(path);

            if (!current.TryGetValue(fnLower, out existing))
            {
                inserted.Add(ua);
            }
            else if (Changed(ua, existing))
            {
                ua.Id = existing.Id;
                updated.Add(ua);
            }

            processed.Add(fnLower);
        }

        foreach (string name in processed)
        {
            if (current.ContainsKey(name))
                current.Remove(name);
        }

        foreach (UserAssembly deletedAsm in current.Values)
        {
            deleted.Add(deletedAsm.Id);
        }

        return new AssemblyUpdate(inserted, updated, deleted);
    }

    private static bool Changed(UserAssembly ua, UserAssembly existing)
    {
        if (ua.Binary == null || existing.Binary == null)
            throw new ArgumentNullException();

        byte[] first = ua.Binary;
        byte[] second = existing.Binary;

        if (first.Length != second.Length)
            return true;

        for (int i = 0; i < first.Length; i++)
            if (first[i] != second[i])
                return true;

        return false;
    }

    private static UserAssembly ReadFromFile(string path)
    {
        byte[] binary = File.ReadAllBytes(path);
        return new UserAssembly(0, Path.GetFileName(path), null, binary);
    }

    private static bool IsDLL(string fileName)
    {
        return Path.GetExtension(fileName) == ".dll";
    }
}

public sealed class UserAssembliesState
{
    public List<UserAssembly> Assemblies { get; set; }
    public Guid AssemblyVersionGuid { get; set; }

    public UserAssembliesState()
    {
        Assemblies = null!;
    }
    public UserAssembliesState(List<UserAssembly> assemblies, Guid assemblyVersionGuid)
    {
        this.Assemblies = assemblies;
        this.AssemblyVersionGuid = assemblyVersionGuid;
    }
}

public sealed class UserAssembly
{
    public long Id {get; set; }
    public string Name { get; set; }
    public byte[]? Sha1Hash { get; set; }
    public byte[]? Binary { get; set; }

    public UserAssembly()
    {
        Name = null!;
    }

	public UserAssembly(long id, string name, byte[]? sha1Hash, byte[]? binary)
	{
        this.Id = id;
		this.Name = name;
		this.Sha1Hash = sha1Hash;
        this.Binary = binary;
	}
}
