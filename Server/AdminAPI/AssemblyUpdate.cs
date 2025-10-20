using System;
using System.Reflection;
using System.Runtime.Loader;

namespace VeloxDB.Server;

public sealed class AssemblyUpdate
{
    private static readonly HashSet<string> dllIgnore = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase) { "vlxdb.dll", "vlxc.dll" };

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

        Dictionary<string, UserAssembly> current = state.Assemblies.ToDictionary(a => a.Name.ToLower(), StringComparer.InvariantCultureIgnoreCase);

        List<UserAssembly> inserted = [];
        List<UserAssembly> updated = [];
        List<long> deleted = [];

        HashSet<string> processed = new(StringComparer.InvariantCultureIgnoreCase);
        HashSet<string> toLoad = new(StringComparer.InvariantCultureIgnoreCase);

        foreach (string path in files)
        {
            string filename = Path.GetFileName(path);
            if (!IsDLL(filename) || dllIgnore.Contains(filename))
                continue;

            toLoad.Add(path);
            GetAdditionalAssemblies(updateAsmDir, path, toLoad, errors);
        }

        foreach(string path in toLoad)
        {
            string filename = Path.GetFileName(path);

            if (processed.Contains(filename))
            {
                errors.Add($"Encountered duplicate filename {filename}.");
                continue;
            }

            UserAssembly? existing;
            UserAssembly ua = ReadFromFile(path);

            if (!current.TryGetValue(filename, out existing))
            {
                inserted.Add(ua);
            }
            else if (Changed(ua, existing))
            {
                ua.Id = existing.Id;
                updated.Add(ua);
            }

            processed.Add(filename);

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

    private static void GetAdditionalAssemblies(string updateAsmDir, string assemblyPath, HashSet<string> additionalAssemblies, List<string> errors)
    {
        AssemblyLoadContext alc = new AssemblyLoadContext("tempContext", true);
		AssemblyDependencyResolver resolver = new(assemblyPath);
        try
        {
            Assembly mainAssembly = alc.LoadFromAssemblyPath(assemblyPath);
            AssemblyName[] referencedAssemblies = mainAssembly.GetReferencedAssemblies();

            foreach (var assemblyName in referencedAssemblies)
            {
                string? resolvedPath = resolver.ResolveAssemblyToPath(assemblyName);

                if (string.IsNullOrEmpty(resolvedPath) ||
                    !File.Exists(resolvedPath) ||
                    (Path.GetDirectoryName(resolvedPath) == updateAsmDir) ||
                    dllIgnore.Contains(Path.GetFileName(resolvedPath)))
                {
                    continue;
                }
                additionalAssemblies.Add(resolvedPath);
            }
        }
        catch (FileNotFoundException ex)
        {
            errors.Add($"Error resolving dependencies: {ex.Message}. Ensure the assembly and its .deps.json file are correctly placed.");
        }
        catch (Exception ex)
        {
            errors.Add($"An unexpected error occurred during dependency resolution: {ex.Message}");
        }
        finally
        {
            alc.Unload();
        }
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
        return string.Equals(Path.GetExtension(fileName), ".dll", StringComparison.InvariantCultureIgnoreCase);
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
