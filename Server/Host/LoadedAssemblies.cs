using System.Reflection;
using System.Runtime.Loader;
using Engine = Velox.Storage.ModelUpdate;

namespace Velox.Server;

internal sealed class LoadedAssemblies : AssemblyLoadContext
{
	Dictionary<string, Engine.UserAssembly> map;
	public Assembly[] Loaded {get; private set;}

	public LoadedAssemblies(Engine.UserAssembly[] userAssemblies) : base(true)
	{
		map = userAssemblies.ToDictionary(ua => ua.Name);
		Loaded = new Assembly[userAssemblies.Length];

		for (int i = 0; i < userAssemblies.Length; i++)
		{
			Loaded[i] = Load(userAssemblies[i]);
		}
	}

	protected override Assembly? Load(AssemblyName assemblyName)
	{
		return null;
	}

	private Assembly Load(Engine.UserAssembly assembly)
	{
		using MemoryStream stream = new MemoryStream(assembly.Binary, false);
		return LoadFromStream(stream);
	}
}
