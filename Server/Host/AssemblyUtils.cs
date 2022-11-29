using System.Reflection;
using System.Runtime.Loader;
using Velox.Protocol;
using Engine = Velox.Storage.ModelUpdate;

namespace Velox.Server;

internal static class AssemblyUtils
{
	public static IEnumerable<Type> GetDBApiTypes(IEnumerable<Assembly> assemblies)
	{
		foreach(Assembly assembly in assemblies)
			foreach (Type classType in assembly.GetExportedTypes())
				if(classType.IsDefined(typeof(DbAPIAttribute)) && !classType.IsInterface)
					yield return classType;
	}
}
