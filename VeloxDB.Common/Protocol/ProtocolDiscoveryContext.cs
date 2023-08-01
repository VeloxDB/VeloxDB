using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using VeloxDB.Common;

namespace VeloxDB.Protocol;

/// <summary>
/// Allows a user to dinamically provide additional assemblies containing protocol classes.
/// </summary>
public interface IAssemblyProvider
{
	/// <summary>
	/// Returns a sequence of assemblies that contain additional classes that might be used as protocl classes.
	/// </summary>
	/// <returns>A sequence of additional assemblies.</returns>
	IEnumerable<Assembly> GetAssemblies();
}

internal sealed class ProtocolDiscoveryContext
{
	readonly object sync = new object();

	ushort inTypeIdCounter = 1;
	ushort outTypeIdCounter = 1;
	ushort interfaceIdCounter = 1;

	Dictionary<Type, ProtocolTypeDescriptor> inTypes;
	Dictionary<Type, ProtocolTypeDescriptor> outTypes;
	Dictionary<Type, ProtocolInterfaceDescriptor> interfaces;

	Dictionary<Type, ProtocolTypeDescriptor> newInTypes, newOutTypes;
	Dictionary<string, Type> typesByName;

	public ProtocolDiscoveryContext()
	{
		inTypes = new Dictionary<Type, ProtocolTypeDescriptor>(64, ReferenceEqualityComparer<Type>.Instance);
		outTypes = new Dictionary<Type, ProtocolTypeDescriptor>(64, ReferenceEqualityComparer<Type>.Instance);
		interfaces = new Dictionary<Type, ProtocolInterfaceDescriptor>(0, ReferenceEqualityComparer<Type>.Instance);
		typesByName = new Dictionary<string, Type>(32);
	}

	public ProtocolInterfaceDescriptor[] Interfaces => interfaces.Values.ToArray();

	public ProtocolInterfaceDescriptor GetInterfaceDescriptor(Type type, IAssemblyProvider assemblyDiscoverer)
	{
		return GetInterfaceDescriptor(type, 0, out _, out _, assemblyDiscoverer);
	}

	public ProtocolInterfaceDescriptor GetInterfaceDescriptor(Type type, int paramSkipCount,
		out ProtocolTypeDescriptor[] discoveredInTypes, out ProtocolTypeDescriptor[] discoveredOutTypes,
		IAssemblyProvider assemblyDiscoverer)
	{
		if (interfaces.TryGetValue(type, out ProtocolInterfaceDescriptor desc))
		{
			discoveredInTypes = null;
			discoveredOutTypes = null;
			return desc;
		}

		lock (sync)
		{
			if (interfaces.TryGetValue(type, out desc))
			{
				discoveredInTypes = null;
				discoveredOutTypes = null;
				return desc;
			}

			newOutTypes = new Dictionary<Type, ProtocolTypeDescriptor>(32, ReferenceEqualityComparer<Type>.Instance);
			newInTypes = new Dictionary<Type, ProtocolTypeDescriptor>(32, ReferenceEqualityComparer<Type>.Instance);

			MethodInfo[] methods = type.GetMethods(BindingFlags.Public | BindingFlags.FlattenHierarchy | BindingFlags.Instance);
			methods = methods.Where(x => IsMethodAnOperation(type, x)).ToArray();

			ProtocolOperationDescriptor[] ops = new ProtocolOperationDescriptor[methods.Length];
			for (int i = 0; i < methods.Length; i++)
			{
				ops[i] = new ProtocolOperationDescriptor((ushort)i, methods[i], this, paramSkipCount);
			}

			DiscoverInheritedTypes(assemblyDiscoverer);
			ProtocolInterfaceDescriptor interfaceDesc = new ProtocolInterfaceDescriptor(GenerateInterfaceId(), type, ops);

			discoveredInTypes = newInTypes.Values.ToArray();
			discoveredOutTypes = newOutTypes.Values.ToArray();
			interfaceDesc.SetTypes(discoveredInTypes, discoveredOutTypes);

			Dictionary<Type, ProtocolInterfaceDescriptor> newInterfaces =
				new Dictionary<Type, ProtocolInterfaceDescriptor>(interfaces, ReferenceEqualityComparer<Type>.Instance);
			newInterfaces.Add(type, interfaceDesc);

			Thread.MemoryBarrier();
			interfaces = newInterfaces;

			this.newOutTypes = null;
			this.newInTypes = null;

			return interfaceDesc;
		}
	}

	public ProtocolTypeDescriptor GetTypeDuringDiscovery(Type type, bool isInput)
	{
		Checker.AssertTrue(Monitor.IsEntered(sync));

		ProtocolTypeDescriptor td;
		if (isInput)
		{
			if (newInTypes.TryGetValue(type, out td))
				return td;
		}
		else
		{
			if (newOutTypes.TryGetValue(type, out td))
				return td;
		}

		td = CreateTypeDescriptor(type, isInput);
		if (typesByName.TryGetValue(td.Name, out Type existingType))
		{
			if (!object.ReferenceEquals(existingType, type))
				throw DbAPIDefinitionException.CreateTypeNameDuplicate(td.Name);
		}
		else
		{
			typesByName.Add(td.Name, type);
		}

		return td;
	}

	private bool IsMethodAnOperation(Type type, MethodInfo methodInfo)
	{
		if (type.IsInterface)
			return true;

		return methodInfo.GetCustomAttribute(typeof(DbAPIOperationAttribute)) != null;
	}

	private void DiscoverInheritedTypes(IAssemblyProvider assemblyDiscoverer)
	{
		HashSet<Assembly> assemblies = newInTypes.Values.Concat(newOutTypes.Values).Select(x => x.TargetType.Assembly).ToHashSet();
		if (assemblyDiscoverer != null)
			assemblies.UnionWith(assemblyDiscoverer.GetAssemblies());

		foreach (Assembly assembly in assemblies)
		{
			foreach (Type type in assembly.GetExportedTypes())
			{
				if (type.IsClass)
				{
					InheritsSerializableType(type, out bool inheritsInType, out bool inheritsOutType);
					if (inheritsInType)
						GetTypeDuringDiscovery(type, true);

					if (inheritsOutType)
						GetTypeDuringDiscovery(type, false);
				}
			}
		}
	}

	private void InheritsSerializableType(Type type, out bool inheritsInType, out bool inheritsOutType)
	{
		inheritsInType = false;
		inheritsOutType = false;

		Type baseType = type.BaseType;
		while (baseType != null)
		{
			if (newInTypes.ContainsKey(baseType))
				inheritsInType = true;

			if (newOutTypes.ContainsKey(baseType))
				inheritsOutType = true;

			baseType = baseType.BaseType;

			if (inheritsInType && inheritsOutType)
				return;
		}
	}

	private ProtocolTypeDescriptor CreateTypeDescriptor(Type type, bool isInput)
	{
		var types = isInput ? inTypes : outTypes;

		if (!types.TryGetValue(type, out ProtocolTypeDescriptor desc))
		{
			if (type.IsArray)
			{
				desc = new ProtocolArrayDescriptor(GenerateTypeId(isInput), type);
			}
			else if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
			{
				desc = new ProtocolListDescriptor(GenerateTypeId(isInput), type);
			}
			else if ((type.IsClass && !typeof(Delegate).IsAssignableFrom(type)) || IsTypeStruct(type))
			{
				desc = new ProtocolClassDescriptor(GenerateTypeId(isInput), type);
			}

			if (desc == null)
				throw DbAPIDefinitionException.CreateNonSerializableType(ProtocolClassDescriptor.GetClassName(type));

			if (isInput)
				newInTypes.Add(type, desc);
			else
				newOutTypes.Add(type, desc);

			types.Add(type, desc);
			desc.Init(type, this, isInput);
		}
		else
		{
			if (isInput)
				newInTypes.Add(type, desc);
			else
				newOutTypes.Add(type, desc);

			desc.DiscoverTypes(type, this, isInput);
		}

		return desc;
	}

	public static bool IsTypeStruct(Type type)
	{
		return type.IsValueType && !type.IsEnum && !type.IsPrimitive;
	}

	private ushort GenerateTypeId(bool isInput)
	{
		if (isInput)
		{
			if (inTypeIdCounter == 0) // Wrapped around
				throw new InvalidOperationException("Maximum number of types exceeded.");

			return inTypeIdCounter++;
		}
		else
		{
			if (outTypeIdCounter == 0) // Wrapped around
				throw new InvalidOperationException("Maximum number of types exceeded.");

			return outTypeIdCounter++;
		}
	}

	private ushort GenerateInterfaceId()
	{
		if (interfaceIdCounter == 0) // Wrapped around
			throw new InvalidOperationException("Maximum number of API operations exceeded.");

		return interfaceIdCounter++;
	}
}

internal sealed class AssemblyProvider : IAssemblyProvider
{
	IEnumerable<Assembly> assemblies;

	public AssemblyProvider(IEnumerable<Assembly> assemblies)
	{
		this.assemblies = assemblies;
	}

	public IEnumerable<Assembly> GetAssemblies()
	{
		return assemblies;
	}
}
