using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;

namespace VeloxDB.AspNet.ControllerGenerator
{
	internal class PropertyCache
	{
		Dictionary<INamedTypeSymbol, Dictionary<string, IPropertySymbol>> cache;

		public PropertyCache()
		{
			cache = new Dictionary<INamedTypeSymbol, Dictionary<string, IPropertySymbol>>(SymbolEqualityComparer.Default);
		}

		public bool TryGetProperty(INamedTypeSymbol newAttributeClass, string name, out IPropertySymbol result)
		{
			Dictionary<string, IPropertySymbol> properties;
			if(!cache.TryGetValue(newAttributeClass, out properties))
			{
				properties = newAttributeClass.GetMembers().OfType<IPropertySymbol>().ToDictionary(prop=>prop.Name);
				cache.Add(newAttributeClass, properties);
			}

			return properties.TryGetValue(name, out result);
		}
	}
}