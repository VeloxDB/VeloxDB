using System;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using System.Linq;

namespace VeloxDB.AspNet.ControllerGenerator
{
	internal class Utils
	{
		public static bool SymEquals(ISymbol first, ISymbol second)
		{
			return SymbolEqualityComparer.Default.Equals(first, second);
		}

		public static bool IsReadOnly(IMethodSymbol method, KnownTypes knownTypes)
		{
			AttributeData attributeData = method.GetAttributes().First(ad => SymEquals(ad.AttributeClass, knownTypes.DbAPIOperationAttribute));
			if (attributeData.NamedArguments.Length == 0)
				return false;

			foreach (var pair in attributeData.NamedArguments)
			{
				if (pair.Key == "OperationType")
				{
					return Convert.ToInt32(pair.Value.Value) == 0;
				}
			}

			return false;
		}

		public static bool HasAttribute(ISymbol type, INamedTypeSymbol attribute)
		{
			ImmutableArray<AttributeData> attr = type.GetAttributes();

			for (int i = 0; i < attr.Length; i++)
			{
				if (SymbolEqualityComparer.Default.Equals(attr[i].AttributeClass, attribute))
				{
					return true;
				}
			}

			return false;
		}

	}
}