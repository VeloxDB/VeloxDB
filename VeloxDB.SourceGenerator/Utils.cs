using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;

namespace VeloxDB.SourceGenerator
{
	static class Utils
	{
		public static bool SymEquals(ISymbol first, ISymbol second)
		{
			return SymbolEqualityComparer.Default.Equals(first, second);
		}

		public static bool HasAttribute(ISymbol type, ITypeSymbol dbcAttrib)
		{
			return TryGetAttribute(type, dbcAttrib, out _);
		}

		public static ITypeSymbol WithoutNullable(ITypeSymbol type)
		{
			ITypeSymbol result = type;

			if(result.NullableAnnotation == NullableAnnotation.Annotated)
				result = result.WithNullableAnnotation(NullableAnnotation.NotAnnotated);

			return result;
		}

		public static bool TryGetAttribute(ISymbol type, ITypeSymbol dbcAttrib, out AttributeData attribute)
		{
			ImmutableArray<AttributeData> attr = type.GetAttributes();

			bool found = false;
			attribute = null;

			for (int i = 0; i < attr.Length; i++)
			{
				if (SymbolEqualityComparer.Default.Equals(attr[i].AttributeClass, dbcAttrib))
				{
					attribute = attr[i];
					found = true;
					break;
				}
			}
			return found;
		}

		public static bool CastAsList(ITypeSymbol propType, Context context, out INamedTypeSymbol list)
		{
			if (propType.TypeKind != TypeKind.Class)
			{
				list = null;
				return false;
			}

			list = (INamedTypeSymbol)propType;

			return SymEquals(list.ConstructUnboundGenericType(), context.Types.List);
		}

		public static bool IsIdArray(Context context, ITypeSymbol type, out string countProp)
		{
			if (type.TypeKind == TypeKind.Array)
			{
				IArrayTypeSymbol arrayType = (IArrayTypeSymbol)type;

				if (SymEquals(arrayType.ElementType, context.Types.Long))
				{
					countProp = "Length";
					return true;
				}
			}
			else if(CastAsList(type, context, out INamedTypeSymbol list))
			{
				if (SymEquals(list.TypeArguments[0], context.Types.Long))
				{
					countProp = "Count";
					return true;
				}
			}

			countProp = null;
			return false;
		}

		public static bool TryGetProperty(string name, ITypeSymbol type, out IPropertySymbol property)
		{

			while (type != null)
			{
				foreach (var member in type.GetMembers(name))
				{
					if (member.Kind == SymbolKind.Property)
					{
						property = (IPropertySymbol)member;
						return true;
					}
				}
				type = type.BaseType;
			}

			property = null;
			return false;
		}

		public static IEnumerable<ISymbol> GetAllMembers(INamedTypeSymbol type, INamedTypeSymbol stop)
		{
			INamedTypeSymbol current = type;

			bool done = false;
			while(!done && current.BaseType != null)
			{
				done = SymEquals(current, stop);

				var array = current.GetMembers();
				for (int i = 0; i < array.Length; i++)
				{
					yield return array[i];
				}
				current = current.BaseType;
			}
		}
	}
}
