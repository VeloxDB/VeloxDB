using System;
using System.Linq;
using Microsoft.CodeAnalysis;
using static VeloxDB.SourceGenerator.Utils;

namespace VeloxDB.SourceGenerator
{
	internal class DBArrayProperty : DBProperty
	{
		public INamedTypeSymbol ElementType { get; }

		public DBArrayProperty(IPropertySymbol property, INamedTypeSymbol elementType):base(property)
		{
			this.ElementType = elementType;
		}

		public override bool IsArray => true;

		public override void AssignTo(Context context, Method method, SourceWriter writer, string objName, IPropertySymbol dtoProperty)
		{
			ITypeSymbol propType = dtoProperty.Type;
			if(propType.TypeKind == TypeKind.Array)
			{
				IArrayTypeSymbol arrayType = (IArrayTypeSymbol)propType;
				if(!SymEquals(arrayType.ElementType, ElementType))
				{
					Report.PropertyTypeMismatch(context, Symbol, dtoProperty);
					return;
				}

				writer.Assign(objName, dtoProperty.Name, Symbol.Name, "ToArray()", true);

			}
			else if (CastAsList(propType, context, out INamedTypeSymbol list))
			{
				if (!SymEquals(list.TypeArguments[0], ElementType))
				{
					Report.PropertyTypeMismatch(context, Symbol, dtoProperty);
					return;
				}

				writer.AppendFormat("{0}.{1} = ({2} == null)?null:new System.Collections.Generic.List<{3}>({2});\n",
									objName, dtoProperty.Name, Symbol.Name, ElementType.ToString());
			}
			else
			{
				Report.PropertyTypeMismatch(context, Symbol, dtoProperty);
			}
		}

		public override void AssignFrom(Context context, Method method, SourceWriter writer, string omName, string paramName,
										IPropertySymbol dtoProperty, string objName, bool update)
		{
			ITypeSymbol dtoPropType = dtoProperty.Type;

			if(!context.Types.IsEnumerable(dtoPropType, ElementType))
			{
				Report.PropertyTypeMismatch(context, Symbol, dtoProperty);
				return;
			}

			ITypeSymbol dboPropType = WithoutNullable(Symbol.Type);

			writer.AppendFormat("{0}.{1} = ({3}.{4} == null)?null:{2}.Create({3}.{4});\n", objName, Symbol.Name, dboPropType.ToString(),
								paramName, dtoProperty.Name);
		}

		public static new DBArrayProperty Create(IPropertySymbol property, Context context)
		{
			INamedTypeSymbol propertyType = (INamedTypeSymbol)property.Type;
			ITypeSymbol arg = propertyType.TypeArguments[0];

			if (!(arg is INamedTypeSymbol))
			{
				Report.InvalidArrayType(context, property);
				return null;
			}

			INamedTypeSymbol namedArg = (INamedTypeSymbol)arg;
			if(!context.Types.IsSimpleType(namedArg))
			{
				Report.InvalidArrayType(context, property);
				return null;
			}

			return new DBArrayProperty(property, namedArg);
		}
	}
}
