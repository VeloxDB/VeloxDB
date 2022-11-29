using System;
using Microsoft.CodeAnalysis;
using static Velox.SourceGenerator.Utils;

namespace Velox.SourceGenerator
{
	internal sealed class DBInvRefSetProperty : DBRefArrayProperty
	{
		string refPropName;
		public override bool IsDirectReference => false;
		protected override bool IsNullable => false;
		bool refIsArray;

		private DBInvRefSetProperty(IPropertySymbol property, INamedTypeSymbol referenceOriginType, string refPropName):base(property, referenceOriginType)
		{
			this.refPropName = refPropName;
		}

		public static DBInvRefSetProperty Create(Context context, IPropertySymbol property)
		{
			if (!(property.Type is INamedTypeSymbol))
			{
				Report.InvalidPropertyType(context, property);
				return null;
			}

			INamedTypeSymbol propType = (INamedTypeSymbol)property.Type;

			if(!SymEquals(propType.ConstructUnboundGenericType(), context.Types.InverseReferenceSet))
			{
				Report.InvalidInverseReferenceType(context, property);
			}

			ITypeSymbol arg = propType.TypeArguments[0];
			if (!(arg is INamedTypeSymbol))
			{
				Report.InvalidRefArrayType(context, property);
				return null;
			}

			INamedTypeSymbol referenceOriginType = (INamedTypeSymbol)arg;
			if(!context.Types.IsDatabaseObject(referenceOriginType))
			{
				Report.InvalidRefArrayType(context, property);
				return null;
			}

			string refPropName = GetRefPropName(property, context);

			return new DBInvRefSetProperty(property, referenceOriginType, refPropName);
		}

		private static string GetRefPropName(IPropertySymbol property, Context context)
		{
			TryGetAttribute(property, context.Types.InverseReferencesAttribute, out var attribute);
			return (string)attribute.ConstructorArguments[0].Value;
		}


		protected override bool OnPrepareReferences(Context context, DBOTypeCollection collection)
		{
			DBProperty refProperty = DBReferencedType.GetPropertyByName(refPropName);

			if(refProperty == null || !refProperty.IsDirectReference)
			{
				Report.InvalidInverseReferenceProperty(context, Symbol, $"{refPropName} is not a DatabaseReference property");
				return false;
			}

			if (!context.IsAssignable(Symbol.ContainingType, ((DBRefPropertyBase)refProperty).ReferencedType))
			{
				Report.InvalidInverseReferenceProperty(context, Symbol,
				$"{refProperty.Symbol.ContainingType.ToString()}.{refProperty.Symbol} unexpected type, expected {Symbol.ContainingType.ToString()}");
				return false;
			}

			if(!SymEquals(ReferencedType, refProperty.Symbol.ContainingType))
			{
				Report.InvalidInverseReferenceProperty(context, Symbol,
				$"{Symbol.ContainingType.ToString()}.{Symbol.Name} unexpected type, expected {refProperty.Symbol.ContainingType.ToString()}");
				return false;
			}

			refIsArray = refProperty.IsArray;
			return true;
		}

		protected override void OnBeforeAssign(Context context, SourceWriter writer, string objName, bool update)
		{
			if(update)
			{
				writer.IfBoolClear(FromDTOMethod.IsUpdateVar, objName, Symbol.Name);
			}
		}

		protected override void AssignToDBO(Context context, SourceWriter writer, string objName, TempVar temp)
		{
			writer.AppendIndent();
			writer.Append(temp);
			writer.Append(".");
			writer.Append(refPropName);
			if (!refIsArray)
			{
				writer.Append(" = ");
				writer.Append(objName);
				writer.Append(";\n");
			}
			else
			{
				writer.Append(".Add(");
				writer.Append(objName);
				writer.Append(");\n");
			}
		}

		protected override void CreateCollection(SourceWriter writer, string paramName, IPropertySymbol dtoProperty, string objName, string dbType, string countProp)
		{
		}
	}
}
