using System;
using Microsoft.CodeAnalysis;
using static VeloxDB.SourceGenerator.Utils;

namespace VeloxDB.SourceGenerator
{
	internal class DBProperty
	{
		public DBProperty(IPropertySymbol property)
		{
			this.Symbol = property;
		}

		public IPropertySymbol Symbol { get; }

		public string Name => Symbol.Name;

		public virtual bool IsArray => false;

		public virtual bool IsDirectReference => false;

		public virtual bool IsReference => false;

		public static DBProperty Create(IPropertySymbol property, Context context)
		{
			if(HasAttribute(property, context.Types.DatabasePropertyAttribute) || property.Name == "Id")
			{
				return CreateDBProperty(property, context);
			}
			else if(HasAttribute(property, context.Types.DatabaseReferenceAttribute))
			{
				return CreateRefProperty(property, context);
			}
			else if(HasAttribute(property, context.Types.InverseReferencesAttribute))
			{
				return DBInvRefSetProperty.Create(context, property);
			}

			return null;
		}

		private static DBProperty CreateRefProperty(IPropertySymbol property, Context context)
		{
			if (!(property.Type is INamedTypeSymbol))
			{
				Report.InvalidPropertyType(context, property);
				return null;
			}

			INamedTypeSymbol propertyType = (INamedTypeSymbol)property.Type;

			if (IsRefArray(context, propertyType))
			{
				return DBRefArrayProperty.Create(property, context);
			}
			else
			{
				return DBRefProperty.Create(property, context);
			}
		}

		private static DBProperty CreateDBProperty(IPropertySymbol property, Context context)
		{
			if (!(property.Type is INamedTypeSymbol))
			{
				Report.InvalidPropertyType(context, property);
				return null;
			}

			INamedTypeSymbol propertyType = (INamedTypeSymbol)property.Type;

			if(context.Types.IsDatabaseObject(propertyType) || IsRefArray(context, propertyType))
			{
				Report.UnexpectedReference(context, property);
				return null;
			}

			if (IsDbArray(context, propertyType))
			{
				return DBArrayProperty.Create(property, context);
			}
			else
			{
				return CreateBase(property, context);
			}
		}

		public virtual void AssignTo(Context context, Method method, SourceWriter writer, string objName, IPropertySymbol dtoProperty)
		{
			if(!SymEquals(dtoProperty.Type, Symbol.Type))
			{
				Report.PropertyTypeMismatch(context, Symbol, dtoProperty);
				return;
			}

			writer.Assign(objName, dtoProperty.Name, "this", Symbol.Name);
		}

		public virtual void AssignFrom(Context context, Method method, SourceWriter writer, string omName, string paramName, IPropertySymbol dtoProperty,
									   string objName, bool supportUpdate)
		{
			if(!SymEquals(dtoProperty.Type, Symbol.Type))
			{
				Report.PropertyTypeMismatch(context, Symbol, dtoProperty);
				return;
			}

			writer.Assign(objName, Symbol.Name, paramName, dtoProperty.Name);
		}

		public virtual void PrepareReferences(Context context, DBOTypeCollection collection)
		{

		}

		private static DBProperty CreateBase(IPropertySymbol property, Context context)
		{
			INamedTypeSymbol propertyType = (INamedTypeSymbol)property.Type;

			if(!context.Types.IsSimpleType(propertyType))
			{
				Report.InvalidPropertyType(context, property);
			}

			return new DBProperty(property);
		}

		public virtual bool IsObjReference(Context context, IPropertySymbol dtoProperty) => false;

		private static bool IsDbArray(Context context, INamedTypeSymbol type)
		{
			return type.IsGenericType && SymEquals(type.ConstructUnboundGenericType(), context.Types.DatabaseArray);
		}

		private static bool IsRefArray(Context context, INamedTypeSymbol type)
		{
			return type.IsGenericType && SymEquals(type.ConstructUnboundGenericType(), context.Types.ReferenceArray);
		}
	}
}
