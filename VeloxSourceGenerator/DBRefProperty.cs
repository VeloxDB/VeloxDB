using System;
using Microsoft.CodeAnalysis;
using static Velox.SourceGenerator.Utils;

namespace Velox.SourceGenerator
{
	internal sealed class DBRefProperty : DBRefPropertyBase
	{
		public override INamedTypeSymbol ReferencedType { get; }

		public DBRefProperty(IPropertySymbol property) : base(property)
		{
			ReferencedType = (INamedTypeSymbol)property.Type;
		}

		public override bool IsObjReference(Context context, IPropertySymbol dtoProperty)
		{
			return !SymEquals(dtoProperty.Type, context.Types.Long);
		}

		protected override void OnAssignTo(Context context, Method method, SourceWriter writer, string objName, IPropertySymbol dtoProperty)
		{

			if(SymEquals(dtoProperty.Type, context.Types.Long))
			{
				writer.AssignId(objName, dtoProperty.Name, Symbol.Name);
			}
			else
			{
				if(!CheckDTOType(context, method, dtoProperty, dtoProperty.Type, out var refMethod))
				{
					return;
				}

				writer.AppendIndent();
				writer.Append("if(");
				writer.Append(Name);
				writer.Append(" != null && ");
				writer.Append(Name);
				writer.Append(".IsSelected)\n");

				using (writer.Block())
				{
					GenerateCallToDTO(method, writer, Name, objName, dtoProperty.Name, dtoProperty.Type, refMethod,
									  "{0}.{1} = {2};\n", "\t{0}.{1} = ({2}){3};\n");
				}
			}
		}

		protected override void OnAssignFrom(Context context, Method method, SourceWriter writer, string omName, string dtoName, IPropertySymbol dtoProperty,
										string dboName, bool update)
		{
			if(SymEquals(dtoProperty.Type, context.Types.Long))
			{
				ITypeSymbol propType = WithoutNullable(Symbol.Type);
				writer.AppendFormat("{0}.{1} = ({4}.{5} == 0)?null:{2}.GetObjectStrict<{3}>({4}.{5});\n", SourceWriter.ResultVar, Symbol.Name, omName,
									 propType.ToString(), dtoName, dtoProperty.Name);
			}
			else
			{
				if(!CheckDTOType(context, method, dtoProperty, dtoProperty.Type, out var refMethod))
				{
					return;
				}

				writer.AppendIndent();
				writer.Append("if(");
				writer.Append(dtoName);
				writer.Append(".");
				writer.Append(dtoProperty.Name);
				writer.Append(" != null)\n");

				using(writer.Block())
				{
					GenerateCallFromDTO(method, writer, omName, $"{dboName}.{Name}", $"{dtoName}.{dtoProperty.Name}", refMethod);
				}

				writer.AppendLine("else");
				writer.AppendIndent();
				writer.Append("\t");
				writer.Append(dboName);
				writer.Append(".");
				writer.Append(Name);
				writer.Append(" = null;\n");
			}
		}

		public static new DBRefProperty Create(IPropertySymbol property, Context context)
		{
			INamedTypeSymbol propertyType = (INamedTypeSymbol)property.Type;

			if (!context.Types.IsDatabaseObject(propertyType))
			{
				Report.ReferenceMustBeDatabaseObject(context, property);
				return null;
			}

			return new DBRefProperty(property);
		}

		public static bool GetIsNullable(IPropertySymbol property, Context context)
		{
			TryGetAttribute(property, context.Types.DatabaseReferenceAttribute, out var attribute);

			bool isNullable = (bool)attribute.ConstructorArguments[2].Value;
			return isNullable;
		}
	}
}
