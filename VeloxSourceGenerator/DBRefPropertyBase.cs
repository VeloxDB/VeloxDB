using System.Text;
using Microsoft.CodeAnalysis;
using static Velox.SourceGenerator.Utils;

namespace Velox.SourceGenerator
{
	internal abstract class DBRefPropertyBase : DBProperty
	{
		bool isValid;

		protected DBRefPropertyBase(IPropertySymbol property) : base(property)
		{
		}

		public override bool IsDirectReference => true;

		public override bool IsReference => true;

		public abstract INamedTypeSymbol ReferencedType { get; }

		protected DBOType DBReferencedType { get; private set; }

		protected abstract void OnAssignTo(Context context, Method method, SourceWriter writer, string objName, IPropertySymbol dtoProperty);
		protected abstract void OnAssignFrom(Context context, Method method, SourceWriter writer, string omName, string paramName,
											 IPropertySymbol dtoProperty, string objName, bool supportUpdate);

		public override sealed void PrepareReferences(Context context, DBOTypeCollection collection)
		{
			isValid = false;

			if(!collection.TryGet(ReferencedType, out var dbReferencedType))
			{
				Report.InvalidPropertyType(context, Symbol);
				return;
			}

			DBReferencedType = dbReferencedType;
			isValid = OnPrepareReferences(context, collection);
		}

		protected virtual bool OnPrepareReferences(Context context, DBOTypeCollection collection)
		{
			return true;
		}

		public override sealed void AssignTo(Context context, Method method, SourceWriter writer, string objName, IPropertySymbol dtoProperty)
		{
			if(isValid)
				OnAssignTo(context, method, writer, objName, dtoProperty);
		}

		public override sealed void AssignFrom(Context context, Method method, SourceWriter writer, string omName, string paramName, IPropertySymbol dtoProperty, string objName, bool supportUpdate)
		{
			if(isValid)
				OnAssignFrom(context, method, writer, omName, paramName, dtoProperty, objName, supportUpdate);
		}

		protected bool CheckDTOType(Context context, Method method, IPropertySymbol dtoProperty, ITypeSymbol dtoType, out Method refMethod)
		{
			refMethod = null;

			if(dtoType.Kind != SymbolKind.NamedType)
			{
				Report.PropertyTypeMismatch(context, Symbol, dtoProperty);
				return false;
			}

			INamedTypeSymbol dtoNamedType = (INamedTypeSymbol)dtoType;

			refMethod = DBReferencedType.GetMethodByName(method.Name);
			if (refMethod == null)
			{
				Report.ReferencedTypeMissingMethod(context, Symbol, method.Name, dtoNamedType);
				return false;
			}

			if(!context.IsAccessibleFrom(Symbol.ContainingType, refMethod.Symbol))
			{
				Report.InaccessibleReferencedType(context, Symbol.ContainingType, refMethod.Symbol);
				return false;
			}

			if(!SymEquals(refMethod.DTOType, dtoNamedType))
			{
				Report.MethodTypeMismatch(context, Symbol, refMethod.Symbol, dtoNamedType);
				return false;
			}

			return true;
		}

		private static StringBuilder builder = new StringBuilder();
		protected void GenerateCallToDTO(Method method, SourceWriter writer, string dboVar, string objName, string dtoVar,
										 ITypeSymbol dtoElementType, Method refMethod, string addNewFormat, string addExistingFormat)
		{
			dtoElementType = WithoutNullable(dtoElementType);
			TempVar temp = writer.CreateTempVar();
			writer.Declare("object", temp);

			writer.AppendIndent();
			writer.Append("if(!");
			writer.Append(SourceWriter.ContextVar);
			writer.Append(".TryGet(");
			writer.Append(dboVar);
			writer.Append(", out ");
			writer.Append(temp);
			writer.Append("))\n");

			using (writer.Block())
			{
				string toDto;

				if (refMethod.NeedsInternalMethod && !refMethod.IsDeclaringMethod)
				{
					builder.Append("(");
					builder.Append(dtoElementType.ToString());
					builder.Append(")");
				}

				builder.Append(dboVar);
				builder.Append(".");
				if (refMethod.NeedsInternalMethod)
				{
					builder.Append(SourceWriter.VlxPrefix);
					builder.Append(method.Name);
					builder.Append("(");
					builder.Append(SourceWriter.ContextVar);
					builder.Append(",");
					builder.Append(SourceWriter.DepthVar);
					builder.Append(" + 1)");
				}
				else
				{
					builder.Append(method.Name);
					builder.Append("()");
				}

				toDto = builder.ToString();
				builder.Clear();

				writer.AppendFormat(addNewFormat, objName, dtoVar, toDto);

				if (!refMethod.NeedsInternalMethod)
				{
					writer.AppendIndent();
					writer.Append(SourceWriter.ContextVar);
					writer.Append(".Add(");
					writer.Append(dboVar);
					writer.Append(", ");
					writer.Append(objName);
					writer.Append(".");
					writer.Append(dtoVar);
					writer.Append(");\n");
				}
			}

			writer.AppendLine("else");
			writer.AppendFormat(addExistingFormat, objName, dtoVar, dtoElementType.ToString(), temp.ToString());
		}

		protected void GenerateCallFromDTO(Method method, SourceWriter writer, string omName, string dboVar, string dtoVar,
										   Method refMethod)
		{
			TempVar temp = writer.CreateTempVar();
			writer.Declare("object", temp);

			writer.AppendIndent();
			writer.Append("if(!");
			writer.Append(SourceWriter.ContextVar);
			writer.Append(".TryGet(");
			writer.Append(dtoVar);
			writer.Append(", out ");
			writer.Append(temp);
			writer.Append("))\n");

			using (writer.Block())
			{
				writer.AppendIndent();
				writer.Append(dboVar);
				writer.Append(" = ");

				if (refMethod.NeedsInternalMethod && !refMethod.IsDeclaringMethod)
				{
					writer.Append("(");
					writer.Append(DBReferencedType.FullName);
					writer.Append(")");
				}

				writer.Append(refMethod.MethodGroup.DeclaringMethod.Type.FullName);
				writer.Append(".");

				if (refMethod.NeedsInternalMethod)
				{
					writer.Append(SourceWriter.VlxPrefix);
					writer.Append(method.Name);
					writer.Append("Dispatch");
				}
				else
				{
					writer.Append(SourceWriter.VlxPrefix);
					writer.Append(method.Name);
				}

				writer.Append("(");
				writer.Append(omName);
				writer.Append(",");
				writer.Append(dtoVar);
				writer.Append(",");
				writer.Append(SourceWriter.ContextVar);
				writer.Append(",");
				writer.Append(SourceWriter.DepthVar);
				writer.Append(" + 1);\n");
			}

			writer.AppendLine("else");

			writer.AppendIndent();
			writer.Append("\t");
			writer.Append(dboVar);
			writer.Append(" = (");
			writer.Append(DBReferencedType.FullName);
			writer.Append(")");
			writer.Append(temp);
			writer.Append(";\n");
		}

	}
}
