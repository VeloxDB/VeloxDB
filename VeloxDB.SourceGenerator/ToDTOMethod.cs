using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Microsoft.CodeAnalysis;

namespace VeloxDB.SourceGenerator
{
	internal sealed class ToDTOMethod : Method
	{
		public override INamedTypeSymbol DTOType => (INamedTypeSymbol)Symbol.ReturnType;

		private const string dtoVar = SourceWriter.VlxPrefix + "Dto";
		private const string dtoVarCast = SourceWriter.VlxPrefix + "DtoCast";

		private ToDTOMethod(IMethodSymbol symbol, DBOType type, Polymorphism polymorphism, bool hasReferences) :
				base(symbol, type, polymorphism, hasReferences)
		{
		}

		protected override void GenerateBody(Context context, SourceWriter writer)
		{
			writer.AppendLine("Select();");
			if (!NeedsInternalMethod)
			{
				GenerateCopyBody(context, writer, out _);
			}
			else
			{
				GenerateInternalBody(context, writer);
			}
		}

		private MethodDesc CreateAdditionalMethodDesc()
		{
			Debug.Assert(PolymorphismEnabled || HasReferences);

			IMethodSymbol declaringMethodSym = MethodGroup.DeclaringMethod.Symbol;
			Accessibility accessibility = declaringMethodSym.DeclaredAccessibility;

			return new MethodDesc()
			{
				Name = this.Name,
				DeclaredAccessibility = accessibility,
				IsStatic = false,
				ReturnType = declaringMethodSym.ReturnType.ToString(),
				Parameters = new ParamDesc[]
				{
					new ParamDesc("VeloxDB.Common.AutomapperThreadContext", SourceWriter.ContextVar),
					new ParamDesc("System.Int32", SourceWriter.DepthVar)
				}
			};
		}

		private MethodDesc CreateResumeMethodDesc()
		{
			IMethodSymbol declaringMethodSym = MethodGroup.DeclaringMethod.Symbol;

			return new MethodDesc()
			{
				Name = $"{this.Name}Resume",
				DeclaredAccessibility = Accessibility.Private,
				IsStatic = false,
				Parameters = new ParamDesc[]
				{
					new ParamDesc("VeloxDB.Common.AutomapperThreadContext", SourceWriter.ContextVar),
					new ParamDesc("System.Int32", SourceWriter.DepthVar),
					new ParamDesc("object", dtoVar.ToString()),
				}
			};
		}

		protected override void AdditionalGenerate(Context context, SourceWriter writer)
		{
			if (!NeedsInternalMethod)
				return;

			string modifier = string.Empty;

			if(PolymorphismEnabled)
				modifier = IsBaseMethod ? "virtual" : "override";

			List<PropMatch> objRefProp;
			using(writer.Method(CreateAdditionalMethodDesc(), modifier))
			{
				GenerateCopyBody(context, writer, out objRefProp);
			}

			using(writer.Method(CreateResumeMethodDesc()))
			{
				writer.AppendIndent();
				writer.Append(Symbol.ReturnType.ToString());
				writer.Append(" ");
				writer.Append(dtoVarCast);
				writer.Append(" = (");
				writer.Append(Symbol.ReturnType.ToString());
				writer.Append(")");
				writer.Append(dtoVar);
				writer.Append(";\n");

				if(objRefProp != null)
					GenerateRefPropAssignBody(context, writer, objRefProp, dtoVarCast);
			}
		}

		private void GenerateInternalBody(Context context, SourceWriter writer)
		{
			TempVar contextVar = writer.CreateTempVar();

			writer.AppendIndent();
			writer.Append("var ");
			writer.Append(contextVar);
			writer.Append(" = VeloxDB.Common.AutomapperThreadContext.Instance;\n");

			writer.AppendIndent();
			writer.Append("var ");
			writer.Append(SourceWriter.ResultVar);
			writer.Append(" = (");
			writer.Append(Symbol.ReturnType.ToString());
			writer.Append(")");
			writer.Append(SourceWriter.VlxPrefix);
			writer.Append(Name);
			writer.Append("(");
			writer.Append(contextVar);
			writer.Append(", 0);\n");

			writer.DequeueLoop(contextVar);

			writer.AppendIndent();
			writer.Append(contextVar);
			writer.Append(".Clear();\n");

			writer.AppendIndent();
			writer.Append("return ");
			writer.Append(SourceWriter.ResultVar);
			writer.Append(";\n");
		}

		private void GenerateCopyBody(Context context, SourceWriter writer, out List<PropMatch> objRefProps)
		{
			if(Type.IsAbstract)
			{
				writer.AppendFormat("throw new System.InvalidOperationException(\"Class {0} is abstract.\");\n", Type.FullName);
				objRefProps = new List<PropMatch>();
				return;
			}

			writer.AppendFormat("{0} {1} = new {0}();\n", Symbol.ReturnType, SourceWriter.ResultVar);

			if (NeedsInternalMethod)
			{
				writer.AppendIndent();
				writer.Append(SourceWriter.ContextVar);
				writer.Append(".Add(this, ");
				writer.Append(SourceWriter.ResultVar);
				writer.Append(");\n");
			}

			INamedTypeSymbol dtoType = (INamedTypeSymbol)Symbol.ReturnType;
			INamedTypeSymbol dbType = (INamedTypeSymbol)Symbol.ContainingType;

			(List<PropMatch> matches, List<PropMatch> objRefPropsTemp) = MapProperties(context);

			foreach(PropMatch match in matches)
			{
				match.DBProperty.AssignTo(context, this, writer, SourceWriter.ResultVar, match.DTOProperty);
			}

			if(objRefPropsTemp != null)
				GenerateRefPropAssign(context, writer, objRefPropsTemp);

			objRefProps = objRefPropsTemp;

			writer.AppendIndent();
			writer.Append("return ");
			writer.Append(SourceWriter.ResultVar);
			writer.Append(";\n");
		}

		private void GenerateRefPropAssign(Context context, SourceWriter writer, List<PropMatch> objRefProps)
		{
			using(writer.IfMaxDepth())
			{
				GenerateRefPropAssignBody(context, writer, objRefProps, SourceWriter.ResultVar);
			}
			TempVar lambdaTemp = writer.CreateTempVar();
			writer.AppendIndent();
			writer.Append("else\n");
			writer.AppendIndent();
			writer.Append("\t");
			writer.Append(SourceWriter.ContextVar);
			writer.Append(".Enqueue((");
			writer.Append(lambdaTemp);
			writer.Append(")=>");
			writer.Append(SourceWriter.VlxPrefix);
			writer.Append(Name);
			writer.Append("Resume(");
			writer.Append(lambdaTemp);
			writer.Append(", 0, ");
			writer.Append(SourceWriter.ResultVar);
			writer.Append("));\n");
		}

		private void GenerateRefPropAssignBody(Context context, SourceWriter writer, List<PropMatch> objRefProps, string dtoVar)
		{
			for (int i = 0; i < objRefProps.Count; i++)
			{
				objRefProps[i].DBProperty.AssignTo(context, this, writer, dtoVar, objRefProps[i].DTOProperty);
			}
		}

		public static Method Create(Context context, IMethodSymbol methodSym, DBOType type, Polymorphism polymorphism)
		{
			if(!methodSym.Name.StartsWith("To"))
				return null;

			if (methodSym.Parameters.Length != 0)
			{
				Report.ToMethodHasArguments(context, methodSym);
				return null;
			}

			if(methodSym.IsStatic)
			{
				Report.ToMethodCantBeStatic(context, methodSym);
				return null;
			}

			if(methodSym.IsVirtual)
			{
				Report.ToMethodCantBeVirtual(context, methodSym);
				return null;
			}

			ITypeSymbol dtoType = methodSym.ReturnType;
			if(!CheckDTOType(context, dtoType, methodSym, type))
			{
				return null;
			}

			bool hasReferences = GetHasReferences(context, type, (INamedTypeSymbol)methodSym.ReturnType);

			return new ToDTOMethod(methodSym, type, polymorphism, hasReferences);
		}
	}

}
