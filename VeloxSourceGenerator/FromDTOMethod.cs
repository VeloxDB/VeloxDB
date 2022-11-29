using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.CodeAnalysis;
using static Velox.SourceGenerator.Utils;

namespace Velox.SourceGenerator
{
	internal sealed class FromDTOMethod : Method
	{
		public const string IsUpdateVar = "vlxIsUpdate31";
		private const string contextAllowUpdateVar = SourceWriter.ContextVar + ".AllowUpdate";
		bool dtoHasId;

		public override INamedTypeSymbol DTOType => (INamedTypeSymbol)Symbol.Parameters[1].Type;

		bool hasAllowUpdate;

		private FromDTOMethod(IMethodSymbol symbol, DBOType type, Polymorphism polymorphism, bool hasReferences) :
				base(symbol, type, polymorphism, hasReferences)
		{
			dtoHasId = HasId(DTOType);
			hasAllowUpdate = Symbol.Parameters.Length == 3;
		}

		protected override void GenerateBody(Context context, SourceWriter writer)
		{
			if (!NeedsInternalMethod)
			{
				string allowUpdateVar = hasAllowUpdate ? Symbol.Parameters[2].Name:string.Empty;
				GenerateCopyBody(context, writer, Symbol.Parameters[0].Name, Symbol.Parameters[1].Name,
								 dtoHasId && hasAllowUpdate, false, allowUpdateVar, out _);
			}
			else
				GenerateCallInternalMethod(context, writer);
		}

		protected override void AdditionalGenerate(Context context, SourceWriter writer)
		{
			if (IsBaseMethod && NeedsInternalMethod)
			{
				GenerateTypeMap(writer);
				GenerateDispatchMethod(writer);
			}

			GenerateInternalFromDTO(context, writer);
		}

		public override void AddToStaticConstructor(Context context, SourceWriter writer)
		{
			if(!NeedsInternalMethod)
				return;

			writer.AppendIndent();
			writer.Append(MethodGroup.DeclaringMethod.Type.FullName);
			writer.Append(".");
			writer.Append(SourceWriter.VlxPrefix);
			writer.Append("RegisterFor");
			writer.Append(Name);
			writer.Append("(");
			writer.Append("typeof(");
			writer.Append(DTOType.ToString());
			writer.Append("), ");
			writer.Append(SourceWriter.VlxPrefix);
			writer.Append(Name);
			writer.Append(");\n");
		}

		private MethodDesc CreateAdditionalMethodDesc()
		{
			return new MethodDesc()
			{
				DeclaredAccessibility = NeedsInternalMethod?Accessibility.Private:Symbol.DeclaredAccessibility,
				IsStatic = true,
				Name = this.Name,
				Parameters = new ParamDesc[]
				{
					new ParamDesc("Velox.ObjectInterface.ObjectModel", "om"),
					new ParamDesc(MethodGroup.DeclaringMethod.DTOType.ToString(), "dto"),
					new ParamDesc("Velox.Common.AutomapperThreadContext", SourceWriter.ContextVar),
					new ParamDesc("System.Int32", SourceWriter.DepthVar)

				},
				ReturnType = MethodGroup.DeclaringMethod.Type.FullName
			};
		}

		private MethodDesc CreateResumeMethodDesc()
		{
			return new MethodDesc()
			{
				DeclaredAccessibility = Accessibility.Private,
				IsStatic = true,
				Name = this.Name + "Resume",
				Parameters = new ParamDesc[]
				{
					new ParamDesc("Velox.ObjectInterface.ObjectModel", "om"),
					new ParamDesc(DTOType.ToString(), "dto"),
					new ParamDesc(Type.FullName, "dbo"),
					new ParamDesc("Velox.Common.AutomapperThreadContext", SourceWriter.ContextVar),
					new ParamDesc("System.Int32", SourceWriter.DepthVar)
				},
			};
		}

		private MethodDesc CreateDispatchMethodDesc()
		{
			MethodDesc result = CreateAdditionalMethodDesc();
			result.Name += "Dispatch";
			result.DeclaredAccessibility = Symbol.DeclaredAccessibility;
			return result;
		}

		private void GenerateInternalFromDTO(Context context, SourceWriter writer)
		{
			MethodDesc fromDTO = CreateAdditionalMethodDesc();

			List<PropMatch> objRefProps = null;

			using(writer.Method(fromDTO))
			{
				writer.Declare(DTOType.ToString(), "dtoConcrete");
				writer.AppendIndent();
				writer.Append("dtoConcrete = (");
				writer.Append(DTOType.ToString());
				writer.Append(")dto;\n");

				GenerateCopyBody(context, writer, "om", "dtoConcrete", dtoHasId, true, contextAllowUpdateVar, out objRefProps);
			}

			if(objRefProps != null)
			{
				using(writer.Method(CreateResumeMethodDesc()))
				{
					if(dtoHasId)
						GenerateIsUpdate(writer, "dto", contextAllowUpdateVar);
					GenerateRefPropAssignBody(context, writer, "om", "dto", "dbo", dtoHasId, objRefProps);
				}
			}
		}

		private void GenerateTypeMap(SourceWriter writer)
		{
			string funcType = $"System.Func<Velox.ObjectInterface.ObjectModel, {DTOType.ToString()}, Velox.Common.AutomapperThreadContext, int, {Type.FullName}>";

			writer.AppendIndent();
			writer.Append("private static readonly ");
			AppendDictType(writer, funcType);
			writer.Append(" ");
			writer.Append(SourceWriter.VlxPrefix);
			writer.Append(Name);
			writer.Append("Map = new ");
			AppendDictType(writer, funcType);
			writer.Append("();\n");

			using(writer.Method(new MethodDesc(){
				Name = $"RegisterFor{Name}",
				IsStatic = true,
				DeclaredAccessibility = Accessibility.Protected,
				Parameters = new ParamDesc[] {new ParamDesc("System.Type", "type"), new ParamDesc(funcType, "func") }
			}))
			{
				writer.AppendIndent();
				writer.Append(SourceWriter.VlxPrefix);
				writer.Append(Name);
				writer.Append("Map.Add(type, func);\n");
			}
		}

		private void AppendDictType(SourceWriter writer, string funcType)
		{
			writer.Append("System.Collections.Generic.Dictionary<System.Type, ");
			writer.Append(funcType);
			writer.Append(">");
		}

		private void GenerateDispatchMethod(SourceWriter writer)
		{
			MethodDesc dispatchMethod = CreateDispatchMethodDesc();

			string omName = dispatchMethod.Parameters[0].Name;
			string dtoName = dispatchMethod.Parameters[1].Name;

			using(writer.Method(dispatchMethod))
			{
				writer.AppendIndent();
				writer.Append("return ");
				writer.Append(SourceWriter.VlxPrefix);
				writer.Append(Name);
				writer.Append("Map[");
				writer.Append(dtoName);
				writer.Append(".GetType()]");
				writer.Append("(");
				writer.Append(omName);
				writer.Append(", ");
				writer.Append(dtoName);
				writer.Append(", ");
				writer.Append(SourceWriter.ContextVar);
				writer.Append(", ");
				writer.Append(SourceWriter.DepthVar);
				writer.Append(");\n");
			}
		}

		private void GenerateCallInternalMethod(Context context, SourceWriter writer)
		{
			string omName = Symbol.Parameters[0].Name;
			string dtoName = Symbol.Parameters[1].Name;

			TempVar contextVar = writer.CreateTempVar();
			TempVar resumeVar = writer.CreateTempVar();

			writer.AppendIndent();
			writer.Append("var ");
			writer.Append(contextVar);
			writer.Append(" = Velox.Common.AutomapperThreadContext.Instance;\n");

			if(hasAllowUpdate)
			{
				writer.AppendIndent();
				writer.Append("Velox.Common.AutomapperThreadContext.Instance.AllowUpdate = ");
				writer.Append(Symbol.Parameters[2].Name);
				writer.Append(";\n");
			}

			writer.AppendIndent();
			writer.Append("var ");
			writer.Append(SourceWriter.ResultVar);
			writer.Append(" = ");
			if(IsBaseMethod)
			{
				writer.Append(SourceWriter.VlxPrefix);
				writer.Append(Name);
				writer.Append("Map[");
				writer.Append(dtoName);
				writer.Append(".GetType()]");
			}
			else
			{
				writer.Append("(");
				writer.Append(Symbol.ReturnType.ToString());
				writer.Append(")");
				writer.Append(MethodGroup.DeclaringMethod.Type.FullName);
				writer.Append(".");
				writer.Append(SourceWriter.VlxPrefix);
				writer.Append(Name);
				writer.Append("Dispatch");
			}

			writer.Append("(");
			writer.Append(omName);
			writer.Append(", ");
			writer.Append(dtoName);
			writer.Append(", ");
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

		private void GenerateCopyBody(Context context, SourceWriter writer, string omName, string dtoName, bool update, bool isInternal,
									  string allowUpdateVar, out List<PropMatch> objRefProps)
		{

			INamedTypeSymbol dtoType = DTOType;
			INamedTypeSymbol dbType = Symbol.ContainingType;

			if(update)
			{
				GenerateCreateWithUpdate(writer, omName, dtoName, dbType, dtoType, allowUpdateVar);
			}
			else
			{
				GenerateCreateObject(writer, omName);
			}

			if (isInternal)
			{
				writer.AppendIndent();
				writer.Append(SourceWriter.ContextVar);
				writer.Append(".Add(");
				writer.Append(dtoName);
				writer.Append(", ");
				writer.Append(SourceWriter.ResultVar);
				writer.Append(");\n");
			}

			(List<PropMatch> matches, List<PropMatch> objRefPropsTemp) = MapProperties(context);
			objRefProps = objRefPropsTemp;

			for (int i = 0; i < matches.Count; i++)
			{
				PropMatch match = matches[i];
				if (match.DBProperty.Name == "Id")
					continue;

				match.DBProperty.AssignFrom(context, this, writer, omName, dtoName, match.DTOProperty, SourceWriter.ResultVar, update);
			}

			if(objRefPropsTemp != null)
			{
				GenerateRefPropAssign(context, writer, omName, dtoName, update, objRefPropsTemp);
			}

			writer.AppendIndent();
			writer.Append("return ");
			writer.Append(SourceWriter.ResultVar);
			writer.Append(";\n");
		}

		private void GenerateRefPropAssign(Context context, SourceWriter writer, string omName, string dtoName, bool update,
										   List<PropMatch> objRefProps)
		{
			TempVar lambdaTemp = writer.CreateTempVar();
			using(writer.IfMaxDepth())
			{
				GenerateRefPropAssignBody(context, writer, omName, dtoName, SourceWriter.ResultVar, update, objRefProps);
			}

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
			writer.Append(omName);
			writer.Append(", ");
			writer.Append(dtoName);
			writer.Append(", ");
			writer.Append(SourceWriter.ResultVar);
			writer.Append(", ");
			writer.Append(lambdaTemp);
			writer.Append(", 0));\n");
		}

		private void GenerateRefPropAssignBody(Context context, SourceWriter writer, string omName, string dtoName, string dboName,
											   bool update, List<PropMatch> objRefProps)
		{
			for (int i = 0; i < objRefProps.Count; i++)
			{
				PropMatch match = objRefProps[i];
				match.DBProperty.AssignFrom(context, this, writer, omName, dtoName, match.DTOProperty, dboName, update);
			}
		}

		private void GenerateCreateWithUpdate(SourceWriter writer, string omName, string dtoName, ITypeSymbol dbType, ITypeSymbol dtoType,
											  string allowUpdateVar)
		{
			writer.Declare(dbType.ToString(), SourceWriter.ResultVar);
			GenerateIsUpdate(writer, dtoName, allowUpdateVar);

			writer.AppendFormat("if(!{0})\n", IsUpdateVar);
			using (writer.Block())
			{
				writer.AppendFormat("{0} = {1}.CreateObject<{2}>();\n", SourceWriter.ResultVar, omName, dbType.ToString());
			}
			writer.AppendLine("else");
			using (writer.Block())
			{
				writer.AppendFormat("{0} = ({3}.Id == 0)?null:{1}.GetObjectStrict<{2}>({3}.Id);\n", SourceWriter.ResultVar, omName,
									dbType.ToString(), dtoName);
			}
		}

		private static void GenerateIsUpdate(SourceWriter writer, string dtoName, string allowUpdateVar)
		{
			writer.Declare("bool", IsUpdateVar);

			writer.AppendIndent();
			writer.Append(IsUpdateVar);
			writer.Append(" = ");
			writer.Append(dtoName);
			writer.Append(".Id != 0 && ");
			writer.Append(allowUpdateVar);
			writer.Append(";\n");
		}

		private void GenerateCreateObject(SourceWriter writer, string omName)
		{
			writer.AppendFormat("{0} {1} = {2}.CreateObject<{0}>();\n", Symbol.ReturnType.ToString(), SourceWriter.ResultVar, omName);
		}

		private bool HasId(ITypeSymbol dtoType)
		{
			return TryGetProperty("Id", dtoType, out _);
		}

		public static Method Create(Context context, IMethodSymbol methodSym, DBOType type, Polymorphism polymorphism)
		{
			if (methodSym.Name.StartsWith("From"))
			{
				if(methodSym.Parameters.Length < 2 || methodSym.Parameters.Length > 3 )
				{
					Report.InvalidArgumentsForFromMethod(context, methodSym);
					return null;
				}

				if(!methodSym.IsStatic)
				{
					Report.FromMethodMustBeStatic(context, methodSym);
					return null;
				}

				if(!SymEquals(methodSym.Parameters[0].Type, context.Types.ObjectModel))
				{
					Report.InvalidFromArgument(context, methodSym, context.Types.ObjectModel, "first");
					return null;
				}

				if(!SymEquals(methodSym.ContainingType, methodSym.ReturnType))
				{
					Report.ReturnTypeMustBeTheSame(context, methodSym);
					return null;
				}

				ITypeSymbol dtoType = methodSym.Parameters[1].Type;
				if(!CheckDTOType(context, dtoType, methodSym))
				{
					return null;
				}

				if(methodSym.Parameters.Length == 3)
				{
					if(!SymEquals(methodSym.Parameters[2].Type, context.Types.Bool))
						Report.InvalidFromArgument(context, methodSym, context.Types.Bool, "third");
				}

				bool hasReferences = GetHasReferences(context, type, (INamedTypeSymbol)dtoType);

				return new FromDTOMethod(methodSym, type, polymorphism, hasReferences);
			}

			return null;
		}
	}

}
