using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace VeloxDB.SourceGenerator
{
	internal sealed class ParamDesc
	{
		public string Name { get; set; }
		public string Type { get; set; }

		public ParamDesc(string type, string name)
		{
			Name = name;
			Type = type;
		}
	}

	internal sealed class MethodDesc
	{
		public string Name { get; set; }
		public string NamePrefix { get; set; }
		public bool IsStatic { get; set; }
		public string ReturnType { get; set; }
		public ParamDesc[] Parameters { get; set; }
		public Accessibility DeclaredAccessibility { get; set; }
		public bool IsNew { get; set; }
		public MethodDesc()
		{
			NamePrefix = SourceWriter.VlxPrefix;
			IsStatic = false;
			ReturnType = "void";
			Parameters = new ParamDesc[0];
			DeclaredAccessibility = Accessibility.Internal;
			IsNew = false;
		}
	}

	internal struct TempVar
	{
		public int Id { get; set; }

		public TempVar(int id)
		{
			Id = id;
		}

		public override string ToString()
		{
			return $"{SourceWriter.TempVarBase}{Id}";
		}
	}

	internal sealed class SourceWriter
	{

		private class Scope : IDisposable
		{
			private SourceWriter sourceWriter;

			public Scope(SourceWriter sourceWriter)
			{
				this.sourceWriter = sourceWriter;
			}

			public void Dispose()
			{
				sourceWriter.DecreaseIndent();
				sourceWriter.AppendLine("}");
			}
		}

		public void Initialize(Context context)
		{
			csTypeMap = new Dictionary<ITypeSymbol, string>(SymbolEqualityComparer.Default);

			INamedTypeSymbol voidType = context.Types.Void;
			csTypeMap.Add(voidType, voidType.ToString());
		}

		StringBuilder builder;
		int indent;
		Stack<int> tempVarStack;
		int tempVarId;
		Dictionary<ITypeSymbol, string> csTypeMap;
		private const int maxIndent = 16;
		public const string VlxPrefix = "zzVlx";
		public const string CounterVar = VlxPrefix + "Counter";
		public const string TempVarBase = VlxPrefix + "Temp";
		public const string ContextVar = VlxPrefix + "Context";
		public const string DepthVar = VlxPrefix + "Depth";
		public const string ResultVar = VlxPrefix + "Result";
		static readonly char[] indentChars;

		static SourceWriter()
		{
			indentChars = new char[maxIndent];
			for (int i = 0; i < maxIndent; i++)
			{
				indentChars[i] = '\t';
			}
		}

		public SourceWriter()
		{
			builder = new StringBuilder(4192);
			tempVarStack = new Stack<int>(maxIndent);
		}

		public void Assign(string obj1, string member1, string obj2, string member2, bool obj2Nullable = false)
		{
			AppendIndent();
			builder.Append(obj1);
			builder.Append(".");
			builder.Append(member1);
			builder.Append(" = ");

			builder.Append(obj2);
			if(obj2Nullable)
				builder.Append("?");
			builder.Append(".");
			builder.Append(member2);
			builder.AppendLine(";");
		}

		public void Assign(string variable, string value)
		{
			AppendIndent();
			builder.Append(variable);
			builder.Append(" = ");
			builder.Append(value);
			builder.AppendLine(";");
		}

		public void GetObject(string omName, string dbType, TempVar temp)
		{
			AppendIndent();
			builder.Append("var ");
			Append(temp);
			builder.Append(" = (id == 0)?null:");
			builder.Append(omName);
			builder.Append(".");
			builder.Append("GetObjectStrict<");
			builder.Append(dbType);
			builder.AppendLine(">(id);");
		}

		public void GetId(string dtoObj, string dtoMember)
		{
			AppendIndent();
			builder.Append("long id = ");
			builder.Append(dtoObj);
			builder.Append(".");
			builder.Append(dtoMember);
			builder.Append("[");
			builder.Append(CounterVar);
			builder.AppendLine("];");
		}

		public IDisposable For(string dtoObj, string dtoMember, string countProp)
		{
			AppendIndent();
			builder.Append("for (int ");
			builder.Append(CounterVar);
			builder.Append(" = 0; ");
			builder.Append(CounterVar);
			builder.Append(" < ");
			builder.Append(dtoObj);
			builder.Append(".");
			builder.Append(dtoMember);
			builder.Append(".");
			builder.Append(countProp);
			builder.Append("; ");
			builder.Append(CounterVar);
			builder.AppendLine("++)");

			return Block();
		}

		public IDisposable IfNull(string obj, string prop)
		{
			AppendIndent();
			builder.Append("if(");
			builder.Append(obj);
			builder.Append(".");
			builder.Append(prop);
			builder.AppendLine(" != null)");
			return Block();
		}

		public void Declare(string type, string name)
		{
			AppendIndent();
			builder.Append(type);
			builder.Append(" ");
			builder.Append(name);
			builder.AppendLine(";");
		}

		public void Declare(string type, TempVar t)
		{
			AppendIndent();
			builder.Append(type);
			builder.Append(" ");
			Append(t);
			builder.Append(";\n");
		}

		public TempVar CreateTempVar()
		{
			return new TempVar(tempVarId++);
		}

		public void AssignId(string obj1, string member1, string member2)
		{
			AppendIndent();
			builder.Append(obj1);
			builder.Append(".");
			builder.Append(member1);
			builder.Append(" = (");
			builder.Append(member2);
			builder.Append(" == null)?0:");
			builder.Append(member2);
			builder.Append(".");
			builder.Append("Id");
			builder.AppendLine(";");
		}

		public IDisposable IfMaxDepth()
		{
			AppendIndent();
			Append("if(");
			Append(SourceWriter.DepthVar);
			Append(" < ");
			Append(AutoMapperGenerator.AbsoluteMaxGraphDepth);
			Append(")\n");
			return Block();
		}

		public void CopyToArray(string srcProp, string dstObj, string dstProp)
		{
			IterateThrough(srcProp);

			IncreaseIndent();
			AppendIndent();
			builder.Append(dstObj);
			builder.Append(".");
			builder.Append(dstProp);
			builder.Append("[");
			builder.Append(CounterVar);
			builder.Append("] = ");
			builder.Append(srcProp);
			builder.Append("[");
			builder.Append(CounterVar);
			builder.AppendLine("].Id;");
			DecreaseIndent();
		}

		public void IfBoolClear(string boolVar, string objName, string propName)
		{
			AppendIndent();
			builder.Append("if(");
			builder.Append(boolVar);
			builder.Append(")\n");

			IncreaseIndent();
			AppendIndent();
			builder.Append(objName);
			builder.Append(".");
			builder.Append(propName);
			builder.Append(".Clear();\n");
			DecreaseIndent();
		}

		public void CopyToList(string srcProp, string dstObj, string dstProp)
		{
			IterateThrough(srcProp);

			IncreaseIndent();
			AppendIndent();
			builder.Append(dstObj);
			builder.Append(".");
			builder.Append(dstProp);
			builder.Append(".Add(");
			builder.Append(srcProp);
			builder.Append("[");
			builder.Append(CounterVar);
			builder.AppendLine("].Id);");
			DecreaseIndent();
		}

		internal void DequeueLoop(TempVar contextVar)
		{
			TempVar resumeVar = CreateTempVar();
			AppendIndent();
			Append("while(");
			Append(contextVar);
			Append(".Dequeue(out var ");
			Append(resumeVar);
			Append("))\n");

			AppendIndent();
			Append("\t");
			Append(resumeVar);
			Append("(");
			Append(contextVar);
			Append(");\n");
		}

		public void IterateThrough(string srcProp)
		{
			AppendIndent();
			builder.Append("for (int ");
			builder.Append(CounterVar);
			builder.Append(" = 0; ");
			builder.Append(CounterVar);
			builder.Append(" < ");
			builder.Append(srcProp);
			builder.Append(".Count; ");
			builder.Append(CounterVar);
			builder.AppendLine("++)");
		}

		public void Clear()
		{
			indent = 0;
			builder.Clear();
		}

		public string GetResult()
		{
			if (indent != 0)
				throw new InvalidOperationException("indent is not 0");

			return builder.ToString();
		}

		public IDisposable Namespace(string containingNamespace)
		{
			if(containingNamespace == null)
				return null;

			AppendIndent();
			builder.Append("namespace ");
			builder.AppendLine(containingNamespace);

			return Block();
		}

		public IDisposable Block()
		{
			AppendLine("{");
			IncreaseIndent();
			return new Scope(this);
		}

		private void IncreaseIndent()
		{
			if(indent + 1 > maxIndent)
				throw new InvalidOperationException("Max indent exceeded.");

			tempVarStack.Push(tempVarId);
			indent++;
		}

		private void DecreaseIndent()
		{
			if(indent - 1 < 0)
				throw new InvalidOperationException("Indent is less than zero.");

			tempVarId = tempVarStack.Pop();
			indent--;
		}

		public void Using(string ns)
		{
			AppendIndent();
			builder.Append("using ");
			builder.Append(ns);
			builder.AppendLine(";");
		}

		public IDisposable PartialClass(DBOType containingType)
		{
			AppendIndent();
			builder.Append("partial class ");
			builder.AppendLine(containingType.Name);
			return Block();
		}

		public IDisposable Method(MethodDesc method, string modifier = null)
		{
			AppendLine("[System.ComponentModel.EditorBrowsableAttribute(System.ComponentModel.EditorBrowsableState.Never)]");

			AppendIndent();
			builder.Append(ToCS(method.DeclaredAccessibility));

			if(method.IsStatic)
				builder.Append(" static");

			if (modifier != null)
			{
				builder.Append(" ");
				builder.Append(modifier);
			}

			if(method.IsNew)
				builder.Append(" new");

			builder.Append(" ");
			builder.Append(method.ReturnType);

			builder.Append(" ");
			builder.Append(method.NamePrefix);
			builder.Append(method.Name);
			builder.Append("(");

			for (int i = 0; i < method.Parameters.Length; i++)
			{
				builder.Append(method.Parameters[i].Type);
				builder.Append(" ");
				builder.Append(method.Parameters[i].Name);

				if(i != method.Parameters.Length - 1)
					builder.Append(", ");
			}

			builder.Append(")\n");

			return Block();
		}

		public IDisposable PartialMethod(IMethodSymbol method)
		{
			AppendIndent();
			builder.Append(ToCS(method.DeclaredAccessibility));
			if(method.IsStatic)
				builder.Append(" static");

			if(IsNew(method))
				builder.Append(" new");

			builder.Append(" partial ");
			builder.Append(ToCS(method.ReturnType));
			builder.Append(" ");
			builder.Append(method.Name);
			builder.Append("(");

			for (int i = 0; i < method.Parameters.Length; i++)
			{
				builder.Append(method.Parameters[i].Type.ToString());
				builder.Append(" ");
				builder.Append(method.Parameters[i].Name);

				if(i != method.Parameters.Length - 1)
					builder.Append(", ");
			}

			builder.Append(")\n");
			return Block();
		}

		private bool IsNew(IMethodSymbol method)
		{
			if(method.DeclaringSyntaxReferences.Length == 0)
				return false;

			MethodDeclarationSyntax syntax = (MethodDeclarationSyntax)method.DeclaringSyntaxReferences[0].GetSyntax();

			for (var i = 0; i < syntax.Modifiers.Count; i++)
			{
				SyntaxToken modifier = syntax.Modifiers[i];

				if(modifier.IsKind(SyntaxKind.NewKeyword))
					return true;
			}

			return false;
		}

		public void Throw(string exc)
		{
			AppendFormat("throw new {0}();\n", exc);
		}

		public void Throw(string exc, string message)
		{
			AppendFormat("throw new {0}(\"{1}\");\n", exc, message);
		}

		public void AppendLine(string line)
		{
			AppendIndent();
			builder.AppendLine(line);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void AppendIndent()
		{
			builder.Append(indentChars, 0, indent);
		}

		public void Append(string s)
		{
			builder.Append(s);
		}

		public void Append(int i)
		{
			builder.Append(i);
		}

		public void Append(TempVar t)
		{
			builder.Append(TempVarBase);
			builder.Append(t.Id);
		}

		public void AppendFormat(string format, object arg0)
		{
			AppendIndent();
			builder.AppendFormat(format, arg0);
		}

		public void AppendFormat(string format, object arg0, object arg1)
		{
			AppendIndent();
			builder.AppendFormat(format, arg0, arg1);
		}

		public void AppendFormat(string format, object arg0, object arg1, object arg2)
		{
			AppendIndent();
			builder.AppendFormat(format, arg0, arg1, arg2);
		}

		public void AppendFormat(string format, params object[] args)
		{
			AppendIndent();
			builder.AppendFormat(format, args);
		}

		private static string ToCS(Accessibility acc)
		{
			switch(acc)
			{
				case Accessibility.Private:
					return "private";
				case Accessibility.ProtectedAndInternal:
					return "private protected";
				case Accessibility.Protected:
					return "protected";
				case Accessibility.Internal:
					return "internal";
				case Accessibility.ProtectedOrInternal:
					return "protected internal";
				case Accessibility.Public:
					return "public";
			}

			throw new NotSupportedException();
		}

		private string ToCS(ITypeSymbol typeSym)
		{
			string result;
			if(!csTypeMap.TryGetValue(typeSym, out result))
			{
				result = typeSym.ToString();
			}

			return result;
		}

	}
}
