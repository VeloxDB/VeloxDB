using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace VeloxDB.SourceGenerator
{
	[DiagnosticAnalyzer(LanguageNames.CSharp)]
	public sealed class DbApiAnalyzer : DiagnosticAnalyzer
	{
		public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(Report.GenerateClientInterfaceDesc,
																										   Report.DbAPIClassMustBePublicDesc,
																										   Report.DbAPIOperationMustBePublicDesc);

		public override void Initialize(AnalysisContext context)
		{
			context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
			context.EnableConcurrentExecution();
			context.RegisterCompilationStartAction(roslynContext =>
			{
				INamedTypeSymbol dbApiAttribute = roslynContext.Compilation.GetTypeByMetadataName("VeloxDB.Protocol.DbAPIAttribute");
				INamedTypeSymbol dbApiOperationAttribute = roslynContext.Compilation.GetTypeByMetadataName("VeloxDB.Protocol.DbAPIOperationAttribute");

				if (dbApiAttribute == null || dbApiOperationAttribute == null)
				{
					return;
				}

				roslynContext.RegisterSymbolAction(c=>AnalyzeSymbol(c, dbApiAttribute, dbApiOperationAttribute), SymbolKind.NamedType);
			});
		}

		private static void AnalyzeSymbol(SymbolAnalysisContext roslynContext, INamedTypeSymbol dbApiAttribute, INamedTypeSymbol dbApiOperationAttribute)
		{
			INamedTypeSymbol symbol = (INamedTypeSymbol)roslynContext.Symbol;
			if (symbol.TypeKind != TypeKind.Class)
			{
				return;
			}

			SymContext context = new SymContext(roslynContext);

			if (!symbol.GetAttributes().Any(attr => attr.AttributeClass.Equals(dbApiAttribute, SymbolEqualityComparer.Default)))
			{
				// Not DbAPI class
				return;
			}

			Report.GenerateClientInterface(context, symbol);

			// Check if symbol is public
			if (symbol.DeclaredAccessibility != Accessibility.Public)
			{
				Report.DbAPIClassMustBePublic(context, symbol);
			}

			// Check if all DbAPIOperation methods are public
			foreach (ISymbol member in symbol.GetMembers())
			{
				if (member.Kind != SymbolKind.Method)
				{
					continue;
				}

				//Check for DbAPIOperation attribute
				if (!member.GetAttributes().Any(attr => attr.AttributeClass.Equals(dbApiOperationAttribute, SymbolEqualityComparer.Default)))
				{
					continue;
				}

				IMethodSymbol method = (IMethodSymbol)member;
				if (method.DeclaredAccessibility != Accessibility.Public)
				{
					Report.DbAPIOperationMustBePublic(context, method);
				}
			}

		}
	}
}
