using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Composition;
using System.Text;
using System;
using Microsoft.CodeAnalysis.Text;
using System.Collections.Generic;

namespace VeloxDB.SourceGenerator
{
	[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(DbApiAnalyzerCodeFixProvider)), Shared]
	public sealed class DbApiAnalyzerCodeFixProvider : CodeFixProvider
	{
		private const string generateAsyncClientTitle = "Generate async client interface";
		private const string generateClientTitle = "Generate client interface";
		private const string makeClassPublicTitle = "Make class public";
		private const string GenerateClientInterfaceId = "VLX036";
		private const string DbAPIClassMustBePublicId = "VLX037";
		private const string DbAPIOperationMustBePublicId = "VLX038";
		private const string makeMethodPublicTitle = "Make method public";

		public override ImmutableArray<string> FixableDiagnosticIds
		{
			get { return ImmutableArray.Create(GenerateClientInterfaceId, DbAPIClassMustBePublicId, DbAPIOperationMustBePublicId); }
		}

		public override async Task RegisterCodeFixesAsync(CodeFixContext context)
		{
			SyntaxNode root = await context.Document.GetSyntaxRootAsync(context.CancellationToken).ConfigureAwait(false);

			foreach (Diagnostic diagnostic in context.Diagnostics)
			{
				TextSpan diagnosticSpan = diagnostic.Location.SourceSpan;

				if (diagnostic.Id == GenerateClientInterfaceId)
				{
					ClassDeclarationSyntax declaration = root.FindToken(diagnosticSpan.Start).Parent.AncestorsAndSelf().OfType<ClassDeclarationSyntax>().First();
					ImmutableArray<CodeAction> actions = new CodeAction[]
					{
						CodeAction.Create(
							title: generateAsyncClientTitle,
							createChangedSolution: c => GenerateAsyncInterfaceAsync(context.Document, declaration, c),
							equivalenceKey: generateAsyncClientTitle),
						CodeAction.Create(
							title: generateClientTitle,
							createChangedSolution: c => GenerateInterfaceAsync(context.Document, declaration, c),
							equivalenceKey: generateClientTitle),
					}.ToImmutableArray();

					context.RegisterCodeFix(
					CodeAction.Create("Create Client interface", actions, true),
					diagnostic);
				}
				else if(diagnostic.Id == DbAPIClassMustBePublicId)
				{
					ClassDeclarationSyntax declaration = root.FindToken(diagnosticSpan.Start).Parent.AncestorsAndSelf().OfType<ClassDeclarationSyntax>().First();
					context.RegisterCodeFix(
						CodeAction.Create(makeClassPublicTitle, c => MakeClassPublic(context.Document, declaration, c), makeClassPublicTitle),
						diagnostic);
				}
				else if(diagnostic.Id == DbAPIOperationMustBePublicId)
				{
					MethodDeclarationSyntax declaration = root.FindToken(diagnosticSpan.Start).Parent.AncestorsAndSelf().OfType<MethodDeclarationSyntax>().First();
					context.RegisterCodeFix(
						CodeAction.Create(makeMethodPublicTitle, c => MakeMethodPublic(context.Document, declaration, c), makeMethodPublicTitle),
						diagnostic);
				}
				else
				{
					throw new NotSupportedException($"Diagnostic id {diagnostic.Id} is not supported");
				}
			}

		}

		private Task<Document> MakeClassPublic(Document document, ClassDeclarationSyntax declaration, CancellationToken c)
		{
			return MakePublic(document, declaration, declaration.Modifiers, declaration.WithModifiers, c);
		}

		private Task<Document> MakeMethodPublic(Document document, MethodDeclarationSyntax declaration, CancellationToken c)
		{
			return MakePublic(document, declaration, declaration.Modifiers, declaration.WithModifiers, c);
		}

		private async Task<Document> MakePublic(Document document, SyntaxNode node, SyntaxTokenList modifiers, Func<SyntaxTokenList, SyntaxNode> withModifiers, CancellationToken c)
		{
			SyntaxNode newDeclaration = withModifiers(MakePublic(modifiers));
			SyntaxNode root = await document.GetSyntaxRootAsync(c);
			SyntaxNode newRoot = root.ReplaceNode(node, newDeclaration);
			Document newDocument = document.WithSyntaxRoot(newRoot);
			return newDocument;
		}

		private bool IsAccessModifier(SyntaxToken token)
		{
			return token.IsKind(SyntaxKind.PublicKeyword) || token.IsKind(SyntaxKind.ProtectedKeyword) || token.IsKind(SyntaxKind.InternalKeyword) || token.IsKind(SyntaxKind.PrivateKeyword);
		}

		private SyntaxTokenList MakePublic(SyntaxTokenList list)
		{
			List<SyntaxToken> tokens = new List<SyntaxToken>(list.Count+1);
			tokens.Add(SyntaxFactory.Token(SyntaxKind.PublicKeyword));

			foreach (SyntaxToken token in list)
			{
				if (!IsAccessModifier(token))
				{
					tokens.Add(token);
				}
			}

			return SyntaxFactory.TokenList(tokens);
		}

		private async Task<Solution> GenerateAsyncInterfaceAsync(Document document, ClassDeclarationSyntax declaration, CancellationToken c)
		{
			return await GenerateInterfaceAsyncInternal(document, declaration, c, true);
		}

		private async Task<Solution> GenerateInterfaceAsync(Document document, ClassDeclarationSyntax typeDecl, CancellationToken cancellationToken)
		{
			return await GenerateInterfaceAsyncInternal(document, typeDecl, cancellationToken, false);
		}

		private async Task<Solution> GenerateInterfaceAsyncInternal(Document document, ClassDeclarationSyntax typeDecl, CancellationToken cancellationToken, bool @async)
		{
			var identifierToken = typeDecl.Identifier;
			var interfaceName = "I" + identifierToken.Text;

			SemanticModel semanticModel = await document.GetSemanticModelAsync(cancellationToken);
			INamedTypeSymbol namedTypeSymbol = semanticModel.GetDeclaredSymbol(typeDecl);

			AttributeData attribute;
			if (!TryGetAttribute(namedTypeSymbol, dbAPIAttributeName, out attribute))
			{
				throw new InvalidOperationException("Missing DBAPI attribute");
			}

			AttributeSyntax newAttributeSyntax = CreateDbAPIAttribute(namedTypeSymbol, attribute);

			InterfaceDeclarationSyntax interfaceDeclaration = SyntaxFactory.InterfaceDeclaration(interfaceName)
				.AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
				.AddAttributeLists(
					SyntaxFactory.AttributeList(
						SyntaxFactory.SingletonSeparatedList(
							newAttributeSyntax
				)));

			//Iterate over the methods of the class namedTypeSymbol with DBOperationAttribute and add them to the interface
			ImmutableArray<ISymbol> members = namedTypeSymbol.GetMembers();
			foreach (ISymbol member in members)
			{
				if (member.Kind != SymbolKind.Method)
					continue;

				IMethodSymbol method = (IMethodSymbol)member;

				bool hasDbApiOpAttrib = false;
				List<AttributeListSyntax> attributeLists = new List<AttributeListSyntax>();
				foreach(AttributeData current in member.GetAttributes())
				{
					if (CheckType(current.AttributeClass, dbAPIOperationAttributeName))
					{
						hasDbApiOpAttrib = true;
					}
					else if (!CheckType(current.AttributeClass, dbAPIOperationErrorAttributeName))
					{
						//Skip unknown attribute
						continue;
                    }
					
					SeparatedSyntaxList<AttributeSyntax> clone = SyntaxFactory.SingletonSeparatedList((AttributeSyntax)current.ApplicationSyntaxReference.GetSyntax());
					attributeLists.Add(SyntaxFactory.AttributeList(clone));
				}
				
				if (!hasDbApiOpAttrib)
					continue;

				MethodDeclarationSyntax methodSyntax = (MethodDeclarationSyntax)method.DeclaringSyntaxReferences[0].GetSyntax(cancellationToken);

				TypeSyntax returnType = methodSyntax.ReturnType;
				if (@async)
				{
					if (method.ReturnsVoid)
					{
						returnType = SyntaxFactory.IdentifierName("DatabaseTask");
					}
					else
					{
						returnType = SyntaxFactory.GenericName(SyntaxFactory.Identifier("DatabaseTask"))
							.WithTypeArgumentList(
								SyntaxFactory.TypeArgumentList(
									SyntaxFactory.SingletonSeparatedList(
										methodSyntax.ReturnType
									)
								)
							);
					}
				}


				MethodDeclarationSyntax newMethod = SyntaxFactory.MethodDeclaration(
					returnType,
					method.Name
				);

				newMethod = newMethod.AddAttributeLists(attributeLists.ToArray());

				foreach (IParameterSymbol parameter in method.Parameters)
				{
					if (parameter.Type.Kind == SymbolKind.NamedType && CheckType((INamedTypeSymbol)parameter.Type, objectModel))
					{
						continue;
					}

					ParameterSyntax parameterSyntax = (ParameterSyntax)parameter.DeclaringSyntaxReferences[0].GetSyntax(cancellationToken);
					newMethod = newMethod.AddParameterListParameters(
						SyntaxFactory.Parameter(
							SyntaxFactory.Identifier(parameter.Name)
						)
						.WithType(parameterSyntax.Type)
					);
				}

				newMethod = newMethod.WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken));

				interfaceDeclaration = interfaceDeclaration.AddMembers(newMethod);
			}

			var root = await document.GetSyntaxRootAsync(cancellationToken);
			var newRoot = root.InsertNodesBefore(typeDecl, new[] { interfaceDeclaration });

			var newDocument = document.WithSyntaxRoot(newRoot);
			return newDocument.Project.Solution;
		}

		private AttributeSyntax CreateDbAPIAttribute(INamedTypeSymbol namedTypeSymbol, AttributeData attribute)
		{
			var newAttributeSyntax = SyntaxFactory.Attribute(SyntaxFactory.IdentifierName("DbAPI"));

			if (attribute.NamedArguments.Length == 0)
			{
				string fullName = GetFullName(namedTypeSymbol);
				newAttributeSyntax = WithName(fullName, newAttributeSyntax);
			}
			else
			{
				var originalAttributeSyntax = (AttributeSyntax)attribute.ApplicationSyntaxReference.GetSyntax();
				var originalArguments = originalAttributeSyntax.ArgumentList.Arguments;

				newAttributeSyntax = newAttributeSyntax.WithArgumentList(
					SyntaxFactory.AttributeArgumentList(originalArguments)
				);
			}

			return newAttributeSyntax;
		}

		private AttributeSyntax WithName(string name, AttributeSyntax attribute)
		{
			return attribute.WithArgumentList(
				SyntaxFactory.AttributeArgumentList(
					SyntaxFactory.SingletonSeparatedList(
						SyntaxFactory.AttributeArgument(
							SyntaxFactory.NameEquals("Name"),
							null,
							SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(name))
						)
					)
				)
			);
		}

		private string GetFullName(INamedTypeSymbol namedTypeSymbol)
		{
			StringBuilder builder = new StringBuilder();

			void BuildNamespace(INamespaceSymbol ns)
			{
				if (ns.IsGlobalNamespace)
					return;

				BuildNamespace(ns.ContainingNamespace);
				if (builder.Length > 0)
					builder.Append('.');
				builder.Append(ns.Name);
			}

			BuildNamespace(namedTypeSymbol.ContainingNamespace);
			if (builder.Length > 0)
				builder.Append('.');

			builder.Append(namedTypeSymbol.Name);
			return builder.ToString();
		}

		private bool TryGetAttribute(ISymbol namedTypeSymbol, string[] typeName, out AttributeData attribute)
		{
			attribute = null;
			foreach (AttributeData current in namedTypeSymbol.GetAttributes())
			{
				if (CheckType(current.AttributeClass, typeName))
				{
					attribute = current;
					return true;
				}
			}
			return false;
		}

		private readonly string[] objectModel = new string[] { "VeloxDB", "ObjectInterface", "ObjectModel" };
		private readonly string[] dbAPIAttributeName = new string[] { "VeloxDB", "Protocol", "DbAPIAttribute" };
		private readonly string[] dbAPIOperationAttributeName = new string[] { "VeloxDB", "Protocol", "DbAPIOperationAttribute" };
		private readonly string[] dbAPIOperationErrorAttributeName = new string[] { "VeloxDB", "Protocol", "DbAPIOperationErrorAttribute" };

		private static bool CheckType(INamedTypeSymbol type, string[] fullName)
		{
			ISymbol current = type;

			for (int i = fullName.Length - 1; i >= 0; i--)
			{
				if (current == null)
					return false;

				if (current.Name != fullName[i])
					return false;

				current = current.ContainingNamespace;
			}

			return true;
		}

		public override FixAllProvider GetFixAllProvider()
		{
			return null;
		}
	}
}
