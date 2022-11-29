using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Operations;
using static Velox.SourceGenerator.Utils;

namespace Velox.SourceGenerator
{
	internal sealed class Context
	{
		GeneratorExecutionContext genctx;

		public ContextTypes Types { get; private set; }
		public CancellationToken CancellationToken => genctx.CancellationToken;
		public IAssemblySymbol Assembly => genctx.Compilation.Assembly;

		public Context(GeneratorExecutionContext generatorContext)
		{
			genctx = generatorContext;
			Types = new ContextTypes(generatorContext.Compilation);
		}

		public void Report(DiagnosticDescriptor descriptor, ISymbol locSymbol, params object[] messageArgs)
		{
			genctx.ReportDiagnostic(Diagnostic.Create(descriptor, GetLocation(locSymbol), messageArgs));
		}

		private Location GetLocation(ISymbol locSymbol)
		{
			if(locSymbol.DeclaringSyntaxReferences.Length == 0)
				return null;

			return locSymbol.DeclaringSyntaxReferences[0].GetSyntax().GetLocation();
		}

		public class ContextTypes
		{
			INamedTypeSymbol autoMapperIgnoreClassAttribute;
			HashSet<ITypeSymbol> simpleTypes;

			private readonly string[] simpleTypeNames = new string[]{
				"System.Byte",
				"System.Int16",
				"System.Int32",
				"System.Int64",
				"System.Single",
				"System.Double",
				"System.Boolean",
				"System.DateTime",
				"System.String"
			};

			public ContextTypes(Compilation compilation)
			{
				CreateSimpleTypes(compilation);

				Void = GetType(compilation, "System.Void");
				Long = GetType(compilation, "System.Int64");
				Bool = GetType(compilation, "System.Boolean");
				Object = GetType(compilation, "System.Object");
				List = GetType(compilation, "System.Collections.Generic.List`1").ConstructUnboundGenericType();
				Enumerable = GetType(compilation, "System.Collections.Generic.IEnumerable`1").ConstructedFrom;

				DatabaseObject = GetType(compilation, "Velox.ObjectInterface.DatabaseObject");
				ObjectModel = GetType(compilation, "Velox.ObjectInterface.ObjectModel");
				DatabaseArray = GetType(compilation, "Velox.ObjectInterface.DatabaseArray`1").ConstructUnboundGenericType();
				ReferenceArray = GetType(compilation, "Velox.ObjectInterface.ReferenceArray`1").ConstructUnboundGenericType();
				InverseReferenceSet = GetType(compilation, "Velox.ObjectInterface.InverseReferenceSet`1").ConstructUnboundGenericType();

				DatabaseClassAttribute = GetType(compilation, "Velox.ObjectInterface.DatabaseClassAttribute");
				DatabasePropertyAttribute = GetType(compilation, "Velox.ObjectInterface.DatabasePropertyAttribute");
				DatabaseReferenceAttribute = GetType(compilation, "Velox.ObjectInterface.DatabaseReferenceAttribute");
				InverseReferencesAttribute = GetType(compilation, "Velox.ObjectInterface.InverseReferencesAttribute");
				SupportPolymorphismAttribute = GetType(compilation, "Velox.ObjectInterface.SupportPolymorphismAttribute");

				autoMapperIgnoreClassAttribute = compilation.GetTypeByMetadataName("Velox.ObjectInterface.AutoMapperIgnoreClassAttribute");
			}

			public bool IsEnumerable(ITypeSymbol type, INamedTypeSymbol elementType)
			{
				INamedTypeSymbol targetType = Enumerable.Construct(elementType);

				for (int i = 0; i < type.AllInterfaces.Length; i++)
				{
					INamedTypeSymbol @interface = type.AllInterfaces[i];
					if (SymEquals(@interface, targetType))
						return true;
				}

				return false;
			}

			public bool IgnoreClass(INamedTypeSymbol type)
			{
				return autoMapperIgnoreClassAttribute != null && HasAttribute(type, autoMapperIgnoreClassAttribute);
			}

			public bool IsSimpleType(INamedTypeSymbol type) => type.TypeKind == TypeKind.Enum || simpleTypes.Contains(type);

			public bool IsDatabaseObject(INamedTypeSymbol type)
			{
				INamedTypeSymbol current = type;

				while (current != null)
				{
					if (Microsoft.CodeAnalysis.SymbolEqualityComparer.Default.Equals(current, DatabaseObject))
						return true;
					current = current.BaseType;
				}

				return false;
			}

			private void CreateSimpleTypes(Compilation compilation)
			{
				simpleTypes = new HashSet<ITypeSymbol>(SymbolEqualityComparer.Default);

				foreach (string name in simpleTypeNames)
				{
					simpleTypes.Add(GetType(compilation, name));
				}
			}

			private static INamedTypeSymbol GetType(Compilation compilation, string fullName)
			{
				INamedTypeSymbol result = compilation.GetTypeByMetadataName(fullName);

				if (result == null)
					throw new InvalidOperationException($"Couldn't find {fullName}");

				return result;
			}

			public INamedTypeSymbol Void { get; }
			public INamedTypeSymbol Long { get; }
			public INamedTypeSymbol Bool { get; }
			public INamedTypeSymbol Object { get; }
			public INamedTypeSymbol List { get; }
			public INamedTypeSymbol Enumerable { get; }

			public INamedTypeSymbol DatabaseObject { get; }
			public INamedTypeSymbol ObjectModel { get; }
			public INamedTypeSymbol DatabaseArray { get;  }
			public INamedTypeSymbol ReferenceArray { get; }
			public INamedTypeSymbol InverseReferenceSet { get; }

			public INamedTypeSymbol DatabaseClassAttribute { get; }
			public INamedTypeSymbol DatabasePropertyAttribute { get; }
			public INamedTypeSymbol DatabaseReferenceAttribute { get; }
			public INamedTypeSymbol InverseReferencesAttribute { get; }
			public INamedTypeSymbol SupportPolymorphismAttribute { get; }
		}

		public bool IsAccessibleFrom(ITypeSymbol from, IMethodSymbol toCheck)
		{
			return genctx.Compilation.IsSymbolAccessibleWithin(toCheck, from);
		}

		public bool IsAssignable(INamedTypeSymbol source, INamedTypeSymbol destination)
		{
			CommonConversion conversion = genctx.Compilation.ClassifyCommonConversion(source, destination);
			return conversion.Exists && conversion.IsImplicit;
		}
	}
}
