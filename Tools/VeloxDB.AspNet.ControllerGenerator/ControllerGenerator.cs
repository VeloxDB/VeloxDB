using System;
using System.Linq;
using System.Collections.Immutable;
using System.Threading;
using Microsoft.CodeAnalysis;
using System.Collections.Generic;
using static VeloxDB.AspNet.ControllerGenerator.Utils;
using System.Text;

namespace VeloxDB.AspNet.ControllerGenerator
{
	[Generator]
	public class ControllerGenerator : ISourceGenerator
	{
		KnownTypes knownTypes;

		private static readonly string[] requiredAssemblies = new string[]{ "vlxc", "vlxasp" };

		public void Execute(GeneratorExecutionContext executionContext)
		{
			knownTypes = KnownTypes.Create(executionContext.Compilation);
			if(knownTypes == null)
				return;

			if(!CheckReferencedAssemblies(executionContext))
				return;

			ImmutableArray<INamedTypeSymbol> dbApis = GetAllDbApiInterfaces(executionContext.Compilation.Assembly, executionContext.CancellationToken);

			StringBuilder builder = new StringBuilder();
			PropertyCache cache = new PropertyCache();
			ControllerGeneratorContext context = new ControllerGeneratorContext(executionContext, knownTypes, cache);

			foreach(INamedTypeSymbol api in dbApis)
			{
				builder.AppendClass(api, context);
			}

			executionContext.AddSource("Controllers.cs", builder.ToString());
		}

		private bool CheckReferencedAssemblies(GeneratorExecutionContext context)
		{
			HashSet<string> set = new HashSet<string>(requiredAssemblies);
			set.ExceptWith(context.Compilation.ReferencedAssemblyNames.Select(a=>a.Name));

			if(set.Count != 0)
			{
				Report.MissingRequiredReference(context, set);
				return false;
			}

			return true;
		}

		private ImmutableArray<INamedTypeSymbol> GetAllDbApiInterfaces(IAssemblySymbol assembly, CancellationToken token)
		{
			ImmutableArray<INamedTypeSymbol>.Builder builder = ImmutableArray.CreateBuilder<INamedTypeSymbol>();

			void OnType(INamedTypeSymbol type)
			{
				if(type.TypeKind != TypeKind.Interface)
					return;

				if(!HasAttribute(type, knownTypes.DbAPIAttribute) || HasAttribute(type, knownTypes.DoNotGenerateControllerAttribute))
					return;

				builder.Add(type);
			}

			TypeVisitor visitor = new TypeVisitor(OnType, token);
			assembly.Accept(visitor);

			return builder.ToImmutable();
		}

		public void Initialize(GeneratorInitializationContext context)
		{

		}
	}
}