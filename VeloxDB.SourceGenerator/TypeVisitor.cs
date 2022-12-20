using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using Microsoft.CodeAnalysis;

namespace VeloxDB.SourceGenerator
{
	internal class TypeVisitor : SymbolVisitor
	{
		Action<INamedTypeSymbol> onType;
		CancellationToken token;

		public TypeVisitor(Action<INamedTypeSymbol> onType, CancellationToken token)
		{
			this.onType = onType;
			this.token = token;
		}

		private void VisitChildren<T>(IEnumerable<T> children)
			where T : ISymbol
		{
			foreach (var item in children)
			{
				if(token.IsCancellationRequested)
					break;
				item.Accept(this);
			}
		}

		public override void VisitNamedType(INamedTypeSymbol type)
		{
			onType(type);
			VisitChildren(type.GetMembers());
		}

		public override void VisitAssembly(IAssemblySymbol symbol)
		{
			VisitChildren(symbol.Modules);
		}

		public override void VisitModule(IModuleSymbol symbol)
		{
			if(token.IsCancellationRequested)
				return;

			symbol.GlobalNamespace.Accept(this);
		}

		public override void VisitNamespace(INamespaceSymbol symbol)
		{
			VisitChildren(symbol.GetMembers());
		}
	}
}
