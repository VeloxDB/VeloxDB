using System.Linq;
using Microsoft.CodeAnalysis;
using System.Collections.Generic;

namespace VeloxDB.AspNet.ControllerGenerator
{
	class ControllerGeneratorContext
	{
		private GeneratorExecutionContext context;
		public KnownTypes KnownTypes { get; }
		public PropertyCache Cache { get; }
		public IEnumerable<string> ReferencedAssemblyNames => context.Compilation.ReferencedAssemblyNames.Select(a => a.Name);

		public ControllerGeneratorContext(GeneratorExecutionContext executionContext, KnownTypes knownTypes, PropertyCache cache)
		{
			this.context = executionContext;
			this.KnownTypes = knownTypes;
			this.Cache = cache;
		}

		public void ReportDiagnostic(DiagnosticDescriptor desc, Location location, params object[] messageArgs)
		{
			context.ReportDiagnostic(Diagnostic.Create(desc, location, messageArgs));
		}
	}
}