using System;
using System.IO;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace Velox.SourceGenerator
{
	[Generator]
	public sealed class AutoMapperGenerator : ISourceGenerator
	{
		public const int AbsoluteMaxGraphDepth = 200;

		SourceWriter writer;

		public AutoMapperGenerator()
		{
			writer = new SourceWriter();
		}

		public void Execute(GeneratorExecutionContext generatorContext)
		{
			Context context = new Context(generatorContext);
			writer.Initialize(context);
			CancellationToken token = generatorContext.CancellationToken;

			DBOTypeCollection collection = DBOTypeCollection.Create(context);

			if (collection == null)
			{
				return;
			}

			foreach (DBOType type in collection.PartialTypes)
			{
				if (token.IsCancellationRequested)
					throw new OperationCanceledException();

				string source = type.GenerateSource(context, writer);
				generatorContext.AddSource($"{type.FullName}.g.cs", source);
			}
		}

		public void Initialize(GeneratorInitializationContext context)
		{
		}
	}
}
