using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using Microsoft.CodeAnalysis;

namespace Velox.SourceGenerator
{
	internal class DBOTypeCollection
	{
		ImmutableDictionary<INamedTypeSymbol, DBOType> typeToDBO;

		private DBOTypeCollection(ImmutableArray<DBOType> databaseObjects, ImmutableArray<DBOType> partialTypes,
								  ImmutableArray<DBOType> toValidate, ImmutableDictionary<INamedTypeSymbol, DBOType> typeToDBO)
		{
			this.AllTypes = databaseObjects;
			this.PartialTypes = partialTypes;
			this.WithReferences = toValidate;

			this.typeToDBO = typeToDBO;
		}

		public ImmutableArray<DBOType> PartialTypes { get; }
		public ImmutableArray<DBOType> AllTypes { get; }
		public ImmutableArray<DBOType> WithReferences { get; }

		public static DBOTypeCollection Create(Context context)
		{
			CancellationToken token = context.CancellationToken;

			ImmutableArray<DBOType>.Builder allTypes = ImmutableArray.CreateBuilder<DBOType>();
			ImmutableArray<DBOType>.Builder partialDbTypes = ImmutableArray.CreateBuilder<DBOType>();
			ImmutableArray<DBOType>.Builder withRefs = ImmutableArray.CreateBuilder<DBOType>();

			var typeToDBO = ImmutableDictionary.CreateBuilder<INamedTypeSymbol, DBOType>(SymbolEqualityComparer.Default);

			DBOType OnType(INamedTypeSymbol type)
			{
				DBOType dboType;
				if(typeToDBO.TryGetValue(type, out dboType))
					return dboType;

				dboType = DBOType.Create(type, context);

				if(dboType == null)
					return null;

				typeToDBO.Add(type, dboType);

				if (type.BaseType != null)
				{
					DBOType parent = OnType(type.BaseType);
					if(parent != null)
						dboType.Parent = parent;
				}

				allTypes.Add(dboType);

				if (dboType.IsPartial)
				{
					partialDbTypes.Add(dboType);
				}

				if (dboType.ReferencesOtherTypes)
				{
					withRefs.Add(dboType);
				}

				return dboType;
			}

			void OnTypeNoReturn(INamedTypeSymbol type)
			{
				OnType(type);
			}

			TypeVisitor visitor = new TypeVisitor(OnTypeNoReturn, token);
			context.Assembly.Accept(visitor);

			DBOTypeCollection result = new DBOTypeCollection(allTypes.ToImmutable(), partialDbTypes.ToImmutable(), withRefs.ToImmutable(),
										 typeToDBO.ToImmutable());

			result.Prepare(context);
			return result;
		}

		private void Prepare(Context context)
		{
			for (int i = 0; i < WithReferences.Length; i++)
				WithReferences[i].PrepareReferences(context, this);

			for (int i = 0; i < AllTypes.Length; i++)
				AllTypes[i].PreparePolymorphism(context);
		}

		public bool TryGet(INamedTypeSymbol referenceType, out DBOType dboType)
		{
			return typeToDBO.TryGetValue(referenceType, out dboType);
		}
	}
}
