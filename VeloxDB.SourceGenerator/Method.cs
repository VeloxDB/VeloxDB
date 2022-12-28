using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;
using Microsoft.CodeAnalysis;

using static VeloxDB.SourceGenerator.Utils;

namespace VeloxDB.SourceGenerator
{
	internal enum Polymorphism
	{
		NotSpecified,
		Enabled
	}
	internal struct PropMatch
	{
		public DBProperty DBProperty { get; set; }
		public IPropertySymbol DTOProperty { get; set; }

		public PropMatch(DBProperty dbProp, IPropertySymbol dtoProp)
		{
			DBProperty = dbProp;
			DTOProperty = dtoProp;
		}
	}

	internal abstract class Method
	{
		public MethodGroup MethodGroup { get; set; }
		public IMethodSymbol Symbol { get; }
		public string Name => Symbol.Name;
		public DBOType Type { get; }
		public Polymorphism Polymorphism { get; private set; }
		public bool HasReferences { get; }
		public bool NeedsInternalMethod => PolymorphismEnabled || HasReferences;

		public abstract INamedTypeSymbol DTOType { get; }

		public Method(IMethodSymbol symbol, DBOType type, Polymorphism polymorphismSupport, bool hasReferences)
		{
			this.Symbol = symbol;
			this.Type = type;
			this.Polymorphism = polymorphismSupport;
			this.HasReferences = hasReferences;
		}

		public void Generate(SourceWriter writer, Context context)
		{
			using(writer.PartialMethod(Symbol))
			{
				GenerateBody(context, writer);
			}

			AdditionalGenerate(context, writer);
		}

		protected abstract void GenerateBody(Context context, SourceWriter writer);

		protected virtual void AdditionalGenerate(Context context, SourceWriter writer)
		{

		}

		private static Method CreateFromSymbol(Context context, IMethodSymbol methodSym, DBOType type)
		{
			Polymorphism polymorphism = GetPolymorphism(context, methodSym);

			if(methodSym.ReturnType.NullableAnnotation == NullableAnnotation.Annotated)
			{
				Report.NullableReturnType(context, methodSym);
				return null;
			}

			Method result = ToDTOMethod.Create(context, methodSym, type, polymorphism);

			if(result == null)
				result = FromDTOMethod.Create(context, methodSym, type, polymorphism);

			return result;
		}

		private static Polymorphism GetPolymorphism(Context context, IMethodSymbol methodSym)
		{
			if(!TryGetAttribute(methodSym, context.Types.SupportPolymorphismAttribute, out var attribute))
				return Polymorphism.NotSpecified;

			return Polymorphism.Enabled;
		}

		public static ImmutableArray<Method> CreatePartialMethods(Context context, DBOType dboType)
		{
			CancellationToken token = context.CancellationToken;

			ImmutableArray<Method>.Builder methods = ImmutableArray.CreateBuilder<Method>();
			HashSet<string> methodSet = new HashSet<string>();

			ImmutableArray<ISymbol> members = dboType.Symbol.GetMembers();
			for (var i = 0; i < members.Length; i++)
			{
				if(token.IsCancellationRequested)
					throw new OperationCanceledException();

				ISymbol memberSym = members[i];

				if(memberSym.Kind != SymbolKind.Method)
					continue;

				IMethodSymbol methodSym = (IMethodSymbol)memberSym;

				if(!ShouldGenerate(methodSym))
					continue;

				if(methodSet.Contains(methodSym.Name))
				{
					Report.OverloadNotSupported(context, methodSym);
					continue;
				}
				else
					methodSet.Add(methodSym.Name);

				Method method = Method.CreateFromSymbol(context, methodSym, dboType);

				if (method != null)
				{
					methods.Add(method);
				}
			}

			return methods.ToImmutable();
		}

		protected (List<PropMatch> matches, List<PropMatch> objRefProps) MapProperties(Context context)
		{
			List<PropMatch> matches = new List<PropMatch>();
			List<PropMatch> objRefProps = null;
			foreach(var match in MapProperties(context, Type, DTOType))
			{
				if(match.DBProperty.IsObjReference(context, match.DTOProperty))
				{
					if(objRefProps == null)
						objRefProps = new List<PropMatch>();

					objRefProps.Add(match);
				}
				else
				{
					matches.Add(match);
				}
			}

			return (matches, objRefProps);
		}

		protected static IEnumerable<PropMatch> MapProperties(Context context, DBOType type, INamedTypeSymbol dtoType)
		{

			var members = GetAllMembers(dtoType, context.Types.Object);
			HashSet<string> matched = new HashSet<string>();

			foreach (var member in members)
			{
				if (member.Kind != SymbolKind.Property)
					continue;

				IPropertySymbol property = (IPropertySymbol)member;

				DBProperty dbProperty = type.GetPropertyByName(property.Name);

				if (dbProperty == null)
					dbProperty = TryMatch(type, property, "Id");

				if (dbProperty == null)
					dbProperty = TryMatch(type, property, "Ids");

				if (dbProperty == null)
				{
					Report.FailedToMatchProperty(context, property);
					continue;
				}

				if(matched.Contains(dbProperty.Name))
				{
					Report.PropertyAlreadyMatched(context, property);
				}
				else
				{
					matched.Add(dbProperty.Name);
				}

				yield return new PropMatch(dbProperty, property);
			}
		}

		protected static bool GetHasReferences(Context context, DBOType type, INamedTypeSymbol dtoType)
		{
			bool result = false;
			foreach (PropMatch match in MapProperties(context, type, dtoType))
			{
				if(match.DBProperty.IsReference && !SymEquals(match.DTOProperty.Type, context.Types.Long))
				{
					result = true;
				}
			}
			return result;
		}

		private static DBProperty TryMatch(DBOType type, IPropertySymbol property, string endsWith)
		{
			DBProperty result = null;
			if (property.Name.EndsWith(endsWith))
			{
				string name = property.Name;
				result = type.GetPropertyByName(name.Substring(0, name.Length - endsWith.Length));
			}

			return result;
		}

		private static bool ShouldGenerate(IMethodSymbol methodSym)
		{
			return methodSym.IsPartialDefinition && methodSym.PartialImplementationPart == null && !methodSym.IsGenericMethod;
		}

		protected static bool CheckDTOType(Context context, ITypeSymbol dtoType, IMethodSymbol methodSym)
		{
			if(dtoType.Kind != SymbolKind.NamedType || ((INamedTypeSymbol)dtoType).TypeKind != TypeKind.Class)
			{
				Report.InvalidDTOType(context, methodSym);
				return false;
			}

			return true;
		}

		protected bool IsBaseMethod => MethodGroup.DeclaringMethod == this;
		protected bool PolymorphismEnabled => MethodGroup.Polymorphism == Polymorphism.Enabled;

		public bool IsDeclaringMethod => PolymorphismEnabled && MethodGroup.DeclaringMethod == this;

		public virtual void AddToStaticConstructor(Context context, SourceWriter writer)
		{
		}
	}
}
