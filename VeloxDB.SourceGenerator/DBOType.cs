using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using static VeloxDB.SourceGenerator.Utils;

namespace VeloxDB.SourceGenerator
{

	internal class DBOType
	{
		ImmutableDictionary<string, DBProperty> propByName;
		ImmutableDictionary<string, Method> methodByName;
		ImmutableDictionary<string, MethodGroup> methodGroups;

		ImmutableArray<Method> methods;

		bool prepared;

		private DBOType(string ns, string fullName, bool partial, ImmutableArray<DBProperty> properties, ImmutableArray<DBProperty> refProps, INamedTypeSymbol type)
		{
			prepared = false;
			Namespace = ns;
			FullName = fullName;
			Properties = properties;
			ReferencingProperties = refProps;
			Symbol = type;
			IsPartial = partial;

			propByName = properties.ToImmutableDictionary(prop => prop.Name, prop => prop);
		}

		public string FullName { get; }
		public string Namespace { get; }
		public bool IsPartial { get; }
		public ImmutableArray<DBProperty> Properties { get; }
		public ImmutableArray<DBProperty> ReferencingProperties { get; }
		public INamedTypeSymbol Symbol { get; }
		public string Name => Symbol.Name;
		public ImmutableArray<Method> PartialMethods
		{
			get => methods;
			private set
			{
				methods = value;
				methodByName = value.ToImmutableDictionary(m => m.Name);
			}
		}
		public bool ReferencesOtherTypes => ReferencingProperties.Length > 0;
		public DBOType Parent { get; set; }

		public static DBOType Create(INamedTypeSymbol type, Context context)
		{
			if(context.Types.IgnoreClass(type))
				return null;

			if(type.DeclaringSyntaxReferences.Length == 0)
				return null;

			bool isDatabaseClass = IsDatabaseClass(type, context);
			if (isDatabaseClass != context.Types.IsDatabaseObject(type))
			{
				Report.ClassMustBeBoth(context, type);
				return null;
			}

			if (!isDatabaseClass)
				return null;

			if (type.ContainingNamespace == null)
			{
				Report.ClassWithoutNamespace(context, type);
				return null;
			}

			if(type.IsGenericType)
			{
				Report.GenericClassNotSupported(context, type);
				return null;
			}

			if(!type.IsAbstract)
			{
				Report.DatabaseClassIsNotAbstract(context, type);
			}

			bool partial = GetIsPartial(context, type);

			string ns = FullNamespace(type.ContainingNamespace);
			string fullName = type.ToString();

			var properties = CreateProperties(context, type);

			DBOType result = new DBOType(ns, fullName, partial, properties.Properties, properties.ReferencingProperties, type);
			ImmutableArray<Method> methods = Method.CreatePartialMethods(context, result);

			result.PartialMethods = methods;
			return result;
		}

		public void PreparePolymorphism(Context context)
		{
			if (prepared)
				return;

			prepared = true;

			if (Parent != null)
				Parent.PreparePolymorphism(context);

			ImmutableDictionary<string, MethodGroup> parentMethodGroups = (Parent != null) ? Parent.methodGroups : ImmutableDictionary<string, MethodGroup>.Empty;
			var builder = ImmutableDictionary.CreateBuilder<string, MethodGroup>();

			var matched = AssignToMethodGroups(context, parentMethodGroups, builder);

			if (matched < parentMethodGroups.Count)
			{
				ReportMissingPolymorphMethods(context, parentMethodGroups);
			}

			if (builder.Count > 0)
			{
				builder.AddRange(parentMethodGroups);
				methodGroups = builder.ToImmutable();
			}
			else
			{
				methodGroups = parentMethodGroups;
			}
		}

		private void ReportMissingPolymorphMethods(Context context, ImmutableDictionary<string, MethodGroup> parentMethodGroups)
		{
			foreach (var group in parentMethodGroups.Values)
			{
				if (methodByName.ContainsKey(group.Name))
				{
					continue;
				}

				if (group.Polymorphism == Polymorphism.Enabled)
				{
					Report.MissingPolymorphicMethod(context, Symbol, group.DeclaringMethod.Symbol);
				}
				else if(group.Polymorphism == Polymorphism.NotSpecified)
				{
					Report.MissingSupportPolymorphism(context, group.DeclaringMethod.Symbol);
				}
			}
		}

		private int AssignToMethodGroups(Context context, ImmutableDictionary<string, MethodGroup> parentMethodGroups, ImmutableDictionary<string, MethodGroup>.Builder builder)
		{
			int matched = 0;

			foreach (Method method in PartialMethods)
			{
				MethodGroup group;
				if (parentMethodGroups.TryGetValue(method.Name, out group))
				{
					matched++;
				}
				else
				{
					group = MethodGroup.Create(context, method);
					builder.Add(group.Name, group);
				}

				group.AddMethod(context, method);
			}

			return matched;
		}

		public void PrepareReferences(Context context, DBOTypeCollection collection)
		{
			for (int i = 0; i < ReferencingProperties.Length; i++)
			{
				ReferencingProperties[i].PrepareReferences(context, collection);
			}
		}

		public string GenerateSource(Context context, SourceWriter writer)
		{
			CancellationToken token = context.CancellationToken;
			writer.Clear();

			writer.Using("System");
			using (writer.Namespace(Namespace))
			{
				using(writer.PartialClass(this))
				{
					GeneratePartialMethods(context, writer, token);
					GenerateStaticConstructor(context, writer, token);
				}
			}

			return writer.GetResult();
		}

		private void GenerateStaticConstructor(Context context, SourceWriter writer, CancellationToken token)
		{
			writer.AppendIndent();
			writer.Append("static ");
			writer.Append(Name);
			writer.Append("()\n");

			using (writer.Block())
			{
				foreach (var method in PartialMethods)
				{
					if (token.IsCancellationRequested)
						throw new OperationCanceledException();

					method.AddToStaticConstructor(context, writer);
				}
			}
		}

		private void GeneratePartialMethods(Context context, SourceWriter writer, CancellationToken token)
		{
			foreach (var method in PartialMethods)
			{
				if (token.IsCancellationRequested)
					throw new OperationCanceledException();

				method.Generate(writer, context);
			}
		}

		public DBProperty GetPropertyByName(string name)
		{
			DBProperty result = null;
			propByName.TryGetValue(name, out result);
			return result;
		}

		public Method GetMethodByName(string name)
		{
			Method result = null;
			methodByName.TryGetValue(name, out result);
			return result;
		}

		private static bool GetIsPartial(Context context, INamedTypeSymbol type)
		{
			if(type.DeclaringSyntaxReferences.Length == 0)
				return false;

			ClassDeclarationSyntax node = (ClassDeclarationSyntax)type.DeclaringSyntaxReferences[0].GetSyntax(context.CancellationToken);

			for (int i = 0; i < node.Modifiers.Count; i++)
			{
				SyntaxToken modifier = node.Modifiers[i];

				if(modifier.IsKind(SyntaxKind.PartialKeyword))
					return true;
			}

			return false;
		}

		private struct PropertyCollection
		{
			public ImmutableArray<DBProperty> Properties { get; set; }
			public ImmutableArray<DBProperty> ReferencingProperties { get; set; }

			public PropertyCollection(ImmutableArray<DBProperty> properties, ImmutableArray<DBProperty> refProps)
			{
				Properties = properties;
				ReferencingProperties = refProps;
			}
		}
		private static PropertyCollection CreateProperties(Context context, INamedTypeSymbol type)
		{

			IEnumerable<ISymbol> members = GetAllMembers(type, context.Types.DatabaseObject);

			ImmutableArray<DBProperty>.Builder properties = ImmutableArray.CreateBuilder<DBProperty>();
			ImmutableArray<DBProperty>.Builder refProps = ImmutableArray.CreateBuilder<DBProperty>();

			foreach(ISymbol member in members)
			{
				if (member.Kind != SymbolKind.Property)
					continue;

				IPropertySymbol property = (IPropertySymbol)member;
				DBProperty dbProperty = DBProperty.Create(property, context);

				if(dbProperty == null)
					continue;

				if(dbProperty.IsReference)
					refProps.Add(dbProperty);

				properties.Add(dbProperty);
			}

			return new PropertyCollection(properties.ToImmutable(), refProps.ToImmutable());
		}

		private static string FullNamespace(INamespaceSymbol ns)
		{
			StringBuilder nsBuilder = new StringBuilder();

			INamespaceSymbol current = ns;
			List<string> namespaces = new List<string>();

			do
			{
				namespaces.Add(current.Name);
				current = current.ContainingNamespace;
			} while (current != null && current.Name != string.Empty);

			for (int i = namespaces.Count - 1; i >= 0; i--)
			{
				nsBuilder.Append(namespaces[i]);

				if (i > 0)
					nsBuilder.Append(".");
			}

			return nsBuilder.ToString();
		}

		private static bool IsDatabaseClass(INamedTypeSymbol type, Context context)
		{
			return HasAttribute(type, context.Types.DatabaseClassAttribute);
		}
	}
}
