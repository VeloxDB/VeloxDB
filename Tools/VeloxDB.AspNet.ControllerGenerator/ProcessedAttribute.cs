using System;
using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using static VeloxDB.AspNet.ControllerGenerator.Utils;

namespace VeloxDB.AspNet.ControllerGenerator
{
	internal class ProcessedAttribute
	{
		public INamedTypeSymbol AttributeClass { get; private set; }
		public string CSharpString { get; private set; }

		public ProcessedAttribute(INamedTypeSymbol attributeClass, string csharpString)
		{
			this.AttributeClass = attributeClass;
			this.CSharpString = csharpString;
		}

		private static readonly StringBuilder builder = new StringBuilder();

		public static ProcessedAttributeCollection ProcessAttributes(ImmutableArray<AttributeData> attributes, ControllerGeneratorContext context)
		{
			ProcessedAttributeCollection result = new ProcessedAttributeCollection(attributes.Length);
			KnownTypes knownTypes = context.KnownTypes;

			for (int i = 0; i < attributes.Length; i++)
			{
				ProcessedAttribute processed = null;
				AttributeData attribute = attributes[i];
				INamedTypeSymbol attributeClass = attribute.AttributeClass;
				if (SymEquals(attributeClass, knownTypes.DbAPIAttribute) || SymEquals(attributeClass, knownTypes.DbAPIOperationAttribute))
					continue;

				if (SymEquals(attributeClass, knownTypes.ForwardAttribute))
				{
					processed = ProcessForwardAttribute(attribute, context);
				}
				else
				{
					processed = new ProcessedAttribute(attributeClass, attribute.ToString());
				}

				if(processed != null)
					result.Add(processed);
			}

			return result;
		}

		private static ProcessedAttribute ProcessForwardAttribute(AttributeData attribute, ControllerGeneratorContext context)
		{
			ProcessedAttribute processed;
			INamedTypeSymbol newAttributeClass = (INamedTypeSymbol)attribute.ConstructorArguments[0].Value;
			KnownTypes knownTypes = context.KnownTypes;

			if (!SymEquals(newAttributeClass.BaseType, knownTypes.Attribute))
			{
				Report.NotAnAttribute(context, attribute, newAttributeClass.ToDisplayString());
				return null;
			}

			builder.Clear();
			builder.Append(newAttributeClass.ToDisplayString());
			builder.Append("(");

			AppendConstructorArguments(attribute);
			AppendNamedArguments(attribute, newAttributeClass, context);

			builder.Append(")");

			processed = new ProcessedAttribute(newAttributeClass, builder.ToString());
			return processed;
		}

		private static void AppendNamedArguments(AttributeData attribute, INamedTypeSymbol newAttributeClass,
												 ControllerGeneratorContext context)
		{
			PropertyCache cache = context.Cache;

			if (attribute.NamedArguments.Length == 1)
			{
				var namedArguments = attribute.NamedArguments[0].Value.Values;

				if (namedArguments.Length % 2 != 0)
				{
					Report.OddNumberOfArguments(context, attribute);
					return;
				}

				bool first = true;
				bool hasConstructorArgs = attribute.ConstructorArguments.Length > 1;

				for (int i = 0; i < namedArguments.Length / 2; i++)
				{
					string argName = (string)namedArguments[i * 2].Value;

					IPropertySymbol propertySymbol = null;

					if (!cache.TryGetProperty(newAttributeClass, argName, out propertySymbol))
					{
						Report.UnknownNamedArgument(context, attribute, newAttributeClass.Name, argName);
						continue;
					}

					var value = namedArguments[i * 2 + 1];

					if (!SymEquals(propertySymbol.Type, value.Type))
					{
						Report.WrongType(context, attribute, newAttributeClass.Name, argName, propertySymbol.Type, value.Type);
						continue;
					}

					if(hasConstructorArgs && first)
					{
						// Append comma only after all validations have passed
						builder.Append(", ");
						first = false;
					}

					builder.Append(argName);
					builder.Append(" = ");
					builder.Append(value.ToCSharpString());

					if (i < namedArguments.Length / 2 - 1)
					{
						builder.Append(", ");
					}
				}

			}
		}

		private static void AppendConstructorArguments(AttributeData attribute)
		{
			if(attribute.ConstructorArguments.Length != 2)
				return;

			ImmutableArray<TypedConstant> constructorArguments = attribute.ConstructorArguments[1].Values;

			for (int i = 0; i < constructorArguments.Length; i++)
			{
				builder.Append(constructorArguments[i].ToCSharpString());
				if (i != constructorArguments.Length - 1)
					builder.Append(", ");
			}
		}
	}

}