using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;

namespace VeloxDB.AspNet.ControllerGenerator
{
	internal class Report
	{
		private static readonly DiagnosticDescriptor MissingRequiredReferenceDesc = new DiagnosticDescriptor("VLXASP001", "Missing requried reference", "Missing reference to the following assemblies: {0}", "Error", DiagnosticSeverity.Error, true);
		public static void MissingRequiredReference(GeneratorExecutionContext context, IEnumerable<string> missing)
		{
			context.ReportDiagnostic(Diagnostic.Create(MissingRequiredReferenceDesc, null, string.Join(", ", missing)));
		}

		private static readonly DiagnosticDescriptor NotAnAttributeDesc = new DiagnosticDescriptor("VLXASP002", "Not an attribute", "The type '{0}' is not an attribute", "Error", DiagnosticSeverity.Error, true);
		public static void NotAnAttribute(ControllerGeneratorContext context, AttributeData attribute, string className)
		{
			context.ReportDiagnostic(NotAnAttributeDesc, attribute.ApplicationSyntaxReference?.GetSyntax().GetLocation(), className);
		}

		private static readonly DiagnosticDescriptor OddNumberOfArgumentsDesc = new DiagnosticDescriptor("VLXASP003", "Odd number of arguments", "The named argument of ForwardAttribute must contain an even number of elements, as they represent key-value pairs", "Error", DiagnosticSeverity.Error, true);
		public static void OddNumberOfArguments(ControllerGeneratorContext context, AttributeData attribute)
		{
			context.ReportDiagnostic(OddNumberOfArgumentsDesc, attribute.ApplicationSyntaxReference?.GetSyntax().GetLocation());
		}

		private static readonly DiagnosticDescriptor UnknownNamedArgumentDesc = new DiagnosticDescriptor("VLXASP004", "Unknown named argument", "The '{0}' attribute does not have a named argument named '{1}'", "Error", DiagnosticSeverity.Error, true);

		public static void UnknownNamedArgument(ControllerGeneratorContext context, AttributeData attribute, string attributeClassName, string argName)
		{
			context.ReportDiagnostic(UnknownNamedArgumentDesc, attribute.ApplicationSyntaxReference?.GetSyntax().GetLocation(), attributeClassName, argName);
		}

		private static readonly DiagnosticDescriptor WrongTypeDesc = new DiagnosticDescriptor("VLXASP005", "Wrong type", "The value provided for the named argument '{0}' of the attribute '{1}' is of the wrong type, expected '{2}' but found '{3}'", "Error", DiagnosticSeverity.Error, true);

		public static void WrongType(ControllerGeneratorContext context, AttributeData attribute, string attributeClassName, string argName,
									 ITypeSymbol expected, ITypeSymbol received)
		{
			context.ReportDiagnostic(WrongTypeDesc, attribute.ApplicationSyntaxReference?.GetSyntax().GetLocation(),
									 argName, attributeClassName, expected.ToDisplayString(), received.ToDisplayString());
		}
	}
}