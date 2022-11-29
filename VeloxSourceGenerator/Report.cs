using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;

namespace Velox.SourceGenerator
{
	internal static class Report
	{
		public static readonly DiagnosticDescriptor ClassMustBeBothDesc = CreateDesc(
			"Missing attribute or wrong base type",
			"{0} must be marked with DatabaseClassAttribute and must inherit from DatabaseObject");

		public static readonly DiagnosticDescriptor ClassWithoutNamespaceDesc = CreateDesc(
			"Class without namespace",
			"Class {0} is not defined in a namespace");

		public static readonly DiagnosticDescriptor InvalidPropertyTypeDesc = CreateDesc(
			"Database property has an invalid type",
			"Property {0} of class {1} has an invalid type");

		public static readonly DiagnosticDescriptor InvalidArrayTypeDesc = CreateDesc(
			"Invalid Array Type",
			"Database Array type can only be simple type");

		public static readonly DiagnosticDescriptor FailedToMatchPropertyDesc = CreateDesc(
			"Failed to match DTO and DatabaseObject property",
			"Property {0} of class {1} couldn't be matched to any database object's property." +
			"Maybe the database property is not marked with appropriate attribute", DiagnosticSeverity.Warning);

		public static readonly DiagnosticDescriptor GenericClassNotSupportedDesc = CreateDesc(
			"Generic classes are not supported as database objects",
			"Class {0} is generic");

		public static readonly DiagnosticDescriptor PropertyTypeMismatchDesc = CreateDesc(
			"DTO and DatabaseObject property types don't match",
			"Type {0} doesn't match database object's type {1}");

		public static readonly DiagnosticDescriptor ReferenceMustBeDatabaseObjectDesc = CreateDesc(
			"Reference must point to DatabaseObject",
			"Type {0} doesn't inherit from DatabaseObject");

		public static readonly DiagnosticDescriptor DatabaseClassIsNotAbstractDesc = CreateDesc(
			"Class not abstract",
			"Database class {0} must be declared as abstract");

		private static readonly DiagnosticDescriptor InvalidFromArgumentDesc = CreateDesc(
			"Invalid argument type for From method",
			"Method {0} must have {1} as {2} argument");

		public static readonly DiagnosticDescriptor ReturnTypeMustBeTheSameDesc = CreateDesc(
			"Return type of the method must be the same as containing class",
			"Method {0} must have {1} as return type");

		public static readonly DiagnosticDescriptor InvalidArgumentsForFromMethodDesc = CreateDesc(
			"From method takes two arguments",
			"Method {0} must take exactly two arguments, ObjectModel and type to convert");

		public static readonly DiagnosticDescriptor FromMethodMustBeStaticDesc = CreateDesc(
			"From method must be static",
			"Method {0} must have be static");

		public static readonly DiagnosticDescriptor InvalidRefArrayTypeDesc = CreateDesc(
			"Invalid reference array type",
			"Reference array type can only be of type that inherits from DatabaseObject");

		public static readonly DiagnosticDescriptor NotYetSupportedDesc = CreateDesc(
			"Feature not supported",
			"{0} is not yet supported");

		public static readonly DiagnosticDescriptor InvalidInverseReferenceTypeDesc = CreateDesc(
			"Invalid inverse reference property type",
			"Inverse reference property {0} must be of InverseReferenceSet<T> type");

		public static readonly DiagnosticDescriptor InvalidInverseReferencePropertyDesc = CreateDesc(
			"Invalid inverse reference property",
			"Invalid inverse reference property: {0}");

		public static readonly DiagnosticDescriptor InvalidDTOTypeDesc = CreateDesc(
			"Invalid DTO Type",
			"From method has invalid DTO type, only classes are allowed");

		public static readonly DiagnosticDescriptor ToMethodHasArgumentsDesc = CreateDesc(
			"To method has arguments",
			"To method mustn't take any arguments");
		public static readonly DiagnosticDescriptor ToMethodCantBeVirtualDesc = CreateDesc(
			"To method can not be virtual",
			"To method can not be virtual, if you need polymorphic behavior consider using SupportPolymorphismAttribute");

		public static readonly DiagnosticDescriptor ToMethodCantBeStaticDesc = CreateDesc(
			"To method can not be static",
			"Only instance methods are allowed for to methods");

		public static readonly DiagnosticDescriptor MissingSupportPolymorphismDesc = CreateDesc(
			"Missing SupportPolymorphismAttribute",
			"Method {0} is not marked with SupportPolymorphismAttribute.",
			DiagnosticSeverity.Error
		);

		public static readonly DiagnosticDescriptor MissingPolymorphicMethodDesc = CreateDesc(
			"Missing polymorphic method",
			"Type {0} does not declare partial method {1} declared in {2}");

		public static readonly DiagnosticDescriptor SupportPolymorphismAlreadyDeclaredDesc = CreateDesc(
			"SupportPolymorphismAttribute already declared",
			"Method {0} has SupportPolymorphismAttribute attribute, when it is already declared for {1}"
		);

		public static readonly DiagnosticDescriptor InvalidDTOInheritanceDesc = CreateDesc(
			"Invalid DTO inheritance",
			"DTO Type {0} does not inherit from DTO Type {1}"
		);

		public static readonly DiagnosticDescriptor DuplicateDTOTypeDesc = CreateDesc(
			"Duplicate DTO Type",
			"Mapping for type {0} is already defined with method {1}"
		);

		public static readonly DiagnosticDescriptor ReferencedTypeMissingMethodDesc = CreateDesc(
			"Referenced type is missing a method",
			"Referenced Type {0} is missing method {1} with return type {2}"
		);

		public static readonly DiagnosticDescriptor MethodTypeMismatchDesc = CreateDesc(
			"Method return type mismatch",
			"{0} method of type {1} returns {2} but {3} was expected"
		);

		public static readonly DiagnosticDescriptor InaccessibleReferencedTypeDesc = CreateDesc(
			"Inaccessible referenced type",
			"{0} method of type {1} is not accessible from {2}"
		);

		public static readonly DiagnosticDescriptor OverloadNotSupportedDesc = CreateDesc(
			"Overload of automapper methods is not supported",
			"Method {0} is already declared, overloading is not supported, use default arguments where appropriate"
		);

		public static readonly DiagnosticDescriptor PropertyAlreadyMatchedDesc = CreateDesc(
			"DTO property already matched",
			"DTO property {0} has already been matched to database property, only one DTO property is allowed"
		);

		public static readonly DiagnosticDescriptor UnexpectedReferenceDesc = CreateDesc(
			"Unexpected reference",
			"Property {0} is marked as DatabasePropertyAttribute, but it is of reference type. Mark it with DatabaseReferenceAttribute instead"
		);

		static int errorId = 0;

		private static DiagnosticDescriptor CreateDesc(string title, string message, DiagnosticSeverity severity = DiagnosticSeverity.Error)
		{
			return new DiagnosticDescriptor($"VLX{++errorId:000}", title, message, "VLX", severity, true);
		}

		public static void InvalidArrayType(Context context, IPropertySymbol property)
		{
			context.Report(InvalidArrayTypeDesc, property);
		}

		public static void ClassMustBeBoth(Context context, INamedTypeSymbol type)
		{
			context.Report(ClassMustBeBothDesc, type, type.ToString());
		}

		public static void ClassWithoutNamespace(Context context, INamedTypeSymbol type)
		{
			context.Report(ClassWithoutNamespaceDesc,type, type.ToString());
		}

		public static void GenericClassNotSupported(Context context, INamedTypeSymbol type)
		{
			context.Report(GenericClassNotSupportedDesc,type, type.ToString());
		}

		public static void InvalidPropertyType(Context context, IPropertySymbol property)
		{
			context.Report(InvalidPropertyTypeDesc, property, property.Name, property.ContainingType.ToString());
		}

		public static void FailedToMatchProperty(Context context, IPropertySymbol property)
		{
			context.Report(FailedToMatchPropertyDesc, property, property.Name, property.ContainingType.ToString());
		}

		public static void PropertyTypeMismatch(Context context, IPropertySymbol dboProperty, IPropertySymbol dtoProperty)
		{
			context.Report(PropertyTypeMismatchDesc, dtoProperty, dtoProperty.Type, dboProperty.Type);
		}

		public static void ReferenceMustBeDatabaseObject(Context context, IPropertySymbol property)
		{
			context.Report(ReferenceMustBeDatabaseObjectDesc, property, property.Type.ToString());
		}

		public static void DatabaseClassIsNotAbstract(Context context, INamedTypeSymbol type)
		{
			context.Report(DatabaseClassIsNotAbstractDesc, type, type.ToString());
		}

		public static void InvalidFromArgument(Context context, IMethodSymbol method, INamedTypeSymbol type, string position)
		{
			context.Report(InvalidFromArgumentDesc, method, method.ToString(), type.ToString(), position);
		}

		public static void InvalidArgumentsForFromMethod(Context context, IMethodSymbol methodSym)
		{
			context.Report(InvalidArgumentsForFromMethodDesc, methodSym, methodSym.ToString(), methodSym.ContainingType.ToString());
		}

		public static void FromMethodMustBeStatic(Context context, IMethodSymbol methodSym)
		{
			context.Report(FromMethodMustBeStaticDesc, methodSym, methodSym.ToString(), methodSym.ContainingType.ToString());
		}

		public static void ReturnTypeMustBeTheSame(Context context, IMethodSymbol methodSym)
		{
			context.Report(ReturnTypeMustBeTheSameDesc, methodSym, methodSym.ToString(), methodSym.ContainingType.ToString());
		}

		public static void InvalidRefArrayType(Context context, IPropertySymbol property)
		{
			context.Report(InvalidRefArrayTypeDesc, property);
		}

		public static void NotYetSupported(Context context, string message, ISymbol symbol)
		{
			context.Report(NotYetSupportedDesc, symbol, message);
		}

		public static void InvalidInverseReferenceType(Context context, IPropertySymbol property)
		{
			context.Report(InvalidInverseReferenceTypeDesc, property, property.Name);
		}

		public static void InvalidInverseReferenceProperty(Context context, IPropertySymbol property, string message)
		{
			context.Report(InvalidInverseReferencePropertyDesc, property, message);
		}

		public static void InvalidDTOType(Context context, IMethodSymbol methodSym)
		{
			context.Report(InvalidDTOTypeDesc, methodSym);
		}

		public static void ToMethodHasArguments(Context context, IMethodSymbol methodSym)
		{
			context.Report(ToMethodHasArgumentsDesc, methodSym);
		}

		public static void ToMethodCantBeVirtual(Context context, IMethodSymbol methodSym)
		{
			context.Report(ToMethodCantBeVirtualDesc, methodSym);
		}

		public static void ToMethodCantBeStatic(Context context, IMethodSymbol methodSym)
		{
			context.Report( ToMethodCantBeStaticDesc, methodSym);
		}

		private static HashSet<IMethodSymbol> reported = new HashSet<IMethodSymbol>(SymbolEqualityComparer.Default);

		public static void MissingSupportPolymorphism(Context context, IMethodSymbol methodSym)
		{
			if(reported.Contains(methodSym))
				return;
			else
				reported.Add(methodSym);

			context.Report(MissingSupportPolymorphismDesc, methodSym, methodSym.ToString());
		}

		public static void MissingPolymorphicMethod(Context context, INamedTypeSymbol type, IMethodSymbol method)
		{
			context.Report(MissingPolymorphicMethodDesc, type, type.ToString(), method.Name, method.ContainingType.ToString());
		}

		public static void SupportPolymorphismAlreadyDeclared(Context context, IMethodSymbol method, IMethodSymbol baseMethod)
		{
			context.Report(SupportPolymorphismAlreadyDeclaredDesc, method, method.ToString(), baseMethod.ToString());
		}

		public static void InvalidDTOInheritance(Context context, IMethodSymbol method, INamedTypeSymbol dtoType, INamedTypeSymbol parentDtoType)
		{
			context.Report(InvalidDTOInheritanceDesc, method, dtoType.ToString(), parentDtoType.ToString());
		}

		public static void DuplicateDTOType(Context context, IMethodSymbol method, INamedTypeSymbol type, IMethodSymbol origin)
		{
			context.Report(DuplicateDTOTypeDesc, method, type.ToString(), origin.ToString());
		}

		public static void ReferencedTypeMissingMethod(Context context, IPropertySymbol property, string methodName,
													   INamedTypeSymbol dtoType)
		{
			context.Report(ReferencedTypeMissingMethodDesc, property, Utils.WithoutNullable(property.Type).ToString(), methodName, dtoType.ToString());
		}

		public static void MethodTypeMismatch(Context context, IPropertySymbol property, IMethodSymbol method, INamedTypeSymbol dtoType)
		{
			context.Report(MethodTypeMismatchDesc, property, method.Name, method.ContainingType.ToString(), method.ReturnType.ToString(),
						   dtoType.ToString());
		}

		public static void InaccessibleReferencedType(Context context, INamedTypeSymbol type, IMethodSymbol refMethod)
		{
			context.Report(InaccessibleReferencedTypeDesc, refMethod, refMethod.Name, refMethod.ContainingType.ToString(), type.ToString());
		}

		public static void OverloadNotSupported(Context context, IMethodSymbol methodSym)
		{
			context.Report(OverloadNotSupportedDesc, methodSym, methodSym.Name);
		}

		public static void PropertyAlreadyMatched(Context context, IPropertySymbol property)
		{
			context.Report(PropertyAlreadyMatchedDesc, property, property.Name);
		}

		public static void UnexpectedReference(Context context, IPropertySymbol property)
		{
			context.Report(UnexpectedReferenceDesc, property, property.Name);
		}
	}
}
