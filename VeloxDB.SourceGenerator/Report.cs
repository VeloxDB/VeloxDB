using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;

namespace VeloxDB.SourceGenerator
{
	internal static class Report
	{
		public static readonly DiagnosticDescriptor ClassMustBeBothDesc = CreateDesc(
			"Missing attribute or wrong base type",
			"The class {0} must be marked with the DatabaseClassAttribute and must inherit from the DatabaseObject class");

		public static readonly DiagnosticDescriptor ClassWithoutNamespaceDesc = CreateDesc(
			"Class without namespace",
			"The class {0} is not defined in a namespace");

		public static readonly DiagnosticDescriptor InvalidPropertyTypeDesc = CreateDesc(
			"Database property has an invalid type",
			"The property {0} of class {1} has an invalid type");

		public static readonly DiagnosticDescriptor InvalidArrayTypeDesc = CreateDesc(
			"Invalid Array Type",
			"The DatabaseArray type can only be a simple type");

		public static readonly DiagnosticDescriptor FailedToMatchPropertyDesc = CreateDesc(
			"Failed to match DTO and DatabaseObject property",
			"The property {0} of class {1} could not be matched to any database object's property." +
			" Maybe the database property is not marked with the appropriate attribute",
			DiagnosticSeverity.Warning);

		public static readonly DiagnosticDescriptor GenericClassNotSupportedDesc = CreateDesc(
			"Generic classes are not supported as database objects",
			"The class {0} is a generic class");

		public static readonly DiagnosticDescriptor PropertyTypeMismatchDesc = CreateDesc(
			"DTO and DatabaseObject property types don't match",
			"The type {0} does not match the database object's type {1}");

		public static readonly DiagnosticDescriptor ReferenceMustBeDatabaseObjectDesc = CreateDesc(
			"Reference must point to DatabaseObject",
			"The type {0} does not inherit from the DatabaseObject class");

		public static readonly DiagnosticDescriptor DatabaseClassIsNotAbstractDesc = CreateDesc(
			"Class not abstract",
			"The database class {0} must be declared as abstract");

		private static readonly DiagnosticDescriptor InvalidFromArgumentDesc = CreateDesc(
			"Invalid argument type for From method",
			"The argument at position {2} of method {0} must be of type {1}");

		public static readonly DiagnosticDescriptor ReturnTypeMustBeTheSameDesc = CreateDesc(
			"Return type of the method must be the same as containing class",
			"The method {0} must have the {1} type as its return type");

		public static readonly DiagnosticDescriptor InvalidArgumentsForFromMethodDesc = CreateDesc(
			"From method takes two arguments",
			"The method {0} must take exactly two arguments: an ObjectModel and the type to convert");

		public static readonly DiagnosticDescriptor FromMethodMustBeStaticDesc = CreateDesc(
			"From method must be static",
			"The method {0} must be static");

		public static readonly DiagnosticDescriptor InvalidRefArrayTypeDesc = CreateDesc(
			"Invalid reference array type",
			"The ReferenceArray type can only be of a type that inherits from the DatabaseObject class");

		public static readonly DiagnosticDescriptor NotYetSupportedDesc = CreateDesc(
			"Feature not supported",
			"The feature {0} is not yet supported");

		public static readonly DiagnosticDescriptor InvalidInverseReferenceTypeDesc = CreateDesc(
			"Invalid inverse reference property type",
			"The inverse reference property {0} must be of the InverseReferenceSet<T> type");

		public static readonly DiagnosticDescriptor InvalidInverseReferencePropertyDesc = CreateDesc(
			"Invalid inverse reference property",
			"Invalid inverse reference property: {0}");

		public static readonly DiagnosticDescriptor InvalidDTOTypeDesc = CreateDesc(
			"Invalid DTO Type",
			"The From method has an invalid DTO type. Only classes are allowed");

		public static readonly DiagnosticDescriptor ToMethodHasArgumentsDesc = CreateDesc(
			"To method has arguments",
			"The To method must not take any arguments");
		public static readonly DiagnosticDescriptor ToMethodCantBeVirtualDesc = CreateDesc(
			"To method can not be virtual",
			"The To method cannot be virtual. If you need polymorphic behavior, consider using the SupportPolymorphismAttribute");

		public static readonly DiagnosticDescriptor ToMethodCantBeStaticDesc = CreateDesc(
			"To method can not be static",
			"Only instance methods are allowed for To methods");

		public static readonly DiagnosticDescriptor MissingSupportPolymorphismDesc = CreateDesc(
			"Missing SupportPolymorphismAttribute",
			"The method {0} is not marked with the SupportPolymorphismAttribute",
			DiagnosticSeverity.Error
		);

		public static readonly DiagnosticDescriptor MissingPolymorphicMethodDesc = CreateDesc(
			"Missing polymorphic method",
			"The type {0} does not declare the partial method {1} declared in {2}");

		public static readonly DiagnosticDescriptor SupportPolymorphismAlreadyDeclaredDesc = CreateDesc(
			"SupportPolymorphismAttribute already declared",
			"The method {0} has the SupportPolymorphismAttribute attribute, but it is already declared for {1}"
		);

		public static readonly DiagnosticDescriptor InvalidDTOInheritanceDesc = CreateDesc(
			"Invalid DTO inheritance",
			"The DTO type {0} does not inherit from the DTO type {1}"
		);

		public static readonly DiagnosticDescriptor DuplicateDTOTypeDesc = CreateDesc(
			"Duplicate DTO Type",
			"A mapping for type {0} is already defined with the method {1}"
		);

		public static readonly DiagnosticDescriptor ReferencedTypeMissingMethodDesc = CreateDesc(
			"Referenced type is missing a method",
			"The referenced type {0} is missing the method {1} with the return type {2}"
		);

		public static readonly DiagnosticDescriptor MethodTypeMismatchDesc = CreateDesc(
			"Method return type mismatch",
			"The {0} method of type {1} returns {2}, but {3} was expected"
		);

		public static readonly DiagnosticDescriptor InaccessibleReferencedTypeDesc = CreateDesc(
			"Inaccessible referenced type",
			"The {0} method of type {1} is not accessible from {2}"
		);

		public static readonly DiagnosticDescriptor OverloadNotSupportedDesc = CreateDesc(
			"Overload of automapper methods is not supported",
			"The method {0} is already declared. Overloading is not supported. Use default arguments where appropriate"
		);

		public static readonly DiagnosticDescriptor PropertyAlreadyMatchedDesc = CreateDesc(
			"DTO property already matched",
			"The DTO property {0} has already been matched to a database property. Only one DTO property is allowed to be matched to a database property"
		);

		public static readonly DiagnosticDescriptor UnexpectedReferenceDesc = CreateDesc(
			"Unexpected reference",
			"The property {0} is marked as a DatabasePropertyAttribute, but it is of a reference type. Mark it with the DatabaseReferenceAttribute instead"
		);

		public static readonly DiagnosticDescriptor NullableReturnTypeDesc = CreateDesc(
			"Nullable return type",
			"The method {0} has a nullable return type. From/To methods never return null"
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

		public static void NullableReturnType(Context context, IMethodSymbol methodSym)
		{
			context.Report(NullableReturnTypeDesc, methodSym, methodSym.Name);
		}
	}
}
