using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;

namespace VeloxDB.SourceGenerator
{
	internal static class Report
	{
		public static readonly DiagnosticDescriptor ClassMustBeBothDesc = CreateDesc(
			"Missing attribute or wrong base type",
			"The class '{0}' must be marked with the DatabaseClassAttribute and must inherit from the DatabaseObject class");

		public static readonly DiagnosticDescriptor ClassWithoutNamespaceDesc = CreateDesc(
			"Class without namespace",
			"The class '{0}' is not defined in a namespace");

		public static readonly DiagnosticDescriptor InvalidPropertyTypeDesc = CreateDesc(
			"Database property has an invalid type",
			"The property '{0}' of class '{1}' has an invalid type");

		public static readonly DiagnosticDescriptor InvalidArrayTypeDesc = CreateDesc(
			"Invalid Array Type",
			"The DatabaseArray type can only be a simple type");

		public static readonly DiagnosticDescriptor FailedToMatchPropertyDesc = CreateDesc(
			"Failed to match DTO and DatabaseObject property",
			"The property '{0}' of class '{1}' could not be matched to any database object's property." +
			" Maybe the database property is not marked with the appropriate attribute",
			DiagnosticSeverity.Warning);

		public static readonly DiagnosticDescriptor GenericClassNotSupportedDesc = CreateDesc(
			"Generic classes are not supported as database objects",
			"The class '{0}' is a generic class");

		public static readonly DiagnosticDescriptor PropertyTypeMismatchDesc = CreateDesc(
			"DTO and DatabaseObject property types don't match",
			"The type '{0}' does not match the database object's type '{1}'");

		public static readonly DiagnosticDescriptor ReferenceMustBeDatabaseObjectDesc = CreateDesc(
			"Reference must point to DatabaseObject",
			"The type '{0}' does not inherit from the DatabaseObject class");

		public static readonly DiagnosticDescriptor DatabaseClassIsNotAbstractDesc = CreateDesc(
			"Class not abstract",
			"The database class '{0}' must be declared as abstract");

		private static readonly DiagnosticDescriptor InvalidFromArgumentDesc = CreateDesc(
			"Invalid argument type for From method",
			"The argument at position '{2}' of method '{0}' must be of type '{1}'");

		public static readonly DiagnosticDescriptor ReturnTypeMustBeTheSameDesc = CreateDesc(
			"Return type of the method must be the same as containing class",
			"The method '{0}' must have the '{1}' type as its return type");

		public static readonly DiagnosticDescriptor InvalidArgumentsForFromMethodDesc = CreateDesc(
			"From method takes two arguments",
			"The method '{0}' must take exactly two arguments: an ObjectModel and the type to convert");

		public static readonly DiagnosticDescriptor FromMethodMustBeStaticDesc = CreateDesc(
			"From method must be static",
			"The method '{0}' must be static");

		public static readonly DiagnosticDescriptor InvalidRefArrayTypeDesc = CreateDesc(
			"Invalid reference array type",
			"The ReferenceArray type can only be of a type that inherits from the DatabaseObject class");

		public static readonly DiagnosticDescriptor NotYetSupportedDesc = CreateDesc(
			"Feature not supported",
			"The feature '{0}' is not yet supported");

		public static readonly DiagnosticDescriptor InvalidInverseReferenceTypeDesc = CreateDesc(
			"Invalid inverse reference property type",
			"The inverse reference property '{0}' must be of the InverseReferenceSet<T> type");

		public static readonly DiagnosticDescriptor InvalidInverseReferencePropertyDesc = CreateDesc(
			"Invalid inverse reference property",
			"Invalid inverse reference property: '{0}'");

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
			"The method '{0}' is not marked with the SupportPolymorphismAttribute",
			DiagnosticSeverity.Error
		);

		public static readonly DiagnosticDescriptor MissingPolymorphicMethodDesc = CreateDesc(
			"Missing polymorphic method",
			"The type '{0}' does not declare the partial method '{1}' declared in '{2}'");

		public static readonly DiagnosticDescriptor SupportPolymorphismAlreadyDeclaredDesc = CreateDesc(
			"SupportPolymorphismAttribute already declared",
			"The method '{0}' has the SupportPolymorphismAttribute attribute, but it is already declared for '{1}'"
		);

		public static readonly DiagnosticDescriptor InvalidDTOInheritanceDesc = CreateDesc(
			"Invalid DTO inheritance",
			"The DTO type '{0}' does not inherit from the DTO type '{1}'"
		);

		public static readonly DiagnosticDescriptor DuplicateDTOTypeDesc = CreateDesc(
			"Duplicate DTO Type",
			"A mapping for type '{0}' is already defined with the method '{1}'"
		);

		public static readonly DiagnosticDescriptor ReferencedTypeMissingMethodDesc = CreateDesc(
			"Referenced type is missing a method",
			"The referenced type '{0}' is missing the method '{1}' with the return type '{2}'"
		);

		public static readonly DiagnosticDescriptor MethodTypeMismatchDesc = CreateDesc(
			"Method return type mismatch",
			"The '{0}' method of type '{1}' returns '{2}', but '{3}' was expected"
		);

		public static readonly DiagnosticDescriptor InaccessibleReferencedTypeDesc = CreateDesc(
			"Inaccessible referenced type",
			"The '{0}' method of type '{1}' is not accessible from '{2}'"
		);

		public static readonly DiagnosticDescriptor OverloadNotSupportedDesc = CreateDesc(
			"Overload of automapper methods is not supported",
			"The method '{0}' is already declared. Overloading is not supported. Use default arguments where appropriate"
		);

		public static readonly DiagnosticDescriptor PropertyAlreadyMatchedDesc = CreateDesc(
			"DTO property already matched",
			"The DTO property '{0}' has already been matched to a database property. Only one DTO property is allowed to be matched to a database property"
		);

		public static readonly DiagnosticDescriptor UnexpectedReferenceDesc = CreateDesc(
			"Unexpected reference",
			"The property '{0}' is marked as a DatabasePropertyAttribute, but it is of a reference type. Mark it with the DatabaseReferenceAttribute instead"
		);

		public static readonly DiagnosticDescriptor NullableReturnTypeDesc = CreateDesc(
			"Nullable return type",
			"The method '{0}' has a nullable return type. From/To methods never return null"
		);

		public static readonly DiagnosticDescriptor ParentIsNotAbstractDesc = CreateDesc(
			"Parent is not abstract",
			"The type '{0}' is marked as abstract with the 'isAbstract' argument in the DatabaseClassAttribute, but its parent '{1}' is not marked as abstract"
		);

		public static readonly DiagnosticDescriptor DBOAndDTOAbstractMismatchDesc = CreateDesc(
			"DBO and DTO abstract mismatch",
			"Classes '{0}' and '{1}' either both need to be abstract or both be non-abstract"
		);

		public static readonly DiagnosticDescriptor GenerateClientInterfaceDesc = CreateDesc(
			"Generate Client Interface",
			"Class '{0}' is marked with DbAPIAttribute. Use this rule to generate a client interface if needed.",
			DiagnosticSeverity.Info);

		public static readonly DiagnosticDescriptor DbAPIClassMustBePublicDesc = CreateDesc(
			"DbAPI class must be public",
			"The class '{0}' marked with DbAPIAttribute must be public");

		public static readonly DiagnosticDescriptor DbAPIOperationMustBePublicDesc = CreateDesc(
			"DbAPI operation must be public",
			"The method '{0}' marked with DbAPIOperationAttribute must be public");

		static int errorId = 0;

		private static DiagnosticDescriptor CreateDesc(string title, string message, DiagnosticSeverity severity = DiagnosticSeverity.Error)
		{
			return new DiagnosticDescriptor($"VLX{++errorId:000}", title, message, "VLX", severity, true);
		}

		public static void InvalidArrayType(BaseContext context, IPropertySymbol property)
		{
			context.Report(InvalidArrayTypeDesc, property);
		}

		public static void ClassMustBeBoth(BaseContext context, INamedTypeSymbol type)
		{
			context.Report(ClassMustBeBothDesc, type, type.ToString());
		}

		public static void ClassWithoutNamespace(BaseContext context, INamedTypeSymbol type)
		{
			context.Report(ClassWithoutNamespaceDesc,type, type.ToString());
		}

		public static void GenericClassNotSupported(BaseContext context, INamedTypeSymbol type)
		{
			context.Report(GenericClassNotSupportedDesc,type, type.ToString());
		}

		public static void InvalidPropertyType(BaseContext context, IPropertySymbol property)
		{
			context.Report(InvalidPropertyTypeDesc, property, property.Name, property.ContainingType.ToString());
		}

		public static void FailedToMatchProperty(BaseContext context, IPropertySymbol property)
		{
			context.Report(FailedToMatchPropertyDesc, property, property.Name, property.ContainingType.ToString());
		}

		public static void PropertyTypeMismatch(BaseContext context, IPropertySymbol dboProperty, IPropertySymbol dtoProperty)
		{
			context.Report(PropertyTypeMismatchDesc, dtoProperty, dtoProperty.Type, dboProperty.Type);
		}

		public static void ReferenceMustBeDatabaseObject(BaseContext context, IPropertySymbol property)
		{
			context.Report(ReferenceMustBeDatabaseObjectDesc, property, property.Type.ToString());
		}

		public static void DatabaseClassIsNotAbstract(BaseContext context, INamedTypeSymbol type)
		{
			context.Report(DatabaseClassIsNotAbstractDesc, type, type.ToString());
		}

		public static void InvalidFromArgument(BaseContext context, IMethodSymbol method, INamedTypeSymbol type, string position)
		{
			context.Report(InvalidFromArgumentDesc, method, method.ToString(), type.ToString(), position);
		}

		public static void InvalidArgumentsForFromMethod(BaseContext context, IMethodSymbol methodSym)
		{
			context.Report(InvalidArgumentsForFromMethodDesc, methodSym, methodSym.ToString(), methodSym.ContainingType.ToString());
		}

		public static void FromMethodMustBeStatic(BaseContext context, IMethodSymbol methodSym)
		{
			context.Report(FromMethodMustBeStaticDesc, methodSym, methodSym.ToString(), methodSym.ContainingType.ToString());
		}

		public static void ReturnTypeMustBeTheSame(BaseContext context, IMethodSymbol methodSym)
		{
			context.Report(ReturnTypeMustBeTheSameDesc, methodSym, methodSym.ToString(), methodSym.ContainingType.ToString());
		}

		public static void InvalidRefArrayType(BaseContext context, IPropertySymbol property)
		{
			context.Report(InvalidRefArrayTypeDesc, property);
		}

		public static void NotYetSupported(BaseContext context, string message, ISymbol symbol)
		{
			context.Report(NotYetSupportedDesc, symbol, message);
		}

		public static void InvalidInverseReferenceType(BaseContext context, IPropertySymbol property)
		{
			context.Report(InvalidInverseReferenceTypeDesc, property, property.Name);
		}

		public static void InvalidInverseReferenceProperty(BaseContext context, IPropertySymbol property, string message)
		{
			context.Report(InvalidInverseReferencePropertyDesc, property, message);
		}

		public static void InvalidDTOType(BaseContext context, IMethodSymbol methodSym)
		{
			context.Report(InvalidDTOTypeDesc, methodSym);
		}

		public static void ToMethodHasArguments(BaseContext context, IMethodSymbol methodSym)
		{
			context.Report(ToMethodHasArgumentsDesc, methodSym);
		}

		public static void ToMethodCantBeVirtual(BaseContext context, IMethodSymbol methodSym)
		{
			context.Report(ToMethodCantBeVirtualDesc, methodSym);
		}

		public static void ToMethodCantBeStatic(BaseContext context, IMethodSymbol methodSym)
		{
			context.Report( ToMethodCantBeStaticDesc, methodSym);
		}

		private static HashSet<IMethodSymbol> reported = new HashSet<IMethodSymbol>(SymbolEqualityComparer.Default);

		public static void MissingSupportPolymorphism(BaseContext context, IMethodSymbol methodSym)
		{
			if(reported.Contains(methodSym))
				return;
			else
				reported.Add(methodSym);

			context.Report(MissingSupportPolymorphismDesc, methodSym, methodSym.ToString());
		}

		public static void MissingPolymorphicMethod(BaseContext context, INamedTypeSymbol type, IMethodSymbol method)
		{
			context.Report(MissingPolymorphicMethodDesc, type, type.ToString(), method.Name, method.ContainingType.ToString());
		}

		public static void SupportPolymorphismAlreadyDeclared(BaseContext context, IMethodSymbol method, IMethodSymbol baseMethod)
		{
			context.Report(SupportPolymorphismAlreadyDeclaredDesc, method, method.ToString(), baseMethod.ToString());
		}

		public static void InvalidDTOInheritance(BaseContext context, IMethodSymbol method, INamedTypeSymbol dtoType, INamedTypeSymbol parentDtoType)
		{
			context.Report(InvalidDTOInheritanceDesc, method, dtoType.ToString(), parentDtoType.ToString());
		}

		public static void DuplicateDTOType(BaseContext context, IMethodSymbol method, INamedTypeSymbol type, IMethodSymbol origin)
		{
			context.Report(DuplicateDTOTypeDesc, method, type.ToString(), origin.ToString());
		}

		public static void ReferencedTypeMissingMethod(BaseContext context, IPropertySymbol property, INamedTypeSymbol type, string methodName,
													   INamedTypeSymbol dtoType)
		{
			context.Report(ReferencedTypeMissingMethodDesc, property, type.ToString(), methodName, dtoType.ToString());
		}

		public static void MethodTypeMismatch(BaseContext context, IPropertySymbol property, IMethodSymbol method, INamedTypeSymbol dtoType)
		{
			context.Report(MethodTypeMismatchDesc, property, method.Name, method.ContainingType.ToString(), method.ReturnType.ToString(),
						   dtoType.ToString());
		}

		public static void InaccessibleReferencedType(BaseContext context, INamedTypeSymbol type, IMethodSymbol refMethod)
		{
			context.Report(InaccessibleReferencedTypeDesc, refMethod, refMethod.Name, refMethod.ContainingType.ToString(), type.ToString());
		}

		public static void OverloadNotSupported(BaseContext context, IMethodSymbol methodSym)
		{
			context.Report(OverloadNotSupportedDesc, methodSym, methodSym.Name);
		}

		public static void PropertyAlreadyMatched(BaseContext context, IPropertySymbol property)
		{
			context.Report(PropertyAlreadyMatchedDesc, property, property.Name);
		}

		public static void UnexpectedReference(BaseContext context, IPropertySymbol property)
		{
			context.Report(UnexpectedReferenceDesc, property, property.Name);
		}

		public static void NullableReturnType(BaseContext context, IMethodSymbol methodSym)
		{
			context.Report(NullableReturnTypeDesc, methodSym, methodSym.Name);
		}

		public static void ParentIsNotAbstract(BaseContext context, INamedTypeSymbol type)
		{
			context.Report(ParentIsNotAbstractDesc, type, type.ToString(), type.BaseType.ToString());
		}

		public static void DBOAndDTOAbstractMismatch(BaseContext context, ITypeSymbol dtoType, INamedTypeSymbol dboType)
		{
			context.Report(DBOAndDTOAbstractMismatchDesc, dtoType, dboType.ToString(), dtoType.ToString());
		}

		public static void GenerateClientInterface(BaseContext context, INamedTypeSymbol type)
		{
			context.Report(GenerateClientInterfaceDesc, type, type.Name);
		}

		public static void DbAPIClassMustBePublic(BaseContext context, INamedTypeSymbol symbol)
		{
			context.Report(DbAPIClassMustBePublicDesc, symbol, symbol.ToString());
		}

		public static void DbAPIOperationMustBePublic(BaseContext context, IMethodSymbol method)
		{
			context.Report(DbAPIOperationMustBePublicDesc, method, method.ToString());
		}
	}
}
