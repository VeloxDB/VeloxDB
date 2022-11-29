using System;

namespace Velox.Protocol;

#pragma warning disable 1591

/// <summary>
/// Enum containing all possible database API definition errors.
/// </summary>
public enum DbAPIDefinitionErrorType
{
	/// <summary>
	/// Maximum number of parameters exceeded for operation.
	/// </summary>
	MaxParamCountExceeded = 1,

	/// <summary>
	/// Operation error type does not inherit from <see cref="Velox.Protocol.DbAPIErrorException"/>.
	/// </summary>
	InvalidExceptionBaseType = 2,

	/// <summary>
	/// Type is not serializable.
	/// </summary>
	NonSerializableType = 3,

	/// <summary>
	/// Type is not public.
	/// </summary>
	NonAccessibleType = 4,

	/// <summary>
	/// Type is generic. Generic types are not supported.
	/// </summary>
	GenericType = 5,

	/// <summary>
	/// Operation does not define mandatory parameters.
	/// </summary>
	OperationRequiredParamsMissing = 6,
	
	/// <summary>
	/// Operation name is already used by a different operation in the same API.
	/// </summary>
	OperationNameDuplicate = 7,
	
	/// <summary>
	/// Operation defines an out/ref parameter. Out/ref parameters are not supported.
	/// </summary>
	OutParam = 8,
	
	/// <summary>
	/// API contains a property. Properties are not supported.
	/// </summary>
	APIPropertyDefinition = 9,
	
	/// <summary>
	/// API contains an event. Events are not supported.
	/// </summary>
	APIEventDefinition = 10,
	
	/// <summary>
	/// API uses a name that is already used by another API.
	/// </summary>
	APINameDuplicate = 11,

	/// <summary>
	/// Type does not define parameterless constructor.
	/// </summary>
	MissingConstructor = 12,

	/// <summary>
	/// API is implemented by an abstract class or an interface.
	/// </summary>
	AbstractOrInterface = 13,

	/// <summary>
	/// Maximum number of properties in a single class/struct has been exceeded.
	/// </summary>
	MaxPropertyCountExceeded = 14,

	/// <summary>
	///	Type name is already used by a different type.
	/// </summary>
	TypeNameDuplicate = 15,
}

/// <summary>
/// The exception that is thrown when there is error in the database API definition.
/// </summary>
public class DbAPIDefinitionException : DbAPIErrorException
{
	DbAPIDefinitionErrorType errorType;
	string typeName;
	string methodName;

	///
	public DbAPIDefinitionException()
	{
	}

	private DbAPIDefinitionException(DbAPIDefinitionErrorType errorType, string typeName, string methodName)
	{
		this.errorType = errorType;
		this.typeName = typeName;
		this.methodName = methodName;
	}


	/// <summary>
	/// Indicates the type of error.
	/// </summary>
	public DbAPIDefinitionErrorType ErrorType { get => errorType; set => errorType = value; }
	
	/// <summary>
	/// The name of the type that caused an error. 
	/// </summary>
	public string TypeName { get => typeName; set => typeName = value; }
	
	/// <summary>
	/// The name of the method where error occurred.
	/// </summary>
	public string MethodName { get => methodName; set => methodName = value; }

	internal static DbAPIDefinitionException CreateMaxParamExceeded(string methodName, string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.MaxParamCountExceeded, methodName, typeName);
	}

	internal static DbAPIDefinitionException CreateInvalidExceptionBaseType(string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.InvalidExceptionBaseType, null, typeName);
	}

	internal static DbAPIDefinitionException CreateNonSerializableType(string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.NonSerializableType, null, typeName);
	}

	internal static DbAPIDefinitionException CreateNonAccessibleType(string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.NonAccessibleType, null, typeName);
	}

	internal static DbAPIDefinitionException CreateGenericType(string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.GenericType, null, typeName);
	}

	internal static DbAPIDefinitionException CreateOperationRequiredParamsMissing(string methodName, string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.OperationRequiredParamsMissing, typeName, methodName);
	}

	internal static DbAPIDefinitionException CreateOperationNameDuplicate(string methodName, string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.OperationNameDuplicate, methodName, typeName);
	}

	internal static DbAPIDefinitionException CreateTypeNameDuplicate(string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.TypeNameDuplicate, null, typeName);
	}

	internal static DbAPIDefinitionException CreateOutParam(string methodName, string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.OutParam, methodName, typeName);
	}

	internal static DbAPIDefinitionException CreateAPIPropertyDefinition(string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.APIPropertyDefinition, null, typeName);
	}

	internal static DbAPIDefinitionException CreateAPIEventDefinition(string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.APIEventDefinition, null, typeName);
	}

	internal static DbAPIDefinitionException CreateAPINameDuplicate(string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.APINameDuplicate, null, typeName);
	}

	internal static DbAPIDefinitionException CreateMissingConstructor(string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.MissingConstructor, null, typeName);
	}
	
	public static Exception CreateAbstractAPI(string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.AbstractOrInterface, null, typeName);
	}

	public static Exception CreateMaxPropertyCountExceeded(string typeName)
	{
		return new DbAPIDefinitionException(DbAPIDefinitionErrorType.MaxPropertyCountExceeded, null, typeName);
	}

	public override string Message
	{
		get
		{
			switch (errorType)
			{
				case DbAPIDefinitionErrorType.MaxParamCountExceeded:
					return string.Format("Maximum number of parameters exceeded for operation {0} in API {1}.", methodName, typeName);

				case DbAPIDefinitionErrorType.InvalidExceptionBaseType:
					return string.Format("Operation error type {0} does not inherit appropriate base type.", typeName);

				case DbAPIDefinitionErrorType.NonSerializableType:
					return string.Format("Type {0} is not serializable.", typeName);

				case DbAPIDefinitionErrorType.NonAccessibleType:
					return string.Format("Type {0} is not public.", typeName);

				case DbAPIDefinitionErrorType.GenericType:
					return string.Format("Type {0} is generic.", typeName);

				case DbAPIDefinitionErrorType.OperationRequiredParamsMissing:
					return string.Format("Operation {0} in API {1} does not define mandatory parameters.", methodName, typeName);

				case DbAPIDefinitionErrorType.OperationNameDuplicate:
					return string.Format("Operation name {0} in API {1} is already used by a different operation in the same API.",
						methodName, typeName);

				case DbAPIDefinitionErrorType.TypeNameDuplicate:
					return string.Format("Type name {0} is already used by a different type.", methodName, typeName);

				case DbAPIDefinitionErrorType.OutParam:
					return string.Format("Operation {0} in API {1} defines an out/ref parameter.", methodName, typeName);

				case DbAPIDefinitionErrorType.APIPropertyDefinition:
					return string.Format("API {0} contains a property.", typeName);

				case DbAPIDefinitionErrorType.APIEventDefinition:
					return string.Format("API {0} contains an event.", typeName);

				case DbAPIDefinitionErrorType.APINameDuplicate:
					return string.Format("API {0} uses a name that is already used by another API.", typeName);

				case DbAPIDefinitionErrorType.MissingConstructor:
					return string.Format("Type {0} does not define parameterless constructor.", typeName);

				case DbAPIDefinitionErrorType.AbstractOrInterface:
					return string.Format("Database API Type {0} is abstract which is invalid.", typeName);

				case DbAPIDefinitionErrorType.MaxPropertyCountExceeded:
					return string.Format("Maximum number of properties exceeded in database API type {0}.", typeName);

				default:
					throw new ArgumentException();
			}
		}
	}
}
