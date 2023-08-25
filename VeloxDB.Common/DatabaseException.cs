using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using VeloxDB.Common;

namespace VeloxDB;

/// <summary>
///	Exception thrown by VeloxDB.
/// </summary>
public class DatabaseException : Protocol.DbAPIErrorException
{
	DatabaseErrorDetail detail;

	internal DatabaseException()
	{
	}

	internal DatabaseException(DatabaseErrorDetail detail) :
		base(detail.ToString())
	{
		this.detail = detail;
	}

	internal DatabaseException(DatabaseErrorDetail detail, string message) :
		base(message)
	{
		this.detail = detail;
	}

	/// <summary>
	/// Provides detailed information about the cause of exception.
	/// </summary>
	public DatabaseErrorDetail Detail { get => detail; set => detail = value; }

	/// <summary>
	///  Message describing the exception.
	/// </summary>
	public override string Message => detail.ToString();
}

/// <summary>
/// Enum containing all possible database errors.
/// </summary>
public enum DatabaseErrorType
{
	/// <summary>
	/// Object references invalid class.
	/// </summary>
	[ErrorCode("Object of class {5} references invalid class {3} via property {4}.")]
	InvalidReferencedClass = 1,

	/// <summary>
	/// Write operation attempted in a read transaction.
	/// </summary>
	[ErrorCode("Write operation attempted in a read transaction.")]
	ReadTranWriteAttempt = 2,

	/// <summary>
	/// Changeset has an invalid format.
	/// </summary>
	[ErrorCode("Changeset has an invalid format.")]
	InvalidChangesetFormat = 3,

	/// <summary>
	/// Changeset operation uses null (zero) id.
	/// </summary>
	[ErrorCode("Changeset operation uses null (zero) id.")]
	ZeroIdProvided = 4,

	/// <summary>
	/// Log directory does not exist, is invalid, not an absolute path or inaccessible.
	/// </summary>
	[ErrorCode("Log directory does not exist, is invalid, not an absolute path or inaccessible.")]
	InvalidLogDirectory = 5,

	/// <summary>
	/// Log name is invalid.
	/// </summary>
	[ErrorCode("Log name is invalid.")]
	InvalidLogName = 6,

	/// <summary>
	/// Maximum number of allowed logs exceeded.
	/// </summary>
	[ErrorCode("Maximum number of allowed logs exceeded.")]
	LogCountLimitExceeeded = 7,

	/// <summary>
	/// Index type is invalid or one or more index property types are invalid.
	/// </summary>
	[ErrorCode("Index type is invalid or one or more index property types are invalid.")]
	InvalidIndex = 8,

	/// <summary>
	/// Assembly with same name already exist.
	/// </summary>
	[ErrorCode("Assembly with a name {5} already exist.")]
	AssemblyNameAlreadyExists = 9,

	/// <summary>
	/// Referenced assembly doesn't exist.
	/// </summary>
	[ErrorCode("Assembly with a name {5} and id {0} does not exist.")]
	UnknownUserAssembly = 10,

	/// <summary>
	/// Attempted to access transaction from the non-owner thread.
	/// </summary>
	[ErrorCode("Attempted to access transaction from the non-owner thread.")]
	InvalidTransactionThread = 11,

	/// <summary>
	/// Inverse reference is not tracked for referenced property.
	/// See <see cref="T:VeloxDB.ObjectInterface.DatabaseReferenceAttribute"/>.
	/// </summary>
	[ErrorCode("Inverse references are not tracked for reference property {0} of class {5}.")]
	InverseReferenceNotTracked = 12,

	/// <summary>
	/// Invalid request.
	/// </summary>
	[ErrorCode("The request is invalid. {6}")]
	InvalidRequest = 13,

	/// <summary>
	/// Log name is not unique.
	/// </summary>
	[ErrorCode("Log name is not unique.")]
	NonUniqueLogName = 14,

	/// <summary>
	/// Write operation attempted on an abstract class.
	/// </summary>
	[ErrorCode("Write operation attempted on the abstract class {5}.")]
	AbstractClassWriteAttempt = 15,

	/// <summary>
	/// Invalid property modification detected.
	/// </summary>
	[ErrorCode("Invalid property modification detected for property {4} in class {5}.")]
	InvalidPropertyTypeModification = 17,

	/// <summary>
	/// Inserted reference property has an invalid multiplicity of one.
	/// </summary>
	[ErrorCode("Inserted reference property {4} in class {5} has an invalid multiplicity of one.")]
	InsertedReferencePropertyMultiplicity = 18,

	/// <summary>
	/// Index has been added to an existing class, with a newly inserted property.
	/// This has to be done as two separate transactions.
	/// </summary>
	[ErrorCode("Index has been added to an existing class {5} with a newly inserted property {4}.")]
	InsertedPropertyClassAddedToIndex = 19,

	/// <summary>
	///  Server called with null argument.
	/// </summary>
	[ErrorCode("Argument {5} can't be null.")]
	NullArgument = 20,

	/// <summary>
	///  Server called with invalid argument.
	/// </summary>
	[ErrorCode("Argument {5} is invalid. {6}.")]
	InvalidArgument = 21,

	/// <summary>
	/// Invalid assembly filename. Name can contain only following characters a-z, A-Z, 0-9, _, . and space. It must end with ".dll".
	/// </summary>
	[ErrorCode("Invalid assembly filename {5}. Name can contain only following characters [a-z,A-Z,0-9,_,., ] and must end in .dll.")]
	InvalidAssemblyFilename = 22,

	/// <summary>
	/// Invalid assembly, the assembly has failed IL verification.
	/// </summary>
	[ErrorCode("Invalid assembly {5}.")]
	InvalidAssembly = 23,

	/// <summary>
	/// Uploaded assembly references an unknown assembly. Make sure that you have provided all assemblies.
	/// </summary>
	[ErrorCode("Assembly {5} references an unknown assembly {3}.")]
	MissingReferencedAssembly = 24,

	/// <summary>
	/// Invalid directory name. Name can contain only following characters a-z, A-Z, 0-9, _, ., / and space.
	/// </summary>
	[ErrorCode("Invalid directory name {5}. Name can contain only following characters [a-z,A-Z,0-9,_,.,/, ]")]
	InvalidDirectoryName = 25,

	/// <summary>
	/// Not in global write cluster.
	/// </summary>
	[ErrorCode("Not in global write cluster.")]
	NotInGlobalWriteCluster = 26,

	/// <summary>
	/// Not in local write cluster.
	/// </summary>
	[ErrorCode("Not in local write cluster.")]
	NotInLocalWriteCluster = 27,

	/// <summary>
	/// Property has an invalid type. The type is not supported by VeloxDB.
	/// </summary>
	[ErrorCode("Property {4} of class {5} has an invalid type.")]
	InvalidPropertyType = 28,

	/// <summary>
	/// Invalid class id detected.
	/// </summary>
	[ErrorCode("Invalid class id detected for class {5}.")]
	InvalidClassId = 29,

	/// <summary>
	/// Maximum number of indexes per single class exceeded for class.
	/// </summary>
	[ErrorCode("Maximum number of indexes per single class exceeded for class {5}.")]
	MaximumNumberOfIndexesPerClassExceeded = 30,

	/// <summary>
	/// Class contains unknown index.
	/// </summary>
	[ErrorCode("Class {5} contains unknown index {3}.")]
	UnknownIndex = 31,

	/// <summary>
	/// Number of properties in a class exceeds maximum allowed count.
	/// </summary>
	[ErrorCode("Class {5} property count exceeds maximum allowed count.")]
	MaximumNumberOfPropertiesInClassExceeded = 32,

	/// <summary>
	/// Base class could not be found.
	/// </summary>
	[ErrorCode("Base class of class {5} could not be found.")]
	UnknownBaseClass = 33,

	/// <summary>
	/// Abstract class must not inherit from non abstract class.
	/// </summary>
	[ErrorCode("Abstract class {5} must not inherit from non-abstract class.")]
	AbstractClassNonAbstractParent = 34,

	/// <summary>
	/// Circular inheritance chain detected.
	/// </summary>
	[ErrorCode("Circular inheritance chain detected with class {5}.")]
	CircularInheritance = 35,

	/// <summary>
	/// Property with duplicate name detected.
	/// </summary>
	[ErrorCode("Property {4} of class {5} does not have a unique name.")]
	DuplicatePropertyName = 36,

	/// <summary>
	/// Property with duplicate id detected.
	/// </summary>
	[ErrorCode("Property {4} in class {5} does not have a unique property id in an entire model.")]
	DuplicatePropertyId = 37,

	/// <summary>
	/// Index does not have a unique full name.
	/// </summary>
	[ErrorCode("Invalid model. Index {5} with id {0} does not have a unique full name.")]
	DuplicateIndexName = 38,

	/// <summary>
	/// Class with duplicated Id detected.
	/// </summary>
	[ErrorCode("Invalid model. Class {5} with id {0} does no have unique id.")]
	DuplicateClassId = 39,

	/// <summary>
	/// Class with duplicate name detected.
	/// </summary>
	[ErrorCode("Invalid model. Class {5} with id {0} does no have unique full name.")]
	DuplicateClassName = 40,

	/// <summary>
	/// Index does not define any properties.
	/// </summary>
	[ErrorCode("Index {5} does not define any properties.")]
	IndexWithoutProperties = 41,

	/// <summary>
	/// No classes indexed by the index.
	/// </summary>
	[ErrorCode("No classes indexed by the index {5}.")]
	IndexWithoutClasses = 42,

	/// <summary>
	/// Maximum number of properties exceeded in an index.
	/// </summary>
	[ErrorCode("Maximum number of properties exceeded in an index {5}.")]
	MaximumNumberOfPropertiesInIndexExceeded = 43,

	/// <summary>
	/// Class may not be indexed by the index because it does not contain referenced property.
	/// </summary>Index {5} indexes invalid property {4}.
	[ErrorCode("Class {5} may not be indexed by the index {3} because it does not contain property {4}.")]
	IndexIndexesUnknownProperty = 44,

	/// <summary>
	/// Index indexes invalid property.
	/// </summary>
	[ErrorCode("Index {5} indexes invalid property {4}.")]
	IndexIndexesInvalidProperty = 45,

	/// <summary>
	/// Property is indexed multiple times by the same index.
	/// </summary>
	[ErrorCode("Property {4} is indexed multiple times by the same index {5}.")]
	IndexIndexesPropertyMultipleTimes = 46,

	/// <summary>
	/// Reference property references invalid class.
	/// </summary>
	[ErrorCode("Reference property {4} of class {5} references invalid class {3}.")]
	ReferencePropertyReferencesInvalidClass = 47,

	/// <summary>
	/// Inverse reference property is of invalid type. Inverse references must be of type <see cref="T:VeloxDB.ObjectInterface.InverseReferenceSet`1"/>.
	/// </summary>
	[ErrorCode("Inverse reference property {4} of class {5} is of invalid type.")]
	InvalidInverseReferencePropertyType = 48,

	/// <summary>
	/// Inverse reference property must not define a setter.
	/// </summary>
	[ErrorCode("Inverse reference property {4} of class {5} must not define a setter.")]
	ReferencePropertyWithSetter = 49,

	/// <summary>
	/// Property must not specify SetToNull because it has multiplicity set to one.
	/// </summary>
	[ErrorCode("Property {4} of class {5} must not specify SetToNull because it has multiplicity set to One.")]
	PropertyCantBeSetToNull = 50,

	/// <summary>
	/// Delete target action is invalid for reference.
	/// </summary>
	[ErrorCode("Delete target action is invalid for reference {4} of class {5}.")]
	InvalidDeleteTargetAction = 51,

	/// <summary>
	/// Property of class references unknown class.
	/// </summary>
	[ErrorCode("Property {4} of class {5} references unknown class {3}.")]
	PropertyReferencesUnknownClass = 52,

	/// <summary>
	/// Default value for string property is not allowed.
	/// </summary>
	[ErrorCode("Default value for string property {4} in class {5} is not allowed.")]
	StringPropertyCantHaveDefaultValue = 53,

	/// <summary>
	/// Default value for property is invalid.
	/// </summary>
	[ErrorCode("Default value for property {4} in class {5} is invalid")]
	InvalidDefaultValue = 54,

	/// <summary>
	/// Inverse reference property targets unknown class.
	/// </summary>
	[ErrorCode("Inverse reference property {4} of class {5} targets unknown class {3}.")]
	InverseReferencePropertyTargetsUnknownClass = 55,

	/// <summary>
	/// Inverse reference property targets unknown reference property.
	/// </summary>
	[ErrorCode("Inverse reference property {4} of class {5} targets unknown reference property {3}.")]
	InverseReferencePropertyTargetsUnknownProperty = 56,

	/// <summary>
	/// Inverse reference property targets nontracked reference property.
	/// </summary>
	[ErrorCode("Inverse reference property {4} of class {5} targets nontracked reference property {3}.")]
	InverseReferencePropertyTargetsUntrackedProperty = 57,

	/// <summary>
	/// Inverse reference property targets invalid class which does not own the reference.
	/// </summary>
	[ErrorCode("Inverse reference property {4} of class {5} targets invalid class {3} which does not own the reference.")]
	InverseReferencePropertyTargetsInvalidClass = 58,

	/// <summary>
	/// Class is not decorated with <see cref="T:VeloxDB.ObjectInterface.DatabaseClassAttribute"/>.
	/// </summary>
	[ErrorCode("Class {5} is not decorated with DatabaseClassAttribute.")]
	MissingAttribute = 59,

	/// <summary>
	/// Class must be defined in a namespace.
	/// </summary>
	[ErrorCode("Class {5} is not defined in a namespace.")]
	ClassWithoutNamespace = 60,

	/// <summary>
	/// Class does not contain DatabaseObject in its hierarchy.
	/// </summary>
	[ErrorCode("Class does not contain DatabaseObject in its hierarchy.")]
	MustInheritDatabaseObject = 61,

	/// <summary>
	/// Generic classes are not supported as DatabaseObjects.
	/// </summary>
	[ErrorCode("Class {5} is generic.")]
	GenericClassNotSupported = 62,

	/// <summary>
	/// Database class does not provide an empty constructor.
	/// </summary>
	[ErrorCode("Database class {5} does not provide an empty constructor.")]
	MissingEmptyConstructor = 63,

	/// <summary>
	/// Non-abstract class declares abstract property.
	/// </summary>
	[ErrorCode("Non-abstract class {5} declares abstract property {4}.")]
	AbstractPropertyInNonAbstractClass = 64,

	/// <summary>
	/// Non-abstract class declares abstract method.
	/// </summary>
	[ErrorCode("Non-abstract class {5} declares abstract method {4}.")]
	AbstractMethodInNonAbstractClass = 65,

	/// <summary>
	/// Non-abstract class declares abstract event.
	/// </summary>
	[ErrorCode("Non-abstract class {5} declares abstract event {4}.")]
	AbstractEventInNonAbstractClass = 66,

	/// <summary>
	/// Inverse reference property must have a getter.
	/// </summary>
	[ErrorCode("Inverse reference property {4} of class {5} must have a getter.")]
	MissingGetter = 67,

	/// <summary>
	/// Inverse reference property must not define a setter.
	/// </summary>
	[ErrorCode("Inverse reference property {4} of class {5} must not define a setter.")]
	SetterFound = 68,

	/// <summary>
	/// Inverse reference property must be declared abstract.
	/// </summary>
	[ErrorCode("Inverse reference property {4} of class {5} must be declared abstract.")]
	InverseRereferncePropertyIsNotAbstract = 69,

	/// <summary>
	/// Property must be declared abstract.
	/// </summary>
	[ErrorCode("Property {4} of class {5} must be declared abstract.")]
	PropertyIsNotAbstract = 70,

	/// <summary>
	/// Property must define getter and setter.
	/// </summary>
	[ErrorCode("Property {4} of class {5} must define getter and setter.")]
	PropertyMissingGetterAndSetter = 71,

	/// <summary>
	/// Constructor of Database API class throws an exception.
	/// </summary>
	[ErrorCode("Constructor of class {5} threw an exception {6}.")]
	FailedToCreateInstance = 72,

	/// <summary>
	/// Transaction is closed and cannot be committed.
	/// </summary>
	[ErrorCode("Transaction is closed and cannot be committed.")]
	CommitClosedTransaction = 73,

	/// <summary>
	/// Invalid target of inverse reference.
	/// </summary>
	[ErrorCode("Invalid target of inverse reference {4} on class {5}.")]
	InvalidInverseReferenceTarget = 74,

	/// <summary>
	/// Number of references referencing a given class exceeds maximum allowed number.
	/// </summary>
	[ErrorCode("Number of inverse refereces on class {5} exceeds maximum allowed number.")]
	MaximumNumberOfInverseReferencesPerClass = 75,

	/// <summary>
	/// Property has an invalid type. This might occurr if a reference property is marked with
	/// <see cref="T:VeloxDB.ObjectInterface.DatabasePropertyAttribute"/>.
	/// </summary>
	[ErrorCode("Property {4} of class {5} has an invalid type.")]
	PropertyTypeInvalid = 76,

	/// <summary>
	/// Transaction has been closed. It can no longer be used.
	/// </summary>
	[ErrorCode("Transaction has been closed. It can no longer be used.")]
	TransactionClosed = 77,

	/// <summary>
	/// Database class is not public.
	/// </summary>
	[ErrorCode("Database class {5} is not public.")]
	DatabaseClassNotPublic = 78,

	/// <summary>
	/// Base error for all DbAPI errors.
	/// </summary>
	DbAPIBaseError = 2000,

	/// <summary>
	/// Maximum number of parameters exceeded for operation.
	/// </summary>
	[ErrorCode("Maximum number of parameters exceeded for operation {4} in API {5}.")]
	DbAPIMaxParamCountExceeded = 2001,

	/// <summary>
	/// Operation error type does not inherit from <see cref="VeloxDB.Protocol.DbAPIErrorException"/>.
	/// </summary>
	[ErrorCode("Operation error type {5} does not inherit appropriate base type.")]
	DbAPIInvalidExceptionBaseType = 2002,

	/// <summary>
	/// Type is not serializable.
	/// </summary>
	[ErrorCode("Type {5} is not serializable.")]
	DbAPINonSerializableType = 2003,

	/// <summary>
	/// Type is not public.
	/// </summary>
	[ErrorCode("Type {5} is not public.")]
	DbAPIUnaccessibleType = 2004,

	/// <summary>
	/// Type is generic. Generic types are not supported.
	/// </summary>
	[ErrorCode("Type {5} is generic.")]
	DbAPIGenericType = 2005,

	/// <summary>
	/// Operation does not define mandatory parameters.
	/// </summary>
	[ErrorCode("Operation {4} in API {5} does not define mandatory parameters.")]
	DbAPIOperationRequiredParamsMissing = 2006,

	/// <summary>
	/// Operation name is already used by a different operation in the same API.
	/// </summary>
	[ErrorCode("Operation name {4} in API {5} is already used by a different operation in the same API.")]
	DbAPIDuplicateOperationName = 2007,

	/// <summary>
	/// Operation defines an out/ref parameter. Out/ref parameters are not supported.
	/// </summary>
	[ErrorCode("Operation {4} in API {5} defines an out/ref parameter.")]
	DbAPIOutParam = 2008,

	/// <summary>
	/// API contains a property. Properties are not supported.
	/// </summary>
	[ErrorCode("API {5} contains a property.")]
	DbAPIPropertyDefinition = 2009,

	/// <summary>
	/// API contains an event. Events are not supported.
	/// </summary>
	[ErrorCode("API {5} contains an event.")]
	DbAPIEventDefinition = 2010,

	/// <summary>
	/// API uses a name that is already used by another API.
	/// </summary>
	[ErrorCode("API {5} uses a name that is already used by another API.")]
	DbAPINameDuplicate = 2011,

	/// <summary>
	/// Type does not define parameterless constructor.
	/// </summary>
	[ErrorCode("Type {5} does not define parameterless constructor.")]
	DbAPIMissingConstructor = 2012,

	/// <summary>
	/// Type does not define parameterless constructor.
	/// </summary>
	[ErrorCode("Database API type {5} is abstract which is invalid.")]
	DbAPIAbstractOrInterface = 2013,

	/// <summary>
	/// Maximum number of properties exceeded in a single database API type (including inherited properties).
	/// </summary>
	[ErrorCode("Maximum number of properties exceeded in database API type {5}.")]
	DbAPIMaxPropertyCountExceeded = 2014,

	/// <summary>
	/// Property has an invalid null reference.
	/// </summary>
	[ErrorCode("Property {4} of class {5} has an invalid null reference.")]
	NullReferenceNotAllowed = 5001,

	/// <summary>
	/// Object could not be updated because it does not exist in the database.
	/// </summary>
	[ErrorCode("Object {0} of class {5} could not be updated because it does not exist in the database.")]
	UpdateNonExistent = 5002,

	/// <summary>
	/// Object could not be deleted because it does not exist in the database.
	/// </summary>
	[ErrorCode("Object {0} of class {5} could not be deleted because it does not exist in the database.")]
	DeleteNonExistent = 5003,

	/// <summary>
	/// Object could not be deleted because it is being referenced by another object.
	/// </summary>
	[ErrorCode("Object {0} could not be deleted because it is being referenced by the object {1} of class {5} via property {4}.")]
	DeleteReferenced = 5004,

	/// <summary>
	/// Property references unknown object."
	/// </summary>
	[ErrorCode("Property {4} of the object {0} of class {3} references unknown object {2}.")]
	UnknownReference = 5005,

	/// <summary>
	/// Commit request did not produce a valid response. This can occur when primary write replica experiences connectivity issues. The result of the commit attempt is unknown.
	/// </summary>
	[ErrorCode("Commit request did not produce a valid response. This can occur when primary write replica experiences connectivity " +
		"issues. The result of the commit attempt is unknown.")]
	UnavailableCommitResult = 5006,

	/// <summary>
	/// Uniqueness constraint has been violated on the index.
	/// </summary>
	[ErrorCode("Uniqueness constraint has been violated on the index {5}.")]
	UniquenessConstraint = 5007,

	/// <summary>
	/// Id uniqueness constraint has been violated.
	/// </summary>
	[ErrorCode("Id uniqueness constraint has been violated on the class {5}, id = {0}.")]
	NonUniqueId = 5008,

	/// <summary>
	/// User transactions are not allowed because persistance of user database has not been configured.
	/// </summary>
	[ErrorCode("User transactions are not allowed because persistance of user database has not been configured.")]
	MissingPersistanceDescriptor = 5009,

	/// <summary>
	/// Database has been disposed.
	/// </summary>
	[ErrorCode("Database has been disposed.")]
	DatabaseDisposed = 5010,

	/// <summary>
	/// Assembly Version Guid does not match the current assembly version guid. This indicates that assemblies have been changed during the update process.
	/// </summary>
	[ErrorCode("Assembly Version Guid does not match the current assembly version guid. This indicates that assemblies have been changed during the update process.")]
	InvalidAssemblyVersionGuid = 5011,

	/// <summary>
	/// Database failed to generate an id for a newly created object. Id range in the database has been used up.
	/// </summary>
	[ErrorCode("Database failed to generate an id for a newly created object. Id range in the database has been used up.")]
	IdUnavailable = 5012,

	/// <summary>
	/// This error is never returned, all retryable error codes have value larger than this error.
	/// </summary>
	RetryableBaseError = 10000,

	/// <summary>
	/// Object is in conflict with another transaction.
	/// </summary>
	[ErrorCode("Object {0} of class {5} is in conflict with another transaction.")]
	Conflict = 10001,

	/// <summary>
	/// Failed to read index key. Limit for maximum lock contention has been exceeded.
	/// </summary>
	[ErrorCode("Failed to read index {5} key. Limit for maximum lock contention has been exceeded.")]
	IndexLockContentionLimitExceeded = 10002,

	/// <summary>
	/// Conflict occured on a key in an index.
	/// </summary>
	[ErrorCode("Conflict occured on a key in an index.")]
	IndexConflict = 10003,

	/// <summary>
	/// Transaction hase been closed internally by the database.
	/// </summary>
	[ErrorCode("Transaction hase been closed internally by the database.")]
	TransactionCanceled = 10004,

	/// <summary>
	/// Concurrent update of configuration occurred.
	/// </summary>
	[ErrorCode("Concurrent update of configuration occurred.")]
	ConcurrentConfigUpdate = 10005,

	/// <summary>
	/// Transaction is not allowed. This usually occurs when a transaction is attempted on a non primary write replica,
	/// or a write transaction is attempted on a read replica.
	/// </summary>
	[ErrorCode("Transaction is not allowed. This usually occurs when a transaction is attempted on a non primary write replica, or " +
		"a write transaction is attempted on a read replica.")]
	TransactionNotAllowed = 10006,

	/// <summary>
	/// Maximum number of concurrent transactions has been exceeded.
	/// </summary>
	[ErrorCode("Maximum number of concurrent transaction has been exceeded.")]
	ConcurrentTranLimitExceeded = 10007,

	/// <summary>
	/// An internal state change has prevented the transaction from being created.
	/// </summary>
	[ErrorCode("An internal state change has prevented the transaction from being created.")]
	DatabaseBusy = 10008,

	/// <summary>
	/// User database data model has been modified.
	/// </summary>
	[ErrorCode("User database data model has been modified.")]
	InvalidModelDescVersion = 10009,

	/// <summary>
	/// This operation is not applicable.
	/// </summary>
	[ErrorCode("This operation is not applicable.")]
	NotApplicable = 10010,
}

/// <summary>
/// Provides details on the cause of database exception.
/// </summary>
public sealed class DatabaseErrorDetail
{
	private static readonly ErrorCodeFormatter errorFormater;

	DatabaseErrorType errorType;
	string stackTrace;

	[ErrorCodeParam(0)]
	long id;

	[ErrorCodeParam(1)]
	long referencingId;

	[ErrorCodeParam(2)]
	long referencedId;

	[ErrorCodeParam(3)]
	string secondaryName;

	[ErrorCodeParam(4)]
	string memberName;

	[ErrorCodeParam(5)]
	string primaryName;

	[ErrorCodeParam(6)]
	string message;

	static DatabaseErrorDetail()
	{
		errorFormater = new ErrorCodeFormatter(typeof(DatabaseErrorDetail), typeof(DatabaseErrorType));
	}

	internal DatabaseErrorDetail()
	{
	}

	internal DatabaseErrorDetail(DatabaseErrorType errorType, long id = 0, string secondaryName = null,
		string memberName = null, long referencingId = 0, long referencedId = 0, string primaryName = null, string message = null)
	{
		this.errorType = errorType;
		this.id = id;
		this.secondaryName = secondaryName;
		this.memberName = memberName;
		this.referencingId = referencingId;
		this.referencedId = referencedId;
		this.primaryName = primaryName;
		this.message = message;
		this.stackTrace = (new StackTrace(true)).ToString();

		TTTrace.Write((int)errorType, id, secondaryName, memberName, referencingId, referencedId, primaryName, message, this.stackTrace);
	}

	/// <summary>
	/// Specifies the type of error.
	/// </summary>
	public DatabaseErrorType ErrorType { get => errorType; set => errorType = value; }

	/// <summary>
	/// Id of object that caused error. The meaning of this field depends on the value of <see cref="ErrorType"/>.
	/// </summary>
	public long Id { get => id; set => id = value; }

	/// <summary>
	/// Id of the object that references problematic object. The meaning of this field depends on the value of <see cref="ErrorType"/>.
	/// </summary>
	public long ReferencingId { get => referencingId; set => referencingId = value; }

	/// <summary>
	/// Id of object referenced by problematic object. The meaning of this field depends on the value of <see cref="ErrorType"/>.
	/// </summary>
	public long ReferencedId { get => referencedId; set => referencedId = value; }

	/// <summary>
	/// Additional name, often a name of referencing class. The meaning of this field depends on the value of <see cref="ErrorType"/>.
	/// </summary>
	public string SecondaryName { get => secondaryName; set => secondaryName = value; }

	/// <summary>
	/// Name of the member error references. The meaning of this field depends on the value of <see cref="ErrorType"/>.
	/// </summary>
	public string MemberName { get => memberName; set => memberName = value; }

	/// <summary>
	/// This field carries name, often class name. The meaning of this field depends on the value of <see cref="ErrorType"/>.
	/// </summary>
	public string PrimaryName { get => primaryName; set => primaryName = value; }

	/// <summary>
	/// Error message. The meaning of this field depends on the value of <see cref="ErrorType"/>.
	/// </summary>
	public string Message { get => message; set => message = value; }

	internal string StackTrace { get => stackTrace; set => stackTrace = value; }

	internal DatabaseErrorDetail Clone()
	{
		return new DatabaseErrorDetail()
		{
			errorType = this.errorType,
			id = this.id,
			memberName = this.memberName,
			primaryName = this.primaryName,
			secondaryName = this.secondaryName,
			referencedId = this.referencedId,
			referencingId = this.referencingId,
			message = this.message,
			stackTrace = this.stackTrace
		};
	}

	internal static DatabaseErrorDetail Create(DatabaseErrorType errorType)
	{
		return new DatabaseErrorDetail(errorType);
	}

	internal static DatabaseErrorDetail Create(DatabaseErrorType errorType, string message)
	{
		return new DatabaseErrorDetail(errorType, message: message);
	}

	internal static DatabaseErrorDetail CreateUniquenessConstraint(string indexName)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.UniquenessConstraint, 0, null, null, 0, 0, indexName);
	}

	internal static DatabaseErrorDetail CreateAbstractClassWriteAttempt(string className)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.AbstractClassWriteAttempt, 0, null, null, 0, 0, className);
	}

	internal static DatabaseErrorDetail CreateInverseReferenceNotTracked(long id, string className)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.InverseReferenceNotTracked, id, null, null, 0, 0, className);
	}

	internal static DatabaseErrorDetail CreateZeroIdProvided(long id)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.ZeroIdProvided, id);
	}

	internal static DatabaseErrorDetail CreateInvalidChangeset()
	{
		return new DatabaseErrorDetail(DatabaseErrorType.InvalidChangesetFormat, 0);
	}

	internal static DatabaseErrorDetail CreateConflict(long id, string className)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.Conflict, id, null, null, 0, 0, className);
	}

	internal static DatabaseErrorDetail CreateUpdateNonExistent(long id, string className)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.UpdateNonExistent, id, null, null, 0, 0, className);
	}

	internal static DatabaseErrorDetail CreateNonExistentDelete(long id, string className)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.DeleteNonExistent, id, null, null, 0, 0, className);
	}

	internal static DatabaseErrorDetail CreateUnknownReference(long id,
		string invalidRefClassName, string invalidRefPropName, long referencedId)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.UnknownReference, id,
			invalidRefClassName, invalidRefPropName, 0, referencedId, null);
	}

	internal static DatabaseErrorDetail CreateNullReferenceNotAllowed(long id, string className, string invalidRefPropName)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.NullReferenceNotAllowed, id, null, invalidRefPropName, 0, 0, className);
	}

	internal static DatabaseErrorDetail CreateInvalidReferencedClass(long id, string className,
		string invalidRefClassName, string invalidRefPropName, long referencedId)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.InvalidReferencedClass, id,
			invalidRefClassName, invalidRefPropName, 0, referencedId, className);
	}

	internal static DatabaseErrorDetail CreateReferencedDelete(long id, long referencingId,
		string invalidRefClassName, string invalidRefPropName)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.DeleteReferenced, id: id,
			primaryName: invalidRefClassName, memberName: invalidRefPropName, referencingId: referencingId);
	}

	internal static DatabaseErrorDetail CreateNonUniqueId(long id, string className)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.NonUniqueId, id, null, null, 0, 0, className);
	}

	internal static DatabaseErrorDetail CreateIndexLockContentionLimitExceeded(string indexName)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.IndexLockContentionLimitExceeded, 0, null, null, 0, 0, indexName);
	}
	internal static DatabaseErrorDetail CreateInvalidPropertyTypeModification(string className, string propertyName)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.InvalidPropertyTypeModification, 0, null, propertyName, 0, 0, className);
	}

	internal static DatabaseErrorDetail CreateInsertedReferencePropertyMultiplicity(string className, string propertyName)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.InsertedReferencePropertyMultiplicity, 0, null, propertyName, 0, 0, className);
	}

	internal static DatabaseErrorDetail CreateInsertedPropertyClassAddedToIndex(string className, string propertyName)
	{
		return new DatabaseErrorDetail(DatabaseErrorType.InsertedPropertyClassAddedToIndex, 0, null, propertyName, 0, 0, className);
	}

	/// <summary>
	/// Returns if the error can be safely retried.
	/// </summary>
	/// <returns></returns>
	public bool IsRetryable()
	{
		return errorType > DatabaseErrorType.RetryableBaseError;
	}

	/// <summary>
	/// Return string representation of the error.
	/// </summary>
	/// <returns></returns>
	public override string ToString()
	{
		return errorFormater.GetMessage(this, errorType);
	}
}


internal static class Throw
{
	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void UsingDetail(DatabaseErrorDetail detail)
	{
		throw new DatabaseException(detail);
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void AssemblyNameAlreadyExists(string name)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.AssemblyNameAlreadyExists, primaryName: name));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void UnknownUserAssembly(long id, string name)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.UnknownUserAssembly, id, primaryName: name));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void NullArgument(string argumentName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.NullArgument, primaryName: argumentName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InvalidArgument(string argumentName, string reason)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InvalidArgument, primaryName: argumentName, message: reason));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InvalidAssemblyFilename(string name)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InvalidAssemblyFilename, primaryName: name));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InvalidDirectoryName(string name)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InvalidDirectoryName, primaryName: name));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InvalidLogName(string name)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InvalidLogName, primaryName: name));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InvalidAssembly(string name)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InvalidAssembly, primaryName: name));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InvalidPropertyType(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InvalidPropertyType, memberName: propName, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InvalidClassId(string className)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InvalidClassId, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void MaximumNumberOfIndexesPerClassExceeded(string className)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.MaximumNumberOfIndexesPerClassExceeded, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void UnknownIndex(string className, string indexName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.UnknownIndex, primaryName: className, secondaryName: indexName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void MaximumNumberOfPropertiesInClassExceeded(string className)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.MaximumNumberOfPropertiesInClassExceeded, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void MaximumNumberOfInverseReferencesPerClass(string className)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.MaximumNumberOfInverseReferencesPerClass, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void UnknownBaseClass(string className)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.UnknownBaseClass, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void AbstractClassNonAbstractParent(string className)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.AbstractClassNonAbstractParent, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void CircularInheritance(string className)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.CircularInheritance, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void DuplicatePropertyName(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.DuplicatePropertyName, primaryName: className, memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void DuplicatePropertyId(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.DuplicatePropertyId, primaryName: className, memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void DuplicateIndexName(string indexName, short id)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.DuplicateIndexName, primaryName: indexName, id: id));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void DuplicateClassId(string className, short id)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.DuplicateClassId, primaryName: className, id: id));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void DuplicateClassName(string className, short id)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.DuplicateClassName, primaryName: className, id: id));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void IndexWithoutProperties(string indexName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.IndexWithoutProperties, primaryName: indexName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void IndexWithoutClasses(string indexName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.IndexWithoutClasses, primaryName: indexName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void MaximumNumberOfPropertiesInIndexExceeded(string indexName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.MaximumNumberOfPropertiesInIndexExceeded,
															primaryName: indexName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void IndexIndexesUnknownProperty(string className, string indexName, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.IndexIndexesUnknownProperty, primaryName: className,
															secondaryName: indexName, memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void IndexIndexesInvalidProperty(string indexName, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.IndexIndexesInvalidProperty, primaryName: indexName,
															memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void IndexIndexesPropertyMultipleTimes(string indexName, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.IndexIndexesPropertyMultipleTimes,
															primaryName: indexName, memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void ReferencePropertyReferencesInvalidClass(string className, string propName, string refClassName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.ReferencePropertyReferencesInvalidClass,
															primaryName: className, memberName: propName, secondaryName: refClassName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InvalidInverseReferencePropertyType(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InvalidInverseReferencePropertyType,
															primaryName: className, memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void ReferencePropertyWithSetter(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.ReferencePropertyWithSetter,
															primaryName: className, memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void PropertyCantBeSetToNull(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.PropertyCantBeSetToNull,
															primaryName: className, memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InvalidDeleteTargetAction(string className, string refName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InvalidDeleteTargetAction, primaryName: className,
															memberName: refName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void PropertyReferencesUnknownClass(string className, string propName, string refClassName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.PropertyReferencesUnknownClass,
															primaryName: className, memberName: propName, secondaryName: refClassName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void StringPropertyCantHaveDefaultValue(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.StringPropertyCantHaveDefaultValue,
															primaryName: className, memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InvalidDefaultValue(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InvalidDefaultValue,
															primaryName: className, memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InverseReferencePropertyTargetsUnknownClass(string className, string propName, string targetClass)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InverseReferencePropertyTargetsUnknownClass,
															primaryName: className, memberName: propName, secondaryName: targetClass));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InverseReferencePropertyTargetsUnknownProperty(string className, string propName, string targetPropName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InverseReferencePropertyTargetsUnknownProperty,
															primaryName: className, memberName: propName, secondaryName: targetPropName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InverseReferencePropertyTargetsUntrackedProperty(string className, string propName, string targetPropName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InverseReferencePropertyTargetsUntrackedProperty,
															primaryName: className, memberName: propName, secondaryName: targetPropName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InverseReferencePropertyTargetsInvalidClass(string className, string propName, string targetClass)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InverseReferencePropertyTargetsInvalidClass,
															primaryName: className, memberName: propName, secondaryName: targetClass));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void MissingAttribute(string className)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.MissingAttribute, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void ClassWithoutNamespace(string className)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.ClassWithoutNamespace, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void MustInheritDatabaseObject(string className)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.MustInheritDatabaseObject, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void GenericClassNotSupported(string className)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.GenericClassNotSupported, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InvalidInverseReferenceTarget(string className, string propertyName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InvalidInverseReferenceTarget,
			primaryName: className, memberName: propertyName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void DatabaseClassNotPublic(string className)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.DatabaseClassNotPublic, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void MissingEmptyConstructor(string className)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.MissingEmptyConstructor, primaryName: className));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void AbstractPropertyInNonAbstractClass(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.AbstractPropertyInNonAbstractClass, primaryName: className,
															memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void AbstractMethodInNonAbstractClass(string className, string methodName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.AbstractMethodInNonAbstractClass, primaryName: className,
															memberName: methodName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void AbstractEventInNonAbstractClass(string className, string eventName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.AbstractEventInNonAbstractClass, primaryName: className,
															memberName: eventName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void MissingGetter(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.MissingGetter, primaryName: className, memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void SetterFound(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.SetterFound, primaryName: className, memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InverseRereferncePropertyIsNotAbstract(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InverseRereferncePropertyIsNotAbstract, primaryName: className,
															memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void PropertyIsNotAbstract(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.PropertyIsNotAbstract, primaryName: className,
															memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void PropertyMissingGetterAndSetter(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.PropertyMissingGetterAndSetter, primaryName: className,
															memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void PropertyTypeInvalid(string className, string propName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.PropertyTypeInvalid, primaryName: className,
															memberName: propName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void FailedToCreateInstance(string className, string exception)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.FailedToCreateInstance, primaryName: className,
															message: exception));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InvalidInverseReferenceTarget(string apiName)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.InvalidInverseReferenceTarget, primaryName: apiName));
	}

	[DoesNotReturnAttribute]
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void DbAPIDefinitionException(Protocol.DbAPIDefinitionException exc)
	{
		throw new DatabaseException(new DatabaseErrorDetail(DatabaseErrorType.DbAPIBaseError + (int)exc.ErrorType,
															primaryName: exc.TypeName, memberName: exc.MethodName));
	}
}
