using System;

namespace VeloxDB.Protocol;

/// <summary>
/// Specifies possible types of the database operations with regards to whether the operation performs only read or
/// both read and write operations on the database.
/// </summary>
public enum DbAPIOperationType : byte
{
	// Values here must match the values in TransactionType enum.

	/// <summary>
	/// Indicates that the operation performs only read operations on the database. The underlying transaction
	/// created for the operation is of read type.
	/// </summary>
	Read = 0,

	/// <summary>
	/// Indicates that the operation performs both read and write operations on the database. The underlying transaction
	/// created for the operation is of read-write type.
	/// </summary>
	ReadWrite = 1,
}

/// <summary>
/// Spcifies whether the serializer supports proper serialization of object graphs in a given database operation.
/// </summary>
[Flags]
public enum DbAPIObjectGraphSupportType
{
	/// <summary>
	/// Both reqest and response data is not serialized using support for object graphs.
	/// </summary>
	None = 0x00,

	/// <summary>
	/// Request data is serialized using support for object graphs.
	/// </summary>
	Request = 0x01,

	/// <summary>
	/// Response data is serialized using support for object graphs.
	/// </summary>
	Response = 0x02,

	/// <summary>
	/// Both response and response data is serialized using support for object graphs.
	/// </summary>
	Both = Request | Response
}

/// <summary>
/// Specifies that the method is a database API operation. Database API operations can be invoked
/// from the client using VeloxDB protocol.
/// </summary>
[AttributeUsage(AttributeTargets.Method, Inherited = false, AllowMultiple = false)]
public sealed class DbAPIOperationAttribute : Attribute
{
	string name;
	DbAPIOperationType operationType;
	DbAPIObjectGraphSupportType objectGraphSupport;

	///
	public DbAPIOperationAttribute()
	{
		operationType = DbAPIOperationType.ReadWrite;
		objectGraphSupport = DbAPIObjectGraphSupportType.Both;
	}

	/// <summary>
	/// Specifies the name of the database operation. If omitted method's name is used.
	/// </summary>
	public string Name { get => name; set => name = value; }

	/// <summary>
	/// Specifies the type of the database operation with regards to whether the operation performs only read or
	/// both read and write operations on the database. The default value is <see cref="VeloxDB.Protocol.DbAPIOperationType.ReadWrite"/>.
	/// </summary>
	public DbAPIOperationType OperationType { get => operationType; set => operationType = value; }

	/// <summary>
	/// Specifies whether the request and response data require the serializer to support proper serialization of object graphs.
	/// Support for object graphs makes sure that each object is only transfered once in a situation where a single object is
	/// referenced by multiple other objects. It also handles circular references correctly, which whould otherwise produce an exception.
	/// It does introduce overhead in serialization process for situations where no such support is needed (e.g. object graph is a tree).
	/// </summary>
	public DbAPIObjectGraphSupportType ObjectGraphSupport { get => objectGraphSupport; set => objectGraphSupport = value; }
}
