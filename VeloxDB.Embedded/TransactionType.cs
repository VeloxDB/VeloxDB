namespace VeloxDB.Embedded;

/// <summary>
/// Specifies possible types of the database transaction with regards to whether the transaction performs only read or
/// both read and write operations on the database.
/// </summary>
public enum TransactionType
{
	// Values here must match the values in TransactionType enum.

	/// <summary>
	/// Indicates that the transaction performs only read operations on the database.
	/// </summary>
	Read = 0,

	/// <summary>
	/// Indicates that the transaction performs both read and write operations on the database.
	/// </summary>
	ReadWrite = 1
}
