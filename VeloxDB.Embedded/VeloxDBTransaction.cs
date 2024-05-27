using System;
using VeloxDB.ObjectInterface;

namespace VeloxDB.Embedded;

/// <summary>
/// Represents a transaction within the VeloxDB database, providing methods for transaction management.
/// </summary>
public sealed class VeloxDBTransaction : IDisposable
{
    /// <summary>
    /// Gets the <see cref="ObjectModel"/> associated with the transaction. Use this object to access the data in the database.
    /// </summary>
	public ObjectModel ObjectModel { get; private set; }

	internal VeloxDBTransaction(ObjectModel om)
	{
		this.ObjectModel = om;
	}

	/// <summary>
    /// Commits the transaction, applying changes made within the transaction to the database.
    /// </summary>
	public void Commit()
	{
		ObjectModel.CommitAndDispose();
	}

    /// <summary>
    /// Releases all resources used by the <see cref="VeloxDBTransaction"/>. If the transaction is still active, Dispose will roll it back.
    /// </summary>
	public void Dispose()
	{
		ObjectModel.Dispose();
	}

    /// <summary>
    /// Rolls back the transaction, discarding any changes made within the transaction.
    /// </summary>
	public void Rollback()
	{
		ObjectModel.Rollback();
	}
}