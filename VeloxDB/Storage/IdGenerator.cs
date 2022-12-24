using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal unsafe sealed class IdGenerator
{
	public const long StartId = 1024;
	public static readonly long GeneratorId = IdHelper.MakeId(SystemCode.IdGenerator.Id, 4);
	public static readonly long GlobalWriteStateId = IdHelper.MakeId(SystemCode.GlobalWriteState.Id, 5);
	public static readonly long ModelDescId = IdHelper.MakeId(SystemCode.ConfigArtifact.Id, 6);
	public static readonly long PersistenceDescId = IdHelper.MakeId(SystemCode.ConfigArtifact.Id, 7);
	public static readonly long AssembliesVersionId = IdHelper.MakeId(SystemCode.ConfigArtifactVersion.Id, 8);
	public static readonly long ModelVersionId = IdHelper.MakeId(SystemCode.ConfigArtifactVersion.Id, 9);
	public static readonly long PersistenceVersionId = IdHelper.MakeId(SystemCode.ConfigArtifactVersion.Id, 10);

	const long maxRangeSize = 1024 * 1024 * 16;
	const long maxAllowedId = ((long)1 << IdHelper.CounterBitCount);

	static HashSet<DatabaseErrorType> retryableErrors = new HashSet<DatabaseErrorType>() { DatabaseErrorType.Conflict,
		DatabaseErrorType.NonUniqueId };

	Database database;
	ClassDescriptor idGenClass;

	public IdGenerator(Database database)
	{
		TTTrace.Write(database.TraceId, database.Id);

		this.database = database;
		idGenClass = database.ModelDesc.GetClass(SystemCode.IdGenerator.Id);
	}

	public long TakeRange(long count)
	{
		if (count == 0)
			return 0;

		while (true)
		{
			try
			{
				return TryTakeRange(count);
			}
			catch (DatabaseException e)
			{
				if (!retryableErrors.Contains(e.Detail.ErrorType))
					throw;
			}
		}
	}

	private long TryTakeRange(long count)
	{
		if (count > maxRangeSize)
			throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.InvalidArgument));

		long baseId = 0;
		using (Transaction tran = database.Engine.CreateTransaction(database.Id, TransactionType.ReadWrite, TransactionSource.Internal, null, true))
		{
			ChangesetWriter cw = database.Engine.ChangesetWriterPool.Get();
			ObjectReader reader = database.Engine.GetObject(tran, GeneratorId);
			if (reader.IsEmpty())
			{
				baseId = StartId;
				cw.StartInsertBlock(idGenClass).Add(SystemCode.IdGenerator.Value);
				cw.AddLong(GeneratorId);
				cw.AddLong(baseId + count);
				TTTrace.Write(database.TraceId, baseId, count);
			}
			else
			{
				baseId = reader.GetLong(SystemCode.IdGenerator.Value, tran);
				if (baseId + count > maxAllowedId)
					throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.IdUnavailable));

				cw.StartUpdateBlock(idGenClass).Add(SystemCode.IdGenerator.Value);
				cw.AddLong(GeneratorId);
				cw.AddLong(baseId + count);
				TTTrace.Write(database.TraceId, baseId, count);
			}

			using (Changeset ch = cw.FinishWriting())
			{
				database.Engine.ApplyChangeset(tran, ch);
			}

			database.Engine.ChangesetWriterPool.Put(cw);

			database.Engine.CommitTransaction(tran);
		}

		TTTrace.Write(database.TraceId, database.Id, baseId, count);
		database.Engine.Trace.Verbose("Id range reserved in the database, baseId={0}, count={1}.", baseId, count);
		return baseId;
	}
}
