using System;
using System.Collections;
using System.Collections.Generic;
using System.Transactions;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal sealed class ClassScan : IDisposable, IEnumerable<ObjectReader>
{
	const int maxBatchSize = 128;

	Transaction tran;
	ObjectStorage.ScanRange[] ranges;

	ObjectStorage.ScanRange currRange;
	int currRangeIndex;

	Class @class;

	ObjectStorage.ScanContext scanContext;

	internal ClassScan(Transaction tran, ObjectStorage.ScanRange[] ranges)
	{
		this.tran = tran;
		this.ranges = ranges;

		if (ranges.Length > 0)
		{
			currRange = ranges[0];
			@class = currRange.Class;
		}
	}

	public bool IsFinished => currRange == null;
	internal Class Class => @class;

	public bool Next(ObjectReader[] objects, out int count)
	{
		if (tran != null && tran.Source == TransactionSource.Client)
			tran.ValidateUsage();

		if (scanContext != null)
			scanContext.Processed();

		count = 0;
		if (currRange == null)
			return false;

		count = objects.Length;

		int c = 0;
		while (c < count)
		{
			int tc = Math.Min(maxBatchSize, count - c);
			if (!NextInternal(objects, null, c, ref tc))
			{
				count = c;
				return count > 0;
			}

			c += tc;
		}

		return true;
	}

	internal bool NextHandles(ulong[] handles, int baseOffset, ref int count)
	{
		if (scanContext != null)
			scanContext.Processed();

		if (currRange == null)
			return false;

		int c = 0;
		while (c < count)
		{
			int tc = Math.Min(maxBatchSize, count - c);
			if (!NextInternal(null, handles, baseOffset + c, ref tc))
			{
				count = c;
				return count > 0;
			}

			c += tc;
		}

		return true;
	}

	public void Dispose()
	{
		if (tran != null && tran.Source == TransactionSource.Client)
			tran.ValidateUsage();

		if (scanContext != null)
		{
			scanContext.Processed();
			scanContext = null;
		}
	}

	private unsafe bool NextInternal(ObjectReader[] objects, ulong[] handles, int baseOffset, ref int count)
	{
		int i = 0;
		while (i < count)
		{
			if (tran != null && tran.CancelRequested && tran.Source == TransactionSource.Client)
			{
				TTTrace.Write(tran.Engine.TraceId, tran.Database.Id, tran.Id, @class.ClassDesc.Id);
				Dispose();
				tran.Engine.CheckErrorAndRollback(DatabaseErrorDetail.Create(DatabaseErrorType.TransactionCanceled), tran);
			}

			ulong handle;
			while ((handle = currRange.Next(ref scanContext)) == 0)
			{
				currRangeIndex++;
				if (currRangeIndex >= ranges.Length)
				{
					count = i;
					return i > 0;
				}

				currRange = ranges[currRangeIndex];
			}

			ObjectReader r = currRange.Class.GetScanObjectIfInTransaction(handle, tran);
			if (!r.IsEmpty())
			{
				if (handles != null)
					handles[baseOffset + i] = handle;

				if (objects != null)
					objects[baseOffset + i] = r;

				i++;
			}
		}

		return true;
	}

	public IEnumerator<ObjectReader> GetEnumerator()
	{
		ObjectReader[] rs = new ObjectReader[8];
		while (Next(rs, out int count))
		{
			for (int i = 0; i < count; i++)
			{
				yield return rs[i];
			}

			count = rs.Length;
		}
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		return GetEnumerator();
	}
}
