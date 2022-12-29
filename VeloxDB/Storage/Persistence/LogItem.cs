using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using VeloxDB.Common;

namespace VeloxDB.Storage.Persistence;

internal unsafe struct LogItem
{
	const int fixedPartSize =
							8 +		// commitVersion
							4 +		// local term
							16 +	// global term
							1 +		// affected log groups
							8 +		// logSeqNum
							4;		// Changeset count

	ulong commitVersion;
	SimpleGuid globalTerm;
	uint localTerm;
	byte affectedLogsMask;
	AlignmentData alignment;
	ulong logSeqNum;
	Transaction transaction;
	int changesetCount;
	LogChangeset changesetsList;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public LogItem(ulong commitVersion, uint localTerm, SimpleGuid globalTerm, byte affectedLogGroups,
		Transaction tran, AlignmentData alignment, ulong logSeqNum, List<LogChangeset> changesets)
	{
		this.transaction = tran;
		this.commitVersion = commitVersion;
		this.localTerm = localTerm;
		this.globalTerm = globalTerm;
		this.affectedLogsMask = affectedLogGroups;
		this.logSeqNum = logSeqNum;
		this.alignment = alignment;

		this.changesetCount = changesets.Count;
		for (int i = 0; i < changesets.Count - 1; i++)
		{
			changesets[i].Next = changesets[i + 1];
		}

		this.changesetsList = changesetCount > 0 ? changesets[0] : null;
	}

	public int ChangesetCount => changesetCount;
	public LogChangeset ChangesetsList => changesetsList;
	public ulong CommitVersion => commitVersion;
	public SimpleGuid GlobalTerm => globalTerm;
	public uint LocalTerm => localTerm;
	public byte AffectedLogsMask => affectedLogsMask;
	public ulong LogSeqNum => logSeqNum;
	public AlignmentData Alignment => alignment;
	public Transaction Transaction => transaction;

	public void DisposeFirstChangeset()
	{
		LogChangeset next = changesetsList.Next;
		changesetsList.Dispose();
		changesetsList = next;
		changesetCount--;
	}

	public bool CanRunParallel => ChangesetReader.PeekOperationType(changesetsList) != OperationType.DefaultValue;
	public static bool CanLogRunParallel(LogChangeset ch) => ch == null || ChangesetReader.PeekOperationType(ch) != OperationType.DefaultValue;

	public long SerializedSize
	{
		get
		{
			int serializedSize = fixedPartSize + AlignmentData.Size(alignment);

			LogChangeset curr = changesetsList;
			for (int i = 0; i < changesetCount; i++)
			{
				serializedSize += curr.SerializedSize;
				curr = curr.Next;
			}

			return serializedSize;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static long CalculateSerializedSize(Transaction tran, int logIndex)
	{
		TransactionContext tc = tran.Context;
		long size = fixedPartSize + AlignmentData.Size(tc.Alignment);
		if (tc.Changeset != null)
		{
			LogChangeset lc = tc.Changeset.GetLogChangeset(logIndex);
			if (lc != null)
				size += lc.SerializedSize;
		}
		
		return size;
	}

	public static long Serialize(byte* buffer, Transaction tran, LogChangeset lc)
	{
		byte* startBuffer = buffer;

		*((ulong*)buffer) = tran.CommitVersion;
		buffer += 8;

		*((uint*)buffer) = tran.LocalTerm;
		buffer += 4;

		*((long*)buffer) = tran.GlobalTerm.Low;
		buffer += 8;

		*((long*)buffer) = tran.GlobalTerm.Hight;
		buffer += 8;

		*((byte*)buffer) = tran.AffectedLogGroups;
		buffer += 1;

		*((ulong*)buffer) = tran.LogSeqNum;
		buffer += 8;

		AlignmentData.WriteTo(tran.Context.Alignment, ref buffer);

		if (lc == null)
		{
			*(int*)buffer = 0;
			buffer += 4;
		}
		else
		{
			*(int*)buffer = 1;
			buffer += 4;
			lc.WriteTo(ref buffer);
		}

		return (long)buffer - (long)startBuffer;
	}

	public static LogItem Deserialize(MemoryManager memoryManager, ref byte* buffer)
	{
		ulong commitVersion = *((ulong*)buffer);
		buffer += 8;

		uint localTerm = *((uint*)buffer);
		buffer += 4;

		long v1 = *((long*)buffer);
		buffer += 8;

		long v2 = *((long*)buffer);
		buffer += 8;

		byte affectedLogGroups = *buffer;
		buffer++;

		ulong logSeqNum = *((ulong*)buffer);
		buffer += 8;

		AlignmentData alignment = AlignmentData.ReadFrom(ref buffer);

		int changesetCount = *(int*)buffer;
		buffer += 4;

		LogChangeset head = null;
		LogChangeset prev = null;
		for (int i = 0; i < changesetCount; i++)
		{
			LogChangeset ch = new LogChangeset(null, memoryManager);
			ch.ReadFrom(ref buffer);

			if (i == 0)
			{
				head = prev = ch;
			}
			else
			{
				prev.Next = ch;
				prev = ch;
			}
		}

		LogItem li = new LogItem()
		{
			alignment = alignment,
			commitVersion = commitVersion,
			localTerm = localTerm,
			globalTerm = new SimpleGuid(v1, v2),
			affectedLogsMask = affectedLogGroups,
			logSeqNum = logSeqNum,
			changesetCount = changesetCount,
			changesetsList = head,
		};

		return li;
	}

	public void DisposeChangesets()
	{
		LogChangeset curr = changesetsList;
		for (int i = 0; i < changesetCount; i++)
		{
			curr.Dispose();
			curr = curr.Next;
		}
	}

	[Conditional("TTTRACE")]
	public void TTTraceState(long traceId)
	{
		TTTrace.Write(traceId, commitVersion, globalTerm.Low, globalTerm.Hight, localTerm, affectedLogsMask, logSeqNum);

		if (alignment != null && alignment.GlobalVersions != null)
		{
			TTTrace.Write(alignment.GlobalVersions.Length);
			for (int i = 0; i < alignment.GlobalVersions.Length; i++)
			{
				GlobalVersion v = alignment.GlobalVersions[i];
				TTTrace.Write(v.GlobalTerm.Low, v.GlobalTerm.Hight, v.Version);
			}
		}
	}
}
