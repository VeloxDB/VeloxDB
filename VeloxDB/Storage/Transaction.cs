﻿using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage.Replication;

namespace VeloxDB.Storage;

internal enum TransactionType
{
	Read = 0,
	ReadWrite = 1,
}

internal static class TransactionFlags
{
	public const int SourceShift = 1;
	public const int AllowsOtherTransShift = 3;
	public const int ClosedShift = 4;
	public const int AsyncShift = 5;

	public const byte TypeMask = 0x01;
	public const byte SourceMask = 0x06;
	public const byte AllowsOtherTrans = 0x08;
	public const byte Closed = 0x10;
}

[Flags]
internal enum WriteTransactionFlags
{
	None = 0x00,
	PreAssignedCommitVersion = 0x01,
	Alignment = 0x02,
	Propagated = 0x04,
}

internal unsafe sealed partial class Transaction : IDisposable
{
	// Limited by the ReaderInfo slot space (13-bits)
	public const int MaxConcurrentTrans = 8192;

	Database database;
	Thread managedThread;

	ulong id;

	ulong readVersion;
	ulong commitVersion;

	Transaction prevActiveTran;
	Transaction nextActiveTran;
	int activeListIndex;
	CollectableCollections garbage;

	bool cancelRequested;

	byte flags;

	TransactionContext context;

	object asyncCallbackState;
	Action<object, DatabaseException> asyncCallback;
	Transaction nextMerged;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public Transaction(Database database, TransactionType type)
	{
		this.database = database;
		this.Type = type;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Init(ulong id, TransactionSource source, IReplica originReplica, bool allowsOtherTrans)
	{
		this.id = id;
		this.Source = source;
		this.AllowsOtherTrans = allowsOtherTrans;
		this.managedThread = Thread.CurrentThread;

		if (Type == TransactionType.ReadWrite)
		{
			ProvideContext(database);
			context.OriginReplica = originReplica;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void InitReadVersion(ulong readVersion)
	{
		this.readVersion = readVersion;
	}

	public ulong Id => id;
	public ulong CommitVersion => commitVersion;
	public byte InternalFlags => flags;
	public ushort Slot => context.Slot;
	public ulong ReadVersion => readVersion;
	public TransactionContext Context => context;
	public CollectableCollections Garbage => garbage;
	public byte AffectedLogGroups { get => context.AffectedLogGroups; set => context.AffectedLogGroups = value; }
	public IReplica OriginReplica => context.OriginReplica;
	public uint LocalTerm => context.LocalTerm;
	public SimpleGuid GlobalTerm => context.GlobalTerm;
	public ulong LogSeqNum => context.LogSeqNum;
	public Database Database => database;
	public StorageEngine Engine => database.Engine;
	public DataModelDescriptor Model => database.ModelDesc;
	public bool IsEmpty => context.IsTransactionEmpty;
	public AutoResetEvent CommitEvent => context.CommitWaitEvent;

	public TransactionType Type
	{
		get => (TransactionType)(flags & TransactionFlags.TypeMask);
		internal set => flags = (byte)((flags & ~TransactionFlags.TypeMask) | (int)value);
	}

	public TransactionSource Source
	{
		get => (TransactionSource)((int)(flags & TransactionFlags.SourceMask) >> (int)TransactionFlags.SourceShift);
		set => flags = (byte)((flags & ~TransactionFlags.SourceMask) | ((int)value << TransactionFlags.SourceShift));
	}

	public bool IsAsyncCommitScheduled => context != null && asyncCallback != null;

	public bool Closed
	{
		get => (flags & TransactionFlags.Closed) != 0;
		set
		{
			int v = value ? 1 : 0;
			flags = (byte)((flags & ~TransactionFlags.Closed) | (v << TransactionFlags.ClosedShift));
		}
	}

	public bool CancelRequested { get => cancelRequested; set => cancelRequested = value; }
	public bool IsAlignment => (context.WriteFlags & WriteTransactionFlags.Alignment) != 0;
	public bool IsPropagated => (context.WriteFlags & WriteTransactionFlags.Propagated) != 0;
	public bool IsCommitVersionPreAssigned => (context.WriteFlags & WriteTransactionFlags.PreAssignedCommitVersion) != 0;
	public ulong StandbyOrderNum => context.StandbyOrderNum;

	public bool AllowsOtherTrans
	{
		get
		{
			return (flags & TransactionFlags.AllowsOtherTrans) != 0;
		}

		private set
		{
			int v = value ? 1 : 0;
			flags = (byte)((flags & ~TransactionFlags.AllowsOtherTrans) | (v << TransactionFlags.AllowsOtherTransShift));
		}
	}

	public Transaction PrevActiveTran { get => prevActiveTran; set => prevActiveTran = value; }
	public Transaction NextActiveTran { get => nextActiveTran; set => nextActiveTran = value; }
	public int ActiveListIndex { get => activeListIndex; set => activeListIndex = value; }
	public Action<object, DatabaseException> AsyncCallback { get => asyncCallback; set => asyncCallback = value; }
	public object AsyncCallbackState { get => asyncCallbackState; set => asyncCallbackState = value; }
	public Transaction NextMerged { get => nextMerged; set => nextMerged = value; }

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void RegisterAsyncCommitter()
	{
		context.IncAsyncCommitterCount();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void AsyncCommitterFinished(bool result = true)
	{
		if (!result)
		{
			context.AsyncCommitResult = false;
			Thread.MemoryBarrier();
		}

		if (context.DecAsyncCommiterCount() == 0)
			database.PublishTransactionCommit(this);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WaitAsyncCommitters()
	{
		context.CommitWaitEvent.WaitOne();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetPreAssignedCommitVersion(uint localTerm, ulong commitVersion, ulong standbyOrderNum = ulong.MaxValue)
	{
		Checker.AssertTrue(Type == TransactionType.ReadWrite);
		Checker.AssertFalse(IsAlignment);   // Alignment must be set after the preassigned commit id

		context.WriteFlags |= WriteTransactionFlags.PreAssignedCommitVersion;
		this.commitVersion = commitVersion;
		context.StandbyOrderNum = standbyOrderNum;
		context.LocalTerm = localTerm;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetConfigurationUpdateArtifact(bool userAssembliesModified, byte[] newModelDescBinary, byte[] newPersistenceDescBinary)
	{
		TTTrace.Write(Engine.TraceId, userAssembliesModified, newModelDescBinary != null, newPersistenceDescBinary != null);
		Checker.AssertTrue(Type == TransactionType.ReadWrite);
		context.UserAssembliesModified = userAssembliesModified;
		context.NewModelDescBinary = newModelDescBinary;
		context.NewPersistenceDescBinary = newPersistenceDescBinary;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetConfigurationUpdateArtifact(bool userAssembliesModified, byte[] newModelDescBinary)
	{
		TTTrace.Write(Engine.TraceId, userAssembliesModified, newModelDescBinary != null);
		Checker.AssertTrue(Type == TransactionType.ReadWrite);
		context.UserAssembliesModified = userAssembliesModified;
		context.NewModelDescBinary = newModelDescBinary;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetConfigurationUpdateArtifact(byte[] newPersistenceDescBinary)
	{
		TTTrace.Write(Engine.TraceId, newPersistenceDescBinary != null);
		Checker.AssertTrue(Type == TransactionType.ReadWrite);
		context.NewPersistenceDescBinary = newPersistenceDescBinary;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetAlignmentFlag(bool isPropagated, AlignmentData alignment)
	{
		Checker.AssertTrue(Type == TransactionType.ReadWrite);
		Checker.AssertTrue(commitVersion != 0);

		context.WriteFlags |= WriteTransactionFlags.Alignment;
		context.Alignment = alignment;
		if (isPropagated)
			context.WriteFlags |= WriteTransactionFlags.Propagated;

		// This is safe for alignment transactions and improves performance because we do not need to visit object multiple times during commit
		id = commitVersion;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetCommitVersion(ulong commitVersion)
	{
		this.commitVersion = commitVersion;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetLogSeqNum(ulong logSeqNum)
	{
		this.context.LogSeqNum = logSeqNum;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetCommitAndLogSeqNum(ulong commitVersion, ulong logSeqNum)
	{
		this.commitVersion = commitVersion;
		this.context.LogSeqNum = logSeqNum;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetLocalTerm(uint localTerm)
	{
		context.LocalTerm = localTerm;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void SetGlobalTerm(SimpleGuid globalTerm)
	{
		context.GlobalTerm = globalTerm;
	}

	public void ProcessSetToNullAffected<T>(Action<T, long> action, T arg)
	{
		context.ProcessSetToNullAffected(action, arg);
	}

	public void Complete(bool isCommited)
	{
		TTTrace.Write(database.TraceId, id, commitVersion);

		if (Type == TransactionType.ReadWrite)
		{
			PrepareForGarbageCollection(isCommited);
			database.ProcessGarbage(this);
		}

		Closed = true;
	}

	private void PrepareForGarbageCollection(bool isCommited)
	{
		TTTrace.Write(database.TraceId, id);

		if (IsAlignment)
		{
			Checker.AssertTrue(context.AffectedObjects.Count == 0);
			Checker.AssertTrue(context.AffectedInvRefs.Count == 0);
			Checker.AssertTrue(context.KeyReadLocks.Count == 0);
			Checker.AssertTrue(context.ObjectReadLocks.Count == 0);
			return;
		}

		// Inverse reference map read locks that created empty entries inside the map are copied
		// to the affected list so that they get garbage collected. Affected list is now only used for GC
		context.PrepareEmptyInvRefEntriesForGC();

		garbage.objects = context.AffectedObjects.TakeContent();
		garbage.invRefs = context.AffectedInvRefs.TakeContent();

		context.KeyReadLocks.FreeMemory();
		context.ObjectReadLocks.FreeMemory();
	}

	public void MergeWith(Transaction tran)
	{
		Context.Merge(this, tran);
	}

	private void ProvideContext(Database database)
	{
		if (context != null)
			return;

		context = database.Engine.ContextPool.Get();
		context.Init(database, id);
	}

	public void ClearContext()
	{
		if (context == null)
			return;

		context.Clear();
		database.Engine.ContextPool.Put(context);
		context = null;
	}

	/// <summary>
	/// Transfers the ownership of the transaction to the calling thread. This function is not thread safe
	/// meaning it is responsibility of the caller to ensure the transaction is not being used during this call.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void TakeThreadOwnership()
	{
		managedThread = Thread.CurrentThread;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ValidateUsage()
	{
		if (!object.ReferenceEquals(managedThread, Thread.CurrentThread))
			throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.InvalidTransactionThread));

		if (Closed)
			throw new DatabaseException(DatabaseErrorDetail.Create(DatabaseErrorType.TransactionClosed));
	}

	public void Dispose()
	{
		if (!Closed && !IsAsyncCommitScheduled)
			database.Engine.RollbackTransaction(this);
	}

	public struct CollectableCollections
	{
		public ulong objects;
		public ulong invRefs;
	}
}
