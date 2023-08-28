using System;
using System.Text;
using VeloxDB.Common;

namespace VeloxDB.Storage;

internal sealed class StorageEngineSettings
{
	long initClassSize;
	float hashMapLoadFactor;
	bool autoMergeInverseReferences;
	float collectionGrowthFactor;

	int gcWorkerCount;
	int gcItemCountThreshold;
	int gcTranCountThreshold;

	bool autoSnapshotOnShutdown;

	long systemLogMaxSize;

	long resizeSplitSize;
	long scanClassSplitSize;

	long snapshotMaxMemorySize;

	long maxReplicationSendQueueSize;
	TimeSpan replicationSyncTimeout;
	long replicationSyncTresholdSize;

	int commitWorkerCount;
	int maxMergedTransactionCount;
	int maxMergedOperationCount;

	bool usePreAlignment;
	float preAlignmentRatio;

	int bufferPoolSize;

	bool allowInternalParallelization;

	bool allowUnexistingDirectoryForLog;

	int btreeNodeSize;

	public StorageEngineSettings()
	{
		hashMapLoadFactor = 0.75f;
		initClassSize = 512;
		autoMergeInverseReferences = true;
		collectionGrowthFactor = 2.0f;

		gcWorkerCount = Math.Max(2, ProcessorNumber.CoreCount / 4);
		gcTranCountThreshold = 64;
		gcItemCountThreshold = 32 * 1024;

		allowInternalParallelization = true;
		allowUnexistingDirectoryForLog = true;

		// Carefully chosen so that leaf nodes fit closely into 1024 byte buffers and parent nodes never exceed maximum of the MemoryManager
		btreeNodeSize = 118;

		bufferPoolSize = 1024 * 1024 * 32;

		autoSnapshotOnShutdown = false;

		systemLogMaxSize = 1024 * 1024 * 1;

		resizeSplitSize = 200000;
		scanClassSplitSize = 100000;

		snapshotMaxMemorySize = 1024 * 1024 * 128;

		maxReplicationSendQueueSize = 1024 * 1024 * 512;
		replicationSyncTimeout = TimeSpan.FromSeconds(60);
		replicationSyncTresholdSize = 1024 * 1024 * 512;

		commitWorkerCount = Math.Max(ProcessorNumber.CoreCount, 4);
		maxMergedTransactionCount = 512;
		maxMergedOperationCount = 1024 * 4;

		preAlignmentRatio = 0.3f;
		usePreAlignment = false;
	}

	public StorageEngineSettings Clone()
	{
		StorageEngineSettings c = new StorageEngineSettings();

		c.gcWorkerCount = gcWorkerCount;
		c.gcTranCountThreshold = gcTranCountThreshold;
		c.gcItemCountThreshold = gcItemCountThreshold;

		c.initClassSize = initClassSize;
		c.hashMapLoadFactor = hashMapLoadFactor;
		c.autoMergeInverseReferences = autoMergeInverseReferences;
		c.collectionGrowthFactor = collectionGrowthFactor;

		c.allowInternalParallelization = allowInternalParallelization;
		c.allowUnexistingDirectoryForLog = allowUnexistingDirectoryForLog;

		c.btreeNodeSize = btreeNodeSize;

		c.bufferPoolSize = bufferPoolSize;

		c.autoSnapshotOnShutdown = autoSnapshotOnShutdown;

		c.systemLogMaxSize = systemLogMaxSize;

		c.resizeSplitSize = resizeSplitSize;
		c.scanClassSplitSize = scanClassSplitSize;

		c.snapshotMaxMemorySize = snapshotMaxMemorySize;

		c.maxReplicationSendQueueSize = maxReplicationSendQueueSize;
		c.replicationSyncTimeout = replicationSyncTimeout;
		c.replicationSyncTresholdSize = replicationSyncTresholdSize;

		c.commitWorkerCount = commitWorkerCount;
		c.maxMergedTransactionCount = maxMergedTransactionCount;
		c.maxMergedOperationCount = maxMergedOperationCount;

		c.preAlignmentRatio = preAlignmentRatio;
		c.usePreAlignment = usePreAlignment;

		return c;
	}

	public int BufferPoolSize { get => bufferPoolSize; set => bufferPoolSize = value; }
	public long InitClassSize { get => initClassSize; set => initClassSize = value; }
	public float HashLoadFactor { get => hashMapLoadFactor; set => hashMapLoadFactor = value; }
	public bool AutoMergeInvRefs { get => autoMergeInverseReferences; set => autoMergeInverseReferences = value; }
	public float CollectionGrowthFactor { get => collectionGrowthFactor; set => collectionGrowthFactor = value; }
	public int GCWorkerCount { get => gcWorkerCount; set => gcWorkerCount = value; }
	public int GCTranThreshold { get => gcTranCountThreshold; set => gcTranCountThreshold = value; }
	public int GCItemThreshold { get => gcItemCountThreshold; set => gcItemCountThreshold = value; }
	public bool AllowInternalParallelization { get => allowInternalParallelization; set => allowInternalParallelization = value; }
	public bool AllowUnexistingDirectoryForLog { get => allowUnexistingDirectoryForLog; set => allowUnexistingDirectoryForLog = value; }
	public int BTreeNodeSize { get => btreeNodeSize; set => btreeNodeSize = value; }
	public bool AutoSnapshotOnShutdown { get => autoSnapshotOnShutdown; set => autoSnapshotOnShutdown = value; }
	public long SystemLogMaxSize { get => systemLogMaxSize; set => systemLogMaxSize = value; }
	public long ResizeSplitSize { get => resizeSplitSize; set => resizeSplitSize = value; }
	public long ScanClassSplitSize { get => scanClassSplitSize; set => scanClassSplitSize = value; }
	public long SnapshotMaxMemorySize { get => snapshotMaxMemorySize; set => snapshotMaxMemorySize = value; }
	public long MaxReplicationSendQueueSize { get => maxReplicationSendQueueSize; set => maxReplicationSendQueueSize = value; }
	public TimeSpan ReplicationSyncTimeout { get => replicationSyncTimeout; set => replicationSyncTimeout = value; }
	public long ReplicationSyncTresholdSize { get => replicationSyncTresholdSize; set => replicationSyncTresholdSize = value; }
	public int CommitWorkerCount { get => commitWorkerCount; set => commitWorkerCount = value; }
	public int MaxMergedTransactionCount { get => maxMergedTransactionCount; set => maxMergedTransactionCount = value; }
	public int MaxMergedOperationCount { get => maxMergedOperationCount; set => maxMergedOperationCount = value; }
	public float PreAlignmentRatio { get => preAlignmentRatio; set => preAlignmentRatio = value; }
	public bool UsePreAlignment { get => usePreAlignment; set => usePreAlignment = value; }

	internal void Validate()
	{
		Checker.CheckRange((double)hashMapLoadFactor, 0.1, double.MaxValue, nameof(HashLoadFactor));
		Checker.CheckRange(initClassSize, 64, long.MaxValue, nameof(InitClassSize));
		Checker.CheckRange((double)collectionGrowthFactor, 1.1, double.MaxValue, nameof(CollectionGrowthFactor));
		Checker.CheckRange(gcWorkerCount, 0, nameof(GCWorkerCount));
		Checker.CheckRange(gcItemCountThreshold, 0, nameof(GCItemThreshold));
		Checker.CheckRange(bufferPoolSize, 0, nameof(BufferPoolSize));
		Checker.CheckRange(systemLogMaxSize, 0, nameof(SystemLogMaxSize));
		Checker.CheckRange(resizeSplitSize, 1, nameof(ResizeSplitSize));
		Checker.CheckRange(scanClassSplitSize, 1, nameof(ScanClassSplitSize));
		Checker.CheckRange(snapshotMaxMemorySize, 1024 * 1024, nameof(SnapshotMaxMemorySize));
		Checker.CheckRange(maxReplicationSendQueueSize, 0, nameof(MaxReplicationSendQueueSize));
		Checker.CheckRange(replicationSyncTresholdSize, 1024 * 8, nameof(ReplicationSyncTresholdSize));
		Checker.CheckRange(commitWorkerCount, 1, 64, nameof(CommitWorkerCount));
		Checker.CheckRange(maxMergedOperationCount, 1, int.MaxValue, nameof(MaxMergedOperationCount));
		Checker.CheckRange(preAlignmentRatio, 0.0, 1.0f, nameof(preAlignmentRatio));
		Checker.CheckRange(maxMergedTransactionCount, 1, ushort.MaxValue - 1, nameof(MaxMergedTransactionCount));
		Checker.CheckRange(btreeNodeSize, 16, 1024 * 16, nameof(MaxMergedTransactionCount));
	}

	public override string ToString()
	{
		StringBuilder sb = new StringBuilder(1024);
		sb.AppendFormat("BufferPoolSize={0}", BufferPoolSize).AppendLine();
		sb.AppendFormat("InitClassSize={0}", InitClassSize).AppendLine();
		sb.AppendFormat("HashLoadFactor={0}", HashLoadFactor).AppendLine();
		sb.AppendFormat("AutoMergeInvRefs={0}", AutoMergeInvRefs).AppendLine();
		sb.AppendFormat("CollectionGrowthFactor={0}", CollectionGrowthFactor).AppendLine();
		sb.AppendFormat("GCWorkerCount={0}", GCWorkerCount).AppendLine();
		sb.AppendFormat("GCTranThreshold={0}", GCTranThreshold).AppendLine();
		sb.AppendFormat("GCItemThreshold={0}", GCItemThreshold).AppendLine();
		sb.AppendFormat("AllowInternalParallelization={0}", AllowInternalParallelization).AppendLine();
		sb.AppendFormat("allowUnexistingDirectoryForLog={0}", allowUnexistingDirectoryForLog).AppendLine();
		sb.AppendFormat("btreeNodeSize={0}", btreeNodeSize).AppendLine();
		sb.AppendFormat("AutoSnapshotOnShutdown={0}", AutoSnapshotOnShutdown).AppendLine();
		sb.AppendFormat("SystemLogMaxSize={0}", SystemLogMaxSize).AppendLine();
		sb.AppendFormat("ResizeSplitSize={0}", ResizeSplitSize).AppendLine();
		sb.AppendFormat("ScanClassSplitSize={0}", ScanClassSplitSize).AppendLine();
		sb.AppendFormat("SnapshotMaxMemorySize={0}", SnapshotMaxMemorySize).AppendLine();
		sb.AppendFormat("MaxReplicationSendQueueSize={0}", MaxReplicationSendQueueSize).AppendLine();
		sb.AppendFormat("ReplicationSyncTimeout={0}", ReplicationSyncTimeout).AppendLine();
		sb.AppendFormat("ReplicationSyncTresholdSize={0}", ReplicationSyncTresholdSize).AppendLine();
		return sb.ToString();
	}
}
