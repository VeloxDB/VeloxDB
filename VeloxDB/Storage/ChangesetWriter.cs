using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Velox.Common;
using Velox.Descriptor;
using static System.Math;

namespace Velox.Storage;

internal enum OperationType : byte
{
	None = 0,
	Insert = 1,
	Update = 2,
	Delete = 3,
	Rewind = 4,
	DefaultValue = 5
}

internal struct BlockProperties
{
	LogChangesetWriter cw;

	public BlockProperties(LogChangesetWriter cw)
	{
		this.cw = cw;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public BlockProperties Add(int propertyId)
	{
		cw.AddProperty(propertyId);
		return this;
	}
}

[StructLayout(LayoutKind.Sequential, Pack = 1, Size = ChangesetBufferHeader.Size)]
internal unsafe struct ChangesetBufferHeader
{
	public const int Size = 24;

	public ulong handle;
	public long size;
	public ChangesetBufferHeader* next;
}


internal unsafe class ChangesetWriterPool
{
	const int poolSize = 32;

	MultiSpinLock sync = new MultiSpinLock();

	MemoryManager memoryManager;

	byte* poolCounts;
	ChangesetWriter[][] pools;

	public ChangesetWriterPool(MemoryManager memoryManager)
	{
		this.memoryManager = memoryManager;

		poolCounts = CacheLineMemoryManager.Allocate(4, out object owner);
		pools = new ChangesetWriter[ProcessorNumber.CoreCount][];
		for (int i = 0; i < pools.Length; i++)
		{
			pools[i] = new ChangesetWriter[poolSize + AlignedAllocator.CacheLineSize];  // Enforce that no two pools share cache lines
			int* pc = (int*)CacheLineMemoryManager.GetBuffer(poolCounts, i);
			*pc = poolSize;
			for (int j = 0; j < poolSize; j++)
			{
				pools[i][j] = new ChangesetWriter(i, memoryManager);
			}
		}
	}

	public MemoryManager MemoryManager => memoryManager;

	public ChangesetWriter Get()
	{
		int lockHandle = sync.Enter();
		ChangesetWriter writer = null;

		int* pc = (int*)CacheLineMemoryManager.GetBuffer(poolCounts, lockHandle);
		if (*pc > 0)
			writer = pools[lockHandle][--(*pc)];

		sync.Exit(lockHandle);

		if (writer == null)
			writer = new ChangesetWriter(lockHandle, memoryManager);

		return writer;
	}

	public void Put(ChangesetWriter writer)
	{
		writer.CleanUp();

		int lockHandle = writer.ProcNum;
		sync.Enter(lockHandle);

		int* pc = (int*)CacheLineMemoryManager.GetBuffer(poolCounts, lockHandle);
		if (*pc < poolSize)
			pools[lockHandle][(*pc)++] = writer;

		sync.Exit(lockHandle);
	}
}

internal unsafe sealed class ChangesetWriter
{
	const int maxLogCount = 8;

	int procNum;
	int maxLogSeqNum;
	LogChangesetWriter[] logWriters;
	LogChangesetWriter currWriter;

	public ChangesetWriter(int procNum, MemoryManager memoryManager)
	{
		this.procNum = procNum;

		logWriters = new LogChangesetWriter[maxLogCount];
		for (int i = 0; i < logWriters.Length; i++)
		{
			logWriters[i] = new LogChangesetWriter(memoryManager, i);
		}

		maxLogSeqNum = -1;
	}

	public OperationType CurrOpType => currWriter == null ? OperationType.None : currWriter.OperationType;
	public int PropertyCount => currWriter.PropertyCount;
	public bool IsEmpty => maxLogSeqNum == -1;
	public int ProcNum => procNum;

	public void TurnLargeInitSizeOn()
	{
		for (int i = 0; i <= maxLogSeqNum; i++)
		{
			logWriters[i].TurnLargeInitSizeOn();
		}
	}

	public void CleanUp()
	{
		for (int i = 0; i <= maxLogSeqNum; i++)
		{
			logWriters[i].CleanUp();
		}

		maxLogSeqNum = -1;
	}

	public Changeset FinishWriting()
	{
		int c = 0;
		for (int i = 0; i <= maxLogSeqNum; i++)
		{
			if (logWriters[i].IsUsed)
				c++;
		}

		LogChangeset[] chs = new LogChangeset[c];

		c = 0;
		for (int i = 0; i <= maxLogSeqNum; i++)
		{
			if (logWriters[i].IsUsed)
				chs[c++] = logWriters[i].FinishWriting();
		}

		return new Changeset(chs);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void ProvideCurrentChangeset(ClassDescriptor classDesc)
	{
		int logIndex = classDesc.LogIndex;
		if (logIndex == -1) // No persistence is present in the database so just write everything in a single writer
			logIndex = 0;

		if (logIndex > maxLogSeqNum)
			maxLogSeqNum = logIndex;

		currWriter = logWriters[logIndex];
	}

	public BlockProperties StartInsertBlock(ClassDescriptor classDesc)
	{
		Checker.NotNull(classDesc, "classDesc");
		if (classDesc.IsAbstract)
			throw new InvalidOperationException("Abstract class may not be inserted.");

		ProvideCurrentChangeset(classDesc);
		return currWriter.StartInsertBlock(classDesc);
	}

	public void StartInsertBlockUnsafe(ClassDescriptor classDesc)
	{
		ProvideCurrentChangeset(classDesc);
		currWriter.StartInsertBlockUnsafe(classDesc);
	}

	public BlockProperties StartUpdateBlock(ClassDescriptor classDesc)
	{
		Checker.NotNull(classDesc, "classDesc");
		if (classDesc.IsAbstract)
			throw new InvalidOperationException("Abstract class may not be updated.");

		ProvideCurrentChangeset(classDesc);
		return currWriter.StartUpdateBlock(classDesc);
	}

	public BlockProperties StartDefaultValueBlock(ClassDescriptor classDesc)
	{
		Checker.NotNull(classDesc, "classDesc");
		if (classDesc.IsAbstract)
			throw new InvalidOperationException("Abstract class may not be updated.");

		ProvideCurrentChangeset(classDesc);
		return currWriter.StartDefaultValueBlock(classDesc);
	}

	public void StartUpdateBlockUnsafe(ClassDescriptor classDesc)
	{
		ProvideCurrentChangeset(classDesc);
		currWriter.StartUpdateBlockUnsafe(classDesc);
	}

	public void StartDeleteBlock(ClassDescriptor classDesc)
	{
		Checker.NotNull(classDesc, "classDesc");
		if (classDesc.IsAbstract)
			throw new InvalidOperationException("Abstract class may not be deleted.");

		ProvideCurrentChangeset(classDesc);
		currWriter.StartDeleteBlock(classDesc);
	}

	public void StartDeleteBlockUnsafe(ClassDescriptor classDesc)
	{
		ProvideCurrentChangeset(classDesc);
		currWriter.StartDeleteBlockUnsafe(classDesc);
	}

	public void RewindToVersion(DataModelDescriptor modelDesc, ulong version)
	{
		int logCount = 1;
		if (modelDesc.LogCount != 0) // When no persistence is present in the database write everything in a single writer
			logCount = modelDesc.LogCount;

		maxLogSeqNum = logCount - 1;
		for (int i = 0; i < logCount; i++)
		{
			logWriters[i].CreateRewindBlock(version);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void FinishBlock()
	{
		if (currWriter != null)
			currWriter.CloseActiveBlock();
	}

	public void AddPropertyUnsafe(int propertyIndex)
	{
		currWriter.AddPropertyUnsafe(propertyIndex);
	}

	public void AddProperty(int propertyId)
	{
		currWriter.AddProperty(propertyId);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void PropertiesDefined()
	{
		currWriter.ComleteBlockDefinition();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void LastValueWritten()
	{
		currWriter.LastValueWritten();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddByte(byte v)
	{
		currWriter.AddByte(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddShort(short v)
	{
		currWriter.AddShort(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddInt(int v)
	{
		currWriter.AddInt(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddLong(long v)
	{
		currWriter.AddLong(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddVersion(ulong v)
	{
		currWriter.AddVersion(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddReference(long v)
	{
		currWriter.AddReference(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddFloat(float v)
	{
		currWriter.AddFloat(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddDouble(double v)
	{
		currWriter.AddDouble(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddBool(bool v)
	{
		currWriter.AddBool(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddDateTime(DateTime v)
	{
		currWriter.AddDateTime(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddByteArray(byte[] v)
	{
		currWriter.AddByteArray(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddShortArray(short[] v)
	{
		currWriter.AddShortArray(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddIntArray(int[] v)
	{
		currWriter.AddIntArray(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddLongArray(long[] v)
	{
		currWriter.AddLongArray(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddReferenceArray(long[] v)
	{
		currWriter.AddReferenceArray(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public unsafe ChangesetWriter AddReferenceArray(long* v, int count)
	{
		currWriter.AddReferenceArray(v, count);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddFloatArray(float[] v)
	{
		currWriter.AddFloatArray(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddDoubleArray(double[] v)
	{
		currWriter.AddDoubleArray(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddBoolArray(bool[] v)
	{
		currWriter.AddBoolArray(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddDateTimeArray(DateTime[] v)
	{
		currWriter.AddDateTimeArray(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddStringArray(string[] v)
	{
		currWriter.AddStringArray(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddString(string v)
	{
		currWriter.AddString(v);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddDefaultValue(PropertyDescriptor propDesc)
	{
		currWriter.AddDefaultValue(propDesc);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ChangesetWriter AddDelete(long id)
	{
		currWriter.AddDelete(id);
		return this;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void CreatePreviousVersionPlaceholder()
	{
		currWriter.CreatePreviousVersionPlaceHolder();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteByte(byte v)
	{
		currWriter.WriteByte(v);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteShort(short v)
	{
		currWriter.WriteShort(v);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteInt(int v)
	{
		currWriter.WriteInt(v);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteLong(long v)
	{
		currWriter.WriteLong(v);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteFloat(float v)
	{
		currWriter.WriteFloat(v);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDouble(double v)
	{
		currWriter.WriteDouble(v);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteBool(bool v)
	{
		currWriter.WriteBool(v);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDateTime(DateTime v)
	{
		currWriter.WriteDateTime(v);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteNullArray()
	{
		currWriter.WriteByteArrayOptional(null, true);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteByteArray(byte[] v)
	{
		currWriter.WriteByteArrayOptional(v, true);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteByteArrayOptional(byte[] v, bool isDefined)
	{
		currWriter.WriteByteArrayOptional(v, isDefined);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteByteSubArray(byte[] v, int len)
	{
		currWriter.WriteByteSubArray(v, len);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteShortArray(short[] v)
	{
		currWriter.WriteShortArrayOptional(v, true);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteShortArrayOptional(short[] v, bool isDefined)
	{
		currWriter.WriteShortArrayOptional(v, isDefined);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteShortSubArray(short[] v, int len)
	{
		currWriter.WriteShortSubArray(v, len);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteIntArray(int[] v)
	{
		currWriter.WriteIntArrayOptional(v, true);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteIntArrayOptional(int[] v, bool isDefined)
	{
		currWriter.WriteIntArrayOptional(v, isDefined);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteIntSubArray(int[] v, int len)
	{
		currWriter.WriteIntSubArray(v, len);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteLongArray(long[] v)
	{
		currWriter.WriteLongArrayOptional(v, true);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteLongArrayOptional(long[] v, bool isDefined)
	{
		currWriter.WriteLongArrayOptional(v, isDefined);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteLongSubArray(long[] v, int len)
	{
		currWriter.WriteLongSubArray(v, len);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteLongArrayUnsafe(long* v, int len)
	{
		currWriter.WriteLongArrayUnsafe(v, len);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteFloatArray(float[] v)
	{
		currWriter.WriteFloatArrayOptional(v, true);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteFloatArrayOptional(float[] v, bool isDefined)
	{
		currWriter.WriteFloatArrayOptional(v, isDefined);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteFloatSubArray(float[] v, int len)
	{
		currWriter.WriteFloatSubArray(v, len);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDoubleArray(double[] v)
	{
		currWriter.WriteDoubleArrayOptional(v, true);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDoubleArrayOptional(double[] v, bool isDefined)
	{
		currWriter.WriteDoubleArrayOptional(v, isDefined);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDoubleSubArray(double[] v, int len)
	{
		currWriter.WriteDoubleSubArray(v, len);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteBoolArray(bool[] v)
	{
		currWriter.WriteBoolArrayOptional(v, true);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteBoolArrayOptional(bool[] v, bool isDefined)
	{
		currWriter.WriteBoolArrayOptional(v, isDefined);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteBoolSubArray(bool[] v, int len)
	{
		currWriter.WriteBoolSubArray(v, len);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDateTimeArray(DateTime[] v)
	{
		currWriter.WriteDateTimeArrayOptional(v, true);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDateTimeArrayOptional(DateTime[] v, bool isDefined)
	{
		currWriter.WriteDateTimeArrayOptional(v, isDefined);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDateTimeSubArray(DateTime[] v, int len)
	{
		currWriter.WriteDateTimeSubArray(v, len);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteStringArray(string[] v)
	{
		currWriter.WriteStringArrayOptional(v, true);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteStringArrayOptional(string[] v, bool isDefined)
	{
		currWriter.WriteStringArrayOptional(v, isDefined);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteStringSubArray(string[] v, int len)
	{
		currWriter.WriteStringSubArray(v, len);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteString(string s)
	{
		currWriter.WriteStringOptional(s, true);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteStringOptional(string s, bool isDefined)
	{
		currWriter.WriteStringOptional(s, isDefined);
	}
}

internal unsafe sealed class LogChangesetWriter
{
	const int startBufferSize = 512;
	const int largeStartBuferSize = 1024 * 512;
	const int endBufferSize = 1024 * 1024 * 8;

	ClassDescriptor classDesc;
	MemoryManager memoryManager;

	int logIndex;

	int bufferCount;
	byte* head;

	int activeOffset;
	int activeBufferSize;
	byte* activeBuffer;

	bool blockDefined;
	int propertyCount;
	int userPropertyCount;
	int[] propertyIndexes;

	int takenMarker;
	int[] propertyTakenMap;

	int stringCount;
	string[] strings;

	int propertyIndex;
	OperationType operationType;
	int blockOperationCount;
	UShortPlaceholder blockCountPlaceholder;

	public LogChangesetWriter(MemoryManager memoryManager, int logIndex)
	{
		this.memoryManager = memoryManager;
		this.logIndex = logIndex;

		propertyIndexes = new int[ClassDescriptor.MaxPropertyCount];
		propertyTakenMap = new int[ClassDescriptor.MaxPropertyCount];
		strings = new string[1024];

		takenMarker = 1;
	}

	public OperationType OperationType => operationType;
	public int PropertyCount => propertyCount;
	public bool IsUsed => head != null;

	public void TurnLargeInitSizeOn()
	{
		activeBufferSize = largeStartBuferSize / 2;
	}

	public LogChangeset FinishWriting()
	{
		if (operationType != OperationType.None)
			CloseActiveBlock();

		if (activeBuffer != null)
			((ChangesetBufferHeader*)activeBuffer)->size = activeOffset;

		string[] logStrings = null;
		if (stringCount > 0)
		{
			logStrings = new string[stringCount];
			for (int i = 0; i < stringCount; i++)
			{
				logStrings[i] = strings[i];
				strings[i] = null;
			}

			stringCount = 0;
		}

		LogChangeset rs = new LogChangeset(memoryManager, logIndex, bufferCount, head, logStrings);

		bufferCount = 0;
		activeBuffer = null;
		head = null;

		return rs;
	}

	public void CleanUp()
	{
		if (head != null)
		{
			ChangesetBufferHeader* curr = (ChangesetBufferHeader*)head;
			for (int i = 0; i < bufferCount; i++)
			{
				ChangesetBufferHeader* next = curr->next;
				memoryManager.Free(curr->handle);
				curr = next;
			}

			activeBuffer = null;
			head = null;
			bufferCount = 0;
		}

		if (stringCount > 0)
		{
			for (int i = 0; i < stringCount; i++)
			{
				strings[i] = null;
			}

			stringCount = 0;
		}

		activeOffset = 0;
		classDesc = null;
		activeBufferSize = 0;
		activeOffset = 0;
		propertyIndex = 0;
		operationType = OperationType.None;
		blockOperationCount = 0;
		blockCountPlaceholder = new UShortPlaceholder();
		blockDefined = false;
		propertyCount = 0;
	}

	private BlockProperties StartBlock(ClassDescriptor classDesc, OperationType operationType)
	{
		if (this.operationType != OperationType.None)
			CloseActiveBlock();

		this.classDesc = classDesc;
		propertyCount = 1;
		propertyIndexes[0] = classDesc.GetPropertyIndex(SystemCode.DatabaseObject.Id);

		takenMarker++;
		this.operationType = operationType;
		blockOperationCount = 0;
		blockDefined = false;

		WriteByte((byte)operationType);
		WriteShort(classDesc.Id);
		blockCountPlaceholder = CreateUShortPlaceholder();

		return new BlockProperties(this);
	}

	public BlockProperties StartInsertBlock(ClassDescriptor classDesc)
	{
		if (classDesc.IsAbstract)
			throw new InvalidOperationException("Abstract classes may not be inserted.");

		return StartBlock(classDesc, OperationType.Insert);
	}

	public void StartInsertBlockUnsafe(ClassDescriptor classDesc)
	{
		StartBlock(classDesc, OperationType.Insert);
	}

	public BlockProperties StartUpdateBlock(ClassDescriptor classDesc)
	{
		if (classDesc.IsAbstract)
			throw new InvalidOperationException("Abstract classes may not be updated.");

		return StartBlock(classDesc, OperationType.Update);
	}

	public BlockProperties StartDefaultValueBlock(ClassDescriptor classDesc)
	{
		if (classDesc.IsAbstract)
			throw new InvalidOperationException("Abstract classes may not be updated.");

		return StartBlock(classDesc, OperationType.DefaultValue);
	}

	public void StartUpdateBlockUnsafe(ClassDescriptor classDesc)
	{
		StartBlock(classDesc, OperationType.Update);
	}

	public void StartDeleteBlock(ClassDescriptor classDesc)
	{
		if (classDesc.IsAbstract)
			throw new InvalidOperationException("Abstract classes may not be deleted.");

		StartBlock(classDesc, OperationType.Delete);
	}

	public void StartDeleteBlockUnsafe(ClassDescriptor classDesc)
	{
		StartBlock(classDesc, OperationType.Delete);
	}

	public bool IsEmpty() => bufferCount == 0;

	public void CreateRewindBlock(ulong version)
	{
		if (!IsEmpty())
			throw new InvalidOperationException("Rewind operation is not allowed because changeset already contains other blocks.");

		if (operationType != OperationType.None)
			CloseActiveBlock();

		WriteByte((byte)OperationType.Rewind);
		WriteLong((long)version);
		blockDefined = true;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void CloseActiveBlock()
	{
		if (operationType != OperationType.None)
		{
			ComleteBlockDefinition();
			operationType = OperationType.None;
			blockCountPlaceholder.Populate((ushort)blockOperationCount);
			classDesc = null;
		}
	}

	public void AddPropertyUnsafe(int propertyIndex)
	{
		propertyIndexes[propertyCount++] = propertyIndex;
		propertyTakenMap[propertyIndex] = takenMarker;
	}

	public void AddProperty(int propertyId)
	{
		int index = classDesc.GetPropertyIndex(propertyId);

		if (blockDefined)
			throw new InvalidOperationException("Changeset block has been fully defined.");

		if (propertyCount == ClassDescriptor.MaxPropertyCount)
			throw new InvalidOperationException("Exceeded maximum allowed number of properties.");

		if (operationType != OperationType.Insert && operationType != OperationType.Update && operationType != OperationType.DefaultValue)
			throw new InvalidOperationException("Active block is not an insert or update block.");

		if (propertyId == SystemCode.DatabaseObject.Id)
			throw new InvalidOperationException("Id property usage is invalid.");

		if (index == -1)
			Checker.InvalidOperationException("Specified property has not been found.");

		if (propertyTakenMap[index] == takenMarker)
			Checker.InvalidOperationException("Property may not be added to a block more than once.");

		AddPropertyUnsafe(index);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void ComleteBlockDefinition()
	{
		if (blockDefined)
			return;

		DefineMissingProperties();

		blockDefined = true;

		ReadOnlyArray<PropertyDescriptor> props = classDesc.Properties;
		WriteShort((short)propertyCount);
		for (int i = 1; i < propertyCount; i++)
		{
			PropertyDescriptor pd = props[propertyIndexes[i]];
			WriteInt(pd.Id);
			WriteByte((byte)pd.PropertyType);
		}
	}

	private void DefineMissingProperties()
	{
		userPropertyCount = propertyCount;
		if (propertyCount == classDesc.Properties.Length - 1 || operationType != OperationType.Insert)  // Version is never defined in changeset
			return;

		for (int i = 2; i < classDesc.Properties.Length; i++)
		{
			PropertyDescriptor propDesc = classDesc.Properties[i];
			if (propertyTakenMap[i] != takenMarker && propDesc.PropertyType < PropertyType.String)
				AddPropertyUnsafe(i);
		}
	}

	private void RedefineActiveBlock(ClassDescriptor classDesc, OperationType opType)
	{
		this.classDesc = classDesc;
		this.operationType = opType;
		blockOperationCount = 0;
		blockDefined = true;

		WriteByte((byte)operationType);
		WriteShort(classDesc.Id);
		blockCountPlaceholder = CreateUShortPlaceholder();

		ReadOnlyArray<PropertyDescriptor> props = classDesc.Properties;
		WriteShort((short)propertyCount);
		for (int i = 1; i < propertyCount; i++)
		{
			PropertyDescriptor pd = props[propertyIndexes[i]];
			WriteInt(pd.Id);
			WriteByte((byte)pd.PropertyType);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void CheckIfOperationComplete()
	{
		propertyIndex++;
		if (propertyIndex == userPropertyCount)
		{
			WriteDefaultValues();

			propertyIndex = 0;
			blockOperationCount++;

			if (blockOperationCount == ushort.MaxValue)
			{
				ClassDescriptor cd = classDesc;
				OperationType opType = operationType;
				CloseActiveBlock();
				RedefineActiveBlock(cd, opType);
			}
		}
	}

	private void WriteDefaultValues()
	{
		for (int i = userPropertyCount; i < propertyCount; i++)
		{
			PropertyDescriptor propDesc = classDesc.Properties[propertyIndexes[i]];
			if (propDesc.PropertyType < PropertyType.String)
				WriteDefaultValue(propDesc);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void LastValueWritten()
	{
		blockOperationCount++;

		if (blockOperationCount == ushort.MaxValue)
		{
			ClassDescriptor cd = classDesc;
			OperationType opType = operationType;
			CloseActiveBlock();
			RedefineActiveBlock(cd, opType);
		}
	}

	private void ValidateValue(PropertyType type)
	{
		if (propertyIndex >= propertyCount)
			throw new InvalidOperationException("Current operation has been completed.");

		PropertyDescriptor pd = classDesc.Properties[propertyIndexes[propertyIndex]];
		if (pd.PropertyType != type)
			Checker.InvalidOperationException("Invalid property type.");
	}

	public void AddByte(byte v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.Byte);
		WriteByte(v);
		CheckIfOperationComplete();
	}

	public void AddShort(short v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.Short);
		WriteShort(v);
		CheckIfOperationComplete();
	}

	public void AddInt(int v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.Int);
		WriteInt(v);
		CheckIfOperationComplete();
	}

	public void AddLong(long v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.Long);

		if (propertyIndex == 0)
		{
			WriteLong(0);   // Create place holder for the previous version

			if (operationType == OperationType.DefaultValue)
			{
				if (v != 0)
					throw new ArgumentException("Invalid id.");
			}
			else
			{
				if (IdHelper.GetClassId(v) != classDesc.Id)
					throw new ArgumentException("Invalid id.");
			}
		}

		WriteLong(v);
		CheckIfOperationComplete();
	}

	public void AddVersion(ulong v)
	{
		ComleteBlockDefinition();
		WriteLong((long)v);
		CheckIfOperationComplete();
	}

	public void AddReference(long v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.Long);

		ReferencePropertyDescriptor pd = classDesc.Properties[propertyIndexes[propertyIndex]] as ReferencePropertyDescriptor;
		if (pd == null)
			throw new InvalidOperationException("Expected property is not a reference.");

		if (pd.Multiplicity == Multiplicity.Many)
			throw new InvalidOperationException("Reference multiplicity is invalid.");

		WriteLong(v);
		CheckIfOperationComplete();
	}

	public void AddFloat(float v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.Float);
		WriteFloat(v);
		CheckIfOperationComplete();
	}

	public void AddDouble(double v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.Double);
		WriteDouble(v);
		CheckIfOperationComplete();
	}

	public void AddBool(bool v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.Bool);
		WriteBool(v);
		CheckIfOperationComplete();
	}

	public void AddDateTime(DateTime v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.DateTime);
		WriteDateTime(v);
		CheckIfOperationComplete();
	}

	public void AddString(string v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.String);
		WriteStringOptional(v);
		CheckIfOperationComplete();
	}

	public void AddByteArray(byte[] v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.ByteArray);
		WriteByteArrayOptional(v);
		CheckIfOperationComplete();
	}

	public void AddShortArray(short[] v)
	{
		AddShortArray(v, true);
	}

	public void AddShortArray(short[] v, bool isDefined)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.ShortArray);
		WriteShortArrayOptional(v);
		CheckIfOperationComplete();
	}

	public void AddIntArray(int[] v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.IntArray);
		WriteIntArrayOptional(v);
		CheckIfOperationComplete();
	}

	public void AddLongArray(long[] v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.LongArray);
		WriteLongArrayOptional(v);
		CheckIfOperationComplete();
	}

	public void AddReferenceArray(long[] v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.LongArray);

		ReferencePropertyDescriptor pd = classDesc.Properties[propertyIndexes[propertyIndex]] as ReferencePropertyDescriptor;
		if (pd == null)
			throw new InvalidOperationException("Expected property is not a reference.");

		if (pd.Multiplicity != Multiplicity.Many)
			throw new InvalidOperationException("Reference multiplicity is invalid.");

		WriteLongArrayOptional(v);
		CheckIfOperationComplete();
	}

	public unsafe void AddReferenceArray(long* v, int count)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.LongArray);

		ReferencePropertyDescriptor pd = classDesc.Properties[propertyIndexes[propertyIndex]] as ReferencePropertyDescriptor;
		if (pd == null)
			throw new InvalidOperationException("Expected property is not a reference.");

		if (pd.Multiplicity != Multiplicity.Many)
			throw new InvalidOperationException("Reference multiplicity is invalid.");

		WriteLongArrayUnsafe(v, count);
		CheckIfOperationComplete();
	}

	public void AddFloatArray(float[] v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.FloatArray);
		WriteFloatArrayOptional(v);
		CheckIfOperationComplete();
	}

	public void AddDoubleArray(double[] v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.DoubleArray);
		WriteDoubleArrayOptional(v);
		CheckIfOperationComplete();
	}

	public void AddBoolArray(bool[] v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.BoolArray);
		WriteBoolArrayOptional(v);
		CheckIfOperationComplete();
	}

	public void AddDateTimeArray(DateTime[] v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.DateTimeArray);
		WriteDateTimeArrayOptional(v);
		CheckIfOperationComplete();
	}

	public void AddStringArray(string[] v)
	{
		ComleteBlockDefinition();
		ValidateValue(PropertyType.StringArray);
		WriteStringArrayOptional(v);
		CheckIfOperationComplete();
	}

	public void AddDelete(long id)
	{
		ComleteBlockDefinition();

		if (IdHelper.GetClassId(id) != classDesc.Id)
			throw new ArgumentException("Invalid id.");

		if (operationType != OperationType.Delete)
			throw new InvalidOperationException("Active block is not a delete block.");

		WriteLong(0);       // Place holder for previous version
		WriteLong(id);
		CheckIfOperationComplete();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void ProvideSpace(int size)
	{
		if (activeOffset + size > activeBufferSize)
			AllocateNewBuffer();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void AllocateNewBuffer()
	{
		activeBufferSize = Min(endBufferSize, Max(startBufferSize, activeBufferSize * 2));
		ulong handle = memoryManager.Allocate(activeBufferSize);
		ChangesetBufferHeader* newBuffer = (ChangesetBufferHeader*)memoryManager.GetBuffer(handle);

		newBuffer->handle = handle;
		newBuffer->size = activeBufferSize;
		newBuffer->next = null;

		if (head == null)
		{
			head = (byte*)newBuffer;
		}
		else
		{
			((ChangesetBufferHeader*)activeBuffer)->next = newBuffer;
		}

		activeBuffer = (byte*)newBuffer;
		activeOffset = ChangesetBufferHeader.Size;
		bufferCount++;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void CreatePreviousVersionPlaceHolder()
	{
		ProvideSpace(8);
		activeOffset += 8;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteByte(byte v)
	{
		ProvideSpace(1);
		activeBuffer[activeOffset++] = v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteShort(short v)
	{
		ProvideSpace(2);
		*((short*)(activeBuffer + activeOffset)) = v;
		activeOffset += 2;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteInt(int v)
	{
		ProvideSpace(4);
		*((int*)(activeBuffer + activeOffset)) = v;
		activeOffset += 4;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteLong(long v)
	{
		ProvideSpace(8);
		*((long*)(activeBuffer + activeOffset)) = v;
		activeOffset += 8;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteFloat(float v)
	{
		ProvideSpace(4);
		*((float*)(activeBuffer + activeOffset)) = v;
		activeOffset += 4;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDouble(double v)
	{
		ProvideSpace(8);
		*((double*)(activeBuffer + activeOffset)) = v;
		activeOffset += 8;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteBool(bool v)
	{
		ProvideSpace(1);
		*((bool*)(activeBuffer + activeOffset)) = v;
		activeOffset++;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void WriteDateTime(DateTime v)
	{
		ProvideSpace(8);
		*((long*)(activeBuffer + activeOffset)) = v.ToBinary();
		activeOffset += 8;
	}

	public void WriteNullArray()
	{
		WriteByte((byte)0);
	}

	public void WriteByteArrayOptional(byte[] v, bool isDefined = true)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			WriteByte(isDefined ? (byte)1 : (byte)0);
			return;
		}

		int len = v.Length;
		WriteLength(len);

		fixed (byte* ptr = v)
		{
			WriteBytes(ptr, len);
		}
	}

	public void WriteByteSubArray(byte[] v, int len)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			return;
		}

		WriteLength(len);

		fixed (byte* ptr = v)
		{
			WriteBytes(ptr, len);
		}
	}

	public void WriteShortArrayOptional(short[] v, bool isDefined = true)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			WriteByte(isDefined ? (byte)1 : (byte)0);
			return;
		}

		int len = v.Length;
		WriteLength(len);

		fixed (short* ptr = v)
		{
			WriteBytes((byte*)ptr, len << 1);
		}
	}

	public void WriteShortSubArray(short[] v, int len)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			return;
		}

		WriteLength(len);

		fixed (short* ptr = v)
		{
			WriteBytes((byte*)ptr, len << 1);
		}
	}

	public void WriteIntArrayOptional(int[] v, bool isDefined = true)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			WriteByte(isDefined ? (byte)1 : (byte)0);
			return;
		}

		int len = v.Length;
		WriteLength(v.Length);

		fixed (int* ptr = v)
		{
			WriteBytes((byte*)ptr, len << 2);
		}
	}

	public void WriteIntSubArray(int[] v, int len)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			return;
		}

		WriteLength(len);

		fixed (int* ptr = v)
		{
			WriteBytes((byte*)ptr, len << 2);
		}
	}

	public void WriteLongArrayOptional(long[] v, bool isDefined = true)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			WriteByte(isDefined ? (byte)1 : (byte)0);
			return;
		}

		int len = v.Length;
		WriteLength(v.Length);

		fixed (long* ptr = v)
		{
			WriteBytes((byte*)ptr, len << 3);
		}
	}

	public void WriteLongSubArray(long[] v, int len)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			return;
		}

		WriteLength(len);

		fixed (long* ptr = v)
		{
			WriteBytes((byte*)ptr, len << 3);
		}
	}

	public void WriteLongArrayUnsafe(long* v, int len)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			return;
		}

		WriteLength(len);
		WriteBytes((byte*)v, len << 3);
	}

	public void WriteFloatArrayOptional(float[] v, bool isDefined = true)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			WriteByte(isDefined ? (byte)1 : (byte)0);
			return;
		}

		int len = v.Length;
		WriteLength(v.Length);

		fixed (float* ptr = v)
		{
			WriteBytes((byte*)ptr, len << 2);
		}
	}

	public void WriteFloatSubArray(float[] v, int len)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			return;
		}

		WriteLength(len);

		fixed (float* ptr = v)
		{
			WriteBytes((byte*)ptr, len << 2);
		}
	}

	public void WriteDoubleArrayOptional(double[] v, bool isDefined = true)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			WriteByte(isDefined ? (byte)1 : (byte)0);
			return;
		}

		int len = v.Length;
		WriteLength(v.Length);

		fixed (double* ptr = v)
		{
			WriteBytes((byte*)ptr, len << 3);
		}
	}

	public void WriteDoubleSubArray(double[] v, int len)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			return;
		}

		WriteLength(len);

		fixed (double* ptr = v)
		{
			WriteBytes((byte*)ptr, len << 3);
		}
	}

	public void WriteBoolArrayOptional(bool[] v, bool isDefined = true)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			WriteByte(isDefined ? (byte)1 : (byte)0);
			return;
		}

		int len = v.Length;
		WriteLength(len);

		fixed (bool* ptr = v)
		{
			WriteBytes((byte*)ptr, len);
		}
	}

	public void WriteBoolSubArray(bool[] v, int len)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			return;
		}

		WriteLength(len);

		fixed (bool* ptr = v)
		{
			WriteBytes((byte*)ptr, len);
		}
	}

	public void WriteDateTimeArrayOptional(DateTime[] v, bool isDefined = true)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			WriteByte(isDefined ? (byte)1 : (byte)0);
			return;
		}

		int len = v.Length;
		WriteLength(len);

		fixed (DateTime* ptr = v)
		{
			WriteBytes((byte*)ptr, len << 3);
		}
	}

	public void WriteDateTimeSubArray(DateTime[] v, int len)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			return;
		}

		WriteLength(len);

		fixed (DateTime* ptr = v)
		{
			WriteBytes((byte*)ptr, len << 3);
		}
	}

	public void WriteStringArrayOptional(string[] v, bool isDefined = true)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			WriteByte(isDefined ? (byte)1 : (byte)0);
			return;
		}

		WriteByte((byte)1);
		int size = GetStringArrayPackedSize(v);
		WriteInt(size);

		int length = v.Length;
		WriteBytes((byte*)&length, 4);

		for (int i = 0; i < length; i++)
		{
			int strLength = v[i].Length;
			WriteBytes((byte*)&strLength, 4);

			fixed (char* pc = v[i])
			{
				WriteBytes((byte*)pc, strLength * 2);
			}
		}
	}

	public void WriteStringSubArray(string[] v, int len)
	{
		if (v == null)
		{
			WriteByte((byte)0);
			return;
		}

		WriteByte((byte)1);
		int byteLen = GetStringArrayPackedSize(v);
		WriteInt(byteLen);

		WriteBytes((byte*)&len, 4);

		for (int i = 0; i < len; i++)
		{
			int strLen = v[i].Length;
			WriteBytes((byte*)&strLen, 4);

			fixed (char* pc = v[i])
			{
				WriteBytes((byte*)pc, strLen * 2);
			}
		}
	}

	public int GetStringArrayPackedSize(string[] v)
	{
		int len = 4;
		for (int i = 0; i < v.Length; i++)
		{
			len += 4 + (v[i].Length << 1);
		}

		return len;
	}

	public void WriteStringOptional(string s, bool isDefined = true)
	{
		if (s == null)
		{
			WriteByte((byte)0);
			WriteByte(isDefined ? (byte)1 : (byte)0);
			return;
		}

		WriteLength(s.Length);

		if (s.Length > 0)
		{
			if (strings.Length == stringCount)
				Array.Resize(ref strings, strings.Length * 2);

			int n = stringCount++;
			strings[n] = s;
			WriteInt(n);
		}

		fixed (char* pc = s)
		{
			WriteBytes((byte*)pc, s.Length * 2);
		}
	}

	private void WriteBytes(byte* ptr, int length)
	{
		int w = 0;
		while (w < length)
		{
			ProvideSpace(1);
			int wc = Min(length - w, activeBufferSize - activeOffset);
			Utils.CopyMemory(ptr + w, activeBuffer + activeOffset, wc);
			w += wc;
			activeOffset += wc;
		}
	}

	private void WriteLength(int length)
	{
		if (length < 64)
		{
			byte v = (byte)length;
			v |= ChangesetReader.ShortNonNullStringFlag;
			WriteByte(v);
		}
		else
		{
			WriteByte(ChangesetReader.LongNonNullStringFlag);
			WriteInt(length);
		}
	}

	private UShortPlaceholder CreateUShortPlaceholder()
	{
		ProvideSpace(2);
		activeOffset += 2;
		return new UShortPlaceholder((ushort*)(activeBuffer + activeOffset - 2));
	}

	public void WriteDefaultValue(PropertyDescriptor propDesc)
	{
		switch (propDesc.PropertyType)
		{
			case PropertyType.Byte:
				WriteByte((byte)propDesc.DefaultValue);
				break;

			case PropertyType.Short:
				WriteShort((short)propDesc.DefaultValue);
				break;

			case PropertyType.Int:
				WriteInt((int)propDesc.DefaultValue);
				break;

			case PropertyType.Long:
				WriteLong((long)propDesc.DefaultValue);
				break;

			case PropertyType.Float:
				WriteFloat((float)propDesc.DefaultValue);
				break;

			case PropertyType.Double:
				WriteDouble((double)propDesc.DefaultValue);
				break;

			case PropertyType.Bool:
				WriteBool((bool)propDesc.DefaultValue);
				break;

			case PropertyType.DateTime:
				WriteDateTime((DateTime)propDesc.DefaultValue);
				break;

			default:
				throw new ArgumentException();
		}
	}

	public void AddDefaultValue(PropertyDescriptor propDesc)
	{
		switch (propDesc.PropertyType)
		{
			case PropertyType.Byte:
				AddByte((byte)propDesc.DefaultValue);
				break;

			case PropertyType.Short:
				AddShort((short)propDesc.DefaultValue);
				break;

			case PropertyType.Int:
				AddInt((int)propDesc.DefaultValue);
				break;

			case PropertyType.Long:
				AddLong((long)propDesc.DefaultValue);
				break;

			case PropertyType.Float:
				AddFloat((float)propDesc.DefaultValue);
				break;

			case PropertyType.Double:
				AddDouble((double)propDesc.DefaultValue);
				break;

			case PropertyType.Bool:
				AddBool((bool)propDesc.DefaultValue);
				break;

			case PropertyType.DateTime:
				AddDateTime((DateTime)propDesc.DefaultValue);
				break;

			default:
				throw new ArgumentException();
		}
	}

	private struct UShortPlaceholder
	{
		ushort* pointer;

		public UShortPlaceholder(ushort* pointer)
		{
			this.pointer = pointer;
		}

		public void Populate(ushort v)
		{
			*pointer = v;
		}
	}
}
