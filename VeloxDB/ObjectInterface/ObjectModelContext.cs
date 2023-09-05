using System;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage;

namespace VeloxDB.ObjectInterface;

internal sealed class ObjectModelContext
{
	const int objectMapCapacity = 1024 * 8;
	const int inverseRefsSize = 1024;
	const int stringsSize = 1024 * 8;
	const int invRefChangeCapacity = 1024;
	const int invRefChangeItemCapacity = invRefChangeCapacity * 8;
	const int deletedInvRefsCapacity = 256;
	const int toDeletedCapacity = 1024;

	public const int ObjectReadersCapacity = 64;
	const int objectReadersPoolCapacity = 32;

	ObjectModelContextPool owner;
	StorageEngine engine;
	int physCorePool;

	LongDictionary<DatabaseObject> objectMap;
	ChangeList changeList;

	string[] strings;

	long[] inverseReferenceIds;

	int invRefChangesInitCapacity;
	InverseReferenceChanges invRefChanges;
	LongDictionary<int> deletedInvRefs;

	int objectReadersCount;
	ObjectReader[][] objectReadersPool;

	int objectReadersListCount;
	ObjectReaderList[] objectReadersListPool;

	DeletedSet deletedSet;
	ScanClassesSet scanClasses;
	long[] toDelete1, toDelete2;

	IdRange idRange;
	long currId;
	long unusedIdsCount;

	public ObjectModelContext(ObjectModelContextPool owner, StorageEngine engine, IdRange idRange, int physCorePool)
	{
		this.owner = owner;
		this.engine = engine;
		this.physCorePool = physCorePool;
		this.idRange = idRange;

		unusedIdsCount = 0;

		objectMap = new LongDictionary<DatabaseObject>(objectMapCapacity);

		changeList = new ChangeList(engine.UserDatabase.ModelDesc);
		strings = new string[stringsSize];
		inverseReferenceIds = new long[inverseRefsSize];

		invRefChanges = new InverseReferenceChanges(invRefChangeCapacity, invRefChangeItemCapacity);
		invRefChangesInitCapacity = invRefChanges.Capacity;

		deletedInvRefs = new LongDictionary<int>(deletedInvRefsCapacity);

		objectReadersPool = new ObjectReader[objectReadersPoolCapacity][];
		objectReadersCount = objectReadersPoolCapacity;
		for (int i = 0; i < objectReadersPoolCapacity; i++)
		{
			objectReadersPool[i] = new ObjectReader[ObjectReadersCapacity];
		}

		objectReadersListPool = new ObjectReaderList[4];
		for (int i = 0; i < objectReadersListPool.Length; i++)
		{
			objectReadersListPool[i] = new ObjectReaderList(this);
		}

		deletedSet = new DeletedSet();
		scanClasses = new ScanClassesSet();
		toDelete1 = new long[toDeletedCapacity];
		toDelete2 = new long[toDeletedCapacity];
	}

	public StorageEngine Engine => engine;
	public string[] Strings => strings;
	public long[] InverseReferenceIds => inverseReferenceIds;
	public InverseReferenceChanges InverseReferenceChanges => invRefChanges;
	public LongDictionary<DatabaseObject> ObjectMap => objectMap;
	public int PhysCorePool => physCorePool;
	public LongDictionary<int> DeletedInvRefs => deletedInvRefs;
	public ChangeList ChangeList => changeList;
	public DeletedSet DeletedSet => deletedSet;
	public long[] ToDelete1 => toDelete1;
	public long[] ToDelete2 => toDelete2;
	public ScanClassesSet ScanClasses => scanClasses;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void RefreshModelIfNeeded(DataModelDescriptor modelDesc)
	{
		changeList.RefreshModelIfNeeded(modelDesc);
	}

	public void TryReserverIdRange()
	{
		if (unusedIdsCount == 0)
		{
			try
			{
				currId = idRange.GetIdRange(out unusedIdsCount);
			}
			catch (DatabaseException)
			{
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long GetId()
	{
		if (unusedIdsCount == 0)
			currId = idRange.GetIdRange(out unusedIdsCount);

		unusedIdsCount--;
		return currId++;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ObjectReader[] GetObjectReaders()
	{
		if (objectReadersCount == 0)
			return owner.GetObjectReaders();

		return objectReadersPool[--objectReadersCount];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void PutObjectReaders(ObjectReader[] r)
	{
		if (r.Length > ObjectReadersCapacity)
			return;

		for (int i = 0; i < r.Length; i++)
		{
			if (r[i].Class != null)
				r[i] = new ObjectReader();
			else
				break;
		}

		if (objectReadersCount == objectReadersPoolCapacity)
		{
			owner.PutObjectReaders(r);
			return;
		}

		objectReadersPool[objectReadersCount++] = r;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ObjectReaderList GetObjectReaderList()
	{
		if (objectReadersListCount == 0)
			return new ObjectReaderList(this);

		return objectReadersListPool[--objectReadersListCount];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void PutObjectReaderList(ObjectReaderList l)
	{
		l.Release();

		if (objectReadersListCount == objectReadersListPool.Length)
			return;

		objectReadersListPool[objectReadersListCount++] = l;
	}

	public void Reset()
	{
		if (objectMap.Count > objectMapCapacity)
		{
			objectMap = new LongDictionary<DatabaseObject>(objectMapCapacity);
		}
		else
		{
			objectMap.Clear();
		}

		changeList.Clear();

		int i = 0;
		while (i < strings.Length && strings[i] != null)
		{
			strings[i++] = null;
		}

		if (invRefChanges.Capacity > invRefChangesInitCapacity || invRefChanges.ItemCapacity > invRefChangeItemCapacity)
		{
			invRefChanges = new InverseReferenceChanges(invRefChangeCapacity, invRefChangeItemCapacity);
		}
		else
		{
			invRefChanges.Clear();
		}

		if (deletedInvRefs.Count > deletedInvRefsCapacity)
		{
			deletedInvRefs = new LongDictionary<int>(deletedInvRefsCapacity);
		}
		else
		{
			deletedInvRefs.Clear();
		}

		deletedSet.Reset();
	}
}
