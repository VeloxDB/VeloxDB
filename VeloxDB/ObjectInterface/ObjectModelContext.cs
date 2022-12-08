using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Velox.Common;
using Velox.Storage;

namespace Velox.ObjectInterface;

internal sealed class ObjectModelContext
{
	const int objectMapCapacity = 1024 * 8;
	const int inverseRefsSize = 1024;
	const int stringsSize = 1024 * 8;
	const int invRefChangeCapacity = 1024;
	const int invRefChangeItemCapacity = invRefChangeCapacity * 8;
	const int deletedInvRefsCapacity = 256;
	const int objectReadersCapacity = 512;
	const int toDeletedCapacity = 1024;

	StorageEngine engine;
	int physCorePool;

	LongDictionary<DatabaseObject> objectMap;
	ChangeList changeList;

	string[] strings;

	long[] inverseReferenceIds;

	int invRefChangesInitCapacity;
	InverseReferenceChanges invRefChanges;
	LongDictionary<int> deletedInvRefs;

	ObjectReader[] objectReaders;

	DeletedSet deletedSet;
	ScanClassesSet scanClasses;
	long[] toDelete1, toDelete2;

	IdRange idRange;
	long currId;
	long unusedIdsCount;

	public ObjectModelContext(StorageEngine engine, IdRange idRange, int physCorePool)
	{
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

		objectReaders = new ObjectReader[objectReadersCapacity];

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
	public ObjectReader[] ObjectReaders => objectReaders;
	public DeletedSet DeletedSet => deletedSet;
	public long[] ToDelete1 => toDelete1;
	public long[] ToDelete2 => toDelete2;
	public ScanClassesSet ScanClasses => scanClasses;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public long GetId()
	{
		if (unusedIdsCount == 0)
			currId = idRange.GetIdRange(out unusedIdsCount);

		unusedIdsCount--;
		return currId++;
	}

	public void Finished()
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

		deletedSet.Clear();
	}
}
