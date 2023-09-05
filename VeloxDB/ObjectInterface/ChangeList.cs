using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using VeloxDB.Descriptor;

namespace VeloxDB.ObjectInterface;

internal sealed class ChangeList
{
	const int commitListSize = 1024 * 8;

	int count;
	ListItem[] list;
	PerTypeListItem[] perTypeLists;

	public ChangeList(DataModelDescriptor modelDesc)
	{
		list = new ListItem[commitListSize];
		CreatePerTypeLists(modelDesc);
	}

	public int Count => count;

	public DatabaseObject this[int index]
	{
		get
		{
			if (index >= count)
				throw new IndexOutOfRangeException();

			return list[index].obj;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void RefreshModelIfNeeded(DataModelDescriptor modelDesc)
	{
		if (perTypeLists.Length <= modelDesc.ClassCount)
			CreatePerTypeLists(modelDesc);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private void CreatePerTypeLists(DataModelDescriptor modelDesc)
	{
		perTypeLists = new PerTypeListItem[modelDesc.ClassCount + 1];
		for (int i = 0; i < perTypeLists.Length; i++)
		{
			perTypeLists[i] = new PerTypeListItem() { next = -1, count = 0 };
		}
	}

	public void Add(DatabaseObject obj)
	{
		if (list.Length == count)
			Array.Resize(ref list, list.Length * 2);

		int classIndex = obj.ClassData.ClassDesc.Index;
		int index = count++;
		list[index].obj = obj;
		list[index].next = perTypeLists[classIndex].next;
		perTypeLists[classIndex].next = index;
		perTypeLists[classIndex].count++;
	}

	public int GetTypeChangeCount(ClassData classData)
	{
		if (count == 0)
			return 0;

		int s = 0;
		int[] classIndexes = classData.ClassIndexes;
		for (int i = 0; i < classIndexes.Length; i++)
		{
			s += perTypeLists[classIndexes[i]].count;
		}

		return s;
	}

	public bool HasLocalChange(ClassData classData)
	{
		if (count == 0)
			return false;

		int[] classIndexes = classData.ClassIndexes;
		for (int i = 0; i < classIndexes.Length; i++)
		{
			if (perTypeLists[classIndexes[i]].count > 0)
				return true;
		}

		return false;
	}

	public TypeIterator IterateType(ClassData classData)
	{
		return new TypeIterator(this, classData);
	}

	public void Clear()
	{
		for (int i = 0; i < count; i++)
		{
			int n = list[i].obj.ClassData.ClassDesc.Index;
			perTypeLists[n].next = -1;
			perTypeLists[n].count = 0;
			list[i].obj = null;
		}

		if (list.Length > commitListSize)
			this.list = new ListItem[commitListSize];

		count = 0;
	}

	private struct ListItem
	{
		public int next;
		public DatabaseObject obj;
	}

	private struct PerTypeListItem
	{
		public int next;
		public int count;
	}

	public struct TypeIterator
	{
		ChangeList list;
		ClassData classData;

		int listIndex;
		int typeIndex;

		public TypeIterator(ChangeList list, ClassData classData)
		{
			this.list = list;
			this.classData = classData;

			typeIndex = 0;
			listIndex = list.perTypeLists[classData.ClassIndexes[0]].next;

			MoveToNonEmptyType();
		}

		public bool HasMore => listIndex != -1;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public DatabaseObject GetNextAndMove()
		{
			DatabaseObject obj = list.list[listIndex].obj;
			listIndex = list.list[listIndex].next;
			MoveToNonEmptyType();
			return obj;
		}

		private void MoveToNonEmptyType()
		{
			while (listIndex == -1 && typeIndex < classData.ClassIndexes.Length - 1)
			{
				typeIndex++;
				listIndex = list.perTypeLists[classData.ClassIndexes[typeIndex]].next;
			}
		}
	}
}
