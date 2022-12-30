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
	int[] perTypeLists;

	public ChangeList(DataModelDescriptor modelDesc)
	{
		list = new ListItem[commitListSize];
		perTypeLists = new int[modelDesc.ClassCount + 1];
		for (int i = 0; i < perTypeLists.Length; i++)
		{
			perTypeLists[i] = -1;
		}
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

	public void Add(DatabaseObject obj)
	{
		if (list.Length == count)
			Array.Resize(ref list, list.Length * 2);

		int classIndex = obj.ClassData.ClassDesc.Index;
		if (perTypeLists.Length <= classIndex)
			Array.Resize(ref perTypeLists, obj.ClassData.ClassDesc.Model.ClassCount + 1);

		int index = count++;
		list[index].obj = obj;
		list[index].next = perTypeLists[classIndex];
		perTypeLists[classIndex] = index;
	}

	public TypeIterator IterateType(ClassDescriptor classDesc)
	{
		return new TypeIterator(this, classDesc);
	}

	public void Clear()
	{
		for (int i = 0; i < count; i++)
		{
			perTypeLists[list[i].obj.ClassData.ClassDesc.Index] = -1;
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

	public struct TypeIterator
	{
		ChangeList list;
		ClassDescriptor classDesc;

		int listIndex;
		int typeIndex;

		public TypeIterator(ChangeList list, ClassDescriptor classDesc)
		{
			this.list = list;
			this.classDesc = classDesc;

			typeIndex = -1;
			listIndex = list.perTypeLists[classDesc.Index];

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
			while (listIndex == -1 && typeIndex < classDesc.DescendentClassIds.Length - 1)
			{
				typeIndex++;
				ClassDescriptor cd = classDesc.Model.GetClass(classDesc.DescendentClassIds[typeIndex]);
				listIndex = list.perTypeLists[cd.Index];
			}
		}
	}
}
