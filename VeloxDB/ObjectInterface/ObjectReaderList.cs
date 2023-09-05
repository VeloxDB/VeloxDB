using System;
using System.Collections;
using System.Collections.Generic;
using VeloxDB.Storage;

namespace VeloxDB.ObjectInterface;

internal class ObjectReaderList : IList<ObjectReader>
{
	public const int Capacity = chunkSize * 16;

	const int chunkSizeLog = 6;
	const int chunkSize = 1 << chunkSizeLog;
	const int chunkSizeMask = chunkSize - 1;

	ObjectModelContext context;

	int count;
	ObjectReader[][] chunks;

	public ObjectReaderList(ObjectModelContext context)
	{
		this.context = context;

		chunks = new ObjectReader[Capacity >> chunkSizeLog][];
	}

	public ObjectReader this[int index]
	{
		get
		{
			int chunk = index >> chunkSizeLog;
			int offset = index & chunkSizeMask;
			return chunks[chunk][offset];
		}

		set
		{
			int chunk = index >> chunkSizeLog;
			int offset = index & chunkSizeMask;
			chunks[chunk][offset] = value;
		}
	}

	public int Count => count;
	public bool IsReadOnly => false;

	public void Add(ObjectReader item)
	{
		int index = count++;
		int chunk = index >> chunkSizeLog;
		int offset = index & chunkSizeMask;
		if (chunks[chunk] == null)
			chunks[chunk] = context.GetObjectReaders();

		chunks[chunk][offset] = item;
	}

	public void Clear()
	{
		count = 0;
	}

	public void Release()
	{
		for (int i = 0; i < chunks.Length; i++)
		{
			if (chunks[i] == null)
				break;

			context.PutObjectReaders(chunks[i]);
			chunks[i] = null;
		}

		count = 0;
	}

	public IEnumerator<ObjectReader> GetEnumerator()
	{
		throw new NotSupportedException();
	}

	public bool Contains(ObjectReader item)
	{
		throw new NotSupportedException();
	}

	public void CopyTo(ObjectReader[] array, int arrayIndex)
	{
		throw new NotSupportedException();
	}

	public int IndexOf(ObjectReader item)
	{
		throw new NotSupportedException();
	}

	public void Insert(int index, ObjectReader item)
	{
		throw new NotSupportedException();
	}

	public bool Remove(ObjectReader item)
	{
		throw new NotSupportedException();
	}

	public void RemoveAt(int index)
	{
		throw new NotSupportedException();
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		throw new NotSupportedException();
	}
}
