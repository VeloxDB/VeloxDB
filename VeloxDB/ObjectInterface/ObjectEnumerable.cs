using System;
using System.Collections;
using System.Collections.Generic;
using Velox.Storage;

namespace Velox.ObjectInterface;

public unsafe sealed partial class ObjectModel
{
	private sealed class ObjectEnumerable<T> : IEnumerable<T> where T : DatabaseObject
	{
		ObjectModel model;
		ClassData classData;

		public ObjectEnumerable(ObjectModel model, ClassData classData)
		{
			this.model = model;
			this.classData = classData;
		}

		public IEnumerator<T> GetEnumerator()
		{
			return new ObjectEnumerator<T>(model, classData);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}

	private sealed class ObjectEnumerator<T> : IEnumerator<T> where T : DatabaseObject
	{
		const int scanChunkSize = 256;

		ObjectModel model;
		ClassData classData;
		ClassScan scan;

		T curr;

		ChangeList.TypeIterator changeIterator;

		int objectCount;
		int readObjectCount;
		ObjectReader[] objects;

		bool disposed;

		public ObjectEnumerator(ObjectModel model, ClassData classData)
		{
			model.ValidateThread();

			this.model = model;
			this.classData = classData;

			model.classScanCount++;

			scan = model.engine.BeginClassScan(model.transaction, classData.ClassDesc);

			objectCount = 0;
			readObjectCount = objectCount;
			objects = new ObjectReader[scanChunkSize];
		}

		public T Current
		{
			get
			{
				model.ValidateThread();

				if (disposed)
					throw new ObjectDisposedException(nameof(ObjectEnumerator<T>));

				return curr;
			}
		}

		object IEnumerator.Current => Current;

		public void Dispose()
		{
			model.ValidateThread();

			if (disposed)
				return;

			if (scan != null)
				scan.Dispose();

			model.classScanCount--;
			disposed = true;
		}

		public bool MoveNext()
		{
			model.ValidateThread();

			do
			{
				if (!MoveNextInternal())
					return false;
			}
			while (curr == null);

			return true;
		}

		private bool MoveNextInternal()
		{
			if (disposed)
				throw new ObjectDisposedException(nameof(ObjectEnumerator<T>));

			if (scan != null)
			{
				if (readObjectCount >= objectCount)
				{
					readObjectCount = 0;
					objectCount = scanChunkSize;
					if (!scan.Next(objects, 0, ref objectCount))
					{
						scan.Dispose();
						scan = null;
						changeIterator = model.changeList.IterateType(classData.ClassDesc);
					}
				}
			}

			if (scan != null)
			{
				curr = (T)(object)model.GetObjectOrCreate(objects[readObjectCount++], classData);
			}
			else
			{
				DatabaseObject obj;
				do
				{
					if (!changeIterator.HasMore)
					{
						curr = default(T);
						return false;
					}

					obj = changeIterator.GetNextAndMove();
				}
				while (!obj.IsCreated || obj.IsDeleted);

				curr = (T)(object)obj;
			}

			return true;
		}

		public void Reset()
		{
			throw new NotSupportedException();
		}
	}

}
