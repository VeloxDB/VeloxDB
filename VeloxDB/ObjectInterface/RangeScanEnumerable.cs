using System;
using System.Collections;
using System.Collections.Generic;
using VeloxDB.Storage;

namespace VeloxDB.ObjectInterface;

public unsafe sealed partial class ObjectModel
{
	internal sealed class RangeScanEnumerable<T> : IEnumerable<T>, IEnumerator<T> where T : DatabaseObject
	{
		bool usedUp;
		ObjectModel model;
		ClassData classData;
		RangeScan scan;
		ObjectReaderList list;

		T curr;
		int readObjectCount;

		bool disposed;

		public RangeScanEnumerable(ObjectModel model, ClassData classData, RangeScan scan)
		{
			this.model = model;
			this.classData = classData;
			this.scan = scan;
			list = model.Context.GetObjectReaderList();
		}

		object IEnumerator.Current => Current;

		public T Current
		{
			get
			{
				model.ValidateThread();

				if (disposed || model.Disposed)
					throw new ObjectDisposedException(nameof(ClassScanEnumerator<T>));

				return curr;
			}
		}

		public IEnumerator<T> GetEnumerator()
		{
			if (usedUp)
				throw new InvalidOperationException("Range scan enumerator has already been used up.");

			usedUp = true;

			return this;
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public void Dispose()
		{
			model.ValidateThread();

			if (disposed)
				return;

			model.context.PutObjectReaderList(list);
			list = null;

			disposed = true;
		}

		public bool MoveNext()
		{
			model.ValidateThread();

			if (disposed || model.Disposed)
				throw new ObjectDisposedException(nameof(ClassScanEnumerator<T>));

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
			if (disposed || model.Disposed)
				throw new ObjectDisposedException(nameof(ClassScanEnumerator<T>));

			if (readObjectCount >= list.Count)
			{
				readObjectCount = 0;
				list.Clear();
				if (!scan.Next(list, ObjectReaderList.Capacity))
					return false;
			}

			curr = (T)(object)model.GetObjectOrCreate(list[readObjectCount++], classData);
			return true;
		}

		public void Reset()
		{
			throw new NotSupportedException();
		}
	}
}
