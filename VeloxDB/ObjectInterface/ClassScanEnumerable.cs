using System;
using System.Collections;
using System.Collections.Generic;
using VeloxDB.Storage;

namespace VeloxDB.ObjectInterface;

public unsafe sealed partial class ObjectModel
{
	internal sealed class ClassScanEnumerable<T> : IEnumerable<T> where T : DatabaseObject
	{
		ObjectModel model;
		ClassData classData;

		public ClassScanEnumerable(ObjectModel model, ClassData classData)
		{
			this.model = model;
			this.classData = classData;
		}

		public IEnumerator<T> GetEnumerator()
		{
			return new ClassScanEnumerator<T>(model, classData);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}

	private sealed class ClassScanEnumerator<T> : IEnumerator<T> where T : DatabaseObject
	{
		ObjectModel model;
		ClassData classData;
		ClassScan scan;

		T curr;

		int objectCount;
		int readObjectCount;
		ObjectReader[] objects;

		bool disposed;

		public ClassScanEnumerator(ObjectModel model, ClassData classData)
		{
			model.ValidateThread();

			this.model = model;
			this.classData = classData;

			if (model.HasLocalChanges(classData))
				model.ApplyChanges();

			scan = model.engine.BeginClassScan(model.transaction, classData.ClassDesc);

			objectCount = 0;
			readObjectCount = objectCount;
			objects = model.Context.GetObjectReaders();
		}

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

		object IEnumerator.Current => Current;

		public void Dispose()
		{
			model.ValidateThread();

			if (disposed || model.Disposed)
				return;

			model.context.PutObjectReaders(objects);
			objects = null;

			scan.Dispose();
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

			if (readObjectCount >= objectCount)
			{
				readObjectCount = 0;
				if (!scan.Next(objects, out objectCount))
					return false;
			}

			curr = (T)(object)model.GetObjectOrCreate(objects[readObjectCount++], classData);
			return true;
		}

		public void Reset()
		{
			throw new NotSupportedException();
		}
	}
}
