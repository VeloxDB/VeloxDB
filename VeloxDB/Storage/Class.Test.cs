using System;
using System.Collections.Generic;
using System.IO;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

#if TEST_BUILD
internal unsafe sealed partial class Class : ClassBase
{
	public void CheckHashLoad(out long objCount, out long capacity, out long bucketCount)
	{
		objCount = 0;
		bucketCount = 0;

		for (long i = 0; i < this.capacity; i++)
		{
			Bucket* bn = buckets + i;
			ulong objHandle = bn->Handle;
			ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
			if (obj != null)
			{
				bucketCount++;

				while (obj != null)
				{
					objCount++;
					objHandle = obj->nextCollisionHandle;
					obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
				}
			}
		}

		capacity = this.capacity;
	}

	public void DumpToFile(string file)
	{
		using (StreamWriter sw = File.AppendText(file))
		{
			sw.WriteLine("name={0}, idCount={1}, capacity={2}", ClassDesc.Name, resizeCounter.Count, capacity);

			for (long i = 0; i < capacity; i++)
			{
				Bucket* bn = buckets + i;
				ulong objHandle = bn->Handle;
				ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
				while (obj != null)
				{
					ulong verObjHandle = objHandle;
					ClassObject* verObj = obj;
					while (verObj != null)
					{
						sw.WriteLine("bucket={0}, id={1}, version={2}, isDeleted={3}", i, verObj->id, verObj->version, verObj->IsDeleted);
						verObjHandle = verObj->nextVersionHandle;
						verObj = (ClassObject*)ObjectStorage.GetBuffer(verObjHandle);
					}

					objHandle = obj->nextCollisionHandle;
					obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
				}
			}
		}
	}

	public void Validate(List<long> tempList, ulong maxVersion)
	{
		int currIdCount = 0;

		for (long i = 0; i < capacity; i++)
		{
			Bucket* bn = buckets + i;
			ulong objHandle = bn->Handle;
			ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
			tempList.Clear();
			while (obj != null)
			{
				if (tempList.Contains(obj->id))
					throw new InvalidOperationException();

				currIdCount++;

				ulong verObjHandle = objHandle;
				ClassObject* verObj = obj;
				if (verObj->readerInfo.StandardLockCount > 0 || verObj->readerInfo.ExistanceLockCount > 0 || verObj->NewerVersion != 0)
					throw new InvalidOperationException();

				while (verObj != null)
				{
					if (verObj->nextVersionHandle == ClassObject.AlignedFlag || verObj->id == 0)
						throw new InvalidOperationException();

					if (Database.IsCommited(verObj->version) && (verObj->version > maxVersion ||
						verObj->readerInfo.StandardLockCount > 0 || verObj->readerInfo.ExistanceLockCount > 0))
					{
						throw new InvalidOperationException();
					}

					ValidateHashIndexes(verObjHandle, verObj);

					Checker.AssertTrue(ObjectStorage.IsBufferUsed((byte*)verObj));

					ulong version = verObj->version;
					verObjHandle = verObj->nextVersionHandle;
					verObj = (ClassObject*)ObjectStorage.GetBuffer(verObjHandle);

					if (verObj != null)
					{
						if (verObj->NewerVersion != version)
							throw new InvalidOperationException();
					}
				}

				tempList.Add(obj->id);
				objHandle = obj->nextCollisionHandle;
				obj = (ClassObject*)ObjectStorage.GetBuffer(objHandle);
			}
		}

		if (currIdCount != resizeCounter.Count)
			throw new InvalidOperationException();
	}

	internal void ValidateHashIndexes(ulong objectHandle, ClassObject* obj)
	{
		if (obj->IsDeleted)
			return;

		byte* key = (byte*)obj + ClassObject.DataOffset;
		for (int i = 0; i < indexes.Length; i++)
		{
			if (!indexes[i].Index.HasObject(obj, key, objectHandle, indexes[i].Comparer))
				throw new InvalidOperationException();
		}
	}

	public bool IsObjectPresent(ulong objectHandle)
	{
		ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(objectHandle);
		long bucket = CalculateBucket(obj->id);
		Bucket* bn = buckets + bucket;

		ulong handle = bn->Handle;
		while (handle != 0)
		{
			ClassObject* cobj = (ClassObject*)ObjectStorage.GetBuffer(handle);
			ulong vhandle = handle;
			while (vhandle != 0)
			{
				ClassObject* vobj = (ClassObject*)ObjectStorage.GetBuffer(vhandle);
				if (vhandle == objectHandle)
					return !vobj->IsDeleted;

				vhandle = vobj->nextVersionHandle;
			}

			handle = cobj->nextCollisionHandle;
		}

		return false;
	}

	public void ValidateGarbage(ulong readVersion)
	{
		for (long i = 0; i < capacity; i++)
		{
			Bucket* bn = buckets + i;
			ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(bn->Handle);

			while (obj != null)
			{
				ClassObject* verObj = obj;
				if (verObj->IsDeleted && verObj->version <= readVersion)
					throw new InvalidOperationException();

				while (verObj != null && verObj->version > readVersion)
				{
					verObj = (ClassObject*)ObjectStorage.GetBuffer(verObj->nextVersionHandle);
				}

				if (verObj != null)
				{
					if (verObj->nextVersionHandle != 0)
						throw new InvalidOperationException();
				}

				obj = (ClassObject*)ObjectStorage.GetBuffer(obj->nextCollisionHandle);
			}
		}
	}

	public void CollectBlobRefCounts(Dictionary<ulong, int> strings, Dictionary<ulong, int> blobs)
	{
		for (long i = 0; i < capacity; i++)
		{
			Bucket* bn = buckets + i;
			ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(bn->Handle);
			while (obj != null)
			{
				ClassObject* verObj = obj;
				while (verObj != null)
				{
					if (!verObj->IsDeleted)
						CollectObjectBlobRefCounts(verObj, strings, blobs);

					verObj = (ClassObject*)ObjectStorage.GetBuffer(verObj->nextVersionHandle);
				}

				obj = (ClassObject*)ObjectStorage.GetBuffer(obj->nextCollisionHandle);
			}
		}
	}

	private unsafe void CollectObjectBlobRefCounts(ClassObject* obj, Dictionary<ulong, int> strings, Dictionary<ulong, int> blobs)
	{
		int count = ClassDesc.StringPropertyIndexes.Length;
		for (int i = 0; i < count; i++)
		{
			int index = ClassDesc.StringPropertyIndexes[i];
			int offset = propertyOffsets[index];
			ulong* up = (ulong*)((byte*)obj + ClassObject.DataOffset + offset);
			if (!strings.ContainsKey(up[0]))
				strings.Add(up[0], 1);
			else
				strings[up[0]]++;
		}

		count = ClassDesc.BlobPropertyIndexes.Length;
		for (int i = 0; i < count; i++)
		{
			int index = ClassDesc.BlobPropertyIndexes[i];
			int offset = propertyOffsets[index];
			ulong* up = (ulong*)((byte*)obj + ClassObject.DataOffset + offset);
			if (!blobs.ContainsKey(up[0]))
				blobs.Add(up[0], 1);
			else
				blobs[up[0]]++;
		}
	}

	public void CollectInverseRefsAndValidateTran(Transaction tran, Dictionary<ValueTuple<long, int>, List<long>> invRefs)
	{
		for (long i = 0; i < capacity; i++)
		{
			ulong handle = buckets[i].Handle;
			while (handle != 0)
			{
				ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(handle);
				handle = obj->nextCollisionHandle;

				DatabaseErrorDetail err;
				ObjectReader reader = GetObject(tran, obj->id, false, out err);
				if (reader.IsEmpty())
					continue;

				ReaderInfo* rd = &obj->readerInfo;
				if (rd != null && rd->StandardLockCount + rd->ExistanceLockCount != 0)
					throw new InvalidOperationException();

				ClassDescriptor cd = base.ClassDesc;
				for (int j = 0; j < cd.Properties.Length; j++)
				{
					PropertyDescriptor pd = cd.Properties[j];
					if (pd.Kind != PropertyKind.Reference)
						continue;

					ReferencePropertyDescriptor rpd = (ReferencePropertyDescriptor)pd;
					if (!rpd.TrackInverseReferences)
						continue;

					PropertyType propType = pd.PropertyType;
					if (propType == PropertyType.Long)
					{
						int propId = pd.Id;
						long refVal = reader.GetReference(pd.Id, tran);
						AddRefToValidMap(invRefs, obj->id, refVal, propId);
					}
					else if (propType == PropertyType.LongArray)
					{
						int propId = pd.Id;
						long[] refVals = reader.GetReferenceArray(pd.Id, tran);
						if (refVals != null)
						{
							for (int k = 0; k < refVals.Length; k++)
							{
								AddRefToValidMap(invRefs, obj->id, refVals[k], propId);
							}
						}
					}
				}
			}
		}
	}

	private void AddRefToValidMap(Dictionary<ValueTuple<long, int>, List<long>> invRefs, long id, long refVal, int propId)
	{
		if (refVal == 0)
			return;

		ValueTuple<long, int> key = new ValueTuple<long, int>(refVal, propId);

		List<long> l;
		if (!invRefs.TryGetValue(key, out l))
		{
			l = new List<long>(2);
			invRefs.Add(key, l);
		}

		l.Add(id);
	}

	public ClassObject* GetFromObjectPool(ulong handle)
	{
		return (ClassObject*)ObjectStorage.GetBuffer(handle);
	}

	public bool CommitedObjectExistsInDatabase(long id, out ulong commitVersion)
	{
		int lockHandle = resizeCounter.EnterReadLock();

		long bucket = CalculateBucket(id);

		Bucket* bn = buckets + bucket;
		ulong* pbnHandle = Bucket.LockAccess(bn);

		commitVersion = 0;

		try
		{
			ClassObject* obj = FindObject(pbnHandle, id, out ulong* prevObjPointer);

			if (obj != null)
			{
				if (obj->IsDeleted)
					return false;

				commitVersion = obj->version;
				return Database.IsCommited(obj->version);
			}

			return false;
		}
		finally
		{
			Bucket.UnlockAccess(bn);
			resizeCounter.ExitReadLock(lockHandle);
		}
	}

	public ObjectValues[] GetObjectVersions(long id)
	{
		List<IntPtr> l = new List<IntPtr>();

		long bucket = CalculateBucket(id);
		Bucket* bn = buckets + bucket;

		ulong handle = bn->Handle;
		while (handle != 0)
		{
			ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(handle);
			if (obj->id == id)
			{
				ulong vhandle = handle;
				while (vhandle != 0)
				{
					ClassObject* vobj = (ClassObject*)ObjectStorage.GetBuffer(vhandle);
					l.Add((IntPtr)vobj);
					vhandle = vobj->nextVersionHandle;
				}
			}

			handle = obj->nextCollisionHandle;
		}

		ObjectValues[] rs = new ObjectValues[l.Count];
		for (int i = 0; i < l.Count; i++)
		{
			rs[i].Object = (ClassObject*)l[i];

			if (!((ClassObject*)l[i])->IsDeleted)
			{
				object[] vals = new object[ClassDesc.Properties.Length - 2];
				for (int j = 2; j < ClassDesc.Properties.Length; j++)
				{
					PropertyDescriptor pd = ClassDesc.Properties[j];
					byte* addr = (byte*)l[i] + ClassObject.DataOffset + ClassDesc.PropertyByteOffsets[j];
					if (pd.PropertyType == PropertyType.Byte)
						vals[j - 2] = ((byte*)addr)[0];
					else if (pd.PropertyType == PropertyType.Short)
						vals[j - 2] = ((short*)addr)[0];
					else if (pd.PropertyType == PropertyType.Int)
						vals[j - 2] = ((int*)addr)[0];
					else if (pd.PropertyType == PropertyType.Long)
						vals[j - 2] = ((long*)addr)[0];
					else if (pd.PropertyType == PropertyType.Float)
						vals[j - 2] = ((float*)addr)[0];
					else if (pd.PropertyType == PropertyType.Double)
						vals[j - 2] = ((double*)addr)[0];
					else if (pd.PropertyType == PropertyType.Bool)
						vals[j - 2] = ((bool*)addr)[0];
					else if (pd.PropertyType == PropertyType.DateTime)
						vals[j - 2] = DateTime.FromBinary(((long*)addr)[0]);
					else if (pd.PropertyType == PropertyType.String)
						vals[j - 2] = stringStorage.GetString(((ulong*)addr)[0]);
					else
						vals[j - 2] = ((ulong*)addr)[0];
				}

				rs[i].Values = vals;
			}
		}

		return rs;
	}

	internal void FindStrings(ulong shandle, List<string> l)
	{
		if (ClassDesc.StringPropertyIndexes.Length == 0)
			return;

		for (long i = 0; i < capacity; i++)
		{
			Bucket* bn = buckets + i;
			ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(bn->Handle);
			while (obj != null)
			{
				ClassObject* verObj = obj;
				while (verObj != null)
				{
					if (!verObj->IsDeleted)
					{
						for (int j = 0; j < ClassDesc.StringPropertyIndexes.Length; j++)
						{
							int index = ClassDesc.StringPropertyIndexes[j];
							int offset = propertyOffsets[index];
							ulong* up = (ulong*)((byte*)verObj + ClassObject.DataOffset + offset);
							if (*up == shandle)
							{
								l.Add(string.Format("class:{0}, prop:{1}, id:{2}, versions:{3}", ClassDesc.FullName,
									ClassDesc.Properties[index].Name, verObj->id, verObj->version));
							}
						}
					}

					verObj = (ClassObject*)ObjectStorage.GetBuffer(verObj->nextVersionHandle);
				}

				obj = (ClassObject*)ObjectStorage.GetBuffer(obj->nextCollisionHandle);
			}
		}
	}

	public override List<ObjectReader> TestScan(Transaction tran)
	{
		TTTrace.Write(TraceId, ClassDesc.Id);

		int lockHandle = resizeCounter.EnterReadLock();

		List<ObjectReader> l = new List<ObjectReader>();
		for (long i = 0; i < capacity; i++)
		{
			Bucket* bn = buckets + i;
			Bucket.LockAccess(bn);

			ulong handle = bn->Handle;

			while (handle != 0)
			{
				ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(handle);
				handle = obj->nextCollisionHandle;

				while (obj != null)
				{
					if ((obj->version <= tran.ReadVersion || obj->version == tran.Id))
					{
						if (!obj->IsDeleted)
							l.Add(new ObjectReader(ClassObject.ToDataPointer(obj), this));

						break;
					}

					obj = (ClassObject*)ObjectStorage.GetBuffer(obj->nextVersionHandle);
				}

				obj = (ClassObject*)ObjectStorage.GetBuffer(handle);
			}

			Bucket.UnlockAccess(bn);
		}

		resizeCounter.ExitReadLock(lockHandle);

		return l;
	}

	public override long PickRandomObject(Transaction tran, ClassDescriptor classDesc,
		bool includeInherited, Func<long, long> rand, out ObjectReader r)
	{
		ClassObject** lp = stackalloc ClassObject*[8];

		int lockHandle = resizeCounter.EnterReadLock();
		try
		{
			long n = rand(capacity);
			for (int i = 0; i < 100; i++)
			{
				Bucket* bn = buckets + n;
				ulong* pbnHandle = Bucket.LockAccess(bn);

				try
				{
					if (bn->Handle == 0)
					{
						n = (n + 1) % capacity;
						continue;
					}

					int t = 0;
					ulong handle = bn->Handle;
					while (handle != 0 && t < 8)
					{
						ClassObject* obj = (ClassObject*)ObjectStorage.GetBuffer(handle);
						lp[t++] = obj;
						handle = obj->nextCollisionHandle;
					}

					int p = (int)rand(t);
					ClassObject* res = FindVersion(lp[p], tran.ReadVersion, tran.Id);
					if (res == null || res->IsDeleted)
						continue;

					r = new ObjectReader(ClassObject.ToDataPointer(res), this);
					return res->id;
				}
				finally
				{
					Bucket.UnlockAccess(bn);
				}
			}
		}
		finally
		{
			resizeCounter.ExitReadLock(lockHandle);
		}

		r = new ObjectReader();
		return 0;
	}

	internal struct ObjectValues
	{
		public ClassObject* Object { get; set; }
		public object[] Values { get; set; }
	}
}
#endif
