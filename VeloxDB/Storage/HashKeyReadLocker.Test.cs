using System;
using System.Collections.Generic;
using System.Text;
using VeloxDB.Common;

namespace VeloxDB.Storage;

#if TEST_BUILD
internal unsafe sealed partial class HashKeyReadLocker
{
	public void ValidateAndCollectBlobs(ulong readVersion, Dictionary<ulong, int> strings)
	{
		if (resizeCounter.Count != 0)
			throw new InvalidOperationException();

		for (int i = 0; i < capacity; i++)
		{
			Bucket* bn = buckets + i;
			if (bn->Handle != 0)
				throw new InvalidOperationException();
		}
	}

	internal void FindStrings(ulong shandle, List<string> l)
	{
		Dictionary<ulong, int> hm = new Dictionary<ulong, int>(4);
		for (int i = 0; i < capacity; i++)
		{
			Bucket* bn = buckets + i;

			ulong handle = bn->Handle;
			while (handle != 0)
			{
				HashLockerItem* brec = (HashLockerItem*)memoryManager.GetBuffer(handle);
				byte* key = HashLockerItem.GetKey(brec);
				comparer.CollectKeyStrings(key, hm);

				foreach (KeyValuePair<ulong, int> kv in hm)
				{
					if (kv.Key == shandle)
					{
						StringBuilder sb = new StringBuilder();
						for (int j = 0; j < hashIndexDesc.KeySize; j++)
						{
							sb.AppendFormat("{0}, ", key[j]);
						}

						if (sb.Length >= 2)
							sb.Length -= 2;

						l.Add(string.Format("HashIndexLocker:{0}, bucket={1}, count:{2}, key:{3}, item:{4}", hashIndexDesc.Name, i, kv.Value, sb.ToString(), handle));
					}
				}

				hm.Clear();
				handle = brec->nextCollision;
			}
		}
	}
}
#endif
