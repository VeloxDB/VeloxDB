using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

internal unsafe sealed partial class InheritedClass
{
	public ClassBase[] InheritedClasses => inheritedClasses;

#if TEST_BUILD
	public override List<ObjectReader> TestScan(Transaction tran)
	{
		List<ObjectReader> l = new List<ObjectReader>();
		if (MainClass != null)
			l.AddRange(MainClass.TestScan(tran));

		for (int i = 0; i < inheritedClasses.Length; i++)
		{
			l.AddRange(inheritedClasses[i].TestScan(tran));
		}

		return l;
	}

	public override long PickRandomObject(Transaction tran, ClassDescriptor classDesc,
		bool includeInherited, Func<long, long> rand, out ObjectReader r)
	{
		if (!includeInherited)
			return MainClass.PickRandomObject(tran, classDesc, includeInherited, rand, out r);

		r = new ObjectReader();

		long c = 0;
		c += MainClass != null ? MainClass.EstimatedObjectCount : 0;
		for (int i = 0; i < inheritedClasses.Length; i++)
		{
			c += inheritedClasses[i].MainClass != null ? inheritedClasses[i].MainClass.EstimatedObjectCount : 0;
		}

		if (c == 0)
			return 0;

		long n = rand(c);

		for (int i = 0; i < 10; i++)
		{
			long id = 0;
			if (MainClass != null)
			{
				long t = MainClass.EstimatedObjectCount;
				if (n < t)
					id = MainClass.PickRandomObject(tran, classDesc, includeInherited, rand, out r);
				else
					n -= t;
			}

			if (id != 0)
			{
				for (int j = 0; j < inheritedClasses.Length; j++)
				{
					if (inheritedClasses[j].MainClass != null)
					{
						long t = inheritedClasses[j].MainClass.EstimatedObjectCount;
						if (n < t)
						{
							id = inheritedClasses[j].MainClass.PickRandomObject(tran, classDesc, includeInherited, rand, out r);
							break;
						}
						else
						{
							n -= t;
						}
					}
				}
			}

			if (id != 0)
				return id;
		}

		return 0;
	}

#endif
}
