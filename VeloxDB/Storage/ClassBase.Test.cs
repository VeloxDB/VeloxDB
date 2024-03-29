using System;
using System.Collections.Generic;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.Storage;

#if TEST_BUILD
internal unsafe abstract partial class ClassBase
{
	public abstract List<ObjectReader> TestScan(Transaction tran);

	public abstract long PickRandomObject(Transaction tran, ClassDescriptor classDesc,
		bool includeInherited, Func<long, long> rand, out ObjectReader r);
}
#endif
