using System;
using System.Collections.Generic;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.Storage;

#if TEST_BUILD
internal unsafe abstract partial class ClassBase
{
	public abstract List<ObjectReader> TestScan(Transaction tran);

	public abstract long PickRandomObject(Transaction tran, ClassDescriptor classDesc,
		bool includeInherited, Func<long, long> rand, out ObjectReader r);
}
#endif
