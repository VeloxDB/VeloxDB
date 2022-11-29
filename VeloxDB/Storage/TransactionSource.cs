using System;

namespace Velox.Storage;

internal enum TransactionSource : byte
{
	Client = 1,
	Replication = 2,
	Internal = 3
}
