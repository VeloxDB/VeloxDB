using System;

namespace VeloxDB.Storage;

internal enum TransactionSource : byte
{
	Client = 1,
	Replication = 2,
	Internal = 3
}
