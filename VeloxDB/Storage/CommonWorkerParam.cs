using System;
using VeloxDB.Storage.Persistence;

namespace VeloxDB.Storage;

internal struct CommonWorkerParam
{
	public object ReferenceParam { get; set; }
	public DatabaseRestorer.LogWorkerItem LogWorkerItem { get; set; }
}
