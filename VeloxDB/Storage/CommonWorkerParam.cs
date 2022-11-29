using System;
using Velox.Storage.Persistence;

namespace Velox.Storage;

internal struct CommonWorkerParam
{
	public object ReferenceParam { get; set; }
	public DatabaseRestorer.LogWorkerItem LogWorkerItem { get; set; }
}
