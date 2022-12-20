using System;
using System.Collections.Generic;
using System.IO;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal abstract class ModelSettings
{
	public abstract DataModelDescriptor CreateModel(PersistenceSettings persistenceSettings, DataModelDescriptor previousModel);
    public abstract DataModelDescriptor CreateModel(DataModelDescriptor previousModel);
}
