using System;
using System.Collections.Generic;
using System.Linq;
using VeloxDB.Common;
using VeloxDB.Descriptor;
using VeloxDB.Storage.ModelUpdate;

namespace VeloxDB.Storage.Persistence;

internal sealed class PersistenceUpdate
{
	PersistenceDescriptor prevPersistenceDesc;
	PersistenceDescriptor persistenceDesc;
	bool isRecreationRequired;
	ReadOnlyArray<string> updatedLogs;

	public PersistenceUpdate(PersistenceDescriptor prevPersistenceDesc, PersistenceDescriptor persistenceDesc, DataModelUpdate modelUpdate)
	{
		this.prevPersistenceDesc = prevPersistenceDesc;
		this.persistenceDesc = persistenceDesc ?? prevPersistenceDesc.Clone();
		updatedLogs = ReadOnlyArray<string>.Empty;
		DetectLogDiff(modelUpdate);
	}

	public bool IsRecreationRequired => isRecreationRequired;
	public PersistenceDescriptor PrevPersistenceDescriptor => prevPersistenceDesc;
	public PersistenceDescriptor PersistenceDescriptor => persistenceDesc;
	public ReadOnlyArray<string> UpdatedLogs => updatedLogs;

	private bool LogRequiresRecreation(LogDescriptor logDesc1, LogDescriptor logDesc2)
	{
		return logDesc1.IsPackedFormat != logDesc2.IsPackedFormat ||
			!logDesc1.Directory.Equals(logDesc2.Directory, StringComparison.OrdinalIgnoreCase) ||
			!logDesc1.SnapshotDirectory.Equals(logDesc2.SnapshotDirectory, StringComparison.OrdinalIgnoreCase);
	}

	private bool LogUpdated(LogDescriptor logDesc1, LogDescriptor logDesc2)
	{
		return logDesc1.MaxSize != logDesc2.MaxSize;
	}

	private bool HasClassesWithModifiedLog(DataModelUpdate modelUpdate)
	{
		if (modelUpdate == null)
			return false;

		foreach (ClassUpdate cu in modelUpdate.UpdatedClasses.Values)
		{
			if (cu.IsLogModified)
				return true;
		}

		return false;
	}

	private void DetectLogDiff(DataModelUpdate modelUpdate)
	{
		if (modelUpdate != null && HasClassesWithModifiedLog(modelUpdate))
		{
			isRecreationRequired = true;
		}

		if (persistenceDesc == null && prevPersistenceDesc == null)
			return;

		if (persistenceDesc != null && prevPersistenceDesc == null)
		{
			isRecreationRequired = true;
			return;
		}

		List<string> updated = null;
		foreach (LogDescriptor logDesc in persistenceDesc.LogDescriptors)
		{
			LogDescriptor prevLogDesc = prevPersistenceDesc.LogDescriptors.FirstOrDefault(x => x.Name.Equals(logDesc.Name));
			if (prevLogDesc == null)
			{
				isRecreationRequired = true;
				return;
			}
			else
			{
				if (LogRequiresRecreation(prevLogDesc, logDesc))
				{
					isRecreationRequired = true;
					return;
				}
				else if (LogUpdated(prevLogDesc, logDesc))
				{
					updated ??= new List<string>();
					updated.Add(logDesc.Name);
				}
			}
		}

		foreach (LogDescriptor prevLogDesc in prevPersistenceDesc.LogDescriptors)
		{
			LogDescriptor logDesc = persistenceDesc.LogDescriptors.FirstOrDefault(x => x.Name.Equals(prevLogDesc.Name));
			if (logDesc == null)
			{
				isRecreationRequired = true;
				return;
			}
		}

		updatedLogs = ReadOnlyArray<string>.FromNullable(updated);
	}

	public void MarkDirectoriesAsTemp()
	{
		for (int i = 0; i < persistenceDesc.LogDescriptors.Length; i++)
		{
			persistenceDesc.LogDescriptors[i].MarkDirectoriesAsTemp();
		}
	}

	public void UnmarkDirectoriesAsTemp()
	{
		for (int i = 0; i < persistenceDesc.LogDescriptors.Length; i++)
		{
			persistenceDesc.LogDescriptors[i].UnmarkDirectoriesAsTemp();
		}
	}
}
