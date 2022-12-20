using System;
using System.Collections.Generic;
using VeloxDB.Common;

namespace VeloxDB.Storage.ModelUpdate;

internal sealed class AssemblyUpdate
{
	List<UserAssembly> inserted;
	List<UserAssembly> updated;
	List<long> deleted;
	SimpleGuid previousAssemblyVersionGuid;

	public AssemblyUpdate()
	{
		inserted = new List<UserAssembly>();
		updated = new List<UserAssembly>();
		deleted = new List<long>();
		previousAssemblyVersionGuid = SimpleGuid.Zero;
	}

	public AssemblyUpdate(SimpleGuid previousAssemblyVersionGuid)
	{
		inserted = new List<UserAssembly>();
		updated = new List<UserAssembly>();
		deleted = new List<long>();
		this.previousAssemblyVersionGuid = previousAssemblyVersionGuid;
	}

	public AssemblyUpdate(List<UserAssembly> inserted, List<UserAssembly> updated,
		List<long> deleted, SimpleGuid previousAssemblyVersionGuid)
	{
		this.inserted = inserted;
		this.updated = updated;
		this.deleted = deleted;
		this.previousAssemblyVersionGuid = previousAssemblyVersionGuid;
	}

	public List<long> Deleted => deleted;
	public List<UserAssembly> Inserted => inserted;
	public List<UserAssembly> Updated => updated;
	public SimpleGuid PreviousAssemblyVersionGuid => previousAssemblyVersionGuid;
}

internal sealed class UserAssembly
{
	long id;
	string name;
	byte[] binary;

	public UserAssembly(long id, string name, byte[] binary)
	{
		this.id = id;
		this.name = name;
		this.binary = binary;
	}

	public string Name => name;
	public byte[] Binary => binary;
	public long Id => id;
}
