using System;
using Velox.Common;

namespace Velox.Descriptor;

internal sealed class ObjectModelHashIndex
{
	short id;
	string fullName;
	bool isUnique;
	ReadOnlyArray<string> properties;
	Type definingType;

	public ObjectModelHashIndex(Type definingType, short id, string fullName, bool isUnique, ReadOnlyArray<string> properties)
	{
		this.definingType = definingType;
		this.id = id;
		this.fullName = fullName;
		this.isUnique = isUnique;
		this.properties = properties;
	}

	public short Id => id;
	public string FullName => fullName;
	public string Name => fullName.Substring(fullName.LastIndexOf('.') + 1);
	public bool IsUnique => isUnique;
	public ReadOnlyArray<string> Properties => properties;
	public Type DefiningType => definingType;
}
