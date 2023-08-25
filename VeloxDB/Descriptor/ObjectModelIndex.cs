using System;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal abstract class ObjectModelIndex
{
	short id;
	string fullName;
	string cultureName;
	bool caseSensitive;
	bool isUnique;
	ReadOnlyArray<string> properties;
	Type definingType;

	public ObjectModelIndex(Type definingType, short id, string fullName, string cultureName, bool caseSensitive,
		bool isUnique, ReadOnlyArray<string> properties)
	{
		this.definingType = definingType;
		this.id = id;
		this.fullName = fullName;
		this.cultureName = cultureName;
		this.caseSensitive = caseSensitive;
		this.isUnique = isUnique;
		this.properties = properties;
	}

	public short Id => id;
	public string FullName => fullName;
	public string Name => fullName.Substring(fullName.LastIndexOf('.') + 1);
	public string CultureName => cultureName;
	public bool CaseSensitive => caseSensitive;
	public bool IsUnique => isUnique;
	public ReadOnlyArray<string> Properties => properties;
	public Type DefiningType => definingType;
}
