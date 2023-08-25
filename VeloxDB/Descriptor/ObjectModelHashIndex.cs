using System;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal sealed class ObjectModelHashIndex : ObjectModelIndex
{
	public ObjectModelHashIndex(Type definingType, short id, string fullName, string cultureName, bool caseSensitive,
		bool isUnique, ReadOnlyArray<string> properties) :
		base(definingType, id, fullName, cultureName, caseSensitive, isUnique, properties)
	{
	}
}
