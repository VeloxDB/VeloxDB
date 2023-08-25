using System;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

internal sealed class ObjectModelSortedIndex : ObjectModelIndex
{
	ReadOnlyArray<SortOrder> sortOrders;

	public ObjectModelSortedIndex(Type definingType, short id, string fullName, string cultureName, bool caseSensitive,
		bool isUnique, ReadOnlyArray<string> properties, ReadOnlyArray<SortOrder> sortOrders) :
		base(definingType, id, fullName, cultureName, caseSensitive, isUnique, properties)
	{
		this.sortOrders = sortOrders;
	}

	public ReadOnlyArray<SortOrder> SortOrders => sortOrders;
}
