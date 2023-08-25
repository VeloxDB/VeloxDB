using System;
using System.Diagnostics;
using System.IO;
using System.Xml;
using VeloxDB.Common;

namespace VeloxDB.Descriptor;

/// <summary>
/// Sort order of the indexed property.
/// </summary>
public enum SortOrder
{
	/// <summary>
	/// Ascending sort order.
	/// </summary>
	Asc = 1,

	/// <summary>
	/// Descending sort order.
	/// </summary>
	Desc = 2
}

internal sealed class SortedIndexDescriptor : IndexDescriptor
{
	ReadOnlyArray<SortOrder> propertySortOrder;

	public SortedIndexDescriptor() :
		base()
	{
	}

	public SortedIndexDescriptor(NamespaceDescriptor @namespace, ObjectModelSortedIndex objectModelSortedIndex) :
		base(@namespace, objectModelSortedIndex)
	{
		propertySortOrder = objectModelSortedIndex.SortOrders;
	}

	public SortedIndexDescriptor(XmlReader reader, NamespaceDescriptor metaNamespace) :
		base(reader, metaNamespace)
	{
	}

	public SortedIndexDescriptor(DataModelDescriptor modelDesc, string name, short id,
		string cultureName, bool caseSensitive, bool isUnique, string[] propertyNames, SortOrder[] sortOrders) :
		base(modelDesc, name, id, cultureName, caseSensitive, isUnique, propertyNames)
	{
		Checker.AssertTrue(propertyNames.Length == sortOrders.Length);
		this.propertySortOrder = new ReadOnlyArray<SortOrder>(sortOrders);
	}

	protected override void OnCreated()
	{
		propertySortOrder ??= ReadOnlyArray<SortOrder>.Empty;
	}

	protected override void OnPropertyLoaded(XmlReader reader)
	{
		SortOrder[] so = new SortOrder[propertySortOrder.Length + 1];
		for (int i = 0; i < propertySortOrder.Length; i++)
		{
			so[i] = propertySortOrder[i];
		}

		so[so.Length - 1] = SortOrder.Asc;
		string value = reader.GetAttribute("Order");
		if (value != null)
			so[so.Length - 1] = value.Equals("Asc", StringComparison.OrdinalIgnoreCase) ? SortOrder.Asc : SortOrder.Desc;

		propertySortOrder = new ReadOnlyArray<SortOrder>(so);
	}

	public override ModelItemType Type => ModelItemType.SortedIndex;
	public ReadOnlyArray<SortOrder> PropertySortOrder => propertySortOrder;

	protected override void OnSerialize(BinaryWriter writer, ModelDescriptorSerializerContext context)
	{
		writer.Write(propertySortOrder.Length);
		for (int i = 0; i < propertySortOrder.Length; i++)
		{
			writer.Write((byte)propertySortOrder[i]);
		}
	}

	protected override void OnDeserialize(BinaryReader reader, ModelDescriptorDeserializerContext context)
	{
		int c = reader.ReadInt32();
		SortOrder[] so = new SortOrder[c];
		for (int i = 0; i < c; i++)
		{
			so[i] = (SortOrder)reader.ReadByte();
		}

		propertySortOrder = new ReadOnlyArray<SortOrder>(so);
	}

	public override string ToString()
	{
		return $"(SortedIndex {Name})";
	}
}
