using System;
using System.Linq;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.ObjectInterface;

internal struct SortedProperty
{
	public string Name { get; private set; }
	public SortOrder Order { get; private set; }

	public SortedProperty(string name, SortOrder order)
	{
		this.Name = name;
		this.Order = order;
	}

	public static implicit operator SortedProperty(Tuple<string, SortOrder> t)
	{
		return new SortedProperty(t.Item1, t.Item2);
	}
}

/// <summary>
/// Apply this attribute to a <see cref="DatabaseObject"/> class to define a sorted index.
/// </summary>
/// <seealso href="../articles/guide/data_model.html#hash-indexes">VeloxDB The definitive guide: Sorted indexes</seealso>
/// <seealso cref="SortedIndexReader{T, TKey1}"/>
/// <seealso cref="SortedIndexReader{T, TKey1, TKey2}"/>
/// <seealso cref="SortedIndexReader{T, TKey1, TKey2}"/>
/// <seealso cref="SortedIndexReader{T, TKey1, TKey2, TKey3}"/>
/// <seealso cref="SortedIndexReader{T, TKey1, TKey2, TKey3, TKey4}"/>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
public sealed class SortedIndexAttribute : IndexAttribute
{
	SortedProperty[] properties;

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// <param name="properties">Names of the properties that hash index should include.</param>
	public SortedIndexAttribute(string name, bool isUnique, params string[] properties) :
		this(name, null, true, isUnique, properties)
	{
	}

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// <param name="properties">Names of the properties that hash index should include.</param>
	/// /// <param name="caseSensitive">Indicates whether string comparisons inside the index are case sensitive.</param>
	/// <param name="cultureName">The name of the culture to use to compare strings inside the index.</param>
	public SortedIndexAttribute(string name, string cultureName, bool caseSensitive, bool isUnique, params string[] properties) :
		base(name, cultureName, caseSensitive, isUnique)
	{
		this.properties = properties.Select(x => new SortedProperty(x, SortOrder.Asc)).ToArray();
	}

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// <param name="name1">Name of the key property.</param>
	/// <param name="sortOrder1">Sort order of the key property.</param>
	public SortedIndexAttribute(string name, bool isUnique, string name1, SortOrder sortOrder1) :
		this(name, null, true, isUnique, name1, sortOrder1)
	{
	}

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// /// <param name="caseSensitive">Indicates whether string comparisons inside the index are case sensitive.</param>
	/// <param name="cultureName">The name of the culture to use to compare strings inside the index.</param>
	/// /// <param name="name1">Name of the key property.</param>
	/// <param name="sortOrder1">Sort order of the key property.</param>
	public SortedIndexAttribute(string name, string cultureName, bool caseSensitive, bool isUnique, string name1, SortOrder sortOrder1) :
		this(name, cultureName, caseSensitive, isUnique, new SortedProperty[] { new SortedProperty(name1, sortOrder1) })
	{
	}

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// /// <param name="name1">Name of the first key property.</param>
	/// <param name="sortOrder1">Sort order of the first key property.</param>
	/// <param name="name2">Name of the second key property.</param>
	/// <param name="sortOrder2">Sort order of the second key property.</param>
	public SortedIndexAttribute(string name, bool isUnique, string name1, SortOrder sortOrder1, string name2, SortOrder sortOrder2) :
		this(name, null, true, isUnique, name1, sortOrder1, name2, sortOrder2)
	{
	}

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// /// <param name="caseSensitive">Indicates whether string comparisons inside the index are case sensitive.</param>
	/// <param name="cultureName">The name of the culture to use to compare strings inside the index.</param>
	/// /// <param name="name1">Name of the first key property.</param>
	/// <param name="sortOrder1">Sort order of the first key property.</param>
	/// <param name="name2">Name of the second key property.</param>
	/// <param name="sortOrder2">Sort order of the second key property.</param>
	public SortedIndexAttribute(string name, string cultureName, bool caseSensitive, bool isUnique,
		string name1, SortOrder sortOrder1, string name2, SortOrder sortOrder2) :
		this(name, cultureName, caseSensitive, isUnique,
			new SortedProperty[] { new SortedProperty(name1, sortOrder1), new SortedProperty(name2, sortOrder2) })
	{
	}

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// /// <param name="name1">Name of the first key property.</param>
	/// <param name="sortOrder1">Sort order of the first key property.</param>
	/// <param name="name2">Name of the second key property.</param>
	/// <param name="sortOrder2">Sort order of the second key property.</param>
	/// <param name="name3">Name of the third key property.</param>
	/// <param name="sortOrder3">Sort order of the third key property.</param>
	public SortedIndexAttribute(string name, bool isUnique, string name1, SortOrder sortOrder1,
		string name2, SortOrder sortOrder2, string name3, SortOrder sortOrder3) :
		this(name, null, true, isUnique, name1, sortOrder1, name2, sortOrder2, name3, sortOrder3)
	{
	}

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// /// <param name="caseSensitive">Indicates whether string comparisons inside the index are case sensitive.</param>
	/// <param name="cultureName">The name of the culture to use to compare strings inside the index.</param>
	/// <param name="name1">Name of the first key property.</param>
	/// <param name="sortOrder1">Sort order of the first key property.</param>
	/// <param name="name2">Name of the second key property.</param>
	/// <param name="sortOrder2">Sort order of the second key property.</param>
	/// <param name="name3">Name of the third key property.</param>
	/// <param name="sortOrder3">Sort order of the third key property.</param>
	public SortedIndexAttribute(string name, string cultureName, bool caseSensitive, bool isUnique,
		string name1, SortOrder sortOrder1, string name2, SortOrder sortOrder2, string name3, SortOrder sortOrder3) :
		this(name, cultureName, caseSensitive, isUnique,
			new SortedProperty[] { new SortedProperty(name1, sortOrder1), new SortedProperty(name2, sortOrder2),
				new SortedProperty(name3, sortOrder3) })
	{
	}

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// <param name="name1">Name of the first key property.</param>
	/// <param name="sortOrder1">Sort order of the first key property.</param>
	/// <param name="name2">Name of the second key property.</param>
	/// <param name="sortOrder2">Sort order of the second key property.</param>
	/// <param name="name3">Name of the third key property.</param>
	/// <param name="sortOrder3">Sort order of the third key property.</param>
	/// <param name="name4">Name of the fourth key property.</param>
	/// <param name="sortOrder4">Sort order of the fourth key property.</param>
	public SortedIndexAttribute(string name, bool isUnique, string name1, SortOrder sortOrder1,
		string name2, SortOrder sortOrder2, string name3, SortOrder sortOrder3, string name4, SortOrder sortOrder4) :
		this(name, null, true, isUnique, name1, sortOrder1, name2, sortOrder2, name3, sortOrder3, name4, sortOrder4)
	{
	}

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// /// <param name="caseSensitive">Indicates whether string comparisons inside the index are case sensitive.</param>
	/// <param name="cultureName">The name of the culture to use to compare strings inside the index.</param>
	/// <param name="name1">Name of the first key property.</param>
	/// <param name="sortOrder1">Sort order of the first key property.</param>
	/// <param name="name2">Name of the second key property.</param>
	/// <param name="sortOrder2">Sort order of the second key property.</param>
	/// <param name="name3">Name of the third key property.</param>
	/// <param name="sortOrder3">Sort order of the third key property.</param>
	/// <param name="name4">Name of the fourth key property.</param>
	/// <param name="sortOrder4">Sort order of the fourth key property.</param>
	public SortedIndexAttribute(string name, string cultureName, bool caseSensitive, bool isUnique,
		string name1, SortOrder sortOrder1, string name2, SortOrder sortOrder2, string name3, SortOrder sortOrder3,
		string name4, SortOrder sortOrder4) :
		this(name, cultureName, caseSensitive, isUnique,
			new SortedProperty[] { new SortedProperty(name1, sortOrder1), new SortedProperty(name2, sortOrder2),
				new SortedProperty(name3, sortOrder3), new SortedProperty(name4, sortOrder4) })
	{
	}

	internal SortedIndexAttribute(string name, string cultureName, bool caseSensitive, bool isUnique, params SortedProperty[] properties) :
		base(name, cultureName, caseSensitive, isUnique)
	{
		this.properties = properties;
	}

	internal SortedProperty[] Properties => properties;
}
