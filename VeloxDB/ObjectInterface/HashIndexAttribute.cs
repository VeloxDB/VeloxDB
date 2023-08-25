using System;
using VeloxDB.Common;

namespace VeloxDB.ObjectInterface;

/// <summary>
/// Apply this attribute to a <see cref="DatabaseObject"/> class to define a hash index.
/// </summary>
/// <seealso href="../articles/guide/data_model.html#hash-indexes">VeloxDB The definitive guide: Hash indexes</seealso>
/// <seealso cref="HashIndexReader{T, TKey1}"/>
/// <seealso cref="HashIndexReader{T, TKey1, TKey2}"/>
/// <seealso cref="HashIndexReader{T, TKey1, TKey2}"/>
/// <seealso cref="HashIndexReader{T, TKey1, TKey2, TKey3}"/>
/// <seealso cref="HashIndexReader{T, TKey1, TKey2, TKey3, TKey4}"/>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
public sealed class HashIndexAttribute : IndexAttribute
{
	ReadOnlyArray<string> properties;

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// <param name="properties">Names of the properties that hash index should include.</param>
	public HashIndexAttribute(string name, bool isUnique, params string[] properties) :
		base(name, null, true, isUnique)
	{
		this.properties = new ReadOnlyArray<string>(properties, true);
	}

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// <param name="properties">Names of the properties that hash index should include.</param>
	/// <param name="caseSensitive">Indicates whether string comparisons inside the index are case sensitive.</param>
	/// <param name="cultureName">The name of the culture to use to compare strings inside the index.</param>
	public HashIndexAttribute(string name, string cultureName, bool caseSensitive, bool isUnique, params string[] properties) :
		base(name, cultureName, caseSensitive, isUnique)
	{
		this.properties = new ReadOnlyArray<string>(properties, true);
	}

	internal ReadOnlyArray<string> Properties => properties;
}
