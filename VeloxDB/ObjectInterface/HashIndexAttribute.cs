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
public sealed class HashIndexAttribute : Attribute
{
	string name;
	bool isUnique;
	ReadOnlyArray<string> properties;

	/// <param name="name">Hash index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// <param name="properties">Names of the properties that hash index should include.</param>
	public HashIndexAttribute(string name, bool isUnique, params string[] properties)
	{
		this.name = name;
		this.isUnique = isUnique;
		this.properties = new ReadOnlyArray<string>(properties, true);
	}

	/// <summary>
	/// Gets the name.
	/// </summary>
	public string Name => name;

	/// <summary>
	/// Gets if hash index has unique constraint.
	/// </summary>
	public bool IsUnique => isUnique;
	internal ReadOnlyArray<string> Properties => properties;
}
