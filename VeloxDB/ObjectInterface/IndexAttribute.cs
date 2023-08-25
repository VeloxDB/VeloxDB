using System;
using VeloxDB.Common;

namespace VeloxDB.ObjectInterface;

/// <summary>
/// Base class for index attributes supported by the database.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
public abstract class IndexAttribute : Attribute
{
	string name;
	string cultureName;
	bool caseSensitive;
	bool isUnique;

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// <param name="properties">Names of the properties that hash index should include.</param>
	public IndexAttribute(string name, bool isUnique, params string[] properties) :
		this(name, null, true, isUnique)
	{
	}

	/// <param name="name">Index's name</param>
	/// <param name="isUnique">If true, VeloxDB will enforce hash index uniqueness.</param>
	/// <param name="caseSensitive">Indicates whether string comparisons inside the index are case sensitive.</param>
	/// <param name="cultureName">The name of the culture to use to compare strings inside the index.</param>
	public IndexAttribute(string name, string cultureName, bool caseSensitive, bool isUnique)
	{
		this.name = name;
		this.cultureName = cultureName;
		this.caseSensitive = caseSensitive;
		this.isUnique = isUnique;
	}

	/// <summary>
	/// Gets the name.
	/// </summary>
	public string Name => name;

	/// <summary>
	/// Name of the culture used to compare strings in the index.
	/// </summary>
	public string CultureName => cultureName;

	/// <summary>
	/// Indicates whether string comparisons inside the index are case sensitive.
	/// </summary>
	public bool CaseSensitive => caseSensitive;

	/// <summary>
	/// Gets if hash index has unique constraint.
	/// </summary>
	public bool IsUnique => isUnique;
}
