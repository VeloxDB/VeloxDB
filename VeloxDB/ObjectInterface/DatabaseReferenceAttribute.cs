using System;
using Velox.Descriptor;

namespace Velox.ObjectInterface;

/// <summary>
/// Specifies that the property is a database reference.
/// </summary>
/// <seealso cref="DatabasePropertyAttribute"/>
/// <seealso cref="InverseReferencesAttribute"/>
[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class DatabaseReferenceAttribute : Attribute
{
	bool isNullable;
	DeleteTargetAction deleteTargetAction;
	bool trackInverseReferences;

	/// <param name="isNullable">Specifies whether the reference can be null. Default is `true`.</param>
	/// <param name="deleteTargetAction">
	/// Indicates what to do with the object when the referenced object is deleted.
	/// Default is `DeleteTargetAction.PreventDelete`.
	/// </param>
	/// <param name="trackInverseReferences">
	/// Specifies if the database should track inverse references. Default is `true`. For more information see <see cref="InverseReferencesAttribute"/>.
	/// </param>
	public DatabaseReferenceAttribute(bool isNullable = true,
		DeleteTargetAction deleteTargetAction = DeleteTargetAction.PreventDelete, bool trackInverseReferences = true)
	{
		this.isNullable = isNullable;
		this.deleteTargetAction = deleteTargetAction;
		this.trackInverseReferences = trackInverseReferences;
	}

	/// <summary>
	/// Gets if the reference is nullable.
	/// </summary>
	public bool IsNullable => isNullable;

	/// <summary>
	/// Gets if tracking of inverse references is enabled.
	/// </summary>
	public bool TrackInverseReferences => trackInverseReferences;

	/// <summary>
	/// Gets what happens when referenced object is deleted.
	/// </summary>
	public DeleteTargetAction DeleteTargetAction => deleteTargetAction;
}
