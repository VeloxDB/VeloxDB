using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Velox.Common;
using Velox.ObjectInterface;

namespace Velox.Descriptor;

internal sealed class ObjectModelClass
{
	short id;
	bool isAbstract;
	Type classType;
    string logName;
	Dictionary<string, ObjectModelProperty> properties;
	List<ObjectModelInverseReferenceProperty> inverseReferences;
	List<ObjectModelHashIndex> hashIndexes;

	public ObjectModelClass(short id, bool isAbstract, Type classType, string logName,
        IEnumerable<ObjectModelProperty> properties, IEnumerable<ObjectModelInverseReferenceProperty> inverseReferences,
		IEnumerable<ObjectModelHashIndex> hashIndexes)
	{
		this.id = id;
		this.isAbstract = isAbstract;
		this.classType = classType;
        this.logName = logName;
		this.inverseReferences = new List<ObjectModelInverseReferenceProperty>(inverseReferences);
		this.properties = new Dictionary<string, ObjectModelProperty>(properties.Count());
		this.hashIndexes = new List<ObjectModelHashIndex>(hashIndexes);
		foreach (ObjectModelProperty prop in properties)
		{
			this.properties.Add(prop.PropertyInfo.Name, prop);
		}
	}

	public short Id => id;
	public Type ClassType => classType;
	public bool IsAbstract => isAbstract;
	public IEnumerable<ObjectModelProperty> Properties => properties.Values;
	public List<ObjectModelInverseReferenceProperty> InverseReferences => inverseReferences;
	public List<ObjectModelHashIndex> HashIndexes => hashIndexes;
	public string LogName => logName;

	public ObjectModelProperty GetProperty(string name)
	{
		properties.TryGetValue(name, out ObjectModelProperty p);
		return p;
	}
}

internal class ObjectModelProperty
{
	PropertyInfo propertyInfo;
	PropertyType propertyType;
	int id;
	string defaultValue;

	public ObjectModelProperty(PropertyInfo propertyInfo, int id, string defaultValue)
	{
		this.propertyInfo = propertyInfo;
		this.id = id;
		this.defaultValue = defaultValue;

		propertyType = PropertyTypesHelper.ManagedTypeToPropertyType(propertyInfo.PropertyType);
		if (propertyType == PropertyType.None)
			propertyType = DatabaseArray.ManagedToPropertyType(propertyInfo.PropertyType);
	}

	public PropertyInfo PropertyInfo => propertyInfo;
	public int Id => id;
	public string DefaultValue => defaultValue;
	public virtual PropertyType PropertyType => propertyType;
}

internal sealed class ReferenceObjectModelProperty : ObjectModelProperty
{
	bool isNullable;
	DeleteTargetAction deleteTargetAction;
	bool trackInverseReferences;
	Type referencedType;
	bool isArray;

	public ReferenceObjectModelProperty(PropertyInfo propertyInfo, int id, string defaultValue, bool isNullable,
		DeleteTargetAction deleteTargetAction, bool trackInverseReferences) :
		base(propertyInfo, id, defaultValue)
	{
		this.isNullable = isNullable;
		this.deleteTargetAction = deleteTargetAction;
		this.trackInverseReferences = trackInverseReferences;
		isArray = false;
		referencedType = propertyInfo.PropertyType;

		Type propType = propertyInfo.PropertyType;
		if (propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(ReferenceArray<>))
		{
			propType = propType.GetGenericArguments()[0];
			isArray = true;
		}

		referencedType = propType;

		if (!typeof(DatabaseObject).IsAssignableFrom(propType) || !propType.IsDefined(typeof(DatabaseClassAttribute)))
		{
			Throw.ReferencePropertyReferencesInvalidClass(propertyInfo.DeclaringType.FullName, propertyInfo.Name, propType.FullName);
		}
	}

	public bool IsNullable => isNullable;
	public DeleteTargetAction DeleteTargetAction => deleteTargetAction;
	public bool TrackInverseReferences => trackInverseReferences;
	public Type ReferencedType => referencedType;
	public bool IsArray => isArray;
	public override PropertyType PropertyType => isArray ? PropertyType.LongArray : PropertyType.Long;
}

internal sealed class ObjectModelInverseReferenceProperty
{
	PropertyInfo propertyInfo;
	Type referencingClass;
	string targetPropertyName;

	public ObjectModelInverseReferenceProperty(PropertyInfo propertyInfo, string targetPropertyName)
	{
		this.propertyInfo = propertyInfo;
		this.referencingClass = propertyInfo.PropertyType.GetGenericArguments()[0];
		this.targetPropertyName = targetPropertyName;
	}

	public Type ReferencingClass => referencingClass;
	public string TargetPropertyName => targetPropertyName;
	public PropertyInfo PropertyInfo => propertyInfo;
}
