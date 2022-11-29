using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.ObjectInterface;

internal sealed class ObjectModelSettings : ModelSettings
{
	HashSet<Type> classTypes = new HashSet<Type>(128, ReferenceEqualityComparer<Type>.Instance);

	bool usedUp;

	public ObjectModelSettings()
	{
	}

	public void AddAssembly(Assembly assembly)
	{
		if (usedUp)
			throw new InvalidOperationException("Additional classes are not allowed. Meta model has already been instantiated.");

		foreach (Type classType in assembly.GetExportedTypes())
		{
			if (classType.IsDefined(typeof(DatabaseClassAttribute)))
				AddClass(classType);
		}
	}

	private void AddClass(Type classType)
	{
		if (classTypes.Contains(classType))
			return;

		ValidateClass(classType);

		classTypes.Add(classType);

		Type baseType = classType.BaseType;
		if (baseType != typeof(DatabaseObject))
		{
			if (!baseType.IsDefined(typeof(DatabaseClassAttribute)))
				Throw.MissingAttribute(classType.FullName);

			AddClass(baseType);
		}
	}

	public override DataModelDescriptor CreateModel(PersistenceSettings persistenceSettings, DataModelDescriptor previousModel)
	{
		return CreateModelInternal(persistenceSettings, previousModel);
	}

	public override DataModelDescriptor CreateModel(DataModelDescriptor previousModel)
	{
		return CreateModelInternal(null, previousModel);
	}

	public Dictionary<short, ObjectModelClass> CreateObjectModelClasses(DataModelDescriptor previousModel)
	{
		return CreateObjectModelClasses(previousModel, out _, out _, out _).Values.ToDictionary(x => x.Id);
	}

	public Dictionary<Type, ObjectModelClass> CreateObjectModelClasses(DataModelDescriptor previousModel,
		out short lastClassId, out short lastHashId, out int lastPropertyId)
	{
		lastClassId = 0;
		lastHashId = 0;
		lastPropertyId = 0;
		if (previousModel != null)
		{
			lastClassId = previousModel.LastUsedClassId;
			lastPropertyId = previousModel.LastUsedPropertyId;
			lastHashId = previousModel.LastUsedHashIndexId;
		}

		Dictionary<Type, ObjectModelClass> classes = new Dictionary<Type, ObjectModelClass>(512, ReferenceEqualityComparer<Type>.Instance);
		foreach (Type rootClass in classTypes)
		{
			CreateObjectModelClass(classes, rootClass, previousModel, ref lastClassId, ref lastPropertyId, ref lastHashId);
		}

		ValidateInverseReferenceTargets(classes);

		return classes;
	}

	private DataModelDescriptor CreateModelInternal(PersistenceSettings persistenceSettings, DataModelDescriptor previousModel)
	{
		if (usedUp)
			throw new InvalidOperationException("Model has already been instantiated.");

		usedUp = true;

		Dictionary<Type, ObjectModelClass> classes = CreateObjectModelClasses(previousModel,
			out short lastClassId, out short lastHashId, out int lastPropertyId);

		DataModelDescriptor modelDesc = new DataModelDescriptor(lastClassId, lastPropertyId, lastHashId);
		modelDesc.Register(classes.Values);
		modelDesc.Prepare(persistenceSettings);

		return modelDesc;
	}

	private void ValidateInverseReferenceTargets(Dictionary<Type, ObjectModelClass> classes)
	{
		foreach (var objClass in classes.Values)
		{
			for (int i = 0; i < objClass.InverseReferences.Count; i++)
			{
				ObjectModelInverseReferenceProperty objInvRef = objClass.InverseReferences[i];
				Type targetType = objInvRef.PropertyInfo.PropertyType.GetGenericArguments()[0];
				if (!classes.ContainsKey(targetType))
					Throw.InvalidInverseReferenceTarget(objClass.ClassType.FullName, objInvRef.PropertyInfo.Name);

				if (targetType.GetProperty(objInvRef.TargetPropertyName, BindingFlags.Public | BindingFlags.Instance) == null)
					Throw.InvalidInverseReferenceTarget(objClass.ClassType.FullName, objInvRef.PropertyInfo.Name);
			}
		}
	}

	private void ValidateClass(Type classType)
	{
		if (classType.Namespace == null)
			Throw.ClassWithoutNamespace(classType.FullName);

		if (!typeof(DatabaseObject).IsAssignableFrom(classType))
			Throw.MustInheritDatabaseObject(classType.FullName);

		if (classType.IsGenericType || classType.IsGenericTypeDefinition)
			Throw.GenericClassNotSupported(classType.FullName);

		ConstructorInfo ctor = classType.GetConstructor(
			BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance, null, EmptyArray<Type>.Instance, null);

		if (ctor == null)
			Throw.MissingEmptyConstructor(classType.FullName);

		DatabaseClassAttribute dca = classType.GetCustomAttribute<DatabaseClassAttribute>();
		if (dca.IsAbstract)
			return;

		PropertyInfo[] pis = classType.GetProperties(
			BindingFlags.FlattenHierarchy | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

		HashSet<MethodInfo> allowedMethods = new HashSet<MethodInfo>(pis.Length * 2);
		foreach (PropertyInfo pi in pis)
		{
			MethodInfo gm = pi.GetGetMethod();
			MethodInfo sm = pi.GetSetMethod();
			if (pi.IsDefined(typeof(DatabasePropertyAttribute)) ||
				pi.IsDefined(typeof(DatabaseReferenceAttribute)) || pi.IsDefined(typeof(InverseReferencesAttribute)))
			{
				if (gm != null)
					allowedMethods.Add(gm);

				if (sm != null)
					allowedMethods.Add(sm);
			}
			else
			{
				if (gm != null && gm.IsAbstract || sm != null && sm.IsAbstract)
				{
					Throw.AbstractPropertyInNonAbstractClass(classType.FullName, pi.Name);
				}
			}
		}

		MethodInfo[] mis = classType.GetMethods(
			BindingFlags.FlattenHierarchy | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance); ;

		foreach (MethodInfo mi in mis)
		{
			if (!allowedMethods.Contains(mi) && mi.IsAbstract && mi.DeclaringType != typeof(DatabaseObject))
				Throw.AbstractMethodInNonAbstractClass(classType.FullName, mi.Name);
		}

		EventInfo[] eis = classType.GetEvents(
			BindingFlags.FlattenHierarchy | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);

		foreach (EventInfo ei in eis)
		{
			if (ei.AddMethod != null && ei.AddMethod.IsAbstract || ei.RemoveMethod != null && ei.RemoveMethod.IsAbstract)
				Throw.AbstractEventInNonAbstractClass(classType.FullName, ei.Name);
		}
	}

	public void CreateObjectModelClass(Dictionary<Type, ObjectModelClass> classes, Type classType,
		DataModelDescriptor previousModel, ref short lastClassId, ref int lastPropertyId, ref short lastHashId)
	{
		short classId;
		ClassDescriptor prevClassDesc = previousModel?.GetClass(classType.FullName);
		if (prevClassDesc != null)
		{
			classId = prevClassDesc.Id;
		}
		else
		{
			classId = ++lastClassId;
		}

		PropertyInfo[] pis = classType.GetProperties(
			BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy | BindingFlags.Instance);

		List<ObjectModelProperty> properties = new List<ObjectModelProperty>(pis.Length);
		List<ObjectModelInverseReferenceProperty> inverseReferences = new List<ObjectModelInverseReferenceProperty>(2);
		List<ObjectModelHashIndex> hashIndexes = GetHashIndexes(previousModel, classType, ref lastHashId);

		foreach (PropertyInfo pi in pis)
		{
			if (pi.IsDefined(typeof(InverseReferencesAttribute)))
			{
				if (pi.GetGetMethod() == null)
					Throw.MissingGetter(classType.FullName, pi.Name);

				if(pi.GetSetMethod() != null)
					Throw.SetterFound(classType.FullName, pi.Name);

				if (!pi.GetGetMethod().IsAbstract)
					Throw.InverseRereferncePropertyIsNotAbstract(classType.FullName, pi.Name);

				if (!pi.PropertyType.IsGenericType || pi.PropertyType.GetGenericTypeDefinition() != typeof(InverseReferenceSet<>))
					Throw.InvalidInverseReferencePropertyType(pi.DeclaringType.FullName, pi.Name);

				InverseReferencesAttribute ira = pi.GetCustomAttribute<InverseReferencesAttribute>();
				ObjectModelInverseReferenceProperty invRefProp = new ObjectModelInverseReferenceProperty(pi, ira.PropertyName);
				inverseReferences.Add(invRefProp);
				continue;
			}

			if (pi.DeclaringType != classType)
				continue;

			if (!pi.IsDefined(typeof(DatabasePropertyAttribute)) && !pi.IsDefined(typeof(DatabaseReferenceAttribute)))
				continue;

			if (!pi.GetGetMethod().IsAbstract)
				Throw.PropertyIsNotAbstract(classType.FullName, pi.Name);

			if (pi.GetGetMethod() == null || pi.GetSetMethod() == null)
				Throw.PropertyMissingGetterAndSetter(classType.FullName, pi.Name);

			ObjectModelProperty prop;
			Type propType = pi.PropertyType;
			if (propType.IsEnum)
				propType = propType.GetEnumUnderlyingType();

			PropertyDescriptor prevPropDesc = prevClassDesc?.GetProperty(pi.Name);
			int propId = prevPropDesc != null ? prevPropDesc.Id : ++lastPropertyId;

			if (typeof(DatabaseArray).IsAssignableFrom(propType))
			{
				DatabasePropertyAttribute dpa = pi.GetCustomAttribute<DatabasePropertyAttribute>();
				prop = new ObjectModelProperty(pi, propId, dpa.DefaultValue);
			}
			else if (PropertyTypesHelper.ManagedTypeToPropertyType(propType) == PropertyType.None)
			{
				DatabaseReferenceAttribute ra = pi.GetCustomAttribute<DatabaseReferenceAttribute>();
				if (ra == null)
					Throw.PropertyTypeInvalid(classType.FullName, pi.Name);

				prop = new ReferenceObjectModelProperty(pi, propId, null, ra.IsNullable, ra.DeleteTargetAction, ra.TrackInverseReferences);
			}
			else
			{
				DatabasePropertyAttribute dpa = pi.GetCustomAttribute<DatabasePropertyAttribute>();
				prop = new ObjectModelProperty(pi, propId, dpa.DefaultValue);
			}

			properties.Add(prop);
		}

		bool isAbstract = classType.GetCustomAttribute<DatabaseClassAttribute>().IsAbstract;
		string logName = GetClassLogName(classType);
		classes.Add(classType, new ObjectModelClass(classId, isAbstract, classType, logName, properties, inverseReferences, hashIndexes));
	}

	private string GetClassLogName(Type classType)
	{
		while (classType != typeof(DatabaseObject))
		{
			LogAttribute la = classType.GetCustomAttribute<LogAttribute>();
			if (la == null)
			{
				classType = classType.BaseType;
				continue;
			}

			return la.LogName;
		}

		return null;
	}

	private static List<ObjectModelHashIndex> GetHashIndexes(DataModelDescriptor previousModel, Type classType, ref short lastHashId)
	{
		List<ObjectModelHashIndex> l = new List<ObjectModelHashIndex>();
		foreach (HashIndexAttribute ha in classType.GetCustomAttributes<HashIndexAttribute>())
		{
			string name = $"{classType.Namespace}.{ha.Name}";

			short hashIndexId;
			HashIndexDescriptor prevHashDesc = previousModel?.GetHashIndex(name);
			if (prevHashDesc != null)
			{
				hashIndexId = prevHashDesc.Id;
			}
			else
			{
				hashIndexId = ++lastHashId;
			}

			l.Add(new ObjectModelHashIndex(classType, hashIndexId, name, ha.IsUnique, ha.Properties));
		}

		return l;
	}
}
