using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Threading;
using VeloxDB.Common;
using VeloxDB.Descriptor;

namespace VeloxDB.ObjectInterface;

internal delegate bool ReferenceCheckerDelegate(DatabaseObject obj, LongHashSet deletedIds);

internal sealed class ObjectModelData
{
	const string assemblyName = "__ObjectModel";
	ModuleBuilder moduleBuilder;

	DataModelDescriptor modelDesc;
	Dictionary<Type, ClassData> userTypeMap;
	Dictionary<short, ClassData> typeIdMap;
	Dictionary<short, Delegate> hashIndexComparers;
	Dictionary<int, ReferenceCheckerDelegate> referencePropertyDelegates;

	public ObjectModelData(DataModelDescriptor modelDesc, IEnumerable<Assembly> modelAssemblies = null)
	{
		this.modelDesc = modelDesc;

		if (ObjectModelMissing(modelDesc))
		{
			Checker.AssertNotNull(modelAssemblies);
			PopulateObjectModelInfo(modelDesc, modelAssemblies);
		}

		AssemblyName aName = new AssemblyName(assemblyName);
		AssemblyBuilder ab = AssemblyBuilder.DefineDynamicAssembly(aName, AssemblyBuilderAccess.RunAndCollect);
		moduleBuilder = ab.DefineDynamicModule(aName.Name);

		userTypeMap = new Dictionary<Type, ClassData>(modelDesc.ClassCount);
		typeIdMap = new Dictionary<short, ClassData>(modelDesc.ClassCount);
		hashIndexComparers = new Dictionary<short, Delegate>(modelDesc.HashIndexCount);

		int c = 0;
		foreach (ClassDescriptor classDesc in modelDesc.GetAllClasses())
		{
			for (int i = 0; i < classDesc.RefeferencePropertyIndexes.Length; i++)
			{
				ReferencePropertyDescriptor rpd =
					(ReferencePropertyDescriptor)classDesc.Properties[classDesc.RefeferencePropertyIndexes[i]];

				if (!rpd.TrackInverseReferences && rpd.OwnerClass.Id == classDesc.Id)
					c++;
			}
		}

		referencePropertyDelegates = new Dictionary<int, ReferenceCheckerDelegate>(c);

		foreach (ClassDescriptor classDesc in modelDesc.GetAllClasses())
		{
			if (classDesc.ObjectModelClass == null)
				continue;

			ClassData cd = ClassData.CreateClassData(moduleBuilder, classDesc);
			AddClassData(classDesc.ObjectModelClass.ClassType, cd);

			for (int i = 0; i < cd.ClassDesc.RefeferencePropertyIndexes.Length; i++)
			{
				ReferencePropertyDescriptor rpd =
					(ReferencePropertyDescriptor)cd.ClassDesc.Properties[cd.ClassDesc.RefeferencePropertyIndexes[i]];

				if (!rpd.TrackInverseReferences && rpd.OwnerClass.Id == cd.ClassDesc.Id)
				{
					ReferenceCheckerDelegate rcd = ClassData.CreateReferencePropertyDelegate(cd, rpd);
					AddReferencePropertyDelegates(rpd, rcd);
				}
			}
		}

		foreach (HashIndexDescriptor hdi in modelDesc.GetAllHashIndexes())
		{
			if (hdi.Id < 0)
				continue;

			Delegate d = ClassData.CreateHashIndexComparer(hdi);
			AddHashIndexComparer(hdi.Id, d);
		}
	}

	public DataModelDescriptor ModelDesc => modelDesc;

	private bool ObjectModelMissing(DataModelDescriptor modelDesc)
	{
		NamespaceDescriptor sysNamespace = modelDesc.GetClass(SystemCode.IdGenerator.Id).Namespace;
		foreach (ClassDescriptor classDesc in modelDesc.GetAllClasses())
		{
			if (classDesc.ObjectModelClass == null && !object.ReferenceEquals(classDesc.Namespace, sysNamespace))
				return true;
		}

		return false;
	}

	private void PopulateObjectModelInfo(DataModelDescriptor modelDesc, IEnumerable<Assembly> modelAssemblies)
	{
		ObjectModelSettings s = new ObjectModelSettings();
		foreach (var assembly in modelAssemblies)
		{
			s.AddAssembly(assembly);
		}

		var d = s.CreateObjectModelClasses(modelDesc);
		foreach (ClassDescriptor classDesc in modelDesc.GetAllClasses())
		{
			if (classDesc.Id < 0)   // System classes
				continue;

			ObjectModelClass objClass = d[classDesc.Id];
			classDesc.ObjectModelClass = objClass;
		}
	}

	private void AddClassData(Type userType, ClassData cd)
	{
		userTypeMap.Add(userType, cd);
		typeIdMap.Add(cd.ClassDesc.Id, cd);
	}

	private void AddHashIndexComparer(short id, Delegate comparer)
	{
		hashIndexComparers.Add(id, comparer);
	}

	private void AddReferencePropertyDelegates(ReferencePropertyDescriptor propDesc, ReferenceCheckerDelegate r)
	{
		referencePropertyDelegates.Add(propDesc.Id, r);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ClassData GetClassByUserType(Type type)
	{
		return userTypeMap[type];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool TryGetClassByUserType(Type type, out ClassData cd)
	{
		return userTypeMap.TryGetValue(type, out cd);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ClassData GetClassByTypeId(short id)
	{
		return typeIdMap[id];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool TryGetClassByTypeId(short id, out ClassData cd)
	{
		return typeIdMap.TryGetValue(id, out cd);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public Delegate GetHashIndexComparer(short id)
	{
		return hashIndexComparers[id];
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public ReferenceCheckerDelegate GetReferencePropertyDelegate(int id)
	{
		return referencePropertyDelegates[id];
	}
}
