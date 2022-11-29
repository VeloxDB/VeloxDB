using System;
using System.Collections;
using System.Collections.Generic;
using Velox.Common;
using Velox.Descriptor;

namespace Velox.ObjectInterface;

internal sealed class ScanClassesSet
{
	int poolCount;
	List<ScanedProperty>[] setPool;

	LongDictionary<List<ScanedProperty>> scanClasses;

	public ScanClassesSet()
	{
		scanClasses = new LongDictionary<List<ScanedProperty>>(8);
		poolCount = 8;
		setPool = new List<ScanedProperty>[poolCount];
		for (int i = 0; i < poolCount; i++)
		{
			setPool[i] = new List<ScanedProperty>(4);
		}
	}

	public int Count => scanClasses.Count;

	public void ForEeach(Action<short, List<ScanedProperty>> action)
	{
		scanClasses.ForEach(kv =>
		{
			action((short)kv.Key, kv.Value);
		});
	}

	private List<ScanedProperty> GetSet()
	{
		if (poolCount == 0)
			return new List<ScanedProperty>(4);

		return setPool[--poolCount];
	}

	private void PutSet(List<ScanedProperty> h)
	{
		h.Clear();

		if (setPool.Length == poolCount)
			Array.Resize(ref setPool, setPool.Length * 2);

		setPool[poolCount++] = h;
	}

	public void AddInvReferenceProperty(ReferencePropertyDescriptor propDesc, ObjectModelData modelData)
	{
		if (!scanClasses.TryGetValue(propDesc.OwnerClass.Id, out List<ScanedProperty> scannedProps))
		{
			scannedProps = GetSet();
			scanClasses.Add(propDesc.OwnerClass.Id, scannedProps);
		}

		if (!ContainsProperty(scannedProps, propDesc.Id))
		{
			ReferenceCheckerDelegate d = modelData.GetReferencePropertyDelegate(propDesc.Id);
			scannedProps.Add(new ScanedProperty(propDesc, d));
		}
	}

	private static bool ContainsProperty(List<ScanedProperty> props, int propId)
	{
		for (int i = 0; i < props.Count; i++)
		{
			if (props[i].Property.Id == propId)
				return true;
		}

		return false;
	}

	public void Clear()
	{
		if (scanClasses.Count == 0)
			return;

		scanClasses.ForEach(kv => PutSet(kv.Value));
		scanClasses.Clear();
	}

	public struct ScanedProperty
	{
		public ReferencePropertyDescriptor Property;
		public ReferenceCheckerDelegate ReferenceChecker;

		public ScanedProperty(ReferencePropertyDescriptor property, ReferenceCheckerDelegate referenceChecker)
		{
			this.Property = property;
			this.ReferenceChecker = referenceChecker;
		}
	}
}
