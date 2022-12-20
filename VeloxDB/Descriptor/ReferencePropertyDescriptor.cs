using System;
using System.Xml;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using VeloxDB.Common;
using System.IO;

namespace VeloxDB.Descriptor;

internal enum Multiplicity
{
	One = 1,
	ZeroToOne = 2,
	Many = 3
}

/// <summary>
/// Indicates what to do with an object when referenced object is deleted.
/// </summary>
public enum DeleteTargetAction
{
	// This order is important since reference changes are sorted inside engine according to these values
	/// <summary>
	/// Prevent deletion. Target object can only be deleted if there are no references pointing to it.
	/// </summary>
	PreventDelete = 1,

	/// <summary>
	/// Delete referencing object automatically.
	/// </summary>
	CascadeDelete = 2,

	/// <summary>
	/// Allow delete and set reference to null.
	/// </summary>
	SetToNull = 3,
}

internal sealed class ReferencePropertyDescriptor : PropertyDescriptor
{
	Multiplicity multiplicity;
	ClassDescriptor referencedClass;
	DeleteTargetAction deleteTargetAction;
	bool trackInverseRefs;
	ReadOnlyArray<ClassDescriptor> onDeleteScanClasses;

	public ReferencePropertyDescriptor()
	{
	}

	public ReferencePropertyDescriptor(ReferenceObjectModelProperty objectModelProperty, ClassDescriptor ownerClass) :
		base(objectModelProperty, ownerClass)
	{
		TempData tempData = new TempData();

		if (objectModelProperty.IsArray)
		{
			multiplicity = Multiplicity.Many;
		}
		else
		{
			multiplicity = objectModelProperty.IsNullable ? Multiplicity.ZeroToOne : Multiplicity.One;
		}

		tempData.ReferencedClassName = objectModelProperty.ReferencedType.FullName;

		deleteTargetAction = objectModelProperty.DeleteTargetAction;
		if (deleteTargetAction < DeleteTargetAction.PreventDelete || deleteTargetAction > DeleteTargetAction.SetToNull)
			Throw.InvalidDeleteTargetAction(ownerClass.FullName, Name);

		if (deleteTargetAction == DeleteTargetAction.SetToNull && multiplicity == Multiplicity.One)
			Throw.PropertyCantBeSetToNull(ownerClass.FullName, Name);

		trackInverseRefs = objectModelProperty.TrackInverseReferences;

		base.PropertyType = multiplicity == Multiplicity.Many ? PropertyType.LongArray : PropertyType.Long;

		OwnerClass.Model.LoadingTempData.Add(this, tempData);
	}

	public ReferencePropertyDescriptor(DataModelDescriptor modelDesc, string name, int id, Multiplicity multiplicity,
		string referencedClassName, bool trackInverseRefs, DeleteTargetAction deleteTargetAction) :
		base(name, id, multiplicity == Multiplicity.Many ? PropertyType.LongArray : PropertyType.Long)
	{
		TempData tempData = new TempData();

		this.multiplicity = multiplicity;
		tempData.ReferencedClassName = referencedClassName;
		this.trackInverseRefs = trackInverseRefs;
		this.deleteTargetAction = deleteTargetAction;

		modelDesc.LoadingTempData.Add(this, tempData);
	}

	public ReferencePropertyDescriptor(XmlReader reader, ClassDescriptor ownerClass) :
		base(reader, ownerClass)
	{
		TempData tempData = new TempData();

		tempData.ReferencedClassName = reader.GetAttribute("ReferencedClass");
		if (!tempData.ReferencedClassName.Contains("."))
			tempData.ReferencedClassName = OwnerClass.Namespace.Name + "." + tempData.ReferencedClassName;

		string value = reader.GetAttribute("Multiplicity");
		if (value.Equals("0..1", StringComparison.Ordinal))
		{
			multiplicity = Multiplicity.ZeroToOne;
		}
		else if (value.Equals("1", StringComparison.Ordinal))
		{
			multiplicity = Multiplicity.One;
		}
		else
		{
			multiplicity = Multiplicity.Many;
		}

		trackInverseRefs = true;
		value = reader.GetAttribute("TrackInverseRefs");
		if (value != null)
			trackInverseRefs = bool.Parse(value.ToLowerInvariant());

		deleteTargetAction = multiplicity == Multiplicity.One ? DeleteTargetAction.PreventDelete : DeleteTargetAction.SetToNull;
		value = reader.GetAttribute("OnDeleteTarget");
		if (value != null)
			deleteTargetAction = (DeleteTargetAction)Enum.Parse(typeof(DeleteTargetAction), value);

		if (deleteTargetAction == DeleteTargetAction.SetToNull && multiplicity == Multiplicity.One)
			Throw.InvalidDeleteTargetAction(ownerClass.FullName, Name);

		base.PropertyType = multiplicity == Multiplicity.Many ? PropertyType.LongArray : PropertyType.Long;

		OwnerClass.Model.LoadingTempData.Add(this, tempData);
		reader.Close();
	}

	public override ModelItemType Type => ModelItemType.ReferenceProperty;
	public override PropertyKind Kind => PropertyKind.Reference;
	public ClassDescriptor ReferencedClass => referencedClass;
	public override object DefaultValue => multiplicity == Multiplicity.Many ? null : (object)(long)0;
	public Multiplicity Multiplicity => multiplicity;
	public DeleteTargetAction DeleteTargetAction => deleteTargetAction;
	public bool TrackInverseReferences => trackInverseRefs;
	public ReadOnlyArray<ClassDescriptor> OnDeleteScanClasses => onDeleteScanClasses;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool ReferencedTypeValid(short classId)
	{
		return referencedClass.IsAssignable(classId);
	}

	internal static Dictionary<short, List<ReferencePropertyDescriptor>> GroupInverseRefs(DataModelDescriptor model)
	{
		Dictionary<int, ReferencePropertyDescriptor> invRefGroups = new Dictionary<int, ReferencePropertyDescriptor>(64);
		foreach (ClassDescriptor @class in model.GetAllClasses())
		{
			foreach (PropertyDescriptor prop in @class.Properties)
			{
				if (prop.Kind != PropertyKind.Reference || !object.ReferenceEquals(prop.OwnerClass, @class))
					continue;

				ReferencePropertyDescriptor mrp = (ReferencePropertyDescriptor)prop;
				invRefGroups.Add(mrp.Id, mrp);
			}
		}

		Dictionary<short, List<ReferencePropertyDescriptor>> res = new Dictionary<short, List<ReferencePropertyDescriptor>>(model.ClassCount);
		foreach (ReferencePropertyDescriptor group in invRefGroups.Values)
		{
			ClassDescriptor targetClass = group.ReferencedClass;
			foreach (short classId in targetClass.DescendentClassIds.Concat(targetClass.Id))
			{
				if (!group.ReferencedTypeValid(classId))
					continue;

				List<ReferencePropertyDescriptor> l;
				if (!res.TryGetValue(classId, out l))
				{
					l = new List<ReferencePropertyDescriptor>(2);
					res.Add(classId, l);
				}

				l.Add(group);
			}
		}

		return res;
	}

	internal void Prepare()
	{
		TempData tempData = (TempData)OwnerClass.Model.LoadingTempData[this];

		referencedClass = OwnerClass.Model.GetClass(tempData.ReferencedClassName);
		if (referencedClass == null)
		{
			Throw.ReferencePropertyReferencesInvalidClass(OwnerClass.FullName, Name, tempData.ReferencedClassName);
		}

		if (referencedClass.Id < 0)
		{
			Throw.PropertyReferencesUnknownClass(OwnerClass.FullName, Name, tempData.ReferencedClassName);
		}

		CreateOnDeleteScanClasses();
	}

	private void CreateOnDeleteScanClasses()
	{
		if (trackInverseRefs)
		{
			onDeleteScanClasses = ReadOnlyArray<ClassDescriptor>.Empty;
			return;
		}

		HashSet<short> hs = new HashSet<short>(8);

		UnionOfAffectedClasses(OwnerClass, hs);

		int c = 0;
		ClassDescriptor[] cs = new ClassDescriptor[hs.Count];
		foreach (short classId in hs)
		{
			cs[c++] = OwnerClass.Model.GetClass(classId);
		}

		onDeleteScanClasses = new ReadOnlyArray<ClassDescriptor>(cs);
	}

	private void UnionOfAffectedClasses(ClassDescriptor @class, HashSet<short> hs)
	{
		if (!@class.IsAbstract)
			hs.Add(@class.Id);

		foreach (short mcid in @class.DescendentClassIds)
		{
			ClassDescriptor inhClass = @class.Model.GetClass(mcid);
			if (!inhClass.IsAbstract)
				hs.Add(inhClass.Id);
		}
	}

	public override void Serialize(BinaryWriter writer, ModelDescriptorSerializerContext context)
	{
		base.Serialize(writer, context);
		writer.Write((byte)multiplicity);
		writer.Write((byte)deleteTargetAction);
		writer.Write(trackInverseRefs);

		context.Serialize(referencedClass, writer);

		writer.Write(onDeleteScanClasses.Length);
		for (int i = 0; i < onDeleteScanClasses.Length; i++)
		{
			context.Serialize(onDeleteScanClasses[i], writer);
		}
	}

	public override void Deserialize(BinaryReader reader, ModelDescriptorDeserializerContext context)
	{
		base.Deserialize(reader, context);
		multiplicity = (Multiplicity)reader.ReadByte();
		deleteTargetAction = (DeleteTargetAction)reader.ReadByte();
		trackInverseRefs = reader.ReadBoolean();

		referencedClass = context.Deserialize<ClassDescriptor>(reader);

		int c = reader.ReadInt32();
		ClassDescriptor[] mcs = new ClassDescriptor[c];
		for (int i = 0; i < c; i++)
		{
			mcs[i] = context.Deserialize<ClassDescriptor>(reader);
		}

		onDeleteScanClasses = new ReadOnlyArray<ClassDescriptor>(mcs);
	}

	private sealed class TempData
	{
		public string ReferencedClassName { get; set; }
	}
}
