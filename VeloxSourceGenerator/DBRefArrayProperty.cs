using System;
using Microsoft.CodeAnalysis;
using static Velox.SourceGenerator.Utils;

namespace Velox.SourceGenerator
{
	internal class DBRefArrayProperty : DBRefPropertyBase
	{
		public override bool IsArray => true;

		public INamedTypeSymbol ElementType { get; }

		public override INamedTypeSymbol ReferencedType => ElementType;
		protected virtual bool IsNullable => true;

		protected DBRefArrayProperty(IPropertySymbol property, INamedTypeSymbol elementType) : base(property)
		{
			this.ElementType = elementType;
		}

		public new static DBProperty Create(IPropertySymbol property, Context context)
		{
			INamedTypeSymbol propertyType = (INamedTypeSymbol)property.Type;
			ITypeSymbol arg = propertyType.TypeArguments[0];

			if (!(arg is INamedTypeSymbol))
			{
				Report.InvalidRefArrayType(context, property);
				return null;
			}

			INamedTypeSymbol namedArg = (INamedTypeSymbol)arg;
			if(!context.Types.IsDatabaseObject(namedArg))
			{
				Report.InvalidRefArrayType(context, property);
				return null;
			}

			return new DBRefArrayProperty(property, namedArg);
		}

		public override bool IsObjReference(Context context, IPropertySymbol dtoProperty)
		{
			CollectionBase collection = CollectionBase.Get(context, this, dtoProperty);
			return collection.IsObjReference(context, dtoProperty);
		}

		protected override sealed void OnAssignFrom(Context context, Method method, SourceWriter writer, string omName, string paramName,
											 IPropertySymbol dtoProperty, string objName, bool update)
		{
			CollectionBase collection = CollectionBase.Get(context, this, dtoProperty);

			if(collection == null)
				return;

			bool isId;
			INamedTypeSymbol dtoElementType = collection.GetDTOElType(context, this, dtoProperty, out isId);
			if(dtoElementType == null)
				return;

			string dbType = WithoutNullable(ElementType).ToString();

			string countProp = collection.CountProp;

			OnBeforeAssign(context, writer, objName, update);

			using (writer.IfNull(paramName, dtoProperty.Name))
			{
				CreateCollection(writer, paramName, dtoProperty, objName, dbType, countProp);

				using (writer.For(paramName, dtoProperty.Name, countProp))
				{
					if (isId)
					{
						TempVar temp = writer.CreateTempVar();
						writer.GetId(paramName, dtoProperty.Name);
						writer.GetObject(omName, dbType, temp);

						AssignToDBO(context, writer, objName, temp);
					}
					else
					{
						if (!CheckDTOType(context, method, dtoProperty, dtoElementType, out var refMethod))
						{
							return;
						}

						TempVar temp = writer.CreateTempVar();
						writer.AppendIndent();
						writer.Append("var ");
						writer.Append(temp);
						writer.Append(" = ");
						writer.Append(paramName);
						writer.Append(".");
						writer.Append(dtoProperty.Name);
						writer.Append("[");
						writer.Append(SourceWriter.CounterVar);
						writer.Append("];\n");

						writer.AppendIndent();
						writer.Append("if(");
						writer.Append(temp);
						writer.Append(" == null)\n");
						using (writer.Block())
						{
							writer.Throw("ArgumentNullException", $"{dtoProperty.ContainingType.ToString()}.{dtoProperty.Name}");
						}

						writer.AppendIndent();
						TempVar dbRef = writer.CreateTempVar();
						writer.Append(refMethod.Type.FullName);
						writer.Append(" ");
						writer.Append(dbRef);
						writer.Append(";\n");

						GenerateCallFromDTO(method, writer, omName, dbRef.ToString(), temp.ToString(), refMethod);

						AssignToDBO(context, writer, objName, dbRef);
					}
				}
			}
		}

		protected virtual void CreateCollection(SourceWriter writer, string paramName, IPropertySymbol dtoProperty, string objName, string dbType, string countProp)
		{
			writer.AppendIndent();
			writer.Append(objName);
			writer.Append(".");
			writer.Append(Name);
			writer.Append(" = new Velox.ObjectInterface.ReferenceArray<");
			writer.Append(dbType);
			writer.Append(">(");
			writer.Append(paramName);
			writer.Append(".");
			writer.Append(dtoProperty.Name);
			writer.Append(".");
			writer.Append(countProp);
			writer.Append(");\n");
		}

		protected virtual void AssignToDBO(Context context, SourceWriter writer, string objName, TempVar temp)
		{
			writer.AppendIndent();
			writer.Append(objName);
			writer.Append(".");
			writer.Append(Symbol.Name);
			writer.Append(".Add(");
			writer.Append(temp);
			writer.Append(");\n");
		}

		protected virtual void OnBeforeAssign(Context context, SourceWriter writer, string objName, bool update)
		{

		}

		protected override sealed void OnAssignTo(Context context, Method method, SourceWriter writer, string objName, IPropertySymbol dtoProperty)
		{
			CollectionBase collection = CollectionBase.Get(context, this, dtoProperty);

			if(collection == null)
				return;

			bool isId;
			INamedTypeSymbol dtoElementType = collection.GetDTOElType(context, this, dtoProperty, out isId);
			if(dtoElementType == null)
				return;

			IDisposable block = null;
			if (IsNullable)
			{
				writer.AppendFormat("if({0} != null)\n", Symbol.Name);
				block = writer.Block();
			}

			collection.GenerateCreate(context, writer, objName, this, dtoProperty, dtoElementType);

			if (isId)
			{
				collection.CopyIds(writer, objName, this, dtoProperty);
			}
			else
			{
				collection.CopyObjects(context, writer, objName, method, this, dtoProperty, dtoElementType);
			}

			if(IsNullable)
				block.Dispose();

		}

		private abstract class CollectionBase
		{
			private static readonly ArrayCollection arrayCollection;
			private static readonly ListCollection listCollection;

			protected abstract string AddNewFormat { get; }
			protected abstract string UpdateExistingFormat { get; }
			public abstract string CountProp { get; }
			protected abstract string GetDTOVar(string dtoPropertyName);

			static CollectionBase()
			{
				arrayCollection = new ArrayCollection();
				listCollection = new ListCollection();
			}

			public static CollectionBase Get(Context context, DBRefArrayProperty property, IPropertySymbol dtoProperty)
			{
				if (dtoProperty.Type.TypeKind == TypeKind.Array)
				{
					return arrayCollection;
				}
				else if (CastAsList(dtoProperty.Type, context, out _))
				{
					return listCollection;
				}

				Report.PropertyTypeMismatch(context, property.Symbol, dtoProperty);
				return null;
			}

			public INamedTypeSymbol GetDTOElType(Context context, DBRefArrayProperty property, IPropertySymbol dtoProperty, out bool isId)
			{
				ITypeSymbol elementType = GetDTOElTypeInternal(dtoProperty);

				if(SymEquals(elementType, context.Types.Long))
				{
					isId = true;
				}
				else
				{
					isId = false;
					if (elementType.TypeKind != TypeKind.Class)
					{
						Report.PropertyTypeMismatch(context, property.Symbol, dtoProperty);
						return null;
					}
				}

				return (INamedTypeSymbol)elementType;
			}

			public bool IsObjReference(Context context, IPropertySymbol dtoProperty)
			{
				return !SymEquals(GetDTOElTypeInternal(dtoProperty), context.Types.Long);
			}

			protected abstract ITypeSymbol GetDTOElTypeInternal(IPropertySymbol dtoProperty);

			public abstract void GenerateCreate(Context context, SourceWriter writer, string objName, DBRefArrayProperty property,
														  IPropertySymbol dtoProperty, INamedTypeSymbol dtoElementType);

			public abstract void CopyIds(SourceWriter writer, string objName, DBRefArrayProperty property, IPropertySymbol dtoProperty);

			public void CopyObjects(Context context, SourceWriter writer, string objName, Method method,
									DBRefArrayProperty property, IPropertySymbol dtoProperty,
									INamedTypeSymbol dtoElementType)
			{
				if (!property.CheckDTOType(context, method, dtoProperty, dtoElementType, out var refMethod))
				{
					return;
				}
				writer.IterateThrough(property.Name);
				using(writer.Block())
				{
					TempVar dboVar = writer.CreateTempVar();
					writer.AppendIndent();
					writer.Append(property.ElementType.ToString());
					writer.Append(" ");
					writer.Append(dboVar);
					writer.Append(" = ");
					writer.Append(property.Name);
					writer.Append("[");
					writer.Append(SourceWriter.CounterVar);
					writer.Append("];\n");

					writer.AppendIndent();
					writer.Append("if(!");
					writer.Append(dboVar);
					writer.Append(".IsSelected)\n");
					writer.AppendLine("\tcontinue;\n");

					property.GenerateCallToDTO(method, writer, dboVar.ToString(), objName, GetDTOVar(dtoProperty.Name),
											   dtoElementType, refMethod, AddNewFormat, UpdateExistingFormat);

				}
			}
		}

		private class ArrayCollection : CollectionBase
		{
			protected override string AddNewFormat => "{0}.{1} = {2};\n";

			protected override string UpdateExistingFormat => "\t{0}.{1} = ({2}){3};\n";

			public override string CountProp => "Length";

			protected override string GetDTOVar(string dtoPropertyName) =>  $"{dtoPropertyName}[{SourceWriter.CounterVar}]";

			public override void CopyIds(SourceWriter writer, string objName, DBRefArrayProperty property, IPropertySymbol dtoProperty)
			{
				writer.CopyToArray(property.Name, objName, dtoProperty.Name);
			}

			public override void GenerateCreate(Context context, SourceWriter writer, string objName,
														  DBRefArrayProperty property, IPropertySymbol dtoProperty,
														  INamedTypeSymbol dtoElementType)
			{
				writer.AppendFormat("{0}.{1} = new {2}[{3}.Count];\n", objName, dtoProperty.Name, dtoElementType,
  									property.Name);
			}
			protected override ITypeSymbol GetDTOElTypeInternal(IPropertySymbol dtoProperty)
			{
				return ((IArrayTypeSymbol)dtoProperty.Type).ElementType;
			}
		}

		private class ListCollection : CollectionBase
		{
			public override string CountProp => "Count";
			protected override string AddNewFormat => "{0}.{1}.Add({2});\n";
			protected override string UpdateExistingFormat => "\t{0}.{1}.Add(({2}){3});\n";

			public override void CopyIds(SourceWriter writer, string objName, DBRefArrayProperty property, IPropertySymbol dtoProperty)
			{
				writer.CopyToList(property.Name, objName, dtoProperty.Name);
			}

			public override void GenerateCreate(Context context, SourceWriter writer, string objName, DBRefArrayProperty property,
														  IPropertySymbol dtoProperty, INamedTypeSymbol dtoElementType)
			{
				writer.AppendFormat("{0}.{1} = new System.Collections.Generic.List<{2}>({3}.Count);\n",
											objName, dtoProperty.Name, dtoElementType, property.Name);
			}

			protected override ITypeSymbol GetDTOElTypeInternal(IPropertySymbol dtoProperty)
			{
				return ((INamedTypeSymbol)dtoProperty.Type).TypeArguments[0];
			}

			protected override string GetDTOVar(string dtoPropertyName) => dtoPropertyName;
		}
	}
}
