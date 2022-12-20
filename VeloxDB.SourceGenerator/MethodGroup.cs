using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using static VeloxDB.SourceGenerator.Utils;

namespace VeloxDB.SourceGenerator
{
	internal abstract class MethodGroup
	{
		public string Name { get; }
		public Method DeclaringMethod { get; }
		public Polymorphism Polymorphism => DeclaringMethod.Polymorphism;

		protected MethodGroup(string name, Method declaringMethod)
		{
			Name = name;
			DeclaringMethod = declaringMethod;
			declaringMethod.MethodGroup = this;
		}

		public void AddMethod(Context context, Method method)
		{
			method.MethodGroup = this;

			if(method.Polymorphism != Polymorphism.NotSpecified && method != DeclaringMethod)
			{
				Report.SupportPolymorphismAlreadyDeclared(context, method.Symbol, DeclaringMethod.Symbol);
			}

			OnAddMethod(context, method);
		}

		protected abstract void OnAddMethod(Context context, Method method);

		public static MethodGroup Create(Context context, Method method)
		{
			if(method.Polymorphism == Polymorphism.Enabled)
				return new PolymorphMethodGroup(method.Name, method);
			else
				return new RegularMethodGroup(method.Name, method);
		}
	}

	internal class RegularMethodGroup : MethodGroup
	{
		public RegularMethodGroup(string name, Method declaringMethod) : base(name, declaringMethod)
		{
		}

		protected override void OnAddMethod(Context context, Method method)
		{
			if(Polymorphism == Polymorphism.NotSpecified && method != DeclaringMethod)
			{
				Report.MissingSupportPolymorphism(context, DeclaringMethod.Symbol);
			}
		}
	}

	internal class PolymorphMethodGroup : MethodGroup
	{
		Dictionary<INamedTypeSymbol, Method> dtoTypeMap;

		public PolymorphMethodGroup(string name, Method declaringMethod) : base(name, declaringMethod)
		{
			dtoTypeMap = new Dictionary<INamedTypeSymbol, Method>(SymbolEqualityComparer.Default);
		}

		protected override void OnAddMethod(Context context, Method method)
		{
			if(dtoTypeMap.TryGetValue(method.DTOType, out Method duplicate))
			{
				Report.DuplicateDTOType(context, method.Symbol, method.DTOType, duplicate.Symbol);
				return;
			}

			if (!ValidateInheritance(context, method))
				return;

			dtoTypeMap.Add(method.DTOType, method);
		}

		private bool ValidateInheritance(Context context, Method method)
		{
			DBOType dboType = method.Type;
			INamedTypeSymbol toValidate = method.DTOType;

			if(dboType.Parent == null)
				return true;

			DBOType parent = dboType.Parent;
			Method parentMethod = parent.GetMethodByName(method.Name);

			if(parentMethod == null)
				return false;

			bool result = context.IsAssignable(toValidate, parentMethod.DTOType);

			if(!result)
				Report.InvalidDTOInheritance(context, method.Symbol, toValidate, parentMethod.DTOType);

			return result;
		}
	}
}


