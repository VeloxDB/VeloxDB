using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using static VeloxDB.AspNet.ControllerGenerator.Utils;

namespace VeloxDB.AspNet.ControllerGenerator
{

	internal static class StringBuilderExtensions
	{
		private static readonly string classTemplate =
		@"public class {0} : Microsoft.AspNetCore.Mvc.ControllerBase
{{

 	private {1} api;

	public {0}(VeloxDB.AspNet.IVeloxDBConnectionProvider vlxCP)
	{{
		api = vlxCP.Get<{1}>();
 	}}

 ";
		private static readonly string asyncMethodTemplate =
	   @"public async Task<{0}> {1}({2})
	{{
		return await api.{1}({3});
	}}

";

		static readonly string asyncVoidMethodTemplate =
		@"public async Task {0}({1})
	{{
		await api.{0}({2});
	}}

";

		static readonly string voidMethodTemplate =
		@"public void {0}({1})
	{{
		api.{0}({2});
	}}

";
		static readonly string methodTemplate =
		@"public {0} {1}({2})
	{{
		return api.{1}({3});
	}}

";

		public static void AppendClass(this StringBuilder builder, string className, string apiName)
		{
			builder.AppendFormat(classTemplate, className, apiName);
		}

		public static void AppendAsyncMethod(this StringBuilder builder, string returnType, string name, string paramList, string invokeList)
		{
			builder.AppendFormat(asyncMethodTemplate, returnType, name, paramList, invokeList);
		}

		public static void AppendAsyncVoidMethod(this StringBuilder builder, string name, string paramList, string invokeList)
		{
			builder.AppendFormat(asyncVoidMethodTemplate, name, paramList, invokeList);
		}

		public static void AppendVoidMethod(this StringBuilder builder, string name, string paramList, string invokeList)
		{
			builder.AppendFormat(voidMethodTemplate, name, paramList, invokeList);
		}

		public static void AppendMethod(this StringBuilder builder, string returnType, string name, string paramList, string invokeList)
		{
			builder.AppendFormat(methodTemplate, returnType, name, paramList, invokeList);
		}

		public static void AppendClass(this StringBuilder builder, INamedTypeSymbol api, ControllerGeneratorContext context)
		{
			string withoutInterface = StripInterface(api.Name);
			string className = withoutInterface + "Controller";
			KnownTypes knownTypes = context.KnownTypes;

			ProcessedAttributeCollection attributes = ProcessedAttribute.ProcessAttributes(api.GetAttributes(), context);

			if(!attributes.Contains(knownTypes.ApiControllerAttribute))
			{
				attributes.Add(knownTypes.ApiControllerAttribute);
			}

			if(!attributes.Contains(knownTypes.RouteAttribute))
			{
				attributes.Add(knownTypes.RouteAttribute, withoutInterface);
			}

			builder.AppendAttributes(attributes);
			builder.AppendClass(className, api.ToDisplayString());

			foreach (IMethodSymbol method in api.GetMembers().OfType<IMethodSymbol>())
			{
				if (!HasAttribute(method, knownTypes.DbAPIOperationAttribute))
					continue;
				builder.AppendMethod(method, context);
			}

			builder.Append("}\n");
		}

		public static void AppendAttributes(this StringBuilder builder, IEnumerable<ProcessedAttribute> attributes)
		{
			foreach (ProcessedAttribute attribute in attributes)
			{
				builder.AppendAttribute(attribute);
				builder.AppendLine();
			}
		}

		public static void AppendAttribute(this StringBuilder builder, ProcessedAttribute attribute)
		{
			builder.Append("[");
			builder.Append(attribute.CSharpString);
			builder.Append("]");
		}

		public static void AppendMethod(this StringBuilder builder, IMethodSymbol method, ControllerGeneratorContext context)
		{
			KnownTypes knownTypes = context.KnownTypes;

			StringBuilder paramListBuilder = new StringBuilder();
			StringBuilder invokeListBuilder = new StringBuilder();
			for (int i = 0; i < method.Parameters.Length; i++)
			{
				var parameter = method.Parameters[i];
				paramListBuilder.Append(parameter.Type.ToDisplayString());
				paramListBuilder.Append(" ");
				paramListBuilder.Append(parameter.Name);

				invokeListBuilder.Append(parameter.Name);

				if (i < method.Parameters.Length - 1)
				{
					paramListBuilder.Append(", ");
					invokeListBuilder.Append(", ");
				}
			}

			ProcessedAttributeCollection attributes = ProcessedAttribute.ProcessAttributes(method.GetAttributes(), context);

			if(!attributes.ContainsHttpAttribute(knownTypes))
			{
				INamedTypeSymbol httpMethod = IsReadOnly(method, knownTypes) ? knownTypes.HttpGetAttribute : knownTypes.HttpPostAttribute;
				attributes.Add(httpMethod);
			}

			builder.AppendAttributes(attributes);

			if (SymEquals(method.ReturnType, knownTypes.Void))
			{
				builder.AppendVoidMethod(method.Name, paramListBuilder.ToString(), invokeListBuilder.ToString());
			}
			else if (SymEquals(method.ReturnType, knownTypes.DatabaseTask))
			{
				builder.AppendAsyncVoidMethod(method.Name, paramListBuilder.ToString(), invokeListBuilder.ToString());
			}
			else if (SymEquals(method.ReturnType.OriginalDefinition, knownTypes.DatabaseTaskGeneric))
			{

				ITypeSymbol returnType = ((INamedTypeSymbol)method.ReturnType).TypeArguments[0];

				builder.AppendAsyncMethod(returnType.ToDisplayString(), method.Name, paramListBuilder.ToString(),
										  invokeListBuilder.ToString());
			}
			else
			{
				builder.AppendMethod(method.ReturnType.ToDisplayString(), method.Name,
									 paramListBuilder.ToString(), invokeListBuilder.ToString());
			}

		}

		private static string StripInterface(string apiName)
		{

			if (apiName.Length > 1 && apiName[0] == 'I' && char.IsUpper(apiName[1]))
				return apiName.Substring(1);
			else
				return apiName;
		}

	}
}