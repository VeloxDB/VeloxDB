using System.Collections.Generic;
using Microsoft.CodeAnalysis;

namespace VeloxDB.AspNet.ControllerGenerator
{
	internal class KnownTypes
	{
		public INamedTypeSymbol DbAPIAttribute { get; private set; }
		public INamedTypeSymbol DbAPIOperationAttribute { get; private set; }
		public INamedTypeSymbol DatabaseTask { get; private set; }
		public INamedTypeSymbol DatabaseTaskGeneric { get; private set; }
		public INamedTypeSymbol ForwardAttribute { get; private set; }
		public INamedTypeSymbol DoNotGenerateControllerAttribute { get; private set; }
		public INamedTypeSymbol Attribute { get; private set; }
		public INamedTypeSymbol ApiControllerAttribute { get; private set; }

		public INamedTypeSymbol Void { get; private set; }
		public INamedTypeSymbol RouteAttribute { get; private set; }
		public INamedTypeSymbol HttpGetAttribute { get; private set; }
		public INamedTypeSymbol HttpPostAttribute { get; private set; }

		static readonly string[] httpAttributeNames = new string[]{ "Microsoft.AspNetCore.Mvc.HttpDeleteAttribute",
		"Microsoft.AspNetCore.Mvc.HttpHeadAttribute", "Microsoft.AspNetCore.Mvc.HttpOptionsAttribute",
		"Microsoft.AspNetCore.Mvc.HttpPatchAttribute", "Microsoft.AspNetCore.Mvc.HttpPutAttribute",
		"Microsoft.AspNetCore.Mvc.Routing.HttpMethodAttribute"};
		HashSet<INamedTypeSymbol> httpAttributes;

		private KnownTypes()
		{
			httpAttributes = new HashSet<INamedTypeSymbol>(SymbolEqualityComparer.Default);
		}

		public bool IsHttpAttribute(INamedTypeSymbol attribute)
		{
			return httpAttributes.Contains(attribute);
		}

		public static KnownTypes Create(Compilation compilation)
		{
			KnownTypes types = new KnownTypes();

			types.DbAPIAttribute = compilation.GetTypeByMetadataName("VeloxDB.Protocol.DbAPIAttribute");
			if (types.DbAPIAttribute == null)
				return null;

			types.DbAPIOperationAttribute = compilation.GetTypeByMetadataName("VeloxDB.Protocol.DbAPIOperationAttribute");
			if (types.DbAPIOperationAttribute == null)
				return null;

			types.DatabaseTask = compilation.GetTypeByMetadataName("VeloxDB.Client.DatabaseTask");
			if (types.DatabaseTask == null)
				return null;

			types.DatabaseTaskGeneric = compilation.GetTypeByMetadataName("VeloxDB.Client.DatabaseTask`1");
			if (types.DatabaseTaskGeneric == null)
				return null;

			types.ForwardAttribute = compilation.GetTypeByMetadataName("VeloxDB.AspNet.ForwardAttribute");
			if (types.ForwardAttribute == null)
				return null;

			types.DoNotGenerateControllerAttribute = compilation.GetTypeByMetadataName("VeloxDB.AspNet.DoNotGenerateControllerAttribute");
			if(types.DoNotGenerateControllerAttribute == null)
				return null;

			types.Attribute = compilation.GetTypeByMetadataName("System.Attribute");
			if (types.Attribute == null)
				return null;

			types.ApiControllerAttribute = compilation.GetTypeByMetadataName("Microsoft.AspNetCore.Mvc.ApiControllerAttribute");
			if (types.ApiControllerAttribute == null)
				return null;

			types.RouteAttribute = compilation.GetTypeByMetadataName("Microsoft.AspNetCore.Mvc.RouteAttribute");
			if (types.RouteAttribute == null)
				return null;

			types.HttpGetAttribute = compilation.GetTypeByMetadataName("Microsoft.AspNetCore.Mvc.HttpGetAttribute");
			if (types.HttpGetAttribute == null)
				return null;

			types.HttpPostAttribute = compilation.GetTypeByMetadataName("Microsoft.AspNetCore.Mvc.HttpPostAttribute");
			if (types.HttpPostAttribute == null)
				return null;

			types.Attribute = compilation.GetTypeByMetadataName("System.Attribute");
			types.Void = compilation.GetTypeByMetadataName("System.Void");

			foreach(string name in httpAttributeNames)
				types.httpAttributes.Add(compilation.GetTypeByMetadataName(name));

			if(httpAttributeNames.Length != types.httpAttributes.Count)
				return null;

			types.httpAttributes.Add(types.HttpGetAttribute);
			types.httpAttributes.Add(types.HttpPostAttribute);

			return types;
		}

		private static INamedTypeSymbol GetType(Compilation compilation, string fullName)
		{
			INamedTypeSymbol result = compilation.GetTypeByMetadataName(fullName);
			return result;
		}

	}

}