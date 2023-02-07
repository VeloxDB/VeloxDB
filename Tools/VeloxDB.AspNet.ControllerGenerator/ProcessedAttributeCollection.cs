using System.Collections.Generic;
using System.Text;
using Microsoft.CodeAnalysis;
using static VeloxDB.AspNet.ControllerGenerator.Utils;
using System.Collections;

namespace VeloxDB.AspNet.ControllerGenerator
{
	internal class ProcessedAttributeCollection : IEnumerable<ProcessedAttribute>
	{
		List<ProcessedAttribute> collection;
		private static readonly StringBuilder builder = new StringBuilder();

		public ProcessedAttributeCollection(int capacity)
		{
			this.collection = new List<ProcessedAttribute>(capacity);
		}

		public void Add(ProcessedAttribute processed)
		{
			collection.Add(processed);
		}

		public void Add(INamedTypeSymbol attributeClass)
		{
			builder.Clear();
			builder.Append(attributeClass.ToDisplayString());
			collection.Add(new ProcessedAttribute(attributeClass, builder.ToString()));
		}

		public void Add(INamedTypeSymbol attributeClass, string arg)
		{
			builder.Clear();
			builder.Append(attributeClass.ToDisplayString());
			builder.Append("(\"");
			builder.Append(arg);
			builder.Append("\")");
			collection.Add(new ProcessedAttribute(attributeClass, builder.ToString()));
		}

		public bool Contains(INamedTypeSymbol item)
		{
			foreach(ProcessedAttribute processed in collection)
			{
				if(SymEquals(processed.AttributeClass, item))
					return true;
			}

			return false;
		}

		public IEnumerator<ProcessedAttribute> GetEnumerator()
		{
			return collection.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return collection.GetEnumerator();
		}

		public bool ContainsHttpAttribute(KnownTypes knownTypes)
		{
			foreach(ProcessedAttribute processed in collection)
			{
				if(knownTypes.IsHttpAttribute(processed.AttributeClass))
					return true;
			}
			return false;
		}
	}

}