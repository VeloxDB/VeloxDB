using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Velox.Common;

namespace Velox.Config;

internal abstract class BaseElementConverter<T, TEnum> : JsonConverter<T> where T : class where TEnum : struct
{
	public override bool CanConvert(Type typeToConvert)
	{
		return typeToConvert == typeof(T);
	}

	public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
	{
		Utf8JsonReader readerCopy = reader;

		if(readerCopy.TokenType != JsonTokenType.StartObject)
			throw new JsonException();

		int depth = 0;
		while(readerCopy.Read())
		{
			if(readerCopy.TokenType == JsonTokenType.StartObject)
				depth++;

			if(readerCopy.TokenType == JsonTokenType.EndObject)
			{
				if(depth == 0)
				{
					reader = readerCopy;
					return null;
				}
				depth--;
			}

			if(depth == 0 && readerCopy.TokenType == JsonTokenType.PropertyName)
			{
				string property = readerCopy.GetString();
				if(property == null || string.Compare(property, "Type", true, CultureInfo.InvariantCulture) != 0)
					continue;

				if(!readerCopy.Read())
					throw new JsonException();

				if(readerCopy.TokenType == JsonTokenType.Null)
				{
					continue;
				}

				if(readerCopy.TokenType != JsonTokenType.String)
					throw new JsonException();

				string value = readerCopy.GetString();

				if(value == null)
				{
					continue;
				}

				TEnum elementType;
				if(!Enum.TryParse<TEnum>(value, true, out elementType))
				{
					continue;
				}

				Type type = EnumToType(elementType);

				T element = (T)JsonSerializer.Deserialize(ref reader, type, options);

				return element;
			}
		}

		throw new JsonException();
	}

	protected abstract Type EnumToType(TEnum elementType);

	public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
	{
		JsonSerializer.Serialize(writer, value, value.GetType(), options);
	}
}

internal class ReplicationElementConverter : BaseElementConverter<ReplicationElement, ElementType>
{
	protected override Type EnumToType(ElementType elementType)
	{
		switch(elementType)
		{
			case ElementType.GlobalWrite: return typeof(GlobalWriteCluster);
			case ElementType.LocalWrite:	return typeof(LocalWriteCluster);
			case ElementType.LocalWriteNode: return typeof(LocalWriteNode);
			case ElementType.Node: return typeof(StandaloneNode);
			default: throw new NotSupportedException($"Uncrecongized type {elementType}");
		}
	}
}

internal class WitnessConverter : BaseElementConverter<Witness, WitnessType>
{
	protected override Type EnumToType(WitnessType witnessType)
	{
		switch(witnessType)
		{
			case WitnessType.SharedFolder:
				return typeof(SharedFolderWitness);
			case WitnessType.Standalone:
				return typeof(StandaloneWitness);
			default: throw new NotSupportedException($"Uncrecongized witness type {witnessType}");
		}
	}
}

internal class EndpointConverter : JsonConverter<Endpoint>
{
	public override Endpoint Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
	{
		if(reader.TokenType == JsonTokenType.Null)
			return null;
		if(reader.TokenType != JsonTokenType.String)
			throw new JsonException();

		string s = reader.GetString();
		Checker.AssertNotNull(s);

		return Endpoint.FromString(s);
	}

	public override void Write(Utf8JsonWriter writer, Endpoint value, JsonSerializerOptions options)
	{
		writer.WriteStringValue(value.ToString());
	}
}
