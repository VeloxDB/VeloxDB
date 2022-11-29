using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Velox.Common;
using Velox.Networking;

namespace Velox.Protocol;

internal sealed class ProtocolOperationDescriptor
{
	static readonly HashSet<Type> builtInErrorTypes = new HashSet<Type>(new Type[] {
		typeof(DbAPIProtocolException), typeof(DbAPIMismatchException), typeof(DbAPINotFoundException),
		typeof(DbAPIUnknownErrorException), typeof(DbAPIUnavailableException), typeof(DbAPIObjectGraphDepthLimitExceededException),
		typeof(DbAPIObjectCountLimitExceededException), typeof(DatabaseException)
	}, null);

	ushort id;
	string name;
	ProtocolPropertyDescriptor[] inputParameters;
	ProtocolPropertyDescriptor returnValue;
	DbAPIOperationType operationType;
	DbAPIObjectGraphSupportType objectGraphSupport;

	HashSet<Type> allowedExceptions;

	MethodInfo targetMethod;

	private ProtocolOperationDescriptor()
	{
	}

	public ProtocolOperationDescriptor(ushort id, MethodInfo method, ProtocolDiscoveryContext discoveryContext, int paramSkipCount)
	{
		this.id = id;
		this.name = GetOperationName(method);
		this.targetMethod = method;

		DbAPIOperationAttribute dboa = method.GetCustomAttribute<DbAPIOperationAttribute>();
		operationType = dboa == null ? DbAPIOperationType.ReadWrite : dboa.OperationType;
		objectGraphSupport = dboa == null ? DbAPIObjectGraphSupportType.Both : dboa.ObjectGraphSupport;

		inputParameters = new ProtocolPropertyDescriptor[method.GetParameters().Length - paramSkipCount];
		ParameterInfo[] ps = method.GetParameters();
		int c = 0;
		for (int i = paramSkipCount; i < ps.Length; i++)
		{
			ParameterInfo pi = ps[i];
			if (pi.IsOut || pi.IsRetval)
			{
				throw DbAPIDefinitionException.CreateOutParam(this.name,
					ProtocolInterfaceDescriptor.GetAPIName(method.DeclaringType));
			}

			if (BuiltInTypesHelper.IsBuiltInType(pi.ParameterType))
			{
				inputParameters[c++] = new ProtocolPropertyDescriptor(BuiltInTypesHelper.To(pi.ParameterType), null, pi.Name);
			}
			else
			{
				ProtocolTypeDescriptor desc = discoveryContext.GetTypeDuringDiscovery(pi.ParameterType, true);
				inputParameters[c++] = new ProtocolPropertyDescriptor(BuiltInType.None, desc, pi.Name);
			}
		}

		if (inputParameters.Length > Connection.MaxRequestArguments)
		{
			throw DbAPIDefinitionException.CreateMaxParamExceeded(this.name,
				ProtocolInterfaceDescriptor.GetAPIName(method.DeclaringType));
		}

		Type retType = method.ReturnType;
		if (retType == typeof(Task))
			retType = typeof(void);
		else if (retType.IsGenericType && retType.GetGenericTypeDefinition() == typeof(Task<>))
			retType = retType.GetGenericArguments()[0];

		DbAPIOperationErrorAttribute[] excAttrs = method.GetCustomAttributes<DbAPIOperationErrorAttribute>(false).ToArray();

		allowedExceptions = new HashSet<Type>(excAttrs.Length + builtInErrorTypes.Count);

		returnValue = GetTypeDescriptor(retType, "return", discoveryContext);

		c = 0;
		ProtocolPropertyDescriptor[] errValues = new ProtocolPropertyDescriptor[excAttrs.Length + builtInErrorTypes.Count];
		foreach (Type errType in excAttrs.Select(x => x.Type).Concat(builtInErrorTypes))
		{
			if (!typeof(DbAPIErrorException).IsAssignableFrom(errType))
				throw DbAPIDefinitionException.CreateInvalidExceptionBaseType(errType.FullName);

			allowedExceptions.Add(errType);
			GetTypeDescriptor(errType, "Error", discoveryContext);
		}
	}

	public ushort Id => id;
	public string Name => name;
	public ProtocolPropertyDescriptor[] InputParameters => inputParameters;
	public ProtocolPropertyDescriptor ReturnValue => returnValue;
	public DbAPIOperationType OperationType => operationType;
	public DbAPIObjectGraphSupportType ObjectGraphSupport => objectGraphSupport;
	public MethodInfo TargetMethod { get => targetMethod; set => targetMethod = value; }

	public bool IsExceptionAllowed(Type type)
	{
		return allowedExceptions.Contains(type);
	}

	public static bool IsBuiltInExceptionType(Type type)
	{
		return builtInErrorTypes.Contains(type);
	}

	private ProtocolPropertyDescriptor GetTypeDescriptor(Type type, string name, ProtocolDiscoveryContext discoveryContext)
	{
		if (type == typeof(void))
		{
			return new ProtocolPropertyDescriptor(BuiltInType.None, null, name);
		}
		else if (BuiltInTypesHelper.IsBuiltInType(type))
		{
			return new ProtocolPropertyDescriptor(BuiltInTypesHelper.To(type), null, name);
		}
		else
		{
			return new ProtocolPropertyDescriptor(BuiltInType.None, discoveryContext.GetTypeDuringDiscovery(type, false), name);
		}
	}

	public static void Serialize(ProtocolOperationDescriptor desc, MessageWriter writer, SerializerContext context)
	{
		if (context.TryWriteInstance(writer, desc, 0, true))
			return;

		writer.WriteUShort(desc.id);
		writer.WriteString(desc.name);
		writer.WriteByte((byte)desc.operationType);
		writer.WriteByte((byte)desc.objectGraphSupport);
		writer.WriteInt(desc.inputParameters.Length);
		for (int i = 0; i < desc.inputParameters.Length; i++)
		{
			ProtocolPropertyDescriptor.Serialize(desc.inputParameters[i], writer, context);
		}

		ProtocolPropertyDescriptor.Serialize(desc.returnValue, writer, context);
	}

	public static ProtocolOperationDescriptor Deserialize(MessageReader reader, DeserializerContext context)
	{
		if (context.TryReadInstanceTyped(reader, out ProtocolOperationDescriptor p, 0) == ReadObjectResult.Ready)
			return p;

		p = new ProtocolOperationDescriptor();
		context.AddInstance(p);

		p.id = reader.ReadUShort();
		p.name = reader.ReadString();
		if (string.IsNullOrEmpty(p.name))
			throw new DbAPIProtocolException();

		p.operationType = (DbAPIOperationType)reader.ReadByte();
		p.objectGraphSupport = (DbAPIObjectGraphSupportType)reader.ReadByte();

		int c = reader.ReadInt();
		p.inputParameters = new ProtocolPropertyDescriptor[c];

		for (int i = 0; i < c; i++)
		{
			p.inputParameters[i] = ProtocolPropertyDescriptor.Deserialize(reader, context);
		}

		p.returnValue = ProtocolPropertyDescriptor.Deserialize(reader, context);

		return p;
	}

	public bool IsMatch(ProtocolOperationDescriptor operDesc, Dictionary<ProtocolTypeDescriptor, bool> checkedTypes)
	{
		Checker.AssertTrue(name.Equals(operDesc.Name));
		if (!name.Equals(operDesc.Name))
			return false;

		if (operationType != operDesc.operationType)
			return false;

		if (objectGraphSupport != operDesc.objectGraphSupport)
			return false;

		if (inputParameters.Length != operDesc.inputParameters.Length)
			return false;

		for (int i = 0; i < inputParameters.Length; i++)
		{
			if (!inputParameters[i].IsMatch(operDesc.inputParameters[i], true, checkedTypes))
				return false;
		}

		return returnValue.IsMatch(operDesc.returnValue, false, checkedTypes);
	}

	private static string GetOperationName(MethodInfo method)
	{
		DbAPIOperationAttribute cta = (DbAPIOperationAttribute)method.GetCustomAttribute(typeof(DbAPIOperationAttribute));
		if (cta == null || cta.Name == null)
			return method.Name;

		return cta.Name;
	}
}
