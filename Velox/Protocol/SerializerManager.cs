using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Velox.Client;
using Velox.Common;
using Velox.Networking;
using static Velox.Protocol.ProtocolTypeDescriptor;

namespace Velox.Protocol;

internal delegate void ProtocolSerializeDelegate(MessageWriter writer, object value, SerializerContext context, int depth);

internal sealed class SerializerManager
{
	public const ushort FormatVersion = 1;

	readonly object sync = new object();

	ModuleBuilder moduleBuilder;
	Dictionary<Type, TypeSerializerEntry> typeEntries;
	Dictionary<Type, InterfaceSerializerEntry> interfaceEntries;

	public SerializerManager(ModuleBuilder moduleBuilder)
	{
		this.moduleBuilder = moduleBuilder;

		typeEntries = new Dictionary<Type, TypeSerializerEntry>(2, ReferenceEqualityComparer<Type>.Instance);
		interfaceEntries = new Dictionary<Type, InterfaceSerializerEntry>(2, ReferenceEqualityComparer<Type>.Instance);
	}

	public TypeSerializerEntry GetTypeSerializer(Type type)
	{
		typeEntries.TryGetValue(type, out TypeSerializerEntry e);
		return e;
	}

	public Delegate[] GetInterfaceSerializers(ProtocolInterfaceDescriptor interfaceDesc, IList<ProtocolTypeDescriptor> types,
		ProtocolInterfaceDirection direction, int paramSkipCount)
	{
		Checker.AssertNotNull(interfaceDesc.TargetType);
		if (interfaceEntries.TryGetValue(interfaceDesc.TargetType, out InterfaceSerializerEntry interfaceEntry))
			return interfaceEntry.Delegates;

		lock (sync)
		{
			if (interfaceEntries.TryGetValue(interfaceDesc.TargetType, out interfaceEntry))
				return interfaceEntry.Delegates;

			TypeBuilder tb = moduleBuilder.DefineType("__" + Guid.NewGuid().ToString("N"),
				TypeAttributes.Class | TypeAttributes.Public, typeof(object));

			CreateTypeEntries(tb, types);
			GenerateTypesCode(types);

			MethodInfo[] methods = interfaceDesc.Operations.Select(x => x.TargetMethod).ToArray();
			interfaceEntry = CreateInterfaceEntry(tb, interfaceDesc, methods, direction, paramSkipCount);

			Type builtType = tb.CreateType();

			CreateInterfaceDelegates(interfaceEntry, builtType);
			GenerateTypeDelegates(types, builtType);

			Dictionary<Type, InterfaceSerializerEntry> newInterfaceEntries =
				new Dictionary<Type, InterfaceSerializerEntry>(interfaceEntries, ReferenceEqualityComparer<Type>.Instance);

			newInterfaceEntries.Add(interfaceDesc.TargetType, interfaceEntry);

			Thread.MemoryBarrier();
			interfaceEntries = newInterfaceEntries;
		}

		return interfaceEntry.Delegates;
	}

	private InterfaceSerializerEntry CreateInterfaceEntry(TypeBuilder tb, ProtocolInterfaceDescriptor desc, MethodInfo[] methods,
		ProtocolInterfaceDirection direction, int paramSkipCount)
	{
		InterfaceSerializerEntry ie = new InterfaceSerializerEntry();
		ie.Methods = new MethodInfo[methods.Length];
		for (int i = 0; i < methods.Length; i++)
		{
			if (methods[i] != null)
				ie.Methods[i] = GenerateMethod(tb, desc, desc.Operations[i], methods[i], direction, paramSkipCount);
		}

		return ie;
	}

	private void CreateInterfaceDelegates(InterfaceSerializerEntry entry, Type builtType)
	{
		entry.Delegates = new Delegate[entry.Methods.Length];
		for (int i = 0; i < entry.Methods.Length; i++)
		{
			if (entry.Methods[i] != null)
			{
				entry.Methods[i] = builtType.GetMethod(entry.Methods[i].Name, BindingFlags.Static | BindingFlags.Public);
				entry.Delegates[i] = CreateMethodDelegate(entry.Methods[i]);
			}
		}
	}

	private MethodBuilder GenerateMethod(TypeBuilder tb, ProtocolInterfaceDescriptor interfaceDesc, ProtocolOperationDescriptor opDesc,
		MethodInfo method, ProtocolInterfaceDirection direction, int paramSkipCount)
	{
		Type[] args = EmptyArray<Type>.Instance;
		if (direction == ProtocolInterfaceDirection.Request)
		{
			args = method.GetParameters().Select(x => x.ParameterType).Skip(paramSkipCount).ToArray();
		}
		else if (method.ReturnType != typeof(void) && method.ReturnType != typeof(DatabaseTask))
		{
			if (method.ReturnType.IsGenericType && method.ReturnType.GetGenericTypeDefinition() == typeof(DatabaseTask<>))
			{
				args = new Type[] { method.ReturnType.GetGenericArguments()[0] };
			}
			else
			{
				args = new Type[] { method.ReturnType };
			}
		}

		Type[] delegateArgs = new Type[args.Length + 1];
		Array.Copy(args, 0, delegateArgs, 1, args.Length);
		delegateArgs[0] = typeof(MessageWriter);

		MethodBuilder m = tb.DefineMethod("__" + Guid.NewGuid().ToString("N"),
			MethodAttributes.Static | MethodAttributes.Public, typeof(void), delegateArgs);

		m.SetImplementationFlags(MethodImplAttributes.AggressiveOptimization);

		ILGenerator il = m.GetILGenerator();

		if (direction == ProtocolInterfaceDirection.Request)
		{
			// Write FormatVersion
			il.Emit(OpCodes.Ldarg_0);       // Load writer
			il.Emit(OpCodes.Ldc_I4, SerializerManager.FormatVersion);
			il.Emit(OpCodes.Call, Methods.WriteUShortMethod);

			// Write RequestType.Operation
			il.Emit(OpCodes.Ldarg_0);       // Load writer
			il.Emit(OpCodes.Ldc_I4, (int)RequestType.Operation);
			il.Emit(OpCodes.Call, Methods.WriteByteMethod);

			// Write interface and operation ids
			il.Emit(OpCodes.Ldarg_0);       // Load writer
			il.Emit(OpCodes.Ldc_I4, (int)interfaceDesc.Id);
			il.Emit(OpCodes.Call, Methods.WriteUShortMethod);
			il.Emit(OpCodes.Ldarg_0);       // Load writer
			il.Emit(OpCodes.Ldc_I4, (int)opDesc.Id);
			il.Emit(OpCodes.Call, Methods.WriteUShortMethod);
		}
		else
		{
			// Write ResponseType.Response
			il.Emit(OpCodes.Ldarg_0);       // Load writer
			il.Emit(OpCodes.Ldc_I4, (int)ResponseType.Response);
			il.Emit(OpCodes.Call, Methods.WriteByteMethod);
		}

		LocalBuilder contextVar = il.DeclareLocal(typeof(SerializerContext));
		il.Emit(OpCodes.Call, Methods.SerializerContextGetMethod);
		il.Emit(OpCodes.Stloc, contextVar);

		for (int i = 0; i < args.Length; i++)
		{
			Type argType = args[i];
			il.Emit(OpCodes.Ldarg_0);       // Load writer
			il.Emit(OpCodes.Ldarg, i + 1);  // Load value

			if (BuiltInTypesHelper.IsBuiltInType(argType))
			{
				// Write property value directly to writer
				il.Emit(OpCodes.Call, BuiltInTypesHelper.GetWriteMethod(BuiltInTypesHelper.To(argType)));
			}
			else
			{
				il.Emit(OpCodes.Ldloc, contextVar);     // Load context
				il.Emit(OpCodes.Ldc_I4, 1);     // Load depth

				typeEntries.TryGetValue(argType, out TypeSerializerEntry e);
				if (e != null && (e.TypeDesc.Type != ProtocolTypeType.Class || !((ProtocolClassDescriptor)e.TypeDesc).CanBeInherited))
				{
					// Call serializer method for the property type (sending writer and property value)
					il.Emit(OpCodes.Call, e.Method);
				}
				else
				{
					il.Emit(OpCodes.Call, Methods.SerializePolymorphMethod);
				}
			}
		}

		il.Emit(OpCodes.Ldloc, contextVar);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Call, Methods.SerializerContextSerQueuedMethod);

		il.Emit(OpCodes.Ret);

		return m;
	}

	private static Delegate CreateMethodDelegate(MethodInfo m)
	{
		Type[] args = m.GetParameters().Skip(1).Select(x => x.ParameterType).ToArray();

		if (args.Length == 0)
		{
			return m.CreateDelegate(typeof(SerializerDelegate));
		}
		else if (args.Length == 1)
		{
			return m.CreateDelegate(typeof(SerializerDelegate<>).MakeGenericType(args));
		}
		else if (args.Length == 2)
		{
			return m.CreateDelegate(typeof(SerializerDelegate<,>).MakeGenericType(args));
		}
		else if (args.Length == 3)
		{
			return m.CreateDelegate(typeof(SerializerDelegate<,,>).MakeGenericType(args));
		}
		else if (args.Length == 4)
		{
			return m.CreateDelegate(typeof(SerializerDelegate<,,,>).MakeGenericType(args));
		}
		else if (args.Length == 5)
		{
			return m.CreateDelegate(typeof(SerializerDelegate<,,,,>).MakeGenericType(args));
		}
		else if (args.Length == 6)
		{
			return m.CreateDelegate(typeof(SerializerDelegate<,,,,,>).MakeGenericType(args));
		}
		else if (args.Length == 7)
		{
			return m.CreateDelegate(typeof(SerializerDelegate<,,,,,,>).MakeGenericType(args));
		}
		else
		{
			return m.CreateDelegate(typeof(SerializerDelegate<,,,,,,,>).MakeGenericType(args));
		}
	}

	private void CreateTypeEntries(TypeBuilder tb, IList<ProtocolTypeDescriptor> types)
	{
		Dictionary<Type, TypeSerializerEntry> newTypeEntries =
			new Dictionary<Type, TypeSerializerEntry>(typeEntries.Count + types.Count, ReferenceEqualityComparer<Type>.Instance);
		typeEntries.ForEach(x => newTypeEntries.Add(x.Key, x.Value));

		foreach (ProtocolTypeDescriptor typeDesc in types)
		{
			if (typeDesc.TargetType == null || typeDesc.TargetType.IsAbstract || newTypeEntries.ContainsKey(typeDesc.TargetType))
				continue;

			TypeSerializerEntry e = new TypeSerializerEntry(typeDesc);
			e.Method = tb.DefineMethod("__" + Guid.NewGuid().ToString("N"),
				MethodAttributes.Static | MethodAttributes.Public, typeof(void), new Type[] {
				typeof(MessageWriter), typeDesc.IsValueType ? typeDesc.TargetType : typeof(object), typeof(SerializerContext), typeof(int) });

			((MethodBuilder)e.Method).SetImplementationFlags(
				MethodImplAttributes.AggressiveInlining | MethodImplAttributes.AggressiveOptimization);

			newTypeEntries.Add(typeDesc.TargetType, e);
		}

		Thread.MemoryBarrier();
		typeEntries = newTypeEntries;
	}

	private void GenerateTypesCode(IList<ProtocolTypeDescriptor> types)
	{
		foreach (ProtocolTypeDescriptor typeDesc in types)
		{
			if (typeDesc.TargetType == null || !typeEntries.TryGetValue(typeDesc.TargetType, out TypeSerializerEntry e))
				continue;

			if (e.IsFinished)
				continue;

			ILGenerator il = ((MethodBuilder)e.Method).GetILGenerator();
			typeDesc.GenerateSerializerCode(il, this);
			e.IsFinished = true;
		}
	}

	private void GenerateTypeDelegates(IList<ProtocolTypeDescriptor> types, Type builtType)
	{
		foreach (ProtocolTypeDescriptor typeDesc in types)
		{
			if (typeDesc.TargetType == null || !typeEntries.TryGetValue(typeDesc.TargetType, out TypeSerializerEntry e) || e.Delegate != null)
				continue;

			e.Method = builtType.GetMethod(e.Method.Name, BindingFlags.Static | BindingFlags.Public);
			if (e.TypeDesc.Type != ProtocolTypeType.Class || ((ProtocolClassDescriptor)e.TypeDesc).IsRefType)
				e.Delegate = (ProtocolSerializeDelegate)e.Method.CreateDelegate(typeof(ProtocolSerializeDelegate));
		}
	}
}

internal sealed class InterfaceSerializerEntry
{
	public Delegate[] Delegates { get; set; }
	public MethodInfo[] Methods { get; set; }
}

internal sealed class TypeSerializerEntry
{
	public ProtocolTypeDescriptor TypeDesc { get; set; }
	public ProtocolSerializeDelegate Delegate { get; set; }
	public MethodInfo Method { get; set; }
	public bool IsFinished { get; set; }

	public TypeSerializerEntry(ProtocolTypeDescriptor typeDesc)
	{
		this.TypeDesc = typeDesc;
	}
}
