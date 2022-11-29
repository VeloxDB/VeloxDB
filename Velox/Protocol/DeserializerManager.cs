using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Velox.Common;
using Velox.Networking;
using static Velox.Protocol.ProtocolTypeDescriptor;

namespace Velox.Protocol;

internal delegate object ProtocolDeserializeDelegate(MessageReader reader,
	DeserializerContext context, Delegate[] deserializerTable, int depth);

internal delegate void ProtocolSkipDelegate(MessageReader reader,
	DeserializerContext context, Delegate[] deserializerTable, int depth);

internal sealed class DeserializerManager
{
	readonly object sync = new object();

	ModuleBuilder moduleBuilder;

	Dictionary<string, TypeDeserializerEntry> typeEntriesByName;
	Dictionary<Type, InterfaceDeserializerEntry> interfaceEntries;

	public DeserializerManager()
	{
		AssemblyName aName = new AssemblyName("__dynprotmodule");
		AssemblyBuilder ab = AssemblyBuilder.DefineDynamicAssembly(aName, AssemblyBuilderAccess.RunAndCollect);
		moduleBuilder = ab.DefineDynamicModule(aName.Name);

		typeEntriesByName = new Dictionary<string, TypeDeserializerEntry>(2);
		interfaceEntries = new Dictionary<Type, InterfaceDeserializerEntry>(2, ReferenceEqualityComparer<Type>.Instance);
	}

	public ModuleBuilder ModuleBuilder => moduleBuilder;

	public Delegate[] GetDeserializerTable()
	{
		int maxId = typeEntriesByName.Values.Select(x => x.TypeDesc.Id).DefaultIfEmpty().Max();
		Delegate[] r = new Delegate[maxId + 1];
		typeEntriesByName.Values.ForEach(x => r[x.TypeDesc.Id] = x.Delegate);
		return r;
	}

	public TypeDeserializerEntry GetTypeEntryByName(string name)
	{
		typeEntriesByName.TryGetValue(name, out TypeDeserializerEntry entry);
		return entry;
	}

	public MethodInfo[] GetInterfaceDeserializers(ProtocolInterfaceDescriptor interfaceDesc, IList<ProtocolTypeDescriptor> types,
		ProtocolInterfaceDirection direction, int paramSkipCount)
	{
		Checker.AssertNotNull(interfaceDesc.TargetType);
		if (interfaceEntries.TryGetValue(interfaceDesc.TargetType, out InterfaceDeserializerEntry interfaceEntry))
			return interfaceEntry.Methods;

		lock (sync)
		{
			if (interfaceEntries.TryGetValue(interfaceDesc.TargetType, out interfaceEntry))
				return interfaceEntry.Methods;

			TypeBuilder tb = moduleBuilder.DefineType("__" + Guid.NewGuid().ToString("N"),
				TypeAttributes.Class | TypeAttributes.Public, typeof(object));

			GenerateTypeEntries(tb, types);
			GenerateTypeCode(types);

			MethodInfo[] methods = interfaceDesc.Operations.Select(x => x.TargetMethod).ToArray();
			interfaceEntry = CreateInterfaceEntry(tb, interfaceDesc, methods, interfaceDesc.Operations, direction, paramSkipCount);

			Type builtType = tb.CreateType();

			FinalizeInterfaceMethods(interfaceEntry, builtType);
			GenerateTypeDelegates(types, builtType);

			var newInterfaceEntries = new Dictionary<Type, InterfaceDeserializerEntry>(interfaceEntries, ReferenceEqualityComparer<Type>.Instance);
			newInterfaceEntries.Add(interfaceDesc.TargetType, interfaceEntry);

			Thread.MemoryBarrier();
			interfaceEntries = newInterfaceEntries;
		}

		return interfaceEntry.Methods;
	}

	private void FinalizeInterfaceMethods(InterfaceDeserializerEntry entry, Type builtType)
	{
		for (int i = 0; i < entry.Methods.Length; i++)
		{
			if (entry.Methods[i] != null)
				entry.Methods[i] = builtType.GetMethod(entry.Methods[i].Name, BindingFlags.Static | BindingFlags.Public);
		}
	}

	private InterfaceDeserializerEntry CreateInterfaceEntry(TypeBuilder tb, ProtocolInterfaceDescriptor desc, MethodInfo[] methods,
		ProtocolOperationDescriptor[] ops, ProtocolInterfaceDirection direction, int paramSkipCount)
	{
		InterfaceDeserializerEntry ie = new InterfaceDeserializerEntry();
		ie.Methods = new MethodInfo[methods.Length];
		for (int i = 0; i < methods.Length; i++)
		{
			if (methods[i] != null)
				ie.Methods[i] = GenerateMethod(tb, methods[i], ops[i], direction, ops[i].ObjectGraphSupport, paramSkipCount);
		}

		return ie;
	}

	private MethodInfo GenerateMethod(TypeBuilder tb, MethodInfo method, ProtocolOperationDescriptor opDesc,
		ProtocolInterfaceDirection direction, DbAPIObjectGraphSupportType objectGraphSupport, int paramSkipCount)
	{
		Type[] args = EmptyArray<Type>.Instance;
		if (direction == ProtocolInterfaceDirection.Request)
		{
			args = method.GetParameters().Skip(paramSkipCount).Select(x => x.ParameterType.MakeByRefType()).ToArray();
		}
		else if (method.ReturnType != typeof(void) && method.ReturnType != typeof(Task))
		{
			Type argType;
			if (method.ReturnType.IsGenericType && method.ReturnType.GetGenericTypeDefinition() == typeof(Task<>))
			{
				argType = method.ReturnType.GetGenericArguments()[0];
			}
			else
			{
				argType = method.ReturnType;
			}

			if (argType != typeof(void))
				args = new Type[] { argType.MakeByRefType() };
		}

		Type[] delegateArgs = new Type[args.Length + 2];
		Array.Copy(args, 0, delegateArgs, 2, args.Length);
		delegateArgs[0] = typeof(MessageReader);
		delegateArgs[1] = typeof(ProtocolDeserializeDelegate[]);

		MethodBuilder m = tb.DefineMethod("__" +
			Guid.NewGuid().ToString("N"), MethodAttributes.Public | MethodAttributes.Static, typeof(void), delegateArgs);

		m.SetImplementationFlags(MethodImplAttributes.AggressiveOptimization);

		ILGenerator il = m.GetILGenerator();

		bool supportGraphs = direction == ProtocolInterfaceDirection.Request ?
			((objectGraphSupport & DbAPIObjectGraphSupportType.Request) != 0) :
			((objectGraphSupport & DbAPIObjectGraphSupportType.Response) != 0);

		LocalBuilder contextVar = il.DeclareLocal(typeof(DeserializerContext));
		il.Emit(OpCodes.Ldc_I4, supportGraphs ? 1 : 0);
		il.Emit(OpCodes.Call, Methods.DeserializerContextGetMethod);
		il.Emit(OpCodes.Stloc, contextVar);

		for (int i = 0; i < args.Length; i++)
		{
			Type argType = args[i].GetElementType();
			il.Emit(OpCodes.Ldarg, i + 2);  // Load out argument (its address)
			il.Emit(OpCodes.Ldarg_0);       // Load reader

			if (BuiltInTypesHelper.IsBuiltInType(argType))
			{
				// Read property value directly from reader
				il.Emit(OpCodes.Call, BuiltInTypesHelper.GetReadMethod(BuiltInTypesHelper.To(argType)));
				OpCode storeOpCode = BuiltInTypesHelper.GetIndStoreInstruction(BuiltInTypesHelper.To(argType));
				if (storeOpCode == OpCodes.Stobj)
				{
					il.Emit(OpCodes.Stobj, argType);
				}
				else
				{
					il.Emit(storeOpCode);
				}
			}
			else
			{
				il.Emit(OpCodes.Ldloc, contextVar);     // Load context
				il.Emit(OpCodes.Ldarg_1);               // Load deserializer table
				il.Emit(OpCodes.Ldc_I4, 1);             // Load depth

				ProtocolPropertyDescriptor pd = direction == ProtocolInterfaceDirection.Request ? opDesc.InputParameters[i] : opDesc.ReturnValue;
				TypeDeserializerEntry e = typeEntriesByName[pd.TypeDesc.Name];
				if (e.TypeDesc.Type != ProtocolTypeType.Class || !((ProtocolClassDescriptor)e.TypeDesc).CanBeInherited)
				{
					// Call deserializer method for the property type (sending writer and property value)
					il.Emit(OpCodes.Call, e.Method);
				}
				else
				{
					il.Emit(OpCodes.Call, Methods.DeserializePolymorphMethod);
				}

				if (!e.TypeDesc.IsValueType)
				{
					il.Emit(OpCodes.Stind_Ref);
				}
				else
				{
					il.Emit(OpCodes.Stobj, argType);
				}
			}
		}

		il.Emit(OpCodes.Ldloc, contextVar);
		il.Emit(OpCodes.Ldarg_0);
		il.Emit(OpCodes.Ldarg_1);
		il.Emit(OpCodes.Call, Methods.DeserializerContextDeserQueuedMethod);

		il.Emit(OpCodes.Ldloc, contextVar);
		il.Emit(OpCodes.Call, Methods.DeserializerContextResetMethod);

		il.Emit(OpCodes.Ret);

		return m;
	}

	private void GenerateTypeEntries(TypeBuilder tb, IList<ProtocolTypeDescriptor> types)
	{
		Dictionary<string, TypeDeserializerEntry> newTypeEntries =
			new Dictionary<string, TypeDeserializerEntry>(typeEntriesByName.Count + types.Count);
		typeEntriesByName.ForEach(x => newTypeEntries.Add(x.Key, x.Value));

		foreach (ProtocolTypeDescriptor typeDesc in types)
		{
			if (!newTypeEntries.ContainsKey(typeDesc.Name))
			{
				TypeDeserializerEntry e = new TypeDeserializerEntry(typeDesc);

				if (!typeDesc.IsAbstract)
				{
					Type returnType = typeDesc.TargetType == null ? null : (typeDesc.IsValueType ? typeDesc.TargetType : typeof(object));
					e.Method = tb.DefineMethod("__" + Guid.NewGuid().ToString("N"), MethodAttributes.Static | MethodAttributes.Public, returnType,
						new Type[] { typeof(MessageReader), typeof(DeserializerContext), typeof(Delegate[]), typeof(int) });

					((MethodBuilder)e.Method).SetImplementationFlags(
						MethodImplAttributes.AggressiveInlining | MethodImplAttributes.AggressiveOptimization);
				}

				newTypeEntries.Add(typeDesc.Name, e);
			}
		}

		Thread.MemoryBarrier();
		typeEntriesByName = newTypeEntries;
	}

	private void GenerateTypeCode(IList<ProtocolTypeDescriptor> types)
	{
		foreach (ProtocolTypeDescriptor typeDesc in types)
		{
			if (typeDesc.IsAbstract)
				continue;

			TypeDeserializerEntry e = typeEntriesByName[typeDesc.Name];
			if (e.IsFinished)
				continue;

			ILGenerator il = ((MethodBuilder)e.Method).GetILGenerator();
			e.TypeDesc.GenerateDeserializerCode(il, this);
			e.IsFinished = true;
		}
	}

	private void GenerateTypeDelegates(IList<ProtocolTypeDescriptor> types, Type builtType)
	{
		for (int i = 0; i < types.Count; i++)
		{
			TypeDeserializerEntry e = typeEntriesByName[types[i].Name];
			if (e.TypeDesc.IsAbstract || e.Delegate != null)
				continue;

			e.Method = builtType.GetMethod(e.Method.Name, BindingFlags.Public | BindingFlags.Static);
			if (e.TypeDesc.Type != ProtocolTypeType.Class || ((ProtocolClassDescriptor)e.TypeDesc).IsRefType)
			{
				if (e.Method.ReturnType == typeof(void))
					e.Delegate = (ProtocolSkipDelegate)e.Method.CreateDelegate(typeof(ProtocolSkipDelegate));
				else
					e.Delegate = (ProtocolDeserializeDelegate)e.Method.CreateDelegate(typeof(ProtocolDeserializeDelegate));
			}
		}
	}
}

internal sealed class InterfaceDeserializerEntry
{
	public MethodInfo[] Methods { get; set; }
}

internal sealed class TypeDeserializerEntry
{
	public ProtocolTypeDescriptor TypeDesc { get; private set; }
	public Delegate Delegate { get; set; }
	public MethodInfo Method { get; set; }
	public bool IsFinished { get; set; }

	public TypeDeserializerEntry(ProtocolTypeDescriptor typeDesc)
	{
		this.TypeDesc = typeDesc;
	}
}
