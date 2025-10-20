using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Runtime.InteropServices;
using ILVerify;
using VeloxDB.Common;
using Engine = VeloxDB.Storage.ModelUpdate;

namespace VeloxDB.Server;

internal static class ILValidation
{
	public record Result(DatabaseErrorDetail? Detail, bool Success);

	private static readonly Version runtimeVersion;

	static ILValidation()
	{
		AssemblyName runtimeName = Assembly.Load("System.Runtime").GetName();
		Debug.Assert(runtimeName.Version != null);
		runtimeVersion = runtimeName.Version;
	}

	public static Result Validate(IReadOnlyCollection<Engine.UserAssembly> toValidate)
	{
		using ReaderCollection readers = new ReaderCollection(toValidate);
		Verifier v = new Verifier(new Resolver(readers));
		v.SetSystemModuleName(new AssemblyNameInfo("System.Runtime"));

		DatabaseErrorDetail? error = null;

		foreach(var reader in readers)
		{
			error = CheckTargetFramework(reader);
			if (error != null)
				break;

			try
			{
				IEnumerable<VerificationResult> result = v.Verify(reader.PEReader);
				if(result.Any())
					error = new DatabaseErrorDetail(DatabaseErrorType.InvalidAssembly, primaryName:reader.Name);
			}
			catch (FileNotFoundException e)
			{
				error = new DatabaseErrorDetail(DatabaseErrorType.MissingReferencedAssembly, primaryName:reader.Name, secondaryName:e.FileName);
			}

			if(error != null)
				break;
		}

		return new Result(error, error == null);
	}

	private static DatabaseErrorDetail? CheckTargetFramework(Reader reader)
	{
		DatabaseErrorDetail? error = null;
		MetadataReader metadataReader = reader.PEReader.GetMetadataReader();
		string? targetVersion = GetTargetVersion(metadataReader);

		if (targetVersion != null && string.Compare(targetVersion, AppContext.TargetFrameworkName) > 0)
		{
			error = new DatabaseErrorDetail(DatabaseErrorType.InvalidAssemblyTargetFramework, primaryName: reader.Name,
											secondaryName: targetVersion, memberName: AppContext.TargetFrameworkName);
		}
		else
		{
			Version? targetRuntimeVersion;
			targetRuntimeVersion = GetRuntimeVersion(metadataReader);
			if(targetRuntimeVersion == null)
			{
				error = new DatabaseErrorDetail(DatabaseErrorType.InvalidAssembly, primaryName: reader.Name);
			}else if(targetRuntimeVersion > runtimeVersion)
			{
				error = new DatabaseErrorDetail(DatabaseErrorType.InvalidAssemblyTargetFramework, primaryName: reader.Name,
												secondaryName: targetRuntimeVersion.ToString(), memberName: runtimeVersion.ToString());
			}

		}

		return error;
	}

	private static Version? GetRuntimeVersion(MetadataReader metadataReader)
	{
		foreach (AssemblyReferenceHandle arHandle in metadataReader.AssemblyReferences)
		{
			AssemblyReference ar = metadataReader.GetAssemblyReference(arHandle);
			string name = metadataReader.GetString(ar.Name);
			if (name == "System.Runtime" || name == "System.Private.CoreLib")
			{
				return ar.Version;
			}
		}

		return null;
	}

	private static string? GetTargetVersion(MetadataReader metadataReader)
	{
		string? targetVersion = null;

		AssemblyDefinition assemblyDefinition = metadataReader.GetAssemblyDefinition();
		CustomAttributeHandleCollection customAttributes = assemblyDefinition.GetCustomAttributes();

		foreach (CustomAttributeHandle customAttributeHandle in customAttributes)
		{
			CustomAttribute customAttribute = metadataReader.GetCustomAttribute(customAttributeHandle);

			if (GetAttributeName(metadataReader, customAttribute) == "TargetFrameworkAttribute")
			{
				CustomAttributeValue<string> decoded = customAttribute.DecodeValue<string>(StringCustomAttributeTypeProvider.Default);
				if (decoded.FixedArguments.Length > 0)
				{
					targetVersion = (string?)decoded.FixedArguments[0].Value;
				}
			}
		}

		return targetVersion;
	}

	private static string GetAttributeName(MetadataReader metadataReader, CustomAttribute customAttribute)
	{
		EntityHandle attributeTypeHandle;
		if (customAttribute.Constructor.Kind == HandleKind.MethodDefinition)
		{
			attributeTypeHandle = metadataReader.GetMethodDefinition((MethodDefinitionHandle)customAttribute.Constructor).GetDeclaringType();
		}
		else if (customAttribute.Constructor.Kind == HandleKind.MemberReference)
		{
			attributeTypeHandle = metadataReader.GetMemberReference((MemberReferenceHandle)customAttribute.Constructor).Parent;
		}
		else
		{
			throw new InvalidOperationException();
		}

		StringHandle attributeTypeNameHandle;
		if (attributeTypeHandle.Kind == HandleKind.TypeDefinition)
		{
			attributeTypeNameHandle = metadataReader.GetTypeDefinition((TypeDefinitionHandle)attributeTypeHandle).Name;
		}
		else if (attributeTypeHandle.Kind == HandleKind.TypeReference)
		{
			attributeTypeNameHandle = metadataReader.GetTypeReference((TypeReferenceHandle)attributeTypeHandle).Name;
		}
		else
		{
			throw new InvalidOperationException();
		}

		return metadataReader.GetString(attributeTypeNameHandle);
	}


	private class StringCustomAttributeTypeProvider : ICustomAttributeTypeProvider<string>
	{
		public static ICustomAttributeTypeProvider<string> Default { get; private set; } = new StringCustomAttributeTypeProvider();

		public string GetPrimitiveType(PrimitiveTypeCode typeCode)
		{
			if(typeCode == PrimitiveTypeCode.String)
				return "System.String";
			throw new NotSupportedException();
		}

		public string GetSystemType()
		{
			throw new NotSupportedException();
		}

		public string GetSZArrayType(string elementType)
		{
			throw new NotSupportedException();
		}

		public string GetTypeFromDefinition(MetadataReader reader, TypeDefinitionHandle handle, byte rawTypeKind)
		{
			throw new NotSupportedException();
		}

		public string GetTypeFromReference(MetadataReader reader, TypeReferenceHandle handle, byte rawTypeKind)
		{
			throw new NotSupportedException();
		}

		public string GetModifiedType(string modifier, string unmodifiedType, bool isRequired)
		{
			throw new NotSupportedException();
		}

		public string GetPointerType(string elementType)
		{
			throw new NotSupportedException();
		}

		public string GetGenericInstantiation(string genericType, ImmutableArray<string> typeArguments)
		{
			throw new NotSupportedException();
		}

		public string GetFunctionPointerType(MethodSignature<string> signature)
		{
			throw new NotSupportedException();
		}

		public string GetGenericMethodParameter(int index)
		{
			throw new NotSupportedException();
		}

		public string GetGenericTypeParameter(int index)
		{
			throw new NotSupportedException();
		}

		public string GetTypeFromSerializedName(string name)
		{
			throw new NotSupportedException();
		}

		public PrimitiveTypeCode GetUnderlyingEnumType(string name)
		{
			throw new NotSupportedException();
		}

		public bool IsSystemType(string s)
		{
			throw new NotSupportedException();
		}
	}

	private class Reader : IDisposable
	{
		GCHandle handle;
		public string Name {get; private set;}
		public PEReader PEReader {get; private set;}

		public unsafe Reader(string originalName, byte[] assembly)
		{
			Name = originalName;

			handle = GCHandle.Alloc(assembly, GCHandleType.Pinned);
			byte* pinned = (byte*)handle.AddrOfPinnedObject().ToPointer();
			PEReader = new PEReader(pinned, assembly.Length);
		}

		~Reader()
		{
			CleanUp();
		}

		public void Dispose()
		{
			PEReader.Dispose();
			CleanUp();
			GC.SuppressFinalize(this);
		}

		private void CleanUp()
		{
			handle.Free();
		}
	}

	private class ReaderCollection : IEnumerable<Reader>, IDisposable
	{
		Reader[] readers;

		public ReaderCollection(IReadOnlyCollection<Engine.UserAssembly> assemblies)
		{
			readers = new Reader[assemblies.Count];
			int i = 0;
			foreach(Engine.UserAssembly assembly in assemblies)
			{
				readers[i] = new Reader(assembly.Name, assembly.Binary);
				i++;
			}
		}

		public IEnumerator<Reader> GetEnumerator()
		{
			return ((IEnumerable<Reader>)readers).GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return readers.GetEnumerator();
		}

		public void Dispose()
		{
			foreach(Reader reader in readers)
			{
				reader.Dispose();
			}
		}
	}

	private class Resolver : IResolver
	{
		static readonly string[] sysPaths;

		static Resolver()
		{
			Assembly[] assemblies = new Assembly[]{typeof(Console).Assembly, typeof(Resolver).Assembly, typeof(IResolver).Assembly};
			sysPaths = new string[assemblies.Length];

			for (int i = 0; i < assemblies.Length; i++)
			{
				Assembly assembly = assemblies[i];
				string? path = Path.GetDirectoryName(assembly.Location);

				Checker.AssertNotNull(path);

				sysPaths[i] = path;
			}
		}

		ConcurrentDictionary<string, PEReader> cache;

		public Resolver(ReaderCollection readers)
		{
			cache = new ConcurrentDictionary<string, PEReader>();

			foreach(Reader reader in readers)
			{
				string name = Path.GetFileNameWithoutExtension(reader.Name);
				bool success = cache.TryAdd(name, reader.PEReader);
				Checker.AssertTrue(success);
			}
		}

		private PEReader ResolveCore(string simpleName)
		{
			for(int i = 0; i < sysPaths.Length; i++)
			{
				string path = Path.Combine(sysPaths[i], $"{simpleName}.dll");
				if (File.Exists(path))
					return new PEReader(File.OpenRead(path));
			}

			throw new FileNotFoundException("Referenced assembly {0} could not be found.", simpleName);
		}

		public PEReader Resolve(string simpleName)
		{
			return cache.GetOrAdd(simpleName, ResolveCore);
		}

		public PEReader ResolveAssembly(AssemblyNameInfo assemblyName)
		{
			Debug.Assert(assemblyName.Name != null);
			return Resolve(assemblyName.Name);
		}

		public PEReader ResolveModule(AssemblyNameInfo referencingAssembly, string fileName)
		{
			return Resolve(Path.GetFileNameWithoutExtension(fileName));
		}
	}
}

