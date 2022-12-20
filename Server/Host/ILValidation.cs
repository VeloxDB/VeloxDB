using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Globalization;
using System.Reflection;
using System.Reflection.PortableExecutable;
using System.Runtime.InteropServices;
using ILVerify;
using VeloxDB.Common;
using Engine = VeloxDB.Storage.ModelUpdate;

namespace VeloxDB.Server;

internal static class ILValidation
{
	public record Result(DatabaseErrorDetail? Detail, bool Success);
	public static Result Validate(IReadOnlyCollection<Engine.UserAssembly> toValidate)
	{
		using ReaderCollection readers = new ReaderCollection(toValidate);
		Verifier v = new Verifier(new Resolver(readers));
		v.SetSystemModuleName(new AssemblyName("System.Runtime"));

		DatabaseErrorDetail? error = null;

		foreach(var reader in readers)
		{
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
	}
}

