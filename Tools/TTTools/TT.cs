using System.IO;
using System.Diagnostics;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

struct SourceFile
{
	public ushort Id {get; set;}
	public string Filename {get; set;}

	public SourceFile(ushort id, string filename)
	{
		Id = id;
		Filename = filename;
	}

	public static SourceFile FromStream(BinaryReader reader)
	{
		ushort id = reader.ReadUInt16();
		ushort length = reader.ReadUInt16();
		char[] chars = reader.ReadChars(length);
		return new SourceFile(id, new string(chars));
	}
}


private enum VariableType : byte
{
	Byte = 1,
	SByte = 2,
	Short = 3,
	UShort = 4,
	Int = 5,
	UInt = 6,
	Long = 7,
	ULong = 8,
	Float = 9,
	Double = 10,
	Bool = 11,
	String = 12
}

class TTEntryLocationComparer : IEqualityComparer<TTEntry>
{
    public bool Equals(TTEntry x, TTEntry y)
    {
        if (ReferenceEquals(x, y))
            return true;

        if (x is null || y is null)
            return false;

        return x.FileId == y.FileId && x.LineNumber == y.LineNumber;
    }

    public int GetHashCode(TTEntry obj)
    {
        unchecked
        {
            int hash = 17;
            hash = hash * 23 + obj.FileId.GetHashCode();
            hash = hash * 23 + obj.LineNumber.GetHashCode();
            return hash;
        }
    }
}

class TTEntry
{
	public byte ArgCount {get; set;}
	public int TId {get; set;}
	public ushort FileId {get; set;}
	public ushort LineNumber {get; set;}
	public int Index {get; set;}

	public object[] Values {get; set;}

	public TimeTravel Owner { get; set; }

	public TTEntry(byte argCount, int tid, ushort fileId, ushort lineNumber, object[] values)
	{
		this.ArgCount = argCount;
		this.TId = tid;
		this.FileId = fileId;
		this.LineNumber = lineNumber;
		this.Values = values;
	}

	public static TTEntry FromStream(BinaryReader reader)
	{
		byte argCount = reader.ReadByte();
		Debug.Assert((argCount & 15) == 3);

		argCount = (byte)(argCount >> 4);
		int tid = reader.ReadInt32();
		ushort fileId = reader.ReadUInt16();
		ushort lineNumber = reader.ReadUInt16();

		object[] values = new object[argCount];

		for(int i = 0; i < argCount; i++)
		{
			byte type = reader.ReadByte();
			values[i] = readers[type](reader);
		}

		return new TTEntry(argCount, tid, fileId, lineNumber, values);
	}

	public bool Any<T>(T t) => Values.Any(v => v.Equals(t));

	public bool SameLocation(TTEntry other)
	{
		return FileId == other.FileId && LineNumber == other.LineNumber;
	}

	public string ToHTML() => Owner.ToHTML(this);

	static readonly Func<BinaryReader,object>[] readers = new[]{
		InvalidValue,
		ReadByte,
		ReadSByte,
		ReadShort,
		ReadUShort,
		ReadInt,
		ReadUInt,
		ReadLong,
		ReadULong,
		ReadFloat,
		ReadDouble,
		ReadBool,
		ReadString
	};

	private static object InvalidValue(BinaryReader reader)
	{
		throw new InvalidOperationException();
	}

	private static object ReadByte(BinaryReader reader)
	{
		return reader.ReadByte();
	}

	private static object ReadSByte(BinaryReader reader)
	{
		return reader.ReadSByte();
	}

	private static object ReadShort(BinaryReader reader)
	{
		return reader.ReadInt16();
	}
	private static object ReadUShort(BinaryReader reader)
	{
		return reader.ReadUInt16();
	}
	private static object ReadInt(BinaryReader reader)
	{
		return reader.ReadInt32();
	}
	private static object ReadUInt(BinaryReader reader)
	{
		return reader.ReadUInt32();
	}
	private static object ReadLong(BinaryReader reader)
	{
		return reader.ReadInt64();
	}
	private static object ReadULong(BinaryReader reader)
	{
		return reader.ReadUInt64();
	}
	private static object ReadFloat(BinaryReader reader)
	{
		return reader.ReadSingle();
	}
	private static object ReadDouble (BinaryReader reader)
	{
		return reader.ReadDouble();
	}

	private static object ReadBool (BinaryReader reader)
	{
		return reader.ReadByte() != 0;
	}

	private static object ReadString(BinaryReader reader)
	{
		short length = reader.ReadInt16();
		char[] chars = reader.ReadChars(length);
		return new string(chars);
	}
}

class TimeTravel
{
	public record ParsedSource(CSharpSyntaxTree Tree, SourceText Source);

	public Dictionary<ushort, string> SourceFiles {get; set;}
	public List<TTEntry> Entries{get; set;}

	public Dictionary<ushort, ParsedSource> treeCache;
	public Dictionary<TTEntry, string[]> valueNamesCache;


	public TimeTravel(Dictionary<ushort, string> sourceFiles, List<TTEntry> entries)
	{
		treeCache = new();
		valueNamesCache = new(new TTEntryLocationComparer());
		this.SourceFiles = sourceFiles;
		this.Entries = entries;

		foreach(var entry in entries)
		{
			entry.Owner = this;
		}
	}

	public static TimeTravel Load(string root)
	{
		var sourceFiles = LoadSourceFiles(Path.Combine(root, "dotnet_0.trm"));
		var entries = LoadEntries(Path.Combine(root,"dotnet_0.trd"));
		return new TimeTravel(sourceFiles, entries);
	}

	public int FindFileId(string match) => SourceFiles.Where(sf => sf.Value.Contains(match)).First().Key;

	public string GetSourceFile(TTEntry entry) => SourceFiles[entry.FileId];

	private static readonly CSharpParseOptions parseOptions = CSharpParseOptions.Default.WithPreprocessorSymbols(new string[]{"TEST_BUILD"});

	public string[] GetValueNames(TTEntry entry)
	{
		string[] result;
		if(valueNamesCache.TryGetValue(entry, out result))
			return result;

		ParsedSource parsed = null;
		if(!treeCache.TryGetValue(entry.FileId, out parsed))
		{
			string fileName = GetSourceFile(entry);
			SourceText source = SourceText.From(File.ReadAllText(fileName));
			var syntaxTree = (CSharpSyntaxTree)CSharpSyntaxTree.ParseText(source, parseOptions);
			parsed = new ParsedSource(syntaxTree, source);
			treeCache.Add(entry.FileId, parsed);
		}

		TextSpan span = parsed.Source.Lines[entry.LineNumber-1].Span;
		var methodInvoc = parsed.Tree.GetRoot().FindNode(span).DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>().First();

		result = new string[methodInvoc.ArgumentList.Arguments.Count];

		for(int i = 0; i < result.Length; i++)
			result[i] = methodInvoc.ArgumentList.Arguments[i].ToString();

		valueNamesCache.Add(entry, result);
		return result;
	}

	public string ToString(TTEntry entry)
	{
		string sourceFile = GetSourceFile(entry);
		string[] valueNames = GetValueNames(entry);

		StringBuilder builder = new StringBuilder();

		builder.Append(sourceFile);
		builder.Append(":");
		builder.Append(entry.LineNumber);
		builder.AppendLine();
		builder.AppendFormat("Thread Id: {0}\n", entry.TId);
		builder.AppendLine();
		builder.AppendFormat("Index: {0}\n", entry.Index);

		if(valueNames.Length != entry.ArgCount)
			throw new InvalidOperationException();

		for(int i = 0; i < valueNames.Length; i++)
		{
			builder.Append(valueNames[i]);
			builder.Append(" = ");
			builder.Append(entry.Values[i]);
			builder.AppendLine();
		}

		return builder.ToString();
	}

	public string ToHTML(TTEntry entry)
	{
		string sourceFile = GetSourceFile(entry);
		string[] valueNames = GetValueNames(entry);

		StringBuilder builder = new StringBuilder();
		builder.Append($"<a href=\"{sourceFile}:{entry.LineNumber}\">{Path.GetFileName(sourceFile)}:{entry.LineNumber}</a>");
		builder.AppendLine("<br/>");
		builder.AppendFormat("Thread Id: {0}<br/>", entry.TId);
		builder.AppendFormat("Index: {0}<br/>", entry.Index);

		if(valueNames.Length != entry.ArgCount)
			throw new InvalidOperationException();

		for(int i = 0; i < valueNames.Length; i++)
		{
			builder.Append(valueNames[i]);
			builder.Append(" = ");
			builder.Append(entry.Values[i]);
			builder.AppendLine("<br/>");
		}

		return builder.ToString();
	}

	private static List<TTEntry> LoadEntries(string filename)
	{
		using FileStream fileStream = new FileStream(filename, FileMode.Open, FileAccess.Read);
		using BufferedStream bufferedStream = new BufferedStream(fileStream, 1024*1024);
		using BinaryReader br = new BinaryReader(bufferedStream, Encoding.GetEncoding("UTF-16"));

		List<TTEntry> result = new();

		while(bufferedStream.Position < bufferedStream.Length)
		{
			result.Add(TTEntry.FromStream(br));
			result[result.Count-1].Index = result.Count-1;
		}
		return result;
	}

	private static Dictionary<ushort, string> LoadSourceFiles(string filename)
	{
		using FileStream fileStream = new FileStream(filename, FileMode.Open, FileAccess.Read);
		using BufferedStream bufferedStream = new BufferedStream(fileStream, 1024*1024);
		using BinaryReader br = new BinaryReader(bufferedStream, Encoding.GetEncoding("UTF-16"));
		Dictionary<ushort, string> result = new Dictionary<ushort, string>();

		while(bufferedStream.Position < bufferedStream.Length)
		{
			SourceFile sourceFile = SourceFile.FromStream(br);
			result.Add(sourceFile.Id, sourceFile.Filename);
		}

		return result;
	}
}