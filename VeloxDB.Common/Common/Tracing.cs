using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace VeloxDB.Common;

internal delegate void ExceptionFormatter(Exception e, StringBuilder sb);

/// <summary>
/// Specifies the severity level of a trace message.
/// </summary>
public enum TraceLevel
{
	/// <summary>
	/// User this tracing level to turn off tracing.
	/// </summary>
	None = 0,

	/// <summary>
	/// Recoverable error.
	/// </summary>
	Error = 1,

	/// <summary>
	/// Noncritical problem.
	/// </summary>
	Warning = 2,

	/// <summary>
	/// Informational message.
	/// </summary>
	Info = 3,

	/// <summary>
	/// Infrequent debugging trace.
	/// </summary>
	Debug = 4,

	/// <summary>
	/// Frequent debugging trace.
	/// </summary>
	Verbose = 5,
}

internal interface ITraceCollector
{
	void AddTrace(TraceLevel level, StringBuilder formattedText);
	void SetTraceLevel(TraceLevel level);
}

internal static class Tracing
{
	static readonly object sync = new object();
	static readonly Source globalSource;
	static List<Source> sources;
	static List<ITraceCollector> collectors;

	static Tracing()
	{
		sources = new List<Source>(8);
		collectors = new List<ITraceCollector>();
		globalSource = CreateSource(null);
	}

	public static Source GlobalSource => globalSource;

	public static Source CreateSource(string name)
	{
		lock (sync)
		{
			Source source = sources.Find(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase));
			if (source != null)
				return source;

			source = new Source(name);
			sources.Add(source);

			source.SetTraceLevel(globalSource == null ? TraceLevel.Debug : globalSource.Level);
			return source;
		}
	}

	public static void AddCollector(ITraceCollector collector)
	{
		lock (sync)
		{
			for (int i = 0; i < sources.Count; i++)
			{
				sources[i].AddCollector(collector);
			}
		}
	}

	public static TextFileTraceCollector CreateTextFileCollector(string directoryPath)
	{
		lock (sync)
		{
			Process p = Process.GetCurrentProcess();
			string fileName = Path.Combine(directoryPath, string.Format("{0}_{1}.log", p.ProcessName, p.Id));
			TextFileTraceCollector c = new TextFileTraceCollector(fileName);
			collectors.Add(c);
			return c;
		}
	}

	public static void SetTraceLevel(TraceLevel level)
	{
		lock (sync)
		{
			for (int i = 0; i < sources.Count; i++)
			{
				sources[i].SetTraceLevel(level);
			}
		}
	}

	public static void Error(Exception e, string format, params object[] args)
	{
		globalSource.Error(e, format, args);
	}

	public static void Error(Exception e)
	{
		globalSource.Error(e, null);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error(string format)
	{
		if (globalSource.Level < TraceLevel.Error)
			return;

		globalSource.TraceAlways(TraceLevel.Error, format);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Warning(string format)
	{
		if (globalSource.Level < TraceLevel.Warning)
			return;

		globalSource.TraceAlways(TraceLevel.Warning, format);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Info(string format)
	{
		if (globalSource.Level < TraceLevel.Info)
			return;

		globalSource.TraceAlways(TraceLevel.Info, format);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Debug(string format)
	{
		if (globalSource.Level < TraceLevel.Debug)
			return;

		globalSource.TraceAlways(TraceLevel.Debug, format);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Verbose(string format)
	{
		if (globalSource.Level < TraceLevel.Verbose)
			return;

		globalSource.TraceAlways(TraceLevel.Verbose, format);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error(string format, params object[] args)
	{
		if (globalSource.Level < TraceLevel.Error)
			return;

		globalSource.TraceAlways(TraceLevel.Error, format, args);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Warning(string format, params object[] args)
	{
		if (globalSource.Level < TraceLevel.Warning)
			return;

		globalSource.TraceAlways(TraceLevel.Warning, format, args);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Info(string format, params object[] args)
	{
		if (globalSource.Level < TraceLevel.Info)
			return;

		globalSource.TraceAlways(TraceLevel.Info, format, args);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error<T1>(string format, T1 v1)
	{
		if (globalSource.Level < TraceLevel.Error)
			return;

		globalSource.TraceAlways(TraceLevel.Error, format, v1);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Warning<T1>(string format, T1 v1)
	{
		if (globalSource.Level < TraceLevel.Warning)
			return;

		globalSource.TraceAlways(TraceLevel.Warning, format, v1);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Info<T1>(string format, T1 v1)
	{
		if (globalSource.Level < TraceLevel.Info)
			return;

		globalSource.TraceAlways(TraceLevel.Info, format, v1);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Debug<T1>(string format, T1 v1)
	{
		if (globalSource.Level < TraceLevel.Debug)
			return;

		globalSource.TraceAlways(TraceLevel.Debug, format, v1);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Verbose<T1>(string format, T1 v1)
	{
		if (globalSource.Level < TraceLevel.Verbose)
			return;

		globalSource.TraceAlways(TraceLevel.Verbose, format, v1);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error<T1, T2>(string format, T1 v1, T2 v2)
	{
		if (globalSource.Level < TraceLevel.Error)
			return;

		globalSource.TraceAlways(TraceLevel.Error, format, v1, v2);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Warning<T1, T2>(string format, T1 v1, T2 v2)
	{
		if (globalSource.Level < TraceLevel.Warning)
			return;

		globalSource.TraceAlways(TraceLevel.Warning, format, v1, v2);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Info<T1, T2>(string format, T1 v1, T2 v2)
	{
		if (globalSource.Level < TraceLevel.Info)
			return;

		globalSource.TraceAlways(TraceLevel.Info, format, v1, v2);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Debug<T1, T2>(string format, T1 v1, T2 v2)
	{
		if (globalSource.Level < TraceLevel.Debug)
			return;

		globalSource.TraceAlways(TraceLevel.Debug, format, v1, v2);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Verbose<T1, T2>(string format, T1 v1, T2 v2)
	{
		if (globalSource.Level < TraceLevel.Verbose)
			return;

		globalSource.TraceAlways(TraceLevel.Verbose, format, v1, v2);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
	{
		if (globalSource.Level < TraceLevel.Error)
			return;

		globalSource.TraceAlways(TraceLevel.Error, format, v1, v2, v3);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Warning<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
	{
		if (globalSource.Level < TraceLevel.Warning)
			return;

		globalSource.TraceAlways(TraceLevel.Warning, format, v1, v2, v3);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Info<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
	{
		if (globalSource.Level < TraceLevel.Info)
			return;

		globalSource.TraceAlways(TraceLevel.Info, format, v1, v2, v3);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Debug<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
	{
		if (globalSource.Level < TraceLevel.Debug)
			return;

		globalSource.TraceAlways(TraceLevel.Debug, format, v1, v2, v3);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Verbose<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
	{
		if (globalSource.Level < TraceLevel.Verbose)
			return;

		globalSource.TraceAlways(TraceLevel.Verbose, format, v1, v2, v3);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
	{
		if (globalSource.Level < TraceLevel.Error)
			return;

		globalSource.TraceAlways(TraceLevel.Error, format, v1, v2, v3, v4);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Warning<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
	{
		if (globalSource.Level < TraceLevel.Warning)
			return;

		globalSource.TraceAlways(TraceLevel.Warning, format, v1, v2, v3, v4);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Info<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
	{
		if (globalSource.Level < TraceLevel.Info)
			return;

		globalSource.TraceAlways(TraceLevel.Info, format, v1, v2, v3, v4);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Debug<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
	{
		if (globalSource.Level < TraceLevel.Debug)
			return;

		globalSource.TraceAlways(TraceLevel.Debug, format, v1, v2, v3, v4);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Verbose<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
	{
		if (globalSource.Level < TraceLevel.Verbose)
			return;

		globalSource.TraceAlways(TraceLevel.Verbose, format, v1, v2, v3, v4);
	}

	public static void RemoveTextFileCollector()
	{
		lock (sync)
		{
			for (int i = 0; i < collectors.Count; i++)
			{
				if (collectors[i].GetType() == typeof(TextFileTraceCollector))
				{
					(collectors[i] as TextFileTraceCollector).Dispose();
					sources[i].RemoveCollector(collectors[i]);
				}
			}
		}
	}

	internal sealed class Source
	{
		static ItemPool<StringBuilder> stringBuilderPool;
		static string[] traceLevelStrings;

		readonly object sync = new object();
		TraceLevel level;
		string name;

		static Source()
		{
			traceLevelStrings = new string[Utils.MaxEnumValue(typeof(TraceLevel)) + 1];
			traceLevelStrings[(int)TraceLevel.Error] = "Error";
			traceLevelStrings[(int)TraceLevel.Warning] = "Warning";
			traceLevelStrings[(int)TraceLevel.Info] = "Info";
			traceLevelStrings[(int)TraceLevel.Debug] = "Debug";
			traceLevelStrings[(int)TraceLevel.Verbose] = "Verbose";

			stringBuilderPool = new ItemPool<StringBuilder>(Environment.ProcessorCount, new StringBuilderFactory());
		}

		public Source(string name)
		{
			this.name = name;
			level = TraceLevel.Debug;
		}

		public TraceLevel Level => level;
		public string Name => name;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool ShouldTrace(TraceLevel level)
		{
			return this.level >= level;
		}

		public void AddCollector(ITraceCollector collector)
		{
			lock (sync)
			{
				List<ITraceCollector> temp = new List<ITraceCollector>(collectors);
				temp.Add(collector);
				Thread.MemoryBarrier();
				collectors = temp;
			}
		}

		public void RemoveCollector(ITraceCollector collector)
		{
			lock (sync)
			{
				List<ITraceCollector> temp = new List<ITraceCollector>(collectors);
				temp.Remove(collector);
				collectors = temp;
			}
		}

		public void SetTraceLevel(TraceLevel level)
		{
			this.level = level;
		}

		public void Error(Exception e, string format, params object[] args)
		{
			if (this.level < TraceLevel.Error)
				return;

			StringBuilder sb = new StringBuilder();
			if (format != null)
			{
				sb.AppendFormat(format, args);
				sb.AppendLine();
			}

			FormatException(sb, e);
			Exception ie = e.InnerException;
			while (ie != null)
			{
				sb.AppendLine("Inner Exception: ");
				FormatException(sb, ie);
				ie = ie.InnerException;
			}

			TraceAlways(TraceLevel.Error, sb.ToString());
		}

		public void Error(Exception e)
		{
			Error(e, null);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Error(string format)
		{
			if (level < TraceLevel.Error)
				return;

			TraceAlways(TraceLevel.Error, format);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Warning(string format)
		{
			if (level < TraceLevel.Warning)
				return;

			TraceAlways(TraceLevel.Warning, format);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Info(string format)
		{
			if (level < TraceLevel.Info)
				return;

			TraceAlways(TraceLevel.Info, format);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Debug(string format)
		{
			if (level < TraceLevel.Debug)
				return;

			TraceAlways(TraceLevel.Debug, format);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Verbose(string format)
		{
			if (level < TraceLevel.Verbose)
				return;

			TraceAlways(TraceLevel.Verbose, format);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Error(string format, params object[] args)
		{
			if (level < TraceLevel.Error)
				return;

			TraceAlways(TraceLevel.Error, format, args);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Warning(string format, params object[] args)
		{
			if (level < TraceLevel.Warning)
				return;

			TraceAlways(TraceLevel.Warning, format, args);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Info(string format, params object[] args)
		{
			if (level < TraceLevel.Info)
				return;

			TraceAlways(TraceLevel.Info, format, args);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Debug(string format, params object[] args)
		{
			if (level < TraceLevel.Debug)
				return;

			TraceAlways(TraceLevel.Debug, format, args);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Error<T1>(string format, T1 v1)
		{
			if (level < TraceLevel.Error)
				return;

			TraceAlways(TraceLevel.Error, format, v1);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Warning<T1>(string format, T1 v1)
		{
			if (level < TraceLevel.Warning)
				return;

			TraceAlways(TraceLevel.Warning, format, v1);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Info<T1>(string format, T1 v1)
		{
			if (level < TraceLevel.Info)
				return;

			TraceAlways(TraceLevel.Info, format, v1);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Debug<T1>(string format, T1 v1)
		{
			if (level < TraceLevel.Debug)
				return;

			TraceAlways(TraceLevel.Debug, format, v1);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Verbose<T1>(string format, T1 v1)
		{
			if (level < TraceLevel.Verbose)
				return;

			TraceAlways(TraceLevel.Verbose, format, v1);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Error<T1, T2>(string format, T1 v1, T2 v2)
		{
			if (level < TraceLevel.Error)
				return;

			TraceAlways(TraceLevel.Error, format, v1, v2);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Warning<T1, T2>(string format, T1 v1, T2 v2)
		{
			if (level < TraceLevel.Warning)
				return;

			TraceAlways(TraceLevel.Warning, format, v1, v2);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Info<T1, T2>(string format, T1 v1, T2 v2)
		{
			if (level < TraceLevel.Info)
				return;

			TraceAlways(TraceLevel.Info, format, v1, v2);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Debug<T1, T2>(string format, T1 v1, T2 v2)
		{
			if (level < TraceLevel.Debug)
				return;

			TraceAlways(TraceLevel.Debug, format, v1, v2);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Verbose<T1, T2>(string format, T1 v1, T2 v2)
		{
			if (level < TraceLevel.Verbose)
				return;

			TraceAlways(TraceLevel.Verbose, format, v1, v2);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Error<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
		{
			if (level < TraceLevel.Error)
				return;

			TraceAlways(TraceLevel.Error, format, v1, v2, v3);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Warning<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
		{
			if (level < TraceLevel.Error)
				return;

			TraceAlways(TraceLevel.Error, format, v1, v2, v3);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Info<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
		{
			if (level < TraceLevel.Info)
				return;

			TraceAlways(TraceLevel.Info, format, v1, v2, v3);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Debug<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
		{
			if (level < TraceLevel.Debug)
				return;

			TraceAlways(TraceLevel.Debug, format, v1, v2, v3);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Verbose<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
		{
			if (level < TraceLevel.Verbose)
				return;

			TraceAlways(TraceLevel.Verbose, format, v1, v2, v3);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Error<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
		{
			if (level < TraceLevel.Error)
				return;

			TraceAlways(TraceLevel.Error, format, v1, v2, v3, v4);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Warning<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
		{
			if (level < TraceLevel.Warning)
				return;

			TraceAlways(TraceLevel.Error, format, v1, v2, v3, v4);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Info<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
		{
			if (level < TraceLevel.Info)
				return;

			TraceAlways(TraceLevel.Info, format, v1, v2, v3, v4);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Debug<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
		{
			if (level < TraceLevel.Debug)
				return;

			TraceAlways(TraceLevel.Debug, format, v1, v2, v3, v4);
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public void Verbose<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
		{
			if (level < TraceLevel.Verbose)
				return;

			TraceAlways(TraceLevel.Verbose, format, v1, v2, v3, v4);
		}

		private static void FormatException(StringBuilder sb, Exception e)
		{
			sb.Append(e.GetType().FullName);
			sb.Append(": ");
			sb.AppendLine(e.Message);
			sb.AppendLine(e.StackTrace);
		}

		[MethodImpl(MethodImplOptions.NoInlining)]
		internal void TraceAlways(TraceLevel level, string format, params object[] args)
		{
			DateTime now = DateTime.UtcNow;

			StringBuilder sb = stringBuilderPool.Get();
			sb.AppendFormat("{0:yy-MM-dd HH:mm:ss.fff}", now);
			sb.AppendFormat(" [{0:X}] [", NativeProcessorInfo.GetCurrentNativeThreadId());
			sb.Append(traceLevelStrings[(int)level]);
			sb.Append("] ");

			if (name != null)
			{
				sb.Append('[');
				sb.Append(name);
				sb.Append("] ");
			}

			sb.AppendFormat(format, args);

			lock (sync)
			{
				CollectTrace(level, sb);
			}

			stringBuilderPool.Put(sb);
		}

		private void CollectTrace(TraceLevel level, StringBuilder sb)
		{
			for (int i = 0; i < collectors.Count; i++)
			{
				collectors[i].AddTrace(level, sb);
			}
		}

		private sealed class StringBuilderFactory : IItemFactory<StringBuilder>
		{
			const int maxCapacity = 1024 * 8;

			public StringBuilder Create()
			{
				return new StringBuilder(256);
			}

			public void Destroy(StringBuilder item)
			{
			}

			public void Init(StringBuilder item)
			{
			}

			public void Reset(StringBuilder item)
			{
				item.Clear();
				if (item.Capacity > maxCapacity)
					item.Capacity = maxCapacity;
			}
		}
	}
}
