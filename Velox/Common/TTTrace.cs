using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Velox.Common;

internal unsafe static class TTTrace
{
	static readonly double rTicksPerMilli = 1000.0 / Stopwatch.Frequency;

	static readonly object sync = new object();
	static readonly object metaFileSync = new object();

	static Dictionary<Type, Delegate> writers;

	static int fileOrderNum;

	static int capacity;
	static int offset;
	static byte* buffer;
	static long writtenBytes, writtenMetaBytes;
	static Timer persistTimer;

	static ushort currSourceFileId = 1;
	static Dictionary<string, ushort> sourceFiles;

	static NativeFile dataFile, metaFile;


	static TTTrace()
	{
		capacity = 1024 * 1024 * 256;
		offset = 0;
		buffer = (byte*)NativeAllocator.Allocate(capacity);

		sourceFiles = new Dictionary<string, ushort>(1, ReferenceEqualityComparer<string>.Instance);

		writers = new Dictionary<Type, Delegate>(16);
		writers.Add(typeof(byte), new Action<byte>(WriteByteValue));
		writers.Add(typeof(sbyte), new Action<sbyte>(WriteSByteValue));
		writers.Add(typeof(short), new Action<short>(WriteShortValue));
		writers.Add(typeof(ushort), new Action<ushort>(WriteUShortValue));
		writers.Add(typeof(int), new Action<int>(WriteIntValue));
		writers.Add(typeof(uint), new Action<uint>(WriteUIntValue));
		writers.Add(typeof(long), new Action<long>(WriteLongValue));
		writers.Add(typeof(ulong), new Action<ulong>(WriteULongValue));
		writers.Add(typeof(bool), new Action<bool>(WriteBoolValue));
		writers.Add(typeof(string), new Action<string>(WriteStringValue));
		writers.Add(typeof(float), new Action<float>(WriteFloatValue));
		writers.Add(typeof(double), new Action<double>(WriteDoubleValue));

		AppDomain.CurrentDomain.ProcessExit += (a, b) => FlushAndClose();
		AppDomain.CurrentDomain.UnhandledException += (a, b) => FlushAndClose();

		persistTimer = new Timer(FlushCallback, null, 10000, 10000);
	}

	[Conditional("TTTRACE")]
	public static void Write(Separator s = null, [CallerFilePath] string filePath = null,
		[CallerLineNumber] int lineNumber = 0)
	{
		int tid = Thread.CurrentThread.ManagedThreadId;
		if (!TryGetSourceFileId(filePath, out ushort fileId))
			return;

		lock (sync)
		{
			WriteHeader(lineNumber, tid, fileId, 0);
		}
	}

	[Conditional("TTTRACE")]
	public static void Write<T1>(T1 v1, Separator s = null, [CallerFilePath] string filePath = null,
		[CallerLineNumber] int lineNumber = 0)
	{
		var writer1 = ExtractWriter<T1>();

		int tid = Thread.CurrentThread.ManagedThreadId;
		if (!TryGetSourceFileId(filePath, out ushort fileId))
			return;

		lock (sync)
		{
			WriteHeader(lineNumber, tid, fileId, 1);
			writer1(v1);
		}
	}

	[Conditional("TTTRACE")]
	public static void Write<T1, T2>(T1 v1, T2 v2, Separator s = null, [CallerFilePath] string filePath = null,
		[CallerLineNumber] int lineNumber = 0)
	{
		var writer1 = ExtractWriter<T1>();
		var writer2 = ExtractWriter<T2>();

		int tid = Thread.CurrentThread.ManagedThreadId;
		if (!TryGetSourceFileId(filePath, out ushort fileId))
			return;

		lock (sync)
		{
			WriteHeader(lineNumber, tid, fileId, 2);
			writer1(v1);
			writer2(v2);
		}
	}

	[Conditional("TTTRACE")]
	public static void Write<T1, T2, T3>(T1 v1, T2 v2, T3 v3, Separator s = null,
		[CallerFilePath] string filePath = null, [CallerLineNumber] int lineNumber = 0)
	{
		var writer1 = ExtractWriter<T1>();
		var writer2 = ExtractWriter<T2>();
		var writer3 = ExtractWriter<T3>();

		int tid = Thread.CurrentThread.ManagedThreadId;
		if (!TryGetSourceFileId(filePath, out ushort fileId))
			return;

		lock (sync)
		{
			WriteHeader(lineNumber, tid, fileId, 3);
			writer1(v1);
			writer2(v2);
			writer3(v3);
		}
	}

	[Conditional("TTTRACE")]
	public static void Write<T1, T2, T3, T4>(T1 v1, T2 v2, T3 v3, T4 v4,
		Separator s = null, [CallerFilePath] string filePath = null, [CallerLineNumber] int lineNumber = 0)
	{
		var writer1 = ExtractWriter<T1>();
		var writer2 = ExtractWriter<T2>();
		var writer3 = ExtractWriter<T3>();
		var writer4 = ExtractWriter<T4>();

		int tid = Thread.CurrentThread.ManagedThreadId;
		if (!TryGetSourceFileId(filePath, out ushort fileId))
			return;

		lock (sync)
		{
			WriteHeader(lineNumber, tid, fileId, 4);
			writer1(v1);
			writer2(v2);
			writer3(v3);
			writer4(v4);
		}
	}

	[Conditional("TTTRACE")]
	public static void Write<T1, T2, T3, T4, T5>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5,
		Separator s = null, [CallerFilePath] string filePath = null, [CallerLineNumber] int lineNumber = 0)
	{
		var writer1 = ExtractWriter<T1>();
		var writer2 = ExtractWriter<T2>();
		var writer3 = ExtractWriter<T3>();
		var writer4 = ExtractWriter<T4>();
		var writer5 = ExtractWriter<T5>();

		int tid = Thread.CurrentThread.ManagedThreadId;
		if (!TryGetSourceFileId(filePath, out ushort fileId))
			return;

		lock (sync)
		{
			WriteHeader(lineNumber, tid, fileId, 5);
			writer1(v1);
			writer2(v2);
			writer3(v3);
			writer4(v4);
			writer5(v5);
		}
	}

	[Conditional("TTTRACE")]
	public static void Write<T1, T2, T3, T4, T5, T6>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6,
		Separator s = null, [CallerFilePath] string filePath = null, [CallerLineNumber] int lineNumber = 0)
	{
		var writer1 = ExtractWriter<T1>();
		var writer2 = ExtractWriter<T2>();
		var writer3 = ExtractWriter<T3>();
		var writer4 = ExtractWriter<T4>();
		var writer5 = ExtractWriter<T5>();
		var writer6 = ExtractWriter<T6>();

		int tid = Thread.CurrentThread.ManagedThreadId;
		if (!TryGetSourceFileId(filePath, out ushort fileId))
			return;

		lock (sync)
		{
			WriteHeader(lineNumber, tid, fileId, 6);
			writer1(v1);
			writer2(v2);
			writer3(v3);
			writer4(v4);
			writer5(v5);
			writer6(v6);
		}
	}

	[Conditional("TTTRACE")]
	public static void Write<T1, T2, T3, T4, T5, T6, T7>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6, T7 v7,
		Separator s = null, [CallerFilePath] string filePath = null, [CallerLineNumber] int lineNumber = 0)
	{
		var writer1 = ExtractWriter<T1>();
		var writer2 = ExtractWriter<T2>();
		var writer3 = ExtractWriter<T3>();
		var writer4 = ExtractWriter<T4>();
		var writer5 = ExtractWriter<T5>();
		var writer6 = ExtractWriter<T6>();
		var writer7 = ExtractWriter<T7>();

		int tid = Thread.CurrentThread.ManagedThreadId;
		if (!TryGetSourceFileId(filePath, out ushort fileId))
			return;

		lock (sync)
		{
			WriteHeader(lineNumber, tid, fileId, 7);
			writer1(v1);
			writer2(v2);
			writer3(v3);
			writer4(v4);
			writer5(v5);
			writer6(v6);
			writer7(v7);
		}
	}

	[Conditional("TTTRACE")]
	public static void Write<T1, T2, T3, T4, T5, T6, T7, T8>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6, T7 v7, T8 v8,
		Separator s = null, [CallerFilePath] string filePath = null, [CallerLineNumber] int lineNumber = 0)
	{
		var writer1 = ExtractWriter<T1>();
		var writer2 = ExtractWriter<T2>();
		var writer3 = ExtractWriter<T3>();
		var writer4 = ExtractWriter<T4>();
		var writer5 = ExtractWriter<T5>();
		var writer6 = ExtractWriter<T6>();
		var writer7 = ExtractWriter<T7>();
		var writer8 = ExtractWriter<T8>();

		int tid = Thread.CurrentThread.ManagedThreadId;
		if (!TryGetSourceFileId(filePath, out ushort fileId))
			return;

		lock (sync)
		{
			WriteHeader(lineNumber, tid, fileId, 8);
			writer1(v1);
			writer2(v2);
			writer3(v3);
			writer4(v4);
			writer5(v5);
			writer6(v6);
			writer7(v7);
			writer8(v8);
		}
	}

	[Conditional("TTTRACE")]
	public static void Write<T1, T2, T3, T4, T5, T6, T7, T8, T9>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6, T7 v7,
		T8 v8, T9 v9, Separator s = null, [CallerFilePath] string filePath = null,
		[CallerLineNumber] int lineNumber = 0)
	{
		var writer1 = ExtractWriter<T1>();
		var writer2 = ExtractWriter<T2>();
		var writer3 = ExtractWriter<T3>();
		var writer4 = ExtractWriter<T4>();
		var writer5 = ExtractWriter<T5>();
		var writer6 = ExtractWriter<T6>();
		var writer7 = ExtractWriter<T7>();
		var writer8 = ExtractWriter<T8>();
		var writer9 = ExtractWriter<T9>();

		int tid = Thread.CurrentThread.ManagedThreadId;
		if (!TryGetSourceFileId(filePath, out ushort fileId))
			return;

		lock (sync)
		{
			WriteHeader(lineNumber, tid, fileId, 9);
			writer1(v1);
			writer2(v2);
			writer3(v3);
			writer4(v4);
			writer5(v5);
			writer6(v6);
			writer7(v7);
			writer8(v8);
			writer9(v9);
		}
	}

	[Conditional("TTTRACE")]
	public static void Write<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6,
		T7 v7, T8 v8, T9 v9, T10 v10, Separator s = null, [CallerFilePath] string filePath = null,
		[CallerLineNumber] int lineNumber = 0)
	{
		var writer1 = ExtractWriter<T1>();
		var writer2 = ExtractWriter<T2>();
		var writer3 = ExtractWriter<T3>();
		var writer4 = ExtractWriter<T4>();
		var writer5 = ExtractWriter<T5>();
		var writer6 = ExtractWriter<T6>();
		var writer7 = ExtractWriter<T7>();
		var writer8 = ExtractWriter<T8>();
		var writer9 = ExtractWriter<T9>();
		var writer10 = ExtractWriter<T10>();

		int tid = Thread.CurrentThread.ManagedThreadId;
		if (!TryGetSourceFileId(filePath, out ushort fileId))
			return;

		lock (sync)
		{
			WriteHeader(lineNumber, tid, fileId, 10);
			writer1(v1);
			writer2(v2);
			writer3(v3);
			writer4(v4);
			writer5(v5);
			writer6(v6);
			writer7(v7);
			writer8(v8);
			writer9(v9);
			writer10(v10);
		}
	}

	[Conditional("TTTRACE")]
	public static void Write<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6,
		T7 v7, T8 v8, T9 v9, T10 v10, T11 v11, Separator s = null, [CallerFilePath] string filePath = null,
		[CallerLineNumber] int lineNumber = 0)
	{
		var writer1 = ExtractWriter<T1>();
		var writer2 = ExtractWriter<T2>();
		var writer3 = ExtractWriter<T3>();
		var writer4 = ExtractWriter<T4>();
		var writer5 = ExtractWriter<T5>();
		var writer6 = ExtractWriter<T6>();
		var writer7 = ExtractWriter<T7>();
		var writer8 = ExtractWriter<T8>();
		var writer9 = ExtractWriter<T9>();
		var writer10 = ExtractWriter<T10>();
		var writer11 = ExtractWriter<T11>();

		int tid = Thread.CurrentThread.ManagedThreadId;
		if (!TryGetSourceFileId(filePath, out ushort fileId))
			return;

		lock (sync)
		{
			WriteHeader(lineNumber, tid, fileId, 11);
			writer1(v1);
			writer2(v2);
			writer3(v3);
			writer4(v4);
			writer5(v5);
			writer6(v6);
			writer7(v7);
			writer8(v8);
			writer9(v9);
			writer10(v10);
			writer11(v11);
		}
	}

	[Conditional("TTTRACE")]
	public static void Write<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5,
		T6 v6, T7 v7, T8 v8, T9 v9, T10 v10, T11 v11, T12 v12, Separator s = null,
		[CallerFilePath] string filePath = null, [CallerLineNumber] int lineNumber = 0)
	{
		var writer1 = ExtractWriter<T1>();
		var writer2 = ExtractWriter<T2>();
		var writer3 = ExtractWriter<T3>();
		var writer4 = ExtractWriter<T4>();
		var writer5 = ExtractWriter<T5>();
		var writer6 = ExtractWriter<T6>();
		var writer7 = ExtractWriter<T7>();
		var writer8 = ExtractWriter<T8>();
		var writer9 = ExtractWriter<T9>();
		var writer10 = ExtractWriter<T10>();
		var writer11 = ExtractWriter<T11>();
		var writer12 = ExtractWriter<T12>();

		int tid = Thread.CurrentThread.ManagedThreadId;
		if (!TryGetSourceFileId(filePath, out ushort fileId))
			return;

		lock (sync)
		{
			WriteHeader(lineNumber, tid, fileId, 12);
			writer1(v1);
			writer2(v2);
			writer3(v3);
			writer4(v4);
			writer5(v5);
			writer6(v6);
			writer7(v7);
			writer8(v8);
			writer9(v9);
			writer10(v10);
			writer11(v11);
			writer12(v12);
		}
	}

	private static void WriteHeader(int lineNumber, int tid, ushort fileId, int argCount)
	{
		// Tool that analyses these traces expects an item type (even though we no longer support item types)
		// 3 represent a normal trace value (not a method entry or exit).
		WriteByte((byte)((argCount << 4) | (int)3));
		WriteInt(tid);
		WriteShort((short)fileId);
		WriteShort((short)lineNumber);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static Action<T1> ExtractWriter<T1>()
	{
		if (!writers.TryGetValue(typeof(T1), out Delegate d1))
			throw new NotSupportedException("Argument type not supported.");

		return (Action<T1>)d1;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static bool TryGetSourceFileId(string fileName, out ushort id)
	{
		if (sourceFiles.TryGetValue(fileName, out id))
			return true;

		return TryAddNewSourceFile(fileName, out id);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private static bool TryAddNewSourceFile(string fileName, out ushort id)
	{
		id = 0;

		lock (metaFileSync)
		{
			if (sourceFiles.TryGetValue(fileName, out id))
				return true;

			if (metaFile == null)
				return false;

			id = currSourceFileId++;
			Dictionary<string, ushort> h = new Dictionary<string, ushort>(sourceFiles);
			h.Add(fileName, id);

			fixed (char* cp = fileName)
			{
				ushort tempId = id;
				metaFile.Write((IntPtr)(&tempId), 2);
				writtenMetaBytes += 2;

				ushort len = (ushort)fileName.Length;

				metaFile.Write((IntPtr)(&len), 2);
				writtenMetaBytes += 2;

				metaFile.Write((IntPtr)cp, len * 2);
				writtenMetaBytes += len * 2;
			}

			Thread.MemoryBarrier();
			sourceFiles = h;

			return true;
		}
	}

	public static void Init(string path)
	{
		lock (sync)
		{
			lock (metaFileSync)
			{
				if (dataFile != null)
					FlushAndClose();

				string dName = Path.Combine(path, $"{Process.GetCurrentProcess().ProcessName}_{fileOrderNum}.trd");
				dataFile = NativeFile.Create(dName, FileMode.Create, FileAccess.Write, FileShare.None, FileFlags.None);

				string mName = Path.Combine(path, $"{Process.GetCurrentProcess().ProcessName}_{fileOrderNum}.trm");
				metaFile = NativeFile.Create(mName, FileMode.Create, FileAccess.Write, FileShare.None, FileFlags.None);

				fileOrderNum++;
			}
		}
	}

	private static void FlushCallback(object state)
	{
		lock (sync)
		{
			if (dataFile == null)
				return;

			FlushBuffer();

			lock (metaFileSync)
			{
				dataFile.Flush();
				metaFile.Flush();
			}
		}
	}

	public static void FlushAndCloseAsync()
	{
		Task.Run(() => FlushAndClose());
	}

	public static void FlushAndClose()
	{
		lock (sync)
		{
			if (dataFile == null)
				return;

			FlushBuffer();

			lock (metaFileSync)
			{
				dataFile.Flush();
				dataFile.Dispose();

				metaFile.Flush();
				metaFile.Dispose();

				dataFile = null;
				metaFile = null;

				writtenBytes = 0;
				writtenMetaBytes = 0;

				sourceFiles = new Dictionary<string, ushort>();
				currSourceFileId = 1;
			}
		}
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private static void ProvideSpace(int size)
	{
		if (offset + size > capacity)
			FlushBuffer();
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private static void FlushBuffer()
	{
		if (offset == 0)
			return;

		dataFile.Write((IntPtr)buffer, (uint)offset);

		writtenBytes += offset;
		offset = 0;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteByte(byte v)
	{
		ProvideSpace(1);
		*((byte*)(buffer + offset)) = v;
		offset += 1;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteByteValue(byte v)
	{
		ProvideSpace(2);
		buffer[offset++] = (byte)VariableType.Byte;
		*((byte*)(buffer + offset)) = v;
		offset += 1;
	}

	private static void WriteSByteValue(sbyte v)
	{
		ProvideSpace(2);
		buffer[offset++] = (byte)VariableType.SByte;
		*((sbyte*)(buffer + offset)) = v;
		offset += 1;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteShort(short v)
	{
		ProvideSpace(2);
		*((short*)(buffer + offset)) = v;
		offset += 2;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteShortValue(short v)
	{
		ProvideSpace(3);
		buffer[offset++] = (byte)VariableType.Short;
		*((short*)(buffer + offset)) = v;
		offset += 2;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteUShortValue(ushort v)
	{
		ProvideSpace(3);
		buffer[offset++] = (byte)VariableType.UShort;
		*((ushort*)(buffer + offset)) = v;
		offset += 2;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteInt(int v)
	{
		ProvideSpace(4);
		*((int*)(buffer + offset)) = v;
		offset += 4;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteIntValue(int v)
	{
		ProvideSpace(5);
		buffer[offset++] = (byte)VariableType.Int;
		*((int*)(buffer + offset)) = v;
		offset += 4;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteUIntValue(uint v)
	{
		ProvideSpace(5);
		buffer[offset++] = (byte)VariableType.UInt;
		*((uint*)(buffer + offset)) = v;
		offset += 4;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteLongValue(long v)
	{
		ProvideSpace(9);
		buffer[offset++] = (byte)VariableType.Long;
		*((long*)(buffer + offset)) = v;
		offset += 8;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteULongValue(ulong v)
	{
		ProvideSpace(9);
		buffer[offset++] = (byte)VariableType.ULong;
		*((ulong*)(buffer + offset)) = v;
		offset += 8;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteFloatValue(float v)
	{
		ProvideSpace(5);
		buffer[offset++] = (byte)VariableType.Float;
		*((float*)(buffer + offset)) = v;
		offset += 4;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteDoubleValue(double v)
	{
		ProvideSpace(9);
		buffer[offset++] = (byte)VariableType.Double;
		*((double*)(buffer + offset)) = v;
		offset += 8;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteBoolValue(bool v)
	{
		ProvideSpace(2);
		buffer[offset++] = (byte)VariableType.Bool;
		*((bool*)(buffer + offset)) = v;
		offset += 1;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static void WriteStringValue(string v)
	{
		if (v == null)
			v = "null";

		if (v.Length > 1024 * 8)
			v = "Maximum length exceeded.";

		short length = (short)v.Length;
		ProvideSpace(length * 2 + 3);

		buffer[offset++] = (byte)VariableType.String;
		*((short*)(buffer + offset)) = length;
		offset += 2;

		if (v != null)
		{
			fixed (char* cp1 = v)
			{
				char* cp2 = (char*)(buffer + offset);
				for (int i = 0; i < v.Length; i++)
				{
					cp2[i] = cp1[i];
				}

				offset += v.Length * 2;
			}
		}
	}

	/// Used to separate trace arguments form the filePath in HPTrace functions since string arguments
	/// are indistinguishable from filePath argument in some overloads.
	public class Separator
	{
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
}
