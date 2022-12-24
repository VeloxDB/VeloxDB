using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace VeloxDB.Common;

internal unsafe static class Utils
{
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Exchange<T>(ref T v1, ref T v2)
	{
		T t = v1;
		v1 = v2;
		v2 = t;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Exchange(ref byte* v1, ref byte* v2)
	{
		byte* t = v1;
		v1 = v2;
		v2 = t;
	}

	public static ulong GetNextPow2(ulong n)
	{
		int low = 0;
		int hight = 63;
		ulong v;
		while (low < hight)
		{
			int mid = (low + hight) >> 1;
			v = ((ulong)1 << mid);
			if (v < n)
			{
				low = mid + 1;
			}
			else if (v < (n << 1))
			{
				return v;
			}
			else
			{
				hight = mid - 1;
			}
		}

		v = ((ulong)1 << low);
		Checker.AssertTrue(v >= n && v < (n << 1));
		return v;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool IsPowerOf2(long n)
	{
		return n >= 1 && (n & (n - 1)) == 0;
	}

	public static int MaxEnumValue(Type enumType)
	{
		int maxVal = -1;
		foreach (object val in Enum.GetValues(enumType))
		{
			int ival = (int)Convert.ChangeType(val, typeof(int));
			maxVal = Math.Max(maxVal, ival);
		}

		return maxVal;
	}

	public static Stream GetResourceStream(Assembly assembly, string resourceName)
	{

		return assembly.GetManifestResourceStream(resourceName);
	}

	public static IEnumerable<T> Concat<T>(this IEnumerable<T> objs, T obj)
	{
		if (objs != null)
		{
			foreach (T e in objs)
			{
				yield return e;
			}
		}

		yield return obj;
	}

	public static void RunAsObservedTask(Action action, string threadName = null,
		TaskCreationOptions options = TaskCreationOptions.None, int delay = 0)
	{
		if (options == TaskCreationOptions.LongRunning)
		{
			Thread t = new Thread(() =>
			{
				if (delay > 0)
					Thread.Sleep(delay);

				action();
			});

			t.Name = threadName;
			t.Start();
		}
		else
		{
			Task task;
			if (delay != 0)
				task = Task.Delay(delay).ContinueWith(t => action());
			else
				task = Task.Run(action);

			task.ContinueWith(x =>
			{
				if (x.Exception != null)
				{
					// Since thread pool does not catch all exceptions like tasks do
					ThreadPool.UnsafeQueueUserWorkItem(y => { throw x.Exception; }, null);
				}
			});
		}
	}

	public static Thread RunThreadWithSupressedFlow(ParameterizedThreadStart p, object state,
		string name = null, bool isBackground = true, int maxStackSize = 0)
	{
		AsyncFlowControl? afc = null;
		try
		{
			if (Thread.CurrentThread.ExecutionContext != null && !ExecutionContext.IsFlowSuppressed())
				afc = ExecutionContext.SuppressFlow();

			Thread t = new Thread(p, maxStackSize);
			t.Name = name;
			t.IsBackground = isBackground;
			t.Start(state);

			return t;
		}
		finally
		{
			afc?.Undo();
		}
	}

	public static Thread RunThreadWithSupressedFlow(ThreadStart p, string name = null,
		bool isBackground = true, int maxStackSize = 0)
	{
		AsyncFlowControl? afc = null;
		try
		{
			if (Thread.CurrentThread.ExecutionContext != null && !ExecutionContext.IsFlowSuppressed())
				afc = ExecutionContext.SuppressFlow();

			Thread t = new Thread(p, maxStackSize);
			t.Name = name;
			t.IsBackground = isBackground;
			t.Start();

			return t;
		}
		finally
		{
			afc?.Undo();
		}
	}

	public static void CopyMemory(byte* src, byte* dst, long len)
	{
		ulong* up = (ulong*)src;
		long l = len >> 3;
		for (long i = 0; i < l; i++)
		{
			((ulong*)dst)[i] = up[i];
		}

		long t = l << 3;
		long k = len - t;
		for (long i = 0; i < k; i++)
		{
			dst[t + i] = src[t + i];
		}
	}

	public static void ZeroMemory(byte* dst, long len)
	{
		long l = len >> 3;
		for (long i = 0; i < l; i++)
		{
			((ulong*)dst)[i] = 0;
		}

		long t = l << 3;
		long k = len - t;
		for (long i = 0; i < k; i++)
		{
			dst[t + i] = 0;
		}
	}

	public static void FillMemory(byte* dst, long len, byte val)
	{
		ulong dval = ((ulong)val << 56) | ((ulong)val << 48) | ((ulong)val << 40) | ((ulong)val << 32) |
			((ulong)val << 24) | ((ulong)val << 16) | ((ulong)val << 8) | ((ulong)val << 0);

		long l = len >> 3;
		for (long i = 0; i < l; i++)
		{
			((ulong*)dst)[i] = dval;
		}

		long t = l << 3;
		long k = len - t;
		for (long i = 0; i < k; i++)
		{
			dst[t + i] = val;
		}
	}

	public static void ResizeMem(ref IntPtr p, long size, long newSize)
	{
		byte* bp = (byte*)p;
		ResizeMem(ref bp, size, newSize);
		p = (IntPtr)bp;
	}

	public static void ResizeMem(ref byte* p, long size, long newSize)
	{
		byte* np = (byte*)NativeAllocator.Allocate(newSize);

		if (p != null)
		{
			CopyMemory(p, np, size);
			NativeAllocator.Free((IntPtr)p);
		}

		p = np;
	}

	public static Range[] SplitRange(long capacity, long countPerRange, int maxRanges)
	{
		if (capacity == 0)
			return EmptyArray<Range>.Instance;

		long rangeCount = Math.Min(maxRanges, Math.Min(capacity, capacity / countPerRange + 1));
		double cpr = (double)capacity / rangeCount;
		Range[] ranges = new Range[rangeCount];
		double s = 0.0f;
		for (int i = 0; i < rangeCount; i++)
		{
			double r1 = s;
			double r2 = s + cpr;
			ranges[i] = new Range((long)r1, (long)r2 - (long)r1);
			s = r2;
		}

		ranges[ranges.Length - 1].Count = capacity - ranges[ranges.Length - 1].Offset;

#if DEBUG
		Checker.AssertTrue(ranges[ranges.Length - 1].Count + ranges[ranges.Length - 1].Offset == capacity);
		for (int i = 0; i < ranges.Length - 1; i++)
		{
			Checker.AssertTrue(ranges[i].Offset + ranges[i].Count == ranges[i + 1].Offset);
		}
#endif

		return ranges;
	}

	public static int GetStableStringHashCode(string s)
	{
		int h = 0;
		for (int i = 0; i < s.Length; i++)
		{
			h = h * 31 + (int)s[i];
		}

		return h;
	}

	public static T[] CreateCopy<T>(T[] t)
	{
		if (t == null)
			return null;

		T[] v = new T[t.Length];
		Array.Copy(t, v, t.Length);
		return v;
	}

	public static void ForEach<T>(this IEnumerable<T> v, Action<T> a)
	{
		foreach (T item in v)
		{
			a(item);
		}
	}

	public struct Range
	{
		public long Offset { get; set; }
		public long Count { get; set; }

		public Range(long offset, long count)
		{
			this.Offset = offset;
			this.Count = count;
		}
	}

	public static NotSupportedException OSNotSupportedException()
	{
		return new NotSupportedException("Operating system not supported.");
	}
}
