using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace VeloxDB.Common;

internal static class Checker
{
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void NotDisposed(bool disposed)
	{
		if (disposed)
			throw new ObjectDisposedException("Object is disposed.", (Exception)null);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void CheckRange(long v, long minv, long maxv, string paramName)
	{
		if (v < minv || v > maxv)
			throw new ArgumentOutOfRangeException();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void CheckRange(long v, long minv, string paramName)
	{
		if (v < minv)
			throw new ArgumentOutOfRangeException();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void CheckRange(double v, double minv, double maxv, string paramName)
	{
		if (v < minv || v > maxv)
			throw new ArgumentOutOfRangeException();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void CheckRange(double v, double minv, string paramName)
	{
		if (v < minv)
			throw new ArgumentOutOfRangeException();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void CheckRange(int v, int minv, int maxv, string paramName)
	{
		if (v < minv || v > maxv)
			throw new ArgumentOutOfRangeException();
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void CheckRange(int v, int minv, string paramName)
	{
		if (v < minv)
			throw new ArgumentOutOfRangeException();
	}

	[Conditional("DEBUG")]
	public static void AssertTrue(bool cond, string message = null)
	{
		if (!cond)
		{
			TTTrace.Write(cond);
			throw new AssertEvaluationException(message);
		}
	}

	[Conditional("DEBUG")]
	public static void AssertFalse(bool cond, string message = null)
	{
		if (cond)
		{
			TTTrace.Write(cond);
			throw new AssertEvaluationException(message);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void NotNull(object obj, string paramName)
	{
		if (obj == null)
		{
			TTTrace.Write();
			throw new ArgumentNullException(paramName, "Argument is null.");
		}
	}

	[Conditional("DEBUG")]
	public static void AssertNotNull([NotNull] object obj, string message = null)
	{
		if (obj == null)
		{
			TTTrace.Write();
			throw new AssertEvaluationException(message);
		}
	}

	[Conditional("DEBUG")]
	public static void AssertNotNull([NotNull] object obj1, [NotNull] object obj2, string message = null)
	{
		if (obj1 == null || obj2 == null)
		{
			TTTrace.Write();
			throw new AssertEvaluationException(message);
		}
	}

	[Conditional("DEBUG")]
	public static void AssertNotNull([NotNull] object obj1, [NotNull] object obj2,
		[NotNull] object obj3, string message = null)
	{
		if (obj1 == null || obj2 == null || obj3 == null)
		{
			TTTrace.Write();
			throw new AssertEvaluationException(message);
		}
	}

	[Conditional("DEBUG")]
	public static void AssertNotNull([NotNull] object obj1, [NotNull] object obj2, [NotNull] object obj3,
		[NotNull] object obj4, string message = null)
	{
		if (obj1 == null || obj2 == null || obj3 == null || obj4 == null)
		{
			TTTrace.Write();
			throw new AssertEvaluationException(message);
		}
	}

	[Conditional("DEBUG")]
	public static void AssertNull(object obj, string message = null)
	{
		if (obj != null)
		{
			TTTrace.Write();
			throw new AssertEvaluationException(message);
		}
	}

	[Conditional("DEBUG")]
	public static void AssertNotNullOrWhitespace(string s, string message = null)
	{
		if (string.IsNullOrWhiteSpace(s))
		{
			TTTrace.Write();
			throw new AssertEvaluationException(null);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void NotNullOrWhitespace(string s, string paramName)
	{
		NotNull(s, paramName);

		if (string.IsNullOrWhiteSpace(s))
			throw new ArgumentException(string.Format("Parameter {0} is null or whitespace.", paramName));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void InvalidOperationException(string format, params object[] p)
	{
		throw new InvalidOperationException(string.Format(format, p));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void NotSupportedException(string format, params object[] p)
	{
		throw new NotSupportedException(string.Format(format, p));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void ArgumentOutOfRangeException(string format, params object[] p)
	{
		throw new ArgumentOutOfRangeException(string.Format(format, p));
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void ArgumentException(string format, params object[] p)
	{
		throw new ArgumentException(string.Format(format, p));
	}
}
