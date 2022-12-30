using System;
using System.Collections.Generic;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Xml.Linq;

namespace VeloxDB.Common;

/// <summary>
/// Provides methods to trace execution of the user APIs.
/// </summary>
public static class APITrace
{
	static Tracing.Source source;

	static APITrace()
	{
		source = new Tracing.Source("API");
	}

	/// <summary>
	/// Get the current trace level.
	/// </summary>
	public static TraceLevel Level => source.Level;

	/// <summary>
	/// Gets the value indicating whether a trace message whould be collected given the current trace level.
	/// </summary>
	/// <param name="level"></param>
	/// <returns></returns>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static bool ShouldTrace(TraceLevel level)
	{
		return source.ShouldTrace(level);
	}

	internal static void AddCollector(ITraceCollector collector)
	{
		source.AddCollector(collector);
	}

	internal static void RemoveCollector(ITraceCollector collector)
	{
		source.RemoveCollector(collector);
	}

	internal static void SetTraceLevel(TraceLevel level)
	{
		source.SetTraceLevel(level);
	}

	/// <summary>
	/// Writes an exception as an error message with additional user provided message.
	/// </summary>
	/// <param name="e">The exception.</param>
	/// <param name="format">Message format string.</param>
	/// <param name="args">Format arguments.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error(Exception e, string format, params object[] args)
	{
		source.Error(e, format, args);
	}

	/// <summary>
	/// Writes an exception as an error message.
	/// </summary>
	/// <param name="e">The exception.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error(Exception e)
	{
		Error(e, null);
	}

	/// <summary>
	/// Writes an error message.
	/// </summary>
	/// <param name="message">Message text.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error(string message)
	{
		source.Error(message);
	}

	/// <summary>
	/// Writes a warning message.
	/// </summary>
	/// <param name="message">Message text.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Warning(string message)
	{
		source.Warning(message);
	}

	/// <summary>
	/// Writes an information message.
	/// </summary>
	/// <param name="message">Message text.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Info(string message)
	{
		source.Info(message);
	}

	/// <summary>
	/// Writes a debug message.
	/// </summary>
	/// <param name="message">Message text.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Debug(string message)
	{
		source.Debug(message);
	}

	/// <summary>
	/// Writes a verbose message.
	/// </summary>
	/// <param name="message">Message text.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Verbose(string message)
	{
		source.Verbose(message);
	}

	/// <summary>
	/// Writes a formated error message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="args">Format arguments.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error(string format, params object[] args)
	{
		source.Error(format, args);
	}

	/// <summary>
	/// Writes a formated warning message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="args">Format arguments.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Warning(string format, params object[] args)
	{
		source.Warning(format, args);
	}

	/// <summary>
	/// Writes a formated info message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="args">Format arguments.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Info(string format, params object[] args)
	{
		source.Info(format, args);
	}

	/// <summary>
	/// Writes a formated debug message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="args">Format arguments.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Debug(string format, params object[] args)
	{
		source.Debug(format, args);
	}

	/// <summary>
	/// Writes a formated error message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error<T1>(string format, T1 v1)
	{
		source.Error(format, v1);
	}

	/// <summary>
	/// Writes a formated warning message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Warning<T1>(string format, T1 v1)
	{
		source.Warning(format, v1);
	}

	/// <summary>
	/// Writes a formated information message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Info<T1>(string format, T1 v1)
	{
		source.Info(format, v1);
	}

	/// <summary>
	/// Writes a formated debug message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Debug<T1>(string format, T1 v1)
	{
		source.Debug(format, v1);
	}

	/// <summary>
	/// Writes a formated verbose message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Verbose<T1>(string format, T1 v1)
	{
		source.Verbose(format, v1);
	}

	/// <summary>
	/// Writes a formated error message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error<T1, T2>(string format, T1 v1, T2 v2)
	{
		source.Error(format, v1, v2);
	}

	/// <summary>
	/// Writes a formated warning message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Warning<T1, T2>(string format, T1 v1, T2 v2)
	{
		source.Warning(format, v1, v2);
	}

	/// <summary>
	/// Writes a formated information message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Info<T1, T2>(string format, T1 v1, T2 v2)
	{
		source.Info(format, v1, v2);
	}

	/// <summary>
	/// Writes a formated debug message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Debug<T1, T2>(string format, T1 v1, T2 v2)
	{
		source.Debug(format, v1, v2);
	}

	/// <summary>
	/// Writes a formated verbose message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Verbose<T1, T2>(string format, T1 v1, T2 v2)
	{
		source.Verbose(format, v1, v2);
	}

	/// <summary>
	/// Writes a formated error message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	/// <param name="v3">Third format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
	{
		source.Error(format, v1, v2, v3);
	}

	/// <summary>
	/// Writes a formated warning message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	/// <param name="v3">Third format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Warning<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
	{
		source.Warning(format, v1, v2, v3);
	}

	/// <summary>
	/// Writes a formated information message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	/// <param name="v3">Third format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Info<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
	{
		source.Info(format, v1, v2, v3);
	}

	/// <summary>
	/// Writes a formated debug message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	/// <param name="v3">Third format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Debug<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
	{
		source.Debug(format, v1, v2, v3);
	}

	/// <summary>
	/// Writes a formated verbose message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	/// <param name="v3">Third format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Verbose<T1, T2, T3>(string format, T1 v1, T2 v2, T3 v3)
	{
		source.Verbose(format, v1, v2, v3);
	}

	/// <summary>
	/// Writes a formated error message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	/// <param name="v3">Third format argument.</param>
	/// <param name="v4">Fourth format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Error<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
	{
		source.Error(format, v1, v2, v3, v4);
	}

	/// <summary>
	/// Writes a formated warning message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	/// <param name="v3">Third format argument.</param>
	/// <param name="v4">Fourth format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Warning<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
	{
		source.Warning(format, v1, v2, v3, v4);
	}

	/// <summary>
	/// Writes a formated information message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	/// <param name="v3">Third format argument.</param>
	/// <param name="v4">Fourth format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Info<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
	{
		source.Info(format, v1, v2, v3, v4);
	}

	/// <summary>
	/// Writes a formated debug message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	/// <param name="v3">Third format argument.</param>
	/// <param name="v4">Fourth format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Debug<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
	{
		source.Debug(format, v1, v2, v3, v4);
	}

	/// <summary>
	/// Writes a formated verbose message.
	/// </summary>
	/// <param name="format">Format string.</param>
	/// <param name="v1">First format argument.</param>
	/// <param name="v2">Second format argument.</param>
	/// <param name="v3">Third format argument.</param>
	/// <param name="v4">Fourth format argument.</param>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public static void Verbose<T1, T2, T3, T4>(string format, T1 v1, T2 v2, T3 v3, T4 v4)
	{
		source.Verbose(format, v1, v2, v3, v4);
	}
}
