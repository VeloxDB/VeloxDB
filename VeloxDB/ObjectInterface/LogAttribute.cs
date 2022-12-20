using System;

namespace VeloxDB.ObjectInterface;

/// <summary>
/// Specifies a log to which to write class's data.
/// </summary>
/// <remarks>
///	VeloxDB persists data in log files. It is possible to use multiple log files.
///	For more information about how and when to use additional log files see
///	<see href="~/articles/guide/persistence.md#log-files">Log files</see> in user guide.
/// </remarks>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class LogAttribute : Attribute
{
    string logName;

	/// <param name="logName">Name of the log to use.</param>
    public LogAttribute(string logName)
    {
    }

	/// <summary>
	/// Gets the log name.
	/// </summary>
    public string LogName { get => logName; set => logName = value; }
}
