using System;

namespace Velox.Common;

internal class CriticalDatabaseException : Exception
{
	public CriticalDatabaseException() { }
	public CriticalDatabaseException(string message) : base(message) { }
	public CriticalDatabaseException(string message, Exception inner) : base(message, inner) { }
}
