using System;

namespace VeloxDB.Common;

internal class AssertEvaluationException : Exception
{
	public AssertEvaluationException() { }
	public AssertEvaluationException(string message) : base(message) { }
	public AssertEvaluationException(string message, Exception inner) : base(message, inner) { }
}
