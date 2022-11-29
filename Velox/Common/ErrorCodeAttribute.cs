using System;

namespace Velox.Common;

[AttributeUsage(AttributeTargets.Field, Inherited = false, AllowMultiple = false)]
internal sealed class ErrorCodeAttribute : Attribute
{
	string errorString;

	public ErrorCodeAttribute(string errorString)
	{
		this.errorString = errorString;
	}

	public string ErrorString => errorString;
}

[AttributeUsage(AttributeTargets.Field, Inherited = false, AllowMultiple = false)]
internal sealed class ErrorCodeParamAttribute: Attribute
{
	int orderNum;

	public ErrorCodeParamAttribute()
	{
	}

	public ErrorCodeParamAttribute(int orderNum)
	{
		this.orderNum = orderNum;
	}

	public int OrderNum => orderNum;
}
