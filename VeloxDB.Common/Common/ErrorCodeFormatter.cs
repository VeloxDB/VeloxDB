using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace VeloxDB.Common;

internal sealed class ErrorCodeFormatter
{
	FieldInfo[] paramFields;
	Dictionary<long, string> errorFormatStrings;

	public ErrorCodeFormatter(Type type, Type errorCodeEnumType)
	{
		Checker.AssertTrue(errorCodeEnumType.IsEnum);

		FieldInfo[] fis = type.GetFields(BindingFlags.Instance | BindingFlags.FlattenHierarchy | BindingFlags.Public | BindingFlags.NonPublic);

		int maxParamNum = -1;
		for (int j = 0; j < 2; j++)
		{
			for (int i = 0; i < fis.Length; i++)
			{
				ErrorCodeParamAttribute epa = fis[i].GetCustomAttribute<ErrorCodeParamAttribute>();
				if (epa == null)
					continue;

				if (paramFields != null)
				{
					paramFields[epa.OrderNum] = fis[i];
				}
				else
				{
					maxParamNum = Math.Max(maxParamNum, epa.OrderNum);
				}
			}

			if (paramFields == null)
				paramFields = new FieldInfo[maxParamNum + 1];
		}

		Array values = System.Enum.GetValues(errorCodeEnumType);
		errorFormatStrings = new Dictionary<long, string>(values.Length);
		for (int i = 0; i < values.Length; i++)
		{
			MemberInfo mi = errorCodeEnumType.GetMember(values.GetValue(i).ToString()).
				FirstOrDefault(x => x.DeclaringType == errorCodeEnumType);
			ErrorCodeAttribute eca = mi.GetCustomAttribute<ErrorCodeAttribute>();
			if (eca != null)
			{
				errorFormatStrings.Add(Convert.ToInt64(values.GetValue(i)), eca.ErrorString);
			}
		}
	}

	public string GetMessage<T>(object obj, T errorCode)
	{
		if (!errorFormatStrings.TryGetValue(Convert.ToInt64((object)errorCode), out string formatStr))
			throw new ArgumentException();

		object[] paramVals = new object[paramFields.Length];
		for (int i = 0; i < paramFields.Length; i++)
		{
			if (paramFields[i] != null)
				paramVals[i] = paramFields[i].GetValue(obj);
		}

		return string.Format(formatStr, paramVals);
	}
}
