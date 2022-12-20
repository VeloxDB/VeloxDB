using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text.RegularExpressions;
namespace VeloxDB.Config;

internal static class PathTemplate
{
	const string nodeNameKey = "NodeName";

	static Dictionary<string, string> replacements;
	static Regex regex;

	static readonly Environment.SpecialFolder[] Paths =
	{
		Environment.SpecialFolder.ApplicationData,
		Environment.SpecialFolder.LocalApplicationData,
		Environment.SpecialFolder.UserProfile
	};

	static PathTemplate()
	{
		regex = new Regex("\\$\\{([A-Za-z]*)\\}");
		replacements = new Dictionary<string, string>();

		foreach (Environment.SpecialFolder path in Paths)
		{
			replacements.Add(path.ToString(), Environment.GetFolderPath(path));
		}

		replacements["Base"] = TrimEnd(AppDomain.CurrentDomain.BaseDirectory);
		replacements["Temp"] = TrimEnd(Path.GetTempPath());
	}

	private static string TrimEnd(string path)
	{
		if (path.EndsWith(Path.DirectorySeparatorChar))
			return path.Substring(0, path.Length - 1);

		return path;
	}

	public static string TryEvaluate(string path,out List<string> errors, string nodeName = null)
	{
		List<string> foundErrors = new List<string>();
		string result = regex.Replace(path, match => {
			string value = null;
			string key = match.Groups[1].Value;

			if (!replacements.TryGetValue(key, out value))
			{
				if (nodeName != null && key.Equals(nodeNameKey, StringComparison.Ordinal))
				{
					value = nodeName;
				}
				else
				{
					foundErrors.Add($"Unknown path template: {key}");
					value = $"{{{key}}}";
				}
			}

			return value;
		});
		
		errors = foundErrors;
		return result;
	}
}
