using System;
using System.Reflection;
using System.Text;
using VeloxDB.Common;

namespace VeloxDB.ClientApp;

internal sealed class ArgumentParser
{
	Dictionary<string, List<Type>> commandTypes;

	public ArgumentParser(ProgramMode programMode, Type[] commandTypes)
	{
		this.commandTypes = new Dictionary<string, List<Type>>(64);
		foreach (Type type in commandTypes)
		{
			CommandAttribute ca = type.GetCustomAttribute<CommandAttribute>();
			if (!this.commandTypes.TryGetValue(ca.Name(programMode), out List<Type> l))
			{
				l = new List<Type>();
				this.commandTypes.Add(ca.Name(programMode), l);
			}

			if (programMode == ProgramMode.Direct && ca.SupportsDirectMode && l.Find(x => x.GetCustomAttribute<CommandAttribute>().SupportsDirectMode) != null)
				throw new ArgumentException(String.Format("Command {0} is not unique in direct mode.", ca.Name(programMode)));

			l.Add(type);
		}
	}

	public void ShowHelp(Mode mode, ProgramMode programMode)
	{
		List<Type> types = new List<Type>(commandTypes.Count);
		commandTypes.Values.ForEach(x => types.AddRange(x));
		if (programMode == ProgramMode.Direct)
		{
			types = types.Where(x => x.GetCustomAttribute<CommandAttribute>().SupportsDirectMode).ToList();
		}
		else
		{
			types = types.Where(x =>
			{
				return x.GetCustomAttribute<CommandAttribute>().SupportsInteractiveMode &&
					(mode == null || ((Command)Activator.CreateInstance(x)).IsModeValid(mode));
			}).ToList();
		}

		Console.ForegroundColor = Colors.HelpSection;
		Console.WriteLine("Description:");
		Console.ResetColor();
		Console.WriteLine(programMode == ProgramMode.Interactive ? "  Available commands in current mode." : "Available commands.");
		Console.WriteLine();

		Console.ForegroundColor = Colors.HelpSection;
		Console.WriteLine("Commands:");
		Console.ResetColor();

		types.Sort((x, y) => x.Name.CompareTo(y.Name));

		Table table = new Table(new Table.ColumnDesc[] {
			new Table.ColumnDesc() { Color = Colors.HelpParam, WidthPriority = 10 },
			new Table.ColumnDesc() { WidthPriority = 5, WordWrap = true }
		});

		for (int i = 0; i < types.Count; i++)
		{
			CommandAttribute ca = types[i].GetCustomAttribute<CommandAttribute>();
			string[] s = new string[] { "  " + ca.Name(programMode), ca.Description };
			table.AddRow(s);
		}

		table.Show();
	}

	public string TryParse(string[] args, Mode mode, ProgramMode programMode, out Command command, out string suggestion)
	{
		suggestion = "Run help command to se all available commands. Or run \"command --help\" to see help for a specific command.";

		command = null;
		if (args.Length == 0)
			return "No command specified.";

		if (!commandTypes.TryGetValue(args[0], out List<Type> types))
			return "Invalid command specified.";

		command = CreateCommand(types, mode, programMode, out HashSet<string> mandatoryParams);
		if (command == null)
			return "Invalid command specified.";

		if (args.Length == 2 && (args[1].Equals("--help") || args[1].Equals("-h")))
		{
			command.IsHelp = true;
			return null;
		}

		if (programMode == ProgramMode.Interactive && mode != null && !command.IsModeValid(mode))
			return "Command is invalid in a given mode.";

		CommandAttribute ca = command.GetType().GetCustomAttribute<CommandAttribute>();
		suggestion = $"Run \"{ca.Name(programMode)} --help\" to see how to use this command.";

		int i = 1;
		while (i < args.Length)
		{
			string paramName = args[i++];
			PropertyInfo pi;

			if (paramName.StartsWith("--"))
			{
				pi = FindProperty(command.GetType(), programMode, true, paramName.Substring(2));
			}
			else if (paramName.StartsWith("-"))
			{
				pi = FindProperty(command.GetType(), programMode, false, paramName.Substring(1));
			}
			else
			{
				pi = TryGetOnlyProperty(command.GetType());
				i--;
				if (pi == null)
					return $"Invalid paramter name {paramName}. Paramter names are prefixed with '--' or '-'.";
			}

			if (pi == null)
				return $"Parameter {paramName} not available for command {args[0]}.";

			mandatoryParams.Remove(pi.Name);
			string error = TrySetParameterValue(command, pi, paramName, args, ref i);
			if (error != null)
				return error;
		}

		if (mandatoryParams.Count > 0)
		{
			string error = command.ValidateParams(mandatoryParams);
			if (error != null)
				return error;
		}

		return null;
	}

	private string TrySetParameterValue(Command command, PropertyInfo pi, string paramName, string[] args, ref int index)
	{
		object objValue = null;
		if (pi.PropertyType == typeof(bool))
		{
			if (index == args.Length || args[index].StartsWith("--") || args[index].StartsWith("-"))
			{
				objValue = true;
			}
			else
			{
				objValue = args[index].Equals("true", StringComparison.OrdinalIgnoreCase);
				if (!(bool)objValue && !args[index].Equals("false", StringComparison.OrdinalIgnoreCase))
					return $"Invalid value for parameter {paramName}.";
			}
		}
		else
		{
			if (index == args.Length || args[index].StartsWith("--") || args[index].StartsWith("-"))
				return $"Missing value for parameter {paramName}.";

			try
			{
				if (pi.PropertyType == typeof(string[]))
				{
					List<string> list = new List<string>();
					while (index < args.Length && !args[index].StartsWith("--") && !args[index].StartsWith("-"))
					{
						list.Add(args[index++]);
					}

					objValue = list.ToArray();
				}
				else
				{
					if (pi.PropertyType == typeof(int))
						objValue = int.Parse(args[index]);
					else if (pi.PropertyType == typeof(long))
						objValue = long.Parse(args[index]);
					else if (pi.PropertyType == typeof(double))
						objValue = double.Parse(args[index]);
					else if (pi.PropertyType == typeof(string))
						objValue = args[index];
					else if (pi.PropertyType.IsEnum)
					{
						objValue = Enum.Parse(pi.PropertyType, args[index], true);
					}

					index++;
				}
			}
			catch (Exception e) when (e is FormatException || e is OverflowException)
			{
				return $"Invalid value for parameter {paramName}.";
			}
		}

		pi.SetValue(command, objValue);
		return null;
	}

	private static PropertyInfo TryGetOnlyProperty(Type type)
	{
		PropertyInfo theOne = null;
		foreach (PropertyInfo pi in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy))
		{
			ParamAttribute pa = pi.GetCustomAttribute<ParamAttribute>();
			if (pa != null)
			{
				if (theOne != null)
					return null;

				theOne = pi;
			}
		}

		return theOne;
	}

	private PropertyInfo FindProperty(Type type, ProgramMode programMode, bool isLongName, string name)
	{
		foreach (PropertyInfo pi in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy))
		{
			ParamAttribute pa = pi.GetCustomAttribute<ParamAttribute>();
			if (pa != null)
			{
				if ((isLongName && name.Equals(pa.Name, StringComparison.Ordinal)) ||
					(!isLongName && name.Equals(pa.ShortName, StringComparison.Ordinal)))
				{
					if ((pa.ProgramMode & programMode) != 0)
						return pi;
				}
			}
		}

		return null;
	}

	private Command CreateCommand(List<Type> types, Mode mode, ProgramMode programMode, out HashSet<string> mandatoryParams)
	{
		Type type;
		if (programMode == ProgramMode.Direct)
		{
			type = types.Where(x => x.GetCustomAttribute<CommandAttribute>().SupportsDirectMode).FirstOrDefault();
		}
		else
		{
			type = types.Where(x =>
			{
				return x.GetCustomAttribute<CommandAttribute>().SupportsInteractiveMode &&
				(mode == null || ((Command)Activator.CreateInstance(x)).IsModeValid(mode));
			}).FirstOrDefault();
		}

		if (type == null)
		{
			mandatoryParams = null;
			return null;
		}

		mandatoryParams = new HashSet<string>();
		foreach (PropertyInfo pi in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy))
		{
			ParamAttribute pa = pi.GetCustomAttribute<ParamAttribute>();
			if (pa != null && pa.SupportsMode(programMode) && pa.IsMandatory)
				mandatoryParams.Add(pi.Name);
		}

		return (Command)Activator.CreateInstance(type);
	}
}
