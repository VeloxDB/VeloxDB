using System;
using System.Linq;
using System.Reflection;
using System.Text;
using VeloxDB.ClientApp.Modes;

namespace VeloxDB.ClientApp;

[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
internal sealed class CommandAttribute : Attribute
{
	string name;

	public CommandAttribute(string name, string description)
	{
		this.name = name;
		this.Description = description;
		ProgramMode = ProgramMode.Interactive;
	}

	public string DirectModeName { get; set; }
	public string Description { get; private set; }
	public string Usage { get; set; }
	public ProgramMode ProgramMode { get; set; }

	public bool SupportsDirectMode => (ProgramMode & ProgramMode.Direct) != 0;
	public bool SupportsInteractiveMode => (ProgramMode & ProgramMode.Interactive) != 0;

	public bool SupportsMode(ProgramMode programMode) => (ProgramMode & programMode) != 0;

	public string Name(ProgramMode mode)
	{
		if (mode == ProgramMode.Direct && DirectModeName != null)
			return DirectModeName;
		else
			return name;
	}
}

[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
internal sealed class ParamAttribute : Attribute
{
	public ParamAttribute(string name, string description)
	{
		this.Name = name;
		this.Description = description;
		this.IsMandatory = false;
		this.ProgramMode = ProgramMode.Both;
	}

	public bool IsMandatory { get; set; }
	public string Description { get; private set; }
	public string Name { get; private set; }
	public string ShortName { get; set; }
	public ProgramMode ProgramMode { get; set; }
	public bool SupportsMode(ProgramMode programMode) => (ProgramMode & programMode) != 0;
}

internal abstract class Command
{
	public bool IsHelp { get; set; }

	public abstract bool IsModeValid(Mode mode);
	protected abstract bool OnExecute(Program program);

	protected virtual bool OnPreExecute(Program program)
	{
		return true;
	}

	public int Execute(Program program)
	{
		if (IsHelp)
		{
			ShowHelp(program.ProgramMode);
			return 0;
		}
		else
		{
			if (OnPreExecute(program))
			{
				if (OnExecute(program))
					return 0;
			}

			return 1;
		}
	}

	public virtual string ValidateParams(HashSet<string> missingParams)
	{
		if (missingParams.Count > 0)
			return FormatMandatoryParamListError(missingParams);

		return null;
	}

	private string FormatMandatoryParamListError(HashSet<string> mandatoryParams)
	{
		StringBuilder sb = new StringBuilder();
		sb.Append("Mandatory parameters were not provided: ");
		foreach (string propName in mandatoryParams)
		{
			PropertyInfo pi = this.GetType().GetProperty(propName, BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy);
			sb.Append("--").Append(pi.Name).Append(' ');
		}

		sb.Length--;
		return sb.ToString();
	}

	private void ShowHelp(ProgramMode programMode)
	{
		Type type = this.GetType();
		CommandAttribute ca = type.GetCustomAttribute<CommandAttribute>(false);

		List<ParamAttribute> @params = type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy).
			Where(x =>
			{
				return x.IsDefined(typeof(ParamAttribute)) && x.GetCustomAttribute<ParamAttribute>().SupportsMode(programMode);
			}).Select(x => x.GetCustomAttribute<ParamAttribute>()).ToList();

		Console.ForegroundColor = Colors.HelpSection;
		Console.WriteLine("Description:");
		Console.ResetColor();
		Console.WriteLine("  " + ca.Description);
		Console.WriteLine();

		Console.ForegroundColor = Colors.HelpSection;
		Console.WriteLine("Usage:");
		Console.ResetColor();
		if (ca.Usage != null)
		{
			Console.WriteLine("  " + ca.Usage);
		}
		else
		{
			Console.Write($"  {ca.Name(programMode)}");
			Console.WriteLine(@params.Count > 0 ? $" [parameters]" : string.Empty);
		}

		if (@params.Count > 0)
		{
			Console.WriteLine();

			Console.ForegroundColor = Colors.HelpSection;
			Console.WriteLine("Parameters:");
			Console.ResetColor();

			Table table = new Table(new Table.ColumnDesc[] {
				new Table.ColumnDesc() { Color = Colors.HelpParam, WidthPriority = 10 },
				new Table.ColumnDesc() { WidthPriority = 5, WordWrap = true }
			});

			for (int i = 0; i < @params.Count; i++)
			{
				string[] s = new string[] { "  " + GetParamTitle(@params[i]), (@params[i].IsMandatory ? "*" : String.Empty) + @params[i].Description };
				table.AddRow(s);
			}

			table.Show();
		}
	}

	private string GetParamTitle(ParamAttribute pa)
	{
		if (pa.ShortName != null)
			return $"--{pa.Name}, -{pa.ShortName}";
		else
			return $"--{pa.Name}";
	}

	protected bool CheckClusterBinding(Program program)
	{
		InitialMode initMode = (InitialMode)program.Mode;
		if ((program.Mode as InitialMode).ClusterConfig == null)
		{
			ConsoleHelper.ShowError("Cluster binding has not been established.");
			return false;
		}

		return true;
	}
}
