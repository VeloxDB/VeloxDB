using System.Reflection;
using System.Text;
using VeloxDB.ClientApp.Modes;

namespace VeloxDB.ClientApp;

internal class Program
{
	public const int ConnectionOpenTimeout = 5000;
	public const int ConnectionRetryTimeout = 10000;

	AutoResetEvent commandFinishedEvent;
	ArgumentParser argumentParser;
	string[] args;
	List<Mode> modeList;
	ProgramMode programMode;

	public Program(string[] args, AutoResetEvent commandFinishedEvent)
	{
		this.commandFinishedEvent = commandFinishedEvent;
		this.args = args;
		programMode = args.Length == 0 ? ProgramMode.Interactive : ProgramMode.Direct;
		argumentParser = new ArgumentParser(programMode, DiscoverCommandTypes(programMode));
		modeList = new List<Mode>();
		modeList.Add(new InitialMode());
	}

	public Mode Mode => modeList.Count == 0 ? null : modeList[modeList.Count - 1];
	public ProgramMode ProgramMode => programMode;

	public void EnterMode(Mode mode)
	{
		modeList.Add(mode);
	}

	public void ExitMode()
	{
		modeList.RemoveAt(modeList.Count - 1);
	}

	public static int Main(string[] args)
	{
		Program program = new Program(args, null);
		return program.Run();
	}

	public static void AlternateMain(string[] args, AutoResetEvent commandFinishedEvent)
	{
		Program program = new Program(args, commandFinishedEvent);
		program.Run();
	}

	private int Run()
	{
		if (programMode == ProgramMode.Interactive)
			return RunInteractive();
		else
			return RunDirect();
	}

	private int RunDirect()
	{
		return RunCommand(args);
	}

	private int RunInteractive()
	{
		Console.ForegroundColor = ConsoleColor.White;
		Console.WriteLine("VeloxDB client application, interactive mode.");
		Console.WriteLine();
		Console.ResetColor();

		ReadLine.HistoryEnabled = true;

		while (modeList.Count > 0)
		{
			Console.Write(GetModeTitle());
			commandFinishedEvent?.Set();

			string[] args = ReadArgs(out string error);

			if (args == null)
				break;

			if (error != null)
			{
				ConsoleHelper.ShowError(error);
			}
			else 
			{
				RunCommand(args);
			}

			Console.WriteLine();
		}

		commandFinishedEvent?.Set();
		return 0;
	}

	private int RunCommand(string[] args)
	{
		string error = argumentParser.TryParse(args, this.Mode, this.ProgramMode, out Command command, out string suggestion);
		if (error != null)
		{
			ConsoleHelper.ShowError(error);
			if (suggestion != null)
				Console.WriteLine(suggestion);

			return 1;
		}

		return command.Execute(this);
	}

	private string[] ReadArgs(out string error)
	{
		string s = ReadLine.Read();

		if (s == null)
		{
			error = "";
			return null;
		}

		List<string> args = new List<string>();
		StringBuilder word = new StringBuilder(16);

		bool insideQuotationMarks = false;
		for (int i = 0; i < s.Length; i++)
		{
			if (s[i] == ' ')
			{
				if (!insideQuotationMarks && word.Length > 0)
				{
					args.Add(word.ToString());
					word.Clear();
				}
			}
			else if (s[i] == '\"')
			{
				if (word.Length > 0)
					args.Add(word.ToString());

				word.Clear();
				insideQuotationMarks = !insideQuotationMarks;
			}
			else
			{
				word.Append(s[i]);
			}
		}

		if (insideQuotationMarks)
		{
			error = "Invalid command format.";
			return null;
		}

		if (word.Length > 0)
			args.Add(word.ToString());

		error = null;
		return args.ToArray();
	}

	private string GetModeTitle()
	{
		StringBuilder sb = new StringBuilder(16);
		for (int i = 0; i < modeList.Count; i++)
		{
			sb.Append(modeList[i].Title);
			if (i < modeList.Count - 1)
				sb.Append('/');
		}

		sb.Append("> ");
		return sb.ToString();
	}

	public void ShowHelp()
	{
		argumentParser.ShowHelp(this.Mode, programMode);
	}

	private Type[] DiscoverCommandTypes(ProgramMode programMode)
	{
		Assembly assembly = typeof(Program).Assembly;
		return assembly.GetTypes().Where(x =>
		{
			return x.IsDefined(typeof(CommandAttribute)) && x.GetCustomAttribute<CommandAttribute>().SupportsMode(programMode);
		}).ToArray();
	}
}

[Flags]
internal enum ProgramMode
{
	Direct = 0x01,
	Interactive = 0x02,
	Both = Direct | Interactive
}
