using System;
using System.Diagnostics.CodeAnalysis;
using VeloxDB.Common;

namespace VeloxDB.Server;

internal sealed class Program
{
	private static bool wait;

	public static void Main(string[] args)
	{
		Configuration configuration;

		Arguments parsed = Parse(args);
		wait = parsed.Wait;

		AppDomain.CurrentDomain.UnhandledException += UnhandledExceptionHandler;
		TaskScheduler.UnobservedTaskException += UnobservedTaskExceptionHandler;

		if (parsed.Error)
		{
			Console.Error.WriteLine($"vlxdbsrv: {parsed.Message}\n");
		}

		if (parsed.PrintHelp)
		{
			PrintHelp();
		}

		if (parsed.PrintHelp || parsed.Error)
			return;

		try
		{
			if (parsed.ConfigFile != null)
			{
				configuration = Configuration.Load(parsed.ConfigFile);
			}
			else
			{
				configuration = Configuration.Load();
			}
		}
		catch (ConfigurationException e)
		{
			Console.Error.WriteLine(e.Message);
			return;
		}

		InitLog(configuration, parsed.Interactive);

		try
		{
			if (parsed.PersistanceDir != null)
			{
				InitPersistence(configuration, parsed.PersistanceDir);
			}
		}
		catch (UnauthorizedAccessException e)
		{
			Console.Error.WriteLine(e.Message);
			return;
		}

		bool success = false;
		using (Server host = new Server(configuration, updateAssembliesDir: parsed.UpdateAsmDir, persistenceDir: parsed.PersistanceDir))
		{
			 success = host.Run();
		}

		if (!success)
			CheckKeyPress();

		Tracing.Info("Server shut down.");
	}

	private static void InitPersistence(Configuration configuration, string persistanceDir)
	{
		Checker.AssertNotNull(configuration.Database);

		string systemDir = Path.Join(persistanceDir, "system");

		Directory.CreateDirectory(systemDir);
		configuration.Database.SystemDatabasePath = systemDir;
	}

	private static void PrintHelp()
	{
		string[] messages = new string[]{
			"Usage: vlxdbsrv [Option]",
			"Starts VeloxDB Server\n",
			"\t--config filename               Overrides the config file.",
			"\t--interactive                   Starts in interactive mode, logs to console.",
			"\t--init-persistence directory    Initializes persistence if it's not initialized with the given directory.",
			"\t--update-assemblies directory   Updates assemblies using assemblies in the supplied directory.",
			"\t--wait                          Wait for a key press if an error is encountered during the run.",
			"\t--help                          Displays this help.",
		};

		foreach (string message in messages)
			Console.WriteLine(message);
	}

	private record Arguments(bool Error, string Message, string? ConfigFile, string? UpdateAsmDir, string? PersistanceDir, bool Interactive, bool Wait, bool PrintHelp);

	private static Arguments Parse(string[] args)
	{
		int i = 0;

		bool error = false;
		string message = "";
		string? configFile = null;
		string? updateAsmDir = null;
		string? persistanceDir = null;
		bool interactive = false;
		bool wait = false;
		bool printHelp = false;

		void RaiseError(string m)
		{
			error = true;
			message = m;
			printHelp = true;
		}

		bool TryGetArg(int i, [NotNullWhen(true)] out string? res)
		{
			if (i >= args.Length)
			{
				RaiseError($"Missing argument at position {i}.");
				res = null;
				return false;
			}

			res = args[i];
			return true;
		}

		while (i < args.Length)
		{
			if (!args[i].StartsWith("--"))
			{
				RaiseError("Expected command argument.");
				break;
			}

			string arg = args[i].Substring(2);

			if (arg == "config")
			{
				if (!TryGetArg(i + 1, out configFile))
					break;
				i++;
			}
			else if (arg == "interactive")
			{
				interactive = true;
			}
			else if (arg == "update-assemblies")
			{
				if (!TryGetArg(i + 1, out updateAsmDir))
					break;
				i++;
			}
			else if (arg == "init-persistence")
			{
				if (!TryGetArg(i + 1, out persistanceDir))
					break;
				i++;
			}
			else if(arg == "wait")
			{
				wait = true;
			}
			else if (arg == "help")
			{
				printHelp = true;
			}
			else
			{
				RaiseError($"Uknown argument {arg}.");
				break;
			}

			i++;
		}

		return new Arguments(error, message, configFile, updateAsmDir, persistanceDir, interactive, wait, printHelp);
	}

	private static void InitLog(Configuration configuration, bool logToConsole)
	{
		Checker.AssertNotNull(configuration.Logging);
		Checker.AssertNotNull(configuration.Logging.Level);

		LoggingLevel cfgLevel = (LoggingLevel)configuration.Logging.Level;
		LoggingLevel userCfgLevel = (LoggingLevel)configuration.Logging.Level;

		if (cfgLevel != LoggingLevel.None || userCfgLevel != LoggingLevel.None)
		{
			if (!logToConsole)
				Tracing.CreateTextFileCollector(configuration.Logging.Path);
			else
				Tracing.AddCollector(new ConsoleCollector());

			Tracing.SetTraceLevel((TraceLevel)cfgLevel);
			APITrace.SetTraceLevel((TraceLevel)userCfgLevel);
		}
	}

	private static void UnobservedTaskExceptionHandler(object? sender, UnobservedTaskExceptionEventArgs e)
	{
		Tracing.Error(e.Exception);
	}

	private static void UnhandledExceptionHandler(object sender, UnhandledExceptionEventArgs e)
	{
		if (e.ExceptionObject is Exception)
			Tracing.Error((Exception)e.ExceptionObject);

		CheckKeyPress();

		System.Diagnostics.Process.GetCurrentProcess().Kill();
	}

	private static void CheckKeyPress()
	{
		if (wait)
		{
			Console.WriteLine("Press any key to exit...");
			Console.ReadKey();
		}
	}
}
