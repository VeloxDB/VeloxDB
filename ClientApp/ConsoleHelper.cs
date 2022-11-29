using System;
using System.Net.Sockets;

namespace Velox.ClientApp;

internal static class ConsoleHelper
{
	static readonly char[] emptyChars = Enumerable.Range(0, 120).Select(x => ' ').ToArray();

	public static void ShowMissingClusterBinding()
	{
		ShowErrors(new string[] { "Cluster binding has not been established. See bind command for more details." });
	}

	public static void ShowError(string error)
	{
		ShowErrors(new string[] { error });
	}

	public static void ShowErrors(IEnumerable<string> errors)
	{
		Console.ForegroundColor = Colors.Error;
		foreach (string error in errors)
		{
			Console.WriteLine(error);
		}

		Console.ResetColor();
	}

	public static void ShowError(string error, Exception e)
	{
		Console.ForegroundColor = Colors.Error;
		if (error != null)
			Console.WriteLine(error);

		while (e != null)
		{
			Console.WriteLine(e.Message);
			e = e.InnerException;
		}

		Console.ResetColor();
	}

	public static bool Confirmation(string message)
	{
		Console.Write(message);
		while (true)
		{
			ConsoleKey key;
			if (Console.IsInputRedirected)
			{
				char ch = (char)Console.Read();
				key = (ConsoleKey)(int)ch;
			}
			else
			{
				key = Console.ReadKey(true).Key;
			}

			if (key == ConsoleKey.Y)
			{
				Console.WriteLine('Y');
				return true;
			}
			else if (key == ConsoleKey.N)
			{
				Console.WriteLine('N');
				return false;
			}
		}
	}

	public static int WindowWidth
	{
		get
		{
			if (ReadLine.IsRedirectedOrAlternate)
				return 80;

			return Console.WindowWidth;
		}
	}

	public static int WindowHeight
	{
		get
		{
			if (ReadLine.IsRedirectedOrAlternate)
				return 40;

			return Console.WindowHeight;
		}
	}
}
