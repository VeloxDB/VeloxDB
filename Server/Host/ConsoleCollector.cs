using System.Text;
using VeloxDB.Common;

namespace VeloxDB.Server;

internal sealed class ConsoleCollector : ITraceCollector
{
	readonly object sync = new object();

	TraceLevel current = TraceLevel.Verbose;
	public void AddTrace(TraceLevel level, StringBuilder formattedText)
	{
		if (current < level)
			return;

		string text = formattedText.ToString();

		lock (sync)
		{
			if (level > TraceLevel.Warning)
				Console.WriteLine(text);
			else if (level == TraceLevel.Error)
				WriteColor(text, ConsoleColor.Red);
			else if (level == TraceLevel.Warning)
				WriteColor(text, ConsoleColor.Yellow);
		}
	}

	private void WriteColor(string text, ConsoleColor color)
	{
		ConsoleColor old = Console.ForegroundColor;
		Console.ForegroundColor = color;
		Console.WriteLine(text);
		Console.ForegroundColor = old;
	}

	public void SetTraceLevel(TraceLevel level)
	{
		current = level;
	}
}
