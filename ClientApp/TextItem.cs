using System;

namespace VeloxDB.ClientApp;

internal sealed class TextItem
{
	public string Text { get; set; }
	public ConsoleColor? Color { get; set; }
	public ConsoleColor? BackgroundColor { get; set; }

	public TextItem()
	{
	}

	public static implicit operator TextItem(string s) => new TextItem() { Text = s };

	public void Show(int left, int top, ScreenBuffer buffer)
	{
		ConsoleColor fcol = (ConsoleColor)ScreenBuffer.NoColor;
		if (Color.HasValue)
			fcol = Color.Value;

		ConsoleColor bcol = (ConsoleColor)ScreenBuffer.NoColor;
		if (BackgroundColor.HasValue)
			bcol = BackgroundColor.Value;

		buffer.Write(left, top, Text, fcol, bcol);
	}

	public void Show()
	{
		bool b = false;
		if (Color.HasValue)
		{
			b = true;
			Console.ForegroundColor = Color.Value;
		}

		if (BackgroundColor.HasValue)
		{
			b = true;
			Console.BackgroundColor = BackgroundColor.Value;
		}

		Console.Write(Text);

		if (b)
			Console.ResetColor();
	}
}
