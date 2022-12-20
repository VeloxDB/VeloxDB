using System;
using System.Runtime.CompilerServices;

namespace VeloxDB.ClientApp;

internal sealed class ScreenBuffer
{
	public const byte NoColor = byte.MaxValue;

	int width;
	int height;
	char[][] buffer;
	CellColor[][] colorBuffer;

	public ScreenBuffer()
	{
		Prepare();
	}

	public int Width => width;

	public void Prepare()
	{
		if (width == ConsoleHelper.WindowWidth && height >= ConsoleHelper.WindowHeight)
			return;

		width = ConsoleHelper.WindowWidth;
		height = ConsoleHelper.WindowHeight;

		buffer = new char[height][];
		colorBuffer = new CellColor[height][];
		for (int i = 0; i < buffer.Length; i++)
		{
			buffer[i] = new char[width];
			colorBuffer[i] = new CellColor[width];
			Array.Fill(buffer[i], ' ');
			Array.Fill(colorBuffer[i], new CellColor(NoColor, NoColor));
		}
	}

	private void ResizeHeight(int newHeight = 0)
	{
		newHeight = Math.Max(newHeight, height * 2);

		Array.Resize(ref buffer, newHeight);
		Array.Resize(ref colorBuffer, newHeight);

		for (int i = height; i < buffer.Length; i++)
		{
			buffer[i] = new char[width];
			colorBuffer[i] = new CellColor[width];
			Array.Fill(buffer[i], ' ');
			Array.Fill(colorBuffer[i], new CellColor(NoColor, NoColor));
		}

		height = newHeight;
	}

	public void Clear(int height)
	{
		Prepare();

		CellColor c = new CellColor(NoColor, NoColor);

		if (height > this.height)
			ResizeHeight(height);

		for (int i = 0; i < height; i++)
		{
			for (int j = 0; j < this.width; j++)
			{
				buffer[i][j] = ' ';
				colorBuffer[i][j] = c;
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Write(int left, int top, char ch,
		ConsoleColor foregroundColor = (ConsoleColor)NoColor, ConsoleColor backgroundColor = (ConsoleColor)NoColor)
	{
		if (top >= height)
			ResizeHeight(top);

		if (left < width)
		{
			buffer[top][left] = ch;
			colorBuffer[top][left] = new CellColor((byte)foregroundColor, (byte)backgroundColor);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Write(int left, int top, char ch, int count,
		ConsoleColor foregroundColor = (ConsoleColor)NoColor, ConsoleColor backgroundColor = (ConsoleColor)NoColor)
	{
		if (top >= height)
			ResizeHeight(top);

		int t = Math.Min(count, width - left);
		for (int i = 0; i < t; i++)
		{
			buffer[top][left + i] = ch;
			colorBuffer[top][left] = new CellColor((byte)foregroundColor, (byte)backgroundColor);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Write(int left, int top, string s,
		ConsoleColor foregroundColor = (ConsoleColor)NoColor, ConsoleColor backgroundColor = (ConsoleColor)NoColor)
	{
		if (top >= height)
			ResizeHeight(top);

		int n = Math.Min(s.Length, width - left);
		for (int i = 0; i < n; i++)
		{
			buffer[top][left + i] = s[i];
			colorBuffer[top][left + i] = new CellColor((byte)foregroundColor, (byte)backgroundColor);
		}
	}

	public void Show(int top, int lineCount)
	{
		int n = Math.Min(buffer.Length, lineCount);
		if (!ReadLine.IsRedirectedOrAlternate)
			Console.SetCursorPosition(0, top);

		for (int i = 0; i < n; i++)
		{
			char[] line = buffer[i];
			CellColor[] colorLine = colorBuffer[i];

			int start = 0;
			CellColor color = colorLine[0];
			for (int j = 1; j < line.Length; j++)
			{
				if (!color.Equals(colorLine[j]))
				{
					ShowRange(line, start, j - start, color);
					start = j;
					color = colorLine[j];
				}
			}

			ShowRange(line, start, line.Length - start, color);
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void ShowRange(char[] line, int start, int count, CellColor color)
	{
		if (color.fcol != NoColor)
			Console.ForegroundColor = (ConsoleColor)color.fcol;

		if (color.bcol != NoColor)
			Console.BackgroundColor = (ConsoleColor)color.bcol;

		Console.Write(line, start, count);

		if (color.fcol != NoColor || color.bcol != NoColor)
			Console.ResetColor();
	}

	private struct CellColor : IEquatable<CellColor>
	{
		public byte fcol;
		public byte bcol;

		public CellColor(byte fcol, byte bcol)
		{
			this.fcol = fcol;
			this.bcol = bcol;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool Equals(CellColor other)
		{
			return fcol == other.fcol && bcol == other.bcol;
		}
	}
}
