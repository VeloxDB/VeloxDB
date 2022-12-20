using System;
using System.Text;
using VeloxDB.Common;

namespace VeloxDB.ClientApp;

internal static class Text
{
	static readonly HashSet<char> breakChars = new HashSet<char>() { ' ', '\t', '.', ',', ':', '-', '/', '\\', '!', '?', '=', '+', '*' };

	public static string LimitTextSize(string text, int width)
	{
		if (text.Length <= width)
			return text;

		return text.Substring(0, width - 3) + "...";
	}

	public static RichTextItem LimitTextSize(RichTextItem item, int width)
	{
		if (item.Length <= width)
			return item;

		List<TextItem> textItems = new List<TextItem>(item.Parts.Length);
		for (int i = 0; i < item.Parts.Length; i++)
		{
			TextItem textItem = item.Parts[i];
			if (textItem.Text.Length <= width - 3)
			{
				textItems.Add(textItem);
				width -= textItem.Text.Length;
			}
			else
			{
				textItems.Add(new TextItem()
				{
					BackgroundColor = textItem.BackgroundColor,
					Color = textItem.Color,
					Text = textItem.Text.Substring(0, width - 3) + "..."
				});

				break;
			}
		}

		return new RichTextItem(textItems.ToArray());
	}

	public static string[] WrapText(string text, int width, bool trimLines = true)
	{
		if (text.Length <= width)
			return new string[] { text };

		List<string> lines = new List<string>();
		StringBuilder line = new StringBuilder(width);
		StringBuilder word = new StringBuilder(16);
		for (int i = 0; i < text.Length; i++)
		{
			if (word.Length == 0)
			{
				word.Append(text[i]);
			}
			else if (breakChars.Contains(text[i]))
			{
				word.Append(text[i]);
				AppendToLine(lines, line, word, width, trimLines);
			}
			else
			{
				word.Append(text[i]);
			}
		}

		AppendToLine(lines, line, word, width, trimLines);
		if (line.Length > 0)
			FinishLine(lines, line.ToString(), trimLines);

		return lines.ToArray();
	}

	public static RichTextItem[] WrapText(RichTextItem item, int width)
	{
		if (item.Length <= width)
			return new RichTextItem[] { item };

		StringBuilder s = new StringBuilder(item.Length);
		for (int i = 0; i < item.Parts.Length; i++)
		{
			s.Append(item.Parts[i].Text);
		}

		string[] slines = WrapText(s.ToString(), width, false);

		List<RichTextItem> lines = new List<RichTextItem>(slines.Length);
		List<TextItem> textItems = new List<TextItem>();
		int lineIndex = 0;
		int lineOffset = 0;
		for (int i = 0; i < item.Parts.Length; i++)
		{
			TextItem textItem = item.Parts[i];

			int t = 0;
			while (t < textItem.Text.Length)
			{
				int d = Math.Min(textItem.Text.Length - t, slines[lineIndex].Length - lineOffset);
				string sp = Substring(textItem.Text, t, d);
				textItems.Add(new TextItem() { Text = sp, Color = textItem.Color, BackgroundColor = textItem.BackgroundColor });
				t += d;
				lineOffset += d;
				if (lineOffset == slines[lineIndex].Length)
				{
					lines.Add(new RichTextItem(textItems.ToArray()));
					textItems.Clear();
					lineOffset = 0;
					lineIndex++;
				}
			}
		}

		if (textItems.Count > 0)
			lines.Add(new RichTextItem(textItems.ToArray()));

		lines[0].TrimEnd();
		for (int i = 1; i < lines.Count; i++)
		{
			lines[i].Trim();
		}

		return lines.ToArray();
	}

	private static string Substring(string s, int index, int count)
	{
		if (index == 0 && count == s.Length)
			return s;

		return s.Substring(index, count);
	}

	private static void AppendToLine(List<string> lines, StringBuilder line, StringBuilder word, int width, bool trimLines)
	{
		while (word.Length > 0)
		{
			if (line.Length + word.Length <= width ||
				(line.Length + word.Length == width + 1 && (word[word.Length - 1] == ' ' || word[word.Length - 1] == '\t')))
			{
				line.Append(word);
				word.Clear();
			}
			else if (line.Length == 0)
			{
				line.Append(word, 0, width);
				FinishLine(lines, line.ToString(), trimLines);
				line.Clear();
				word.Remove(0, width);
			}
			else
			{
				FinishLine(lines, line.ToString(), trimLines);
				line.Clear();
			}
		}
	}

	private static void FinishLine(List<string> lines, string line, bool trimLines)
	{
		if (!trimLines)
		{
			lines.Add(line);
			return;
		}

		string s;
		if (line.EndsWith(' ') || line.EndsWith('\t'))
			s = line.Substring(0, line.Length - 1);
		else if (line.EndsWith("\r\n"))
			s = line.Substring(0, line.Length - 2);
		else if (line.EndsWith('\n'))
			s = line.Substring(0, line.Length - 1);
		else
			s = line;

		if (lines.Count > 0)
			s = s.Trim();

		lines.Add(s);
	}
}
