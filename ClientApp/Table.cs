using System;
using System.Text;

namespace VeloxDB.ClientApp;

internal sealed class Table
{
	const int minColumnWidth = 5;
	static readonly char[] emptyChars = Enumerable.Range(0, 120).Select(x => ' ').ToArray();

	bool hasHeader;
	ColumnDesc[] columns;
	List<RichTextItem[]> rows;
	int horizSpacing;

	public Table(ColumnDesc[] columns)
	{
		horizSpacing = 3;
		this.columns = columns;
		rows = new List<RichTextItem[]>();
		hasHeader = columns.Any(x => x.Header != null);
	}

	public int HorizSpacing { get => horizSpacing; set => horizSpacing = value; }

	public void AddRow(string[] row)
	{
		if (row.Length != columns.Length)
			throw new ArgumentException();

		RichTextItem[] crow = new RichTextItem[row.Length];
		for (int i = 0; i < row.Length; i++)
		{
			crow[i] = new RichTextItem(new TextItem[] { row[i] });
		}

		rows.Add(crow);
	}

	public void AddRow(RichTextItem[] row)
	{
		if (row.Length != columns.Length)
			throw new ArgumentException();

		RichTextItem[] crow = new RichTextItem[row.Length];
		for (int i = 0; i < row.Length; i++)
		{
			crow[i] = row[i];
		}

		rows.Add(crow);
	}

	public void Show()
	{
		if ((!hasHeader && rows.Count == 0) || columns.Length == 0)
			return;

		int consoleWidth = Console.IsOutputRedirected ? int.MaxValue : Console.WindowWidth;
		int[] columnWidths = CalculateColumnWidths(consoleWidth);

		List<RichTextItem[]> header = hasHeader ? CreateDisplayRows(new List<RichTextItem[]>() { CreateHeaderRow() }, columnWidths, true) : null;
		List<RichTextItem[]> body = CreateDisplayRows(rows, columnWidths, false);

		if (header != null)
			ShowRows(header, columnWidths);

		ShowRows(body, columnWidths);
	}

	public void Show(ScreenBuffer buffer, int top, ref int clearHeight)
	{
		buffer.Clear(clearHeight);

		if ((!hasHeader && rows.Count == 0) || columns.Length == 0)
			return;

		int[] columnWidths = CalculateColumnWidths(buffer.Width);

		List<RichTextItem[]> header = hasHeader ? CreateDisplayRows(new List<RichTextItem[]>() { CreateHeaderRow() }, columnWidths, true) : null;
		List<RichTextItem[]> body = CreateDisplayRows(rows, columnWidths, false);

		clearHeight = (header?.Count).GetValueOrDefault() + body.Count;

		int ct = 0;
		if (header != null)
			ShowRows(0, ct++, buffer, header, columnWidths);

		ShowRows(0, ct, buffer, body, columnWidths);

		buffer.Show(top, clearHeight);
	}

	private void ShowRows(List<RichTextItem[]> rows, int[] columnWidths)
	{
		for (int i = 0; i < rows.Count; i++)
		{
			for (int j = 0; j < columns.Length; j++)
			{
				RichTextItem item = rows[i][j];
				WriteText(item, columnWidths[j] + (j < columns.Length - 1 ? horizSpacing : 0));
			}

			Console.WriteLine();
		}
	}

	private void ShowRows(int left, int top, ScreenBuffer buffer, List<RichTextItem[]> rows, int[] columnWidths)
	{
		for (int i = 0; i < rows.Count; i++)
		{
			int currLeft = left;
			for (int j = 0; j < columns.Length; j++)
			{
				RichTextItem item = rows[i][j];
				int t = columnWidths[j] + (j < columns.Length - 1 ? horizSpacing : 0);
				item.Show(currLeft, top, buffer);
				currLeft += t;
			}

			top += 1;
		}
	}

	private void WriteText(RichTextItem item, int width)
	{
		item.Show();
		width -= item.Length;
		while (width > 0)
		{
			int t = Math.Min(width, emptyChars.Length);
			Console.Write(emptyChars, 0, t);
			width -= t;
		}
	}

	private List<RichTextItem[]> CreateDisplayRows(List<RichTextItem[]> rows, int[] columnWidths, bool isHeader)
	{
		List<RichTextItem[]> displayRows = new List<RichTextItem[]>(rows.Count);
		for (int i = 0; i < rows.Count; i++)
		{
			RichTextItem[][] textLines = new RichTextItem[columns.Length][];
			int max = 0;
			for (int j = 0; j < columns.Length; j++)
			{
				if (columns[j].WordWrap)
				{
					textLines[j] = Text.WrapText(rows[i][j], columnWidths[j]);
				}
				else
				{
					textLines[j] = new RichTextItem[] { Text.LimitTextSize(rows[i][j], columnWidths[j]) };
				}

				max = Math.Max(max, textLines[j].Length);
			}

			for (int k = 0; k < max; k++)
			{
				RichTextItem[] displayRow = new RichTextItem[columns.Length];
				for (int j = 0; j < columns.Length; j++)
				{
					ConsoleColor? color = isHeader ? columns[j].HeaderColor : columns[j].Color;
					displayRow[j] = k < textLines[j].Length ? textLines[j][k] : RichTextItem.Empty;
					SetColor(displayRow[j], color);
				}

				displayRows.Add(displayRow);
			}
		}

		return displayRows;
	}

	private void SetColor(RichTextItem item, ConsoleColor? color)
	{
		if (!color.HasValue)
			return;

		for (int i = 0; i < item.Parts.Length; i++)
		{
			TextItem textItem = item.Parts[i];
			if (!textItem.Color.HasValue)
				textItem.Color = color;
		}
	}

	private RichTextItem[] CreateHeaderRow()
	{
		RichTextItem[] header = null;
		if (hasHeader)
		{
			header = new RichTextItem[columns.Length];
			for (int i = 0; i < columns.Length; i++)
			{
				if (columns[i].Header != null)
					header[i] = new RichTextItem(new TextItem[] { columns[i].Header });
			}
		}

		return header;
	}

	private int[] CalculateColumnWidths(int consoleWidth)
	{
		int[] widths = new int[columns.Length];
		for (int i = 0; i < columns.Length; i++)
		{
			widths[i] = CalculateColumnWidth(i);
		}

		int totalWidth = widths.Sum() + (columns.Length - 1) * horizSpacing;
		if (totalWidth <= consoleWidth)
			return widths;

		int diff = totalWidth - consoleWidth;

		int[] colIndexes = Enumerable.Range(0, columns.Length).ToArray();
		Array.Sort(columns.ToArray(), colIndexes);

		int index = 0;
		while (diff > 0 && index < columns.Length)
		{
			int count = 1;
			while (index + count < columns.Length && columns[colIndexes[index + count]].WidthPriority == columns[colIndexes[index]].WidthPriority)
				count++;

			AdjustColumnsWidth(colIndexes.Skip(index).Take(count).ToArray(), widths, ref diff);

			index = index + count;
		}

		return widths;
	}

	private void AdjustColumnsWidth(int[] indexes, int[] widths, ref int diff)
	{

		int d = diff / indexes.Length;
		while (diff > 0)
		{
			int s = 0;
			for (int i = 0; i < indexes.Length; i++)
			{
				int index = indexes[i];
				int min = columns[index].Width.HasValue ? columns[index].Width.Value : minColumnWidth;
				int m = Math.Min(widths[index] - min, d);
				s += m;
				widths[index] -= m;
				diff -= m;
			}

			if (diff <= 0)
				return;

			if (s == 0)
				return;

			d = 1;
		}
	}

	private int CalculateColumnWidth(int index)
	{
		if (columns[index].Width.HasValue)
			return columns[index].Width.Value;

		int width = 0;
		if (columns[index].Header != null)
			width = columns[index].Header.Length;

		for (int i = 0; i < rows.Count; i++)
		{
			int len = rows[i][index].Length;
			if (len > width)
				width = len;
		}

		if (columns[index].MaxWidth.HasValue)
			width = Math.Min(width, columns[index].MaxWidth.Value);

		return width;
	}

	public class ColumnDesc : IComparable<ColumnDesc>
	{
		public string Header { get; set; }
		public bool WordWrap { get; set; }
		public int? Width { get; set; }
		public int? MaxWidth { get; set; }
		public ConsoleColor? Color { get; set; }
		public ConsoleColor? HeaderColor { get; set; }
		public int WidthPriority { get; set; }

		public ColumnDesc()
		{
			WidthPriority = 1;
			WordWrap = false;
		}

		public int CompareTo(ColumnDesc other)
		{
			return WidthPriority.CompareTo(other.WidthPriority);
		}
	}
}
