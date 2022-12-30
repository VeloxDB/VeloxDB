using System;

namespace VeloxDB.ClientApp;

internal sealed class RichTextItem
{
	public int Length { get; private set; }
	public TextItem[] Parts { get; private set; }

	public static readonly RichTextItem Empty = new RichTextItem(new TextItem[0]);

	public RichTextItem(TextItem[] parts)
	{
		this.Parts = parts;
		Length = 0;
		for (int i = 0; i < parts.Length; i++)
		{
			Length += parts[i].Text.Length;
		}
	}

	public RichTextItem(TextItem item)
	{
		this.Parts = new TextItem[] { item };
		Length = item.Text.Length;
	}

	public void Show(int left, int top, ScreenBuffer buffer)
	{
		for (int i = 0; i < Parts.Length; i++)
		{
			Parts[i].Show(left, top, buffer);
			left += Parts[i].Text.Length;
		}
	}

	public void Show()
	{
		for (int i = 0; i < Parts.Length; i++)
		{
			Parts[i].Show();
		}
	}

	public void TrimEnd()
	{
		TextItem item = Parts[Parts.Length - 1];
		if (item.Text.EndsWith(' ') || item.Text.EndsWith('\t'))
			Parts[Parts.Length - 1] = new TextItem() { Text = item.Text.TrimEnd(), Color = item.Color, BackgroundColor = item.BackgroundColor };
	}

	public void Trim()
	{
		TextItem item = Parts[Parts.Length - 1];
		if (item.Text.StartsWith(' ') || item.Text.StartsWith('\t') || item.Text.EndsWith(' ') || item.Text.EndsWith('\t'))
			Parts[Parts.Length - 1] = new TextItem() { Text = item.Text.Trim(), Color = item.Color, BackgroundColor = item.BackgroundColor };
	}
}
