using System;
using System.Text;

namespace VeloxDB.ClientApp;

internal sealed class Tree
{
	TreeItem root;
	Table.ColumnDesc[] columns;
	int horizSpacing = 3;
	Table table;

	public Tree(Table.ColumnDesc[] columns, TreeItem root)
	{
		columns[0].MaxWidth = null;
		for (int i = 0; i < columns.Length; i++)
		{
			columns[i].WordWrap = false;
		}

		this.root = root;
		this.columns = columns;
	}

	public int HorizSpacing
	{
		get => horizSpacing;
		set
		{
			table = null;
			horizSpacing = value;
		}
	}

	public void Show()
	{
		if (table == null)
			CreateTable();

		table.Show();
	}

	public void Show(ScreenBuffer buffer, int top, ref int clearHeight)
	{
		if (table == null)
			CreateTable();

		table.Show(buffer, top, ref clearHeight);
	}

	private void CreateTable()
	{
		List<TreeItem> items = new List<TreeItem>(32);
		if (root != null)
			PopulateItems(root, items, 0);

		StringBuilder[] sbs = new StringBuilder[items.Count];
		for (int i = 0; i < sbs.Length; i++)
		{
			sbs[i] = new StringBuilder(16);
		}

		for (int i = 1; i < items.Count; i++)
		{
			TreeItem item = items[i];
			sbs[i].Append(' ', item.Depth * 2 - 2);
			if (item.Items.Select(x => x.Length).Sum() == 0)
			{
				sbs[i].Append(item.IsLastSibling ? ' ' : '│');
				sbs[i].Append(' ');
			}
			else
			{
				sbs[i].Append(item.IsLastSibling ? '└' : '├');
				sbs[i].Append('─');
			}
			TreeItem parent = item.Parent;
			for (int j = item.Depth - 1; j > 0; j--)
			{
				sbs[i][j * 2 - 2] = parent.IsLastSibling ? ' ' : '│';
				sbs[i][j * 2 - 1] = ' ';
				parent = parent.Parent;
			}
		}

		table = new Table(columns);
		table.HorizSpacing = horizSpacing;
		for (int i = 0; i < items.Count; i++)
		{
			TreeItem item = items[i];
			if (sbs[i].Length > 0)
			{
				table.AddRow(InsertText(sbs[i].ToString(), item.Items));
			}
			else
			{
				table.AddRow(item.Items);
			}
		}
	}

	private RichTextItem[] InsertText(string text, RichTextItem[] items)
	{
		TextItem[] textItems = new TextItem[items[0].Parts.Length + 1];
		textItems[0] = new TextItem() { Text = text, Color = Colors.TreeStructureColor };
		for (int i = 0; i < items[0].Parts.Length; i++)
		{
			textItems[i + 1] = items[0].Parts[i];
		}

		RichTextItem[] res = new RichTextItem[items.Length];
		res[0] = new RichTextItem(textItems);
		for (int i = 1; i < items.Length; i++)
		{
			res[i] = items[i];
		}

		return res;
	}

	private static void PopulateItems(TreeItem item, List<TreeItem> items, int depth)
	{
		item.Depth = depth;
		items.Add(item);
		for (int i = 0; i < item.Children.Count; i++)
		{
			PopulateItems(item.Children[i], items, depth + 1);
		}
	}
}

internal sealed class TreeItem
{
	RichTextItem[] items;
	List<TreeItem> children;
	TreeItem parent;
	int depth;

	public TreeItem(RichTextItem[] items)
	{
		this.items = items;
		children = new List<TreeItem>();
	}

	public TreeItem(RichTextItem item)
	{
		this.items = new RichTextItem[] { item };
		children = new List<TreeItem>();
	}

	public List<TreeItem> Children => children;

	public bool IsLastSibling
	{
		get
		{
			if (parent == null)
				return true;

			return object.ReferenceEquals(parent.children[parent.children.Count - 1], this);
		}
	}

	public int Depth { get => depth; set => depth = value; }
	public TreeItem Parent => parent;
	public RichTextItem[] Items => items;

	public void AddChild(TreeItem item)
	{
		item.parent = this;
		children.Add(item);
	}
}
