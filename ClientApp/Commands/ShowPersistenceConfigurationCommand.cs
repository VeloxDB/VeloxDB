using System;
using Velox.ClientApp.Modes;
using Velox.Server;

namespace Velox.ClientApp.Commands;

[Command("show", "Shows the current persistence configuration.")]
internal sealed class ShowPersistenceConfigurationCommand : Command
{
	public override bool IsModeValid(Mode mode)
	{
		return mode is PersistenceConfigMode;
	}

	protected override bool OnExecute(Program program)
	{
		PersistenceConfigMode mode = (PersistenceConfigMode)program.Mode;
		PersistenceDescriptor persistConfig = mode.PersistenceConfig;

		if (persistConfig.LogDescriptors.Count == 0)
		{
			Console.WriteLine("Persistence configuration is empty.");
			return true;
		}

		Table table = new Table(new Table.ColumnDesc[]
		{
			new Table.ColumnDesc() { WidthPriority = 10, Color = Colors.LogDescColor },
			new Table.ColumnDesc() { WidthPriority = 5, WordWrap = true },
		});

		for (int i = 0; i < persistConfig.LogDescriptors.Count; i++)
		{
			LogDescriptor logDesc = persistConfig.LogDescriptors[i];
			table.AddRow(GenerateLogRow(logDesc));
		}

		table.Show();
		return true;
	}

	private RichTextItem[] GenerateLogRow(LogDescriptor logDesc)
	{
		TextItem[] paramItems = new TextItem[8];
		paramItems[0] = new TextItem() { Text = "Log directory: ", Color = Colors.ParamNameColor };
		paramItems[1] = logDesc.Directory + "; ";
		paramItems[2] = new TextItem() { Text = "Snapshot directory: ", Color = Colors.ParamNameColor };
		paramItems[3] = logDesc.SnapshotDirectory + "; ";
		paramItems[4] = new TextItem() { Text = "Size: ", Color = Colors.ParamNameColor };
		paramItems[5] = (logDesc.MaxSize / 1024 / 1024).ToString() + " MB; ";
		paramItems[6] = new TextItem() { Text = "Is packed format: ", Color = Colors.ParamNameColor };
		paramItems[7] = logDesc.IsPackedFormat.ToString();

		return new RichTextItem[] {
			new RichTextItem(new TextItem[] { logDesc.Name }),
			new RichTextItem(paramItems),
		};
	}
}
