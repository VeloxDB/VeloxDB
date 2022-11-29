using System;
using Velox.Client;
using Velox.ClientApp.Modes;
using Velox.Common;
using Velox.Config;
using Velox.Networking;
using Velox.Protocol;
using Velox.Server;

namespace Velox.ClientApp.Commands;

[Command("bind", "Binds to a given VeloxDB cluster allowing you to perform actions " +
	"against the cluster. Most commands require this binding to be established.")]
internal sealed class BindToClusterCommand : Command
{
	[Param("file", "Path to a file containing cluster configuration.", ShortName = "f")]
	public string ConfigFile { get; set; }

	[Param("node", "Addresses of one or more nodes in a cluster. It should be specified in the format adress:[port]. " +
		"If port is ommited, default port is used. If neither this parameter nor the --file parameter is provided, bind is attempted agains " +
		"a node on the local machine on the default port.", ShortName = "n")]
	public string[] NodeEndpoints { get; set; }

	public override bool IsModeValid(Mode mode)
	{
		return mode.GetType() == typeof(InitialMode);
	}

	protected override bool OnExecute(Program program)
	{
		if (ConfigFile != null)
		{
			return BindableCommand.BindFromFile(program, ConfigFile);
		}
		else
		{
			return BindableCommand.BindFromNode(program, NodeEndpoints);
		}
	}
}
