using System.Net.Security;
using VeloxDB.Config;
using VeloxDB.Networking;
using VeloxDB.Storage.Replication.HighAvailability;

namespace VeloxDB.Server;

internal static class ExtensionMethods
{
	public static WitnessConfiguration AsWitnessConfig(this Config.Witness witness, IElectorFactory electorFactory,
													   SslClientOptionsFactory? factory)
	{
		switch(witness.Type)
		{
			case WitnessType.Standalone:
				return CreateWitnessConfig((Config.StandaloneWitness)witness, electorFactory, factory);
			case WitnessType.SharedFolder:
				return CreateWitnessConfig((Config.SharedFolderWitness)witness, electorFactory);
			default:
				throw new NotSupportedException($"Unknown type {witness.Type}");
		}
	}

	private static WitnessConfiguration CreateWitnessConfig(SharedFolderWitness witness, IElectorFactory electorFactory)
	{
		return electorFactory.CreateSharedFolderWitnessConfiguration(witness.Path, (int)(witness.RemoteFileTimeout*1000+0.5f));
	}

	private static WitnessConfiguration CreateWitnessConfig(Config.StandaloneWitness witness, IElectorFactory electorFactory,
															SslClientOptionsFactory? factory)
	{
		SslClientAuthenticationOptions? options = null;
		if(factory != null)
			options = factory.CreateSslOptions(witness.Address.Address);

		return electorFactory.GetWitnessServerConfiguration(witness.Address.ToString(), options);
	}
}
