using VeloxDB.Config;
using VeloxDB.Storage.Replication.HighAvailability;

namespace VeloxDB.Server;

internal static class ExtensionMethods
{
	public static WitnessConfiguration AsWitnessConfig(this Config.Witness witness, IElectorFactory electorFactory)
	{
		switch(witness.Type)
		{
			case WitnessType.Standalone:
				return CreateWitnessConfig((Config.StandaloneWitness)witness, electorFactory);
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

	private static WitnessConfiguration CreateWitnessConfig(Config.StandaloneWitness witness, IElectorFactory electorFactory)
	{
		return electorFactory.GetWitnessServerConfiguration(witness.Address.ToString());
	}
}
