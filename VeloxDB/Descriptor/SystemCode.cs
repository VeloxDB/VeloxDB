using System;

namespace VeloxDB.Descriptor;

internal static class SystemCode
{
	public static class DatabaseObject
	{
		public const short Id = -1;
		public const int IdProp = -1;
		public const int Version = -2;
	}

	public static class IdGenerator
	{
		public const short Id = -2;
		public const int Value = -3;
	}

	public static class GlobalWriteState
	{
		public const short Id = -3;
		public const int IsPrimary = -4;
	}

	public static class ConfigArtifactVersion
	{
		public const short Id = -4;
		public const int GuidV1 = -5;
		public const int GuidV2 = -6;
	}

	public static class Assembly
	{
		public const short Id = -5;
		public const int Name = -7;
		public const int FileName = -8;
		public const int Binary = -9;

		public const short NameIndexId = -1;
	}

	public static class ConfigArtifact
	{
		public const short Id = -6;
		public const int Binary = -10;
	}
}
