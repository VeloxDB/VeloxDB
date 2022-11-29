using System;
using Velox.Common;
using Velox.Networking;

namespace Velox.Storage;

internal enum AlignmentTransactionType : byte
{
	None = 0,
	Beginning = 1,
	Alignment = 2,
	End = 3
}

internal unsafe sealed class AlignmentData
{
	GlobalVersion[] globalVersions;
	AlignmentTransactionType type;
	ClassCapacity[] classCapacities;

	public AlignmentData(GlobalVersion[] globalVersions, AlignmentTransactionType type, ClassCapacity[] classCapacities)
	{
		this.globalVersions = globalVersions;
		this.type = type;
		this.classCapacities = classCapacities;
	}

	public GlobalVersion[] GlobalVersions => globalVersions;
	public AlignmentTransactionType Type => type;
	public ClassCapacity[] ClassCapacities => classCapacities;

	public static int Size(AlignmentData a)
	{
		if (a == null)
			return 1;

		return 1 + 1 + 1 + (a.globalVersions != null ? 4 + a.globalVersions.Length * 24 : 0);
	}

	public static void WriteTo(AlignmentData a, ref byte* buffer)
	{
		if (a == null)
		{
			*buffer = 0;
			buffer++;
			return;
		}

		*buffer = 1;
		buffer++;

		*buffer = (byte)a.type;
		buffer++;

		GlobalVersion[] globalVersions = a.globalVersions;
		if (globalVersions == null)
		{
			*buffer = 0;
			buffer++;
		}
		else
		{
			*buffer = 1;
			buffer++;

			*((int*)buffer) = globalVersions.Length;
			buffer += 4;

			for (int i = 0; i < globalVersions.Length; i++)
			{
				*((long*)buffer) = globalVersions[i].GlobalTerm.Low;
				buffer += 8;

				*((long*)buffer) = globalVersions[i].GlobalTerm.Hight;
				buffer += 8;

				*((ulong*)buffer) = globalVersions[i].Version;
				buffer += 8;
			}
		}
	}

	public static void Serialize(AlignmentData a, MessageWriter writer)
	{
		if (a == null)
		{
			writer.WriteBool(false);
			return;
		}

		writer.WriteBool(true);

		writer.WriteByte((byte)a.type);

		GlobalVersion[] globalVersions = a.globalVersions;
		if (globalVersions == null)
		{
			writer.WriteBool(false);
		}
		else
		{
			writer.WriteBool(true);
			writer.WriteInt(globalVersions.Length);

			for (int i = 0; i < globalVersions.Length; i++)
			{
				writer.WriteLong(globalVersions[i].GlobalTerm.Low);
				writer.WriteLong(globalVersions[i].GlobalTerm.Hight);
				writer.WriteLong((long)globalVersions[i].Version);
			}
		}

		ClassCapacity[] classCapacities = a.classCapacities;
		if (classCapacities == null)
		{
			writer.WriteBool(false);
		}
		else
		{
			writer.WriteBool(true);
			writer.WriteInt(classCapacities.Length);
			for (int i = 0; i < classCapacities.Length; i++)
			{
				writer.WriteShort(classCapacities[i].Id);
				writer.WriteLong(classCapacities[i].Capacity);
			}
		}
	}

	public static AlignmentData ReadFrom(ref byte* buffer)
	{
		byte v = *buffer;
		buffer++;

		if (v == 0)
			return null;

		AlignmentTransactionType type = (AlignmentTransactionType)(*buffer);
		buffer++;

		GlobalVersion[] t = null;

		v = *buffer;
		buffer++;

		if (v != 0)
		{
			int count = *((int*)buffer);
			buffer += 4;

			t = new GlobalVersion[count];
			for (int i = 0; i < count; i++)
			{
				long av1 = *((long*)buffer);
				buffer += 8;

				long av2 = *((long*)buffer);
				buffer += 8;

				ulong ver = *((ulong*)buffer);
				buffer += 8;

				t[i] = new GlobalVersion(new SimpleGuid(av1, av2), ver);
			}
		}

		return new AlignmentData(t, type, null);
	}

	public static AlignmentData Deserialize(MessageReader reader)
	{
		if (!reader.ReadBool())
			return null;

		AlignmentTransactionType type = (AlignmentTransactionType)reader.ReadByte();
		GlobalVersion[] globalVersions = null;

		if (reader.ReadBool())
		{
			int c = reader.ReadInt();
			globalVersions = new GlobalVersion[c];
			for (int i = 0; i < c; i++)
			{
				long v1 = reader.ReadLong();
				long v2 = reader.ReadLong();
				ulong v = (ulong)reader.ReadLong();
				globalVersions[i] = new GlobalVersion(new SimpleGuid(v1, v2), v);
			}
		}

		ClassCapacity[] classCapacities = null;
		if (reader.ReadBool())
		{
			int c = reader.ReadInt();
			classCapacities = new ClassCapacity[c];
			for (int i = 0; i < c; i++)
			{
				classCapacities[i] = new ClassCapacity(reader.ReadShort(), reader.ReadLong());
			}
		}

		return new AlignmentData(globalVersions, type, classCapacities);
	}

	public struct ClassCapacity
	{
		public short Id { get; private set; }
		public long Capacity { get; private set; }

		public ClassCapacity(short id, long capacity)
		{
			this.Id = id;
			this.Capacity = capacity;
		}
	}
}
