using System.Diagnostics;
using System.Reflection.PortableExecutable;
using API;
using VeloxDB.Client;

namespace Client;

public class Program
{
	static int vehicleCount = 1024 * 1024 * 8;
	static int workerCount = 1;

	static string[] vehicleNames = new string[0];

	public static void Main(string[] args)
	{
		string[] addresses = new string[] { "localhost" };
		if (args.Length > 0)
			addresses = args[0].Split('|');

		if (args.Length > 1)
			vehicleCount *= int.Parse(args[1]);

		if (args.Length > 2)
			workerCount = int.Parse(args[2]);

		int connCount = 2;
		if (args.Length > 3)
			connCount = int.Parse(args[3]);

		ConnectionStringParams csp = new ConnectionStringParams();
		for (int i = 0; i < addresses.Length; i++)
		{
			if (!addresses[i].Contains(':'))
				addresses[i] += ":7568";

			csp.AddAddress(addresses[i]);
		}

		csp.PoolSize = connCount;
		csp.BufferPoolSize = 1024 * 1024 * 128;
		csp.OpenTimeout = 5000;

		IMobilityService service = ConnectionFactory.Get<IMobilityService>(csp.GenerateConnectionString());

		PrepareData();

		Statistics[][] statistics = new Statistics[5][];
		AutoResetEvent[] finishedEvents = new AutoResetEvent[workerCount];

		for (int objsPerTran = 1; objsPerTran <= 4; objsPerTran *= 2)
		{
			int tranCount = vehicleCount / objsPerTran;
			int tranPerWorkerCount = tranCount / workerCount;
			int objsPerWorker = vehicleCount / workerCount;
			long[][] vehicleIds = new long[workerCount][];
			long[][] rideIds = new long[workerCount][];

			statistics[objsPerTran] = new Statistics[]
			{
				new Statistics("Insert vehicles"),
				new Statistics("Update vehicles"),
				new Statistics("Read-update vehicles"),
				new Statistics("Insert rides"),
				new Statistics("Update rides"),
				new Statistics("Get vehicles"),
				new Statistics("Get ride vehicles"),
				new Statistics("Get vehicle rides"),
				new Statistics("Delete rides"),
				new Statistics("Delete vehicles"),
			};

			//InsertVehicles(service, statistics[objsPerTran][0], objsPerTran, tranPerWorkerCount, objsPerWorker, vehicleIds, finishedEvents);
			//using (MemoryStream ms = new MemoryStream())
			//{
			//	using (BinaryWriter bw = new BinaryWriter(ms))
			//	{
			//		bw.Write(vehicleIds.Length * vehicleIds[0].Length);
			//		for (int i = 0; i < vehicleIds.Length; i++)
			//		{
			//			for (int j = 0; j < vehicleIds[i].Length; j++)
			//			{
			//				bw.Write(vehicleIds[i][j]);
			//			}
			//		}
			//	}

			//	File.WriteAllBytes("ids.bin", ms.ToArray());
			//}

			//return;

			using (MemoryStream ms = new MemoryStream(File.ReadAllBytes("ids.bin")))
			using (BinaryReader r = new BinaryReader(ms))
			{
				int c = r.ReadInt32();

				vehicleIds = new long[workerCount][];
				int t = c / workerCount;
				for (int i = 0; i < vehicleIds.Length; i++)
				{
					vehicleIds[i] = new long[t];
					for (int j = 0; j < t; j++)
					{
						vehicleIds[i][j] = r.ReadInt64();
					}
				}
			}

			for (int i = 0; i < 1000000; i++)
			{
				GetVehicles(service, statistics[objsPerTran][5], objsPerTran, tranPerWorkerCount, vehicleIds, finishedEvents);
			}

			UpdateVehicles(service, statistics[objsPerTran][1], objsPerTran, tranPerWorkerCount, vehicleIds, finishedEvents);
			CopyVehiclesPosition(service, statistics[objsPerTran][2], objsPerTran, tranPerWorkerCount, vehicleIds, finishedEvents);
			InsertRides(service, statistics[objsPerTran][3], objsPerTran, tranPerWorkerCount, objsPerWorker, vehicleIds, rideIds, finishedEvents);
			UpdateRides(service, statistics[objsPerTran][4], objsPerTran, tranPerWorkerCount, vehicleIds, rideIds, finishedEvents);
			GetRideVehicles(service, statistics[objsPerTran][6], objsPerTran, tranPerWorkerCount, rideIds, finishedEvents);
			GetVehicleRides(service, statistics[objsPerTran][7], objsPerTran, tranPerWorkerCount, vehicleIds, finishedEvents);
			DeleteRides(service, statistics[objsPerTran][8], objsPerTran, tranPerWorkerCount, rideIds, finishedEvents);
			DeleteVehicles(service, statistics[objsPerTran][9], objsPerTran, tranPerWorkerCount, vehicleIds, finishedEvents);
		}

		for (int i = 1; i < statistics.Length; i++)
		{
			if (statistics[i] == null)
				continue;

			Console.WriteLine("Affected object per transaction {0}.", i);
			for (int j = 0; j < statistics[i].Length; j++)
			{
				statistics[i][j].Write();
			}

			Console.WriteLine();
		}
	}

	private static void PrepareData()
	{
		vehicleNames = new string[vehicleCount];
		for (int i = 0; i < vehicleCount; i++)
		{
			vehicleNames[i] = string.Format("Vehicle{0}", i);
		}
	}

	private static void InsertVehicles(IMobilityService service, Statistics statistics, int objsPerTran, int tranPerWorkerCount,
		int objsPerWorker, long[][] ids, AutoResetEvent[] finishedEvents)
	{
		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			finishedEvents[i] = new AutoResetEvent(false);

			Task t = new Task(async (p) =>
			{
				int index = (int)p!;
				ids[index] = await InsertVehiclesWorker(service, statistics, index * objsPerWorker, tranPerWorkerCount, objsPerTran);
				finishedEvents[index].Set();
			}, i);

			t.Start();
		}

		for (int i = 0; i < finishedEvents.Length; i++)
		{
			finishedEvents[i].WaitOne();
		}

		statistics.Stop();
	}

	private static void InsertRides(IMobilityService service, Statistics statistics, int objsPerTran, int tranPerWorkerCount,
		int objsPerWorker, long[][] vehicleIds, long[][] ids, AutoResetEvent[] finishedEvents)
	{
		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			finishedEvents[i] = new AutoResetEvent(false);

			Task t = new Task(async (p) =>
			{
				int index = (int)p!;
				ids[index] = await InsertRidesWorker(service, statistics, vehicleIds[index], index * objsPerWorker, tranPerWorkerCount, objsPerTran);
				finishedEvents[index].Set();
			}, i);

			t.Start();
		}

		for (int i = 0; i < finishedEvents.Length; i++)
		{
			finishedEvents[i].WaitOne();
		}

		statistics.Stop();
	}

	private static void DeleteVehicles(IMobilityService service, Statistics statistics, int objsPerTran, int tranPerWorkerCount,
		long[][] ids, AutoResetEvent[] finishedEvents)
	{
		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			finishedEvents[i] = new AutoResetEvent(false);

			Task t = new Task(async (p) =>
			{
				int index = (int)p!;
				await DeleteVehiclesWorker(service, statistics, ids[index], tranPerWorkerCount, objsPerTran);
				finishedEvents[index].Set();
			}, i);

			t.Start();
		}

		for (int i = 0; i < finishedEvents.Length; i++)
		{
			finishedEvents[i].WaitOne();
		}

		statistics.Stop();
	}

	private static void DeleteRides(IMobilityService service, Statistics statistics, int objsPerTran, int tranPerWorkerCount,
		long[][] ids, AutoResetEvent[] finishedEvents)
	{
		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			finishedEvents[i] = new AutoResetEvent(false);

			Task t = new Task(async (p) =>
			{
				int index = (int)p!;
				await DeleteRidesWorker(service, statistics, ids[index], tranPerWorkerCount, objsPerTran);
				finishedEvents[index].Set();
			}, i);

			t.Start();
		}

		for (int i = 0; i < finishedEvents.Length; i++)
		{
			finishedEvents[i].WaitOne();
		}

		statistics.Stop();
	}

	private static void UpdateVehicles(IMobilityService service, Statistics statistics, int objsPerTran, int tranPerWorkerCount,
		long[][] ids, AutoResetEvent[] finishedEvents)
	{
		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			finishedEvents[i] = new AutoResetEvent(false);

			Task t = new Task(async (p) =>
			{
				int index = (int)p!;
				await UpdateVehiclesWorker(service, statistics, ids[index], tranPerWorkerCount, objsPerTran);
				finishedEvents[index].Set();
			}, i);

			t.Start();
		}

		for (int i = 0; i < finishedEvents.Length; i++)
		{
			finishedEvents[i].WaitOne();
		}

		statistics.Stop();
	}

	private static void UpdateRides(IMobilityService service, Statistics statistics, int objsPerTran, int tranPerWorkerCount,
		long[][] vehicleIds, long[][] ids, AutoResetEvent[] finishedEvents)
	{
		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			finishedEvents[i] = new AutoResetEvent(false);

			Task t = new Task(async (p) =>
			{
				int index = (int)p!;
				await UpdateRidesWorker(service, statistics, vehicleIds[index], ids[index], tranPerWorkerCount, objsPerTran);
				finishedEvents[index].Set();
			}, i);

			t.Start();
		}

		for (int i = 0; i < finishedEvents.Length; i++)
		{
			finishedEvents[i].WaitOne();
		}

		statistics.Stop();
	}

	private static void CopyVehiclesPosition(IMobilityService service, Statistics statistics, int objsPerTran, int tranPerWorkerCount,
		long[][] ids, AutoResetEvent[] finishedEvents)
	{
		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			finishedEvents[i] = new AutoResetEvent(false);

			Task t = new Task(async (p) =>
			{
				int index = (int)p!;
				await CopyVehiclesPositionWorker(service, statistics, ids[index], tranPerWorkerCount, objsPerTran);
				finishedEvents[index].Set();
			}, i);

			t.Start();
		}

		for (int i = 0; i < finishedEvents.Length; i++)
		{
			finishedEvents[i].WaitOne();
		}

		statistics.Stop();
	}

	private static void GetVehicles(IMobilityService service, Statistics statistics, int objsPerTran, int tranPerWorkerCount,
		long[][] ids, AutoResetEvent[] finishedEvents)
	{
		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			finishedEvents[i] = new AutoResetEvent(false);

			Task t = new Task(async (p) =>
			{
				int index = (int)p!;
				await GetVehiclesWorker(service, statistics, ids[index], tranPerWorkerCount, objsPerTran);
				finishedEvents[index].Set();
			}, i);

			t.Start();
		}

		for (int i = 0; i < finishedEvents.Length; i++)
		{
			finishedEvents[i].WaitOne();
		}

		statistics.Stop();
	}

	private static void GetRideVehicles(IMobilityService service, Statistics statistics, int objsPerTran, int tranPerWorkerCount,
		long[][] ids, AutoResetEvent[] finishedEvents)
	{
		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			finishedEvents[i] = new AutoResetEvent(false);

			Task t = new Task(async (p) =>
			{
				int index = (int)p!;
				await GetRideVehiclesWorker(service, statistics, ids[index], tranPerWorkerCount, objsPerTran);
				finishedEvents[index].Set();
			}, i);

			t.Start();
		}

		for (int i = 0; i < finishedEvents.Length; i++)
		{
			finishedEvents[i].WaitOne();
		}

		statistics.Stop();
	}

	private static void GetVehicleRides(IMobilityService service, Statistics statistics, int objsPerTran, int tranPerWorkerCount,
		long[][] ids, AutoResetEvent[] finishedEvents)
	{
		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			finishedEvents[i] = new AutoResetEvent(false);

			Task t = new Task(async (p) =>
			{
				int index = (int)p!;
				await GetVehicleRidesWorker(service, statistics, ids[index], tranPerWorkerCount, objsPerTran);
				finishedEvents[index].Set();
			}, i);

			t.Start();
		}

		for (int i = 0; i < finishedEvents.Length; i++)
		{
			finishedEvents[i].WaitOne();
		}

		statistics.Stop();
	}

	private static async Task<long[]> InsertVehiclesWorker(IMobilityService service,
		Statistics statistics, int offset, int tranCount, int objsPerTran)
	{
		long[] ids = new long[tranCount * objsPerTran];

		int c = 0;
		for (int i = 0; i < tranCount; i++)
		{
			VehicleDTO[] vehicles = new VehicleDTO[objsPerTran];
			for (int j = 0; j < objsPerTran; j++)
			{
				vehicles[j] = new VehicleDTO(0.0f, 0.0f, vehicleNames[offset + c + j], i, j);
			}

			long[] tids = await service.InsertVehicles(vehicles);
			for (int j = 0; j < tids.Length; j++)
			{
				ids[c++] = tids[j];
			}

			statistics.Inc();
		}

		return ids;
	}

	private static async Task<long[]> InsertRidesWorker(IMobilityService service, Statistics statistics,
		long[] vehicleIds, int offset, int tranCount, int objsPerTran)
	{
		long[] ids = new long[tranCount * objsPerTran];

		DateTime dt = DateTime.UtcNow;

		int c = 0;
		for (int i = 0; i < tranCount; i++)
		{
			RideDTO[] rides = new RideDTO[objsPerTran];
			for (int j = 0; j < objsPerTran; j++)
			{
				rides[j] = new RideDTO(vehicleIds[i % vehicleIds.Length], dt, dt, 1.0);
			}

			long[] tids = await service.InsertRides(rides);
			for (int j = 0; j < tids.Length; j++)
			{
				ids[c++] = tids[j];
			}

			statistics.Inc();
		}

		return ids;
	}

	private static async Task UpdateVehiclesWorker(IMobilityService service, Statistics statistics,
		long[] ids, int tranCount, int objsPerTran)
	{
		int c = 0;
		for (int i = 0; i < tranCount; i++)
		{
			long[] vehicleIds = new long[objsPerTran];
			for (int k = 0; k < objsPerTran; k++)
			{
				vehicleIds[k] = ids[c++];
			}

			await service.UpdateVehiclePositions(vehicleIds, 3.0, 3.0);

			statistics.Inc();
		}
	}

	private static async Task UpdateRidesWorker(IMobilityService service, Statistics statistics,
		long[] vehicleIds, long[] ids, int tranCount, int objsPerTran)
	{
		DateTime dt = DateTime.UtcNow;
		int c = 0;
		for (int i = 0; i < tranCount; i++)
		{
			RideDTO[] rides = new RideDTO[objsPerTran];
			for (int k = 0; k < objsPerTran; k++)
			{
				rides[k] = new RideDTO(ids[c++], vehicleIds[(tranCount - i) % vehicleIds.Length], dt, dt, 2.0);
			}

			await service.UpdateRides(rides);

			statistics.Inc();
		}
	}

	private static async Task CopyVehiclesPositionWorker(IMobilityService service, Statistics statistics,
		long[] ids, int tranCount, int objsPerTran)
	{
		int c1 = 0;
		int c2 = ids.Length / 2;

		for (int i = 0; i < tranCount / 2; i++)
		{
			long[] srcVehicleIds = new long[objsPerTran];
			long[] dstVehicleIds = new long[objsPerTran];
			for (int k = 0; k < objsPerTran; k++)
			{
				srcVehicleIds[k] = ids[c1++];
				dstVehicleIds[k] = ids[c2++];
			}

			await service.CopyVehiclePositions(srcVehicleIds, dstVehicleIds);

			statistics.Inc();
		}

		statistics.Inc();
	}

	private static async Task DeleteVehiclesWorker(IMobilityService service, Statistics statistics,
		long[] ids, int tranCount, int objsPerTran)
	{
		int c = 0;
		for (int i = 0; i < tranCount; i++)
		{
			long[] vehicleIds = new long[objsPerTran];
			for (int j = 0; j < objsPerTran; j++)
			{
				vehicleIds[j] = ids[c++];
			}

			await service.DeleteVehicles(vehicleIds);

			statistics.Inc();
		}
	}

	private static async Task DeleteRidesWorker(IMobilityService service, Statistics statistics,
		long[] ids, int tranCount, int objsPerTran)
	{
		int c = 0;
		for (int i = 0; i < tranCount; i++)
		{
			long[] rideIds = new long[objsPerTran];
			for (int j = 0; j < objsPerTran; j++)
			{
				rideIds[j] = ids[c++];
			}

			await service.DeleteRides(rideIds);

			statistics.Inc();
		}
	}

	private static async Task GetVehiclesWorker(IMobilityService service, Statistics statistics,
		long[] ids, int tranCount, int objsPerTran)
	{
		int c = 0;
		for (int i = 0; i < tranCount; i++)
		{
			//long[] vehicleIds = new long[objsPerTran];
			//for (int j = 0; j < objsPerTran; j++)
			//{
			//	vehicleIds[j] = ids[c++];
			//	if (c == ids.Length)
			//		c = 0;
			//}

			//await service.GetVehicles(vehicleIds);

			await service.GetVehicleYear(ids[c++]);
			if (c == ids.Length)
				c = 0;

			statistics.Inc();
		}
	}

	private static async Task GetRideVehiclesWorker(IMobilityService service, Statistics statistics,
		long[] ids, int tranCount, int objsPerTran)
	{
		int c = 0;
		for (int i = 0; i < tranCount; i++)
		{
			long[] rideIds = new long[objsPerTran];
			for (int j = 0; j < objsPerTran; j++)
			{
				rideIds[j] = ids[c++];
			}

			await service.GetRideVehicle(rideIds);

			statistics.Inc();
		}
	}

	private static async Task GetVehicleRidesWorker(IMobilityService service, Statistics statistics,
		long[] ids, int tranCount, int objsPerTran)
	{
		int c = 0;
		for (int i = 0; i < tranCount; i++)
		{
			long[] vehicleIds = new long[objsPerTran];
			for (int j = 0; j < objsPerTran; j++)
			{
				vehicleIds[j] = ids[c++];
			}

			await service.GetVehicleRides(vehicleIds);

			statistics.Inc();
		}
	}
}
