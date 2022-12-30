using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection.PortableExecutable;
using System.Runtime.Serialization;
using API;
using VeloxDB.Client;
using Velox.Common;

namespace Client;

public class Program
{
	static int vehicleCount = 1024 * 1024 * 8;
	const int tranGroupSize = 256;

	static int workerCount = 1;
	static string[] vehicleNames = new string[0];

	public static void Main(string[] args)
	{
		string[] addresses = new string[] { "localhost" };
		if (args.Length > 0)
			addresses = args[0].Split('/');

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

			Console.WriteLine(addresses[i]);
			csp.AddAddress(addresses[i]);
		}

		csp.PoolSize = connCount;
		csp.BufferPoolSize = 1024 * 1024 * 128;
		csp.OpenTimeout = 5000;

		IMobilityService service = ConnectionFactory.Get<IMobilityService>(csp.GenerateConnectionString());

		PrepareData();

		Statistics[][] statistics = new Statistics[5][];

		for (int objsPerTran = 1; objsPerTran <= 4; objsPerTran *= 2)
		{
			statistics[objsPerTran] = new Statistics[]
			{
				new Statistics($"Insert vehicles [{objsPerTran}]"),
				new Statistics($"Update vehicles [{objsPerTran}]"),
				new Statistics($"Read-update vehicles [{objsPerTran}]"),
				new Statistics($"Insert rides [{objsPerTran}]"),
				new Statistics($"Update rides [{objsPerTran}]"),
				new Statistics($"Get vehicles [{objsPerTran}]"),
				new Statistics($"Get ride vehicles [{objsPerTran}]"),
				new Statistics($"Get vehicle rides [{objsPerTran}]"),
				new Statistics($"Delete rides [{objsPerTran}]"),
				new Statistics($"Delete vehicles [{objsPerTran}]"),
			};

			long[][] vehicleIds = InsertVehicles(service, statistics[objsPerTran][0], vehicleCount, workerCount, objsPerTran);
			UpdateVehicles(service, statistics[objsPerTran][1], workerCount, objsPerTran, vehicleIds);
			CopyVehiclesPosition(service, statistics[objsPerTran][2], workerCount, objsPerTran, vehicleIds);
			long[][] rideIds = InsertRides(service, statistics[objsPerTran][3], vehicleCount, workerCount, objsPerTran, vehicleIds);
			UpdateRides(service, statistics[objsPerTran][4], workerCount, objsPerTran, rideIds, vehicleIds);
			GetVehicles(service, statistics[objsPerTran][5], workerCount, objsPerTran, vehicleIds);
			GetRideVehicles(service, statistics[objsPerTran][6], workerCount, objsPerTran, rideIds);
			GetVehicleRides(service, statistics[objsPerTran][7], workerCount, objsPerTran, vehicleIds);
			DeleteRides(service, statistics[objsPerTran][8], workerCount, objsPerTran, rideIds);
			DeleteVehicles(service, statistics[objsPerTran][9], workerCount, objsPerTran, vehicleIds);
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
		vehicleNames = new string[tranGroupSize];
		for (int i = 0; i < vehicleNames.Length; i++)
		{
			vehicleNames[i] = string.Format("Vehicle{0}", i);
		}
	}

	private static long[][] InsertVehicles(IMobilityService service, Statistics statistics, int objectCount, int workerCount,
		int objsPerTran)
	{
		BlockingCollection<long[]> work = new BlockingCollection<long[]>();

		int groupCount = objectCount / tranGroupSize;
		long[][] ids = new long[groupCount][];
		for (int i = 0; i < groupCount; i++)
		{
			ids[i] = new long[tranGroupSize];
			work.Add(ids[i]);
		}

		for (int i = 0; i < workerCount; i++)
		{
			work.Add(null);
		}

		Semaphore finishedEvent = new Semaphore(0, int.MaxValue);

		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			Task t = new Task(async (p) =>
			{
				await InsertVehiclesWorker(service, statistics, objsPerTran, work);
				finishedEvent.Release();
			}, i);

			t.Start();
		}

		for (int i = 0; i < workerCount; i++)
		{
			finishedEvent.WaitOne();
			statistics.Stop();
		}

		return ids;
	}

	private static long[][] InsertRides(IMobilityService service, Statistics statistics, int objectCount, int workerCount,
		int objsPerTran, long[][] vehicleIds)
	{
		BlockingCollection<Tuple<long[], long[]>> work = new BlockingCollection<Tuple<long[], long[]>>();

		int groupCount = vehicleIds.Length;
		long[][] ids = new long[groupCount][];
		for (int i = 0; i < groupCount; i++)
		{
			ids[i] = new long[tranGroupSize];
			work.Add(new Tuple<long[], long[]>(ids[i], vehicleIds[i]));
		}

		for (int i = 0; i < workerCount; i++)
		{
			work.Add(null);
		}

		Semaphore finishedEvent = new Semaphore(0, int.MaxValue);

		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			Task t = new Task(async (p) =>
			{
				await InsertRidesWorker(service, statistics, objsPerTran, work);
				finishedEvent.Release();
			}, i);

			t.Start();
		}

		for (int i = 0; i < workerCount; i++)
		{
			finishedEvent.WaitOne();
			statistics.Stop();
		}

		statistics.Stop();

		return ids;
	}

	private static void DeleteVehicles(IMobilityService service, Statistics statistics, int workerCount, int objsPerTran, long[][] ids)
	{
		BlockingCollection<long[]> work = new BlockingCollection<long[]>();
		for (int i = 0; i < ids.Length; i++)
		{
			work.Add(ids[i]);
		}

		for (int i = 0; i < workerCount; i++)
		{
			work.Add(null);
		}

		Semaphore finishedEvent = new Semaphore(0, int.MaxValue);

		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			Task t = new Task(async (p) =>
			{
				await DeleteVehiclesWorker(service, statistics, objsPerTran, work);
				finishedEvent.Release();
			}, i);

			t.Start();
		}

		for (int i = 0; i < workerCount; i++)
		{
			finishedEvent.WaitOne();
			statistics.Stop();
		}

		statistics.Stop();
	}

	private static void DeleteRides(IMobilityService service, Statistics statistics, int workerCount, int objsPerTran, long[][] ids)
	{
		BlockingCollection<long[]> work = new BlockingCollection<long[]>();
		for (int i = 0; i < ids.Length; i++)
		{
			work.Add(ids[i]);
		}

		for (int i = 0; i < workerCount; i++)
		{
			work.Add(null);
		}

		Semaphore finishedEvent = new Semaphore(0, int.MaxValue);

		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			Task t = new Task(async (p) =>
			{
				await DeleteRidesWorker(service, statistics, objsPerTran, work);
				finishedEvent.Release();
			}, i);

			t.Start();
		}

		for (int i = 0; i < workerCount; i++)
		{
			finishedEvent.WaitOne();
			statistics.Stop();
		}

		statistics.Stop();
	}

	private static void UpdateVehicles(IMobilityService service, Statistics statistics, int workerCount, int objsPerTran, long[][] ids)
	{
		BlockingCollection<long[]> work = new BlockingCollection<long[]>();
		for (int i = 0; i < ids.Length; i++)
		{
			work.Add(ids[i]);
		}

		for (int i = 0; i < workerCount; i++)
		{
			work.Add(null);
		}

		Semaphore finishedEvent = new Semaphore(0, int.MaxValue);

		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			Task t = new Task(async (p) =>
			{
				await UpdateVehiclesWorker(service, statistics, objsPerTran, work);
				finishedEvent.Release();
			}, i);

			t.Start();
		}

		for (int i = 0; i < workerCount; i++)
		{
			finishedEvent.WaitOne();
			statistics.Stop();
		}

		statistics.Stop();
	}

	private static void UpdateRides(IMobilityService service, Statistics statistics, int workerCount, int objsPerTran, long[][] ids,
		long[][] vehicleIds)
	{
		BlockingCollection<Tuple<long[], long[]>> work = new BlockingCollection<Tuple<long[], long[]>>();
		for (int i = 0; i < ids.Length; i++)
		{
			work.Add(new Tuple<long[], long[]>(ids[i], vehicleIds[i]));
		}

		for (int i = 0; i < workerCount; i++)
		{
			work.Add(null);
		}

		Semaphore finishedEvent = new Semaphore(0, int.MaxValue);

		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			Task t = new Task(async (p) =>
			{
				await UpdateRidesWorker(service, statistics, objsPerTran, work);
				finishedEvent.Release();
			}, i);

			t.Start();
		}

		for (int i = 0; i < workerCount; i++)
		{
			finishedEvent.WaitOne();
			statistics.Stop();
		}

		statistics.Stop();
	}

	private static void CopyVehiclesPosition(IMobilityService service, Statistics statistics, int workerCount, int objsPerTran, long[][] ids)
	{
		BlockingCollection<long[]> work = new BlockingCollection<long[]>();
		for (int i = 0; i < ids.Length; i++)
		{
			work.Add(ids[i]);
		}

		for (int i = 0; i < workerCount; i++)
		{
			work.Add(null);
		}

		Semaphore finishedEvent = new Semaphore(0, int.MaxValue);

		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			Task t = new Task(async (p) =>
			{
				await CopyVehiclesPositionWorker(service, statistics, objsPerTran, work);
				finishedEvent.Release();
			}, i);

			t.Start();
		}

		for (int i = 0; i < workerCount; i++)
		{
			finishedEvent.WaitOne();
			statistics.Stop();
		}

		statistics.Stop();
	}

	private static void GetVehicles(IMobilityService service, Statistics statistics, int workerCount, int objsPerTran, long[][] ids)
	{
		BlockingCollection<long[]> work = new BlockingCollection<long[]>();
		for (int i = 0; i < ids.Length; i++)
		{
			work.Add(ids[i]);
		}

		for (int i = 0; i < workerCount; i++)
		{
			work.Add(null);
		}

		Semaphore finishedEvent = new Semaphore(0, int.MaxValue);

		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			Task t = new Task(async (p) =>
			{
				await GetVehiclesWorker(service, statistics, objsPerTran, work);
				finishedEvent.Release();
			}, i);

			t.Start();
		}

		for (int i = 0; i < workerCount; i++)
		{
			finishedEvent.WaitOne();
			statistics.Stop();
		}

		statistics.Stop();
	}

	private static void GetRideVehicles(IMobilityService service, Statistics statistics, int workerCount, int objsPerTran, long[][] ids)
	{
		BlockingCollection<long[]> work = new BlockingCollection<long[]>();
		for (int i = 0; i < ids.Length; i++)
		{
			work.Add(ids[i]);
		}

		for (int i = 0; i < workerCount; i++)
		{
			work.Add(null);
		}

		Semaphore finishedEvent = new Semaphore(0, int.MaxValue);

		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			Task t = new Task(async (p) =>
			{
				await GetRideVehiclesWorker(service, statistics, objsPerTran, work);
				finishedEvent.Release();
			}, i);

			t.Start();
		}

		for (int i = 0; i < workerCount; i++)
		{
			finishedEvent.WaitOne();
			statistics.Stop();
		}

		statistics.Stop();
	}

	private static void GetVehicleRides(IMobilityService service, Statistics statistics, int workerCount, int objsPerTran, long[][] ids)
	{
		BlockingCollection<long[]> work = new BlockingCollection<long[]>();
		for (int i = 0; i < ids.Length; i++)
		{
			work.Add(ids[i]);
		}

		for (int i = 0; i < workerCount; i++)
		{
			work.Add(null);
		}

		Semaphore finishedEvent = new Semaphore(0, int.MaxValue);

		statistics.Start();
		for (int i = 0; i < workerCount; i++)
		{
			Task t = new Task(async (p) =>
			{
				await GetVehicleRidesWorker(service, statistics, objsPerTran, work);
				finishedEvent.Release();
			}, i);

			t.Start();
		}

		for (int i = 0; i < workerCount; i++)
		{
			finishedEvent.WaitOne();
			statistics.Stop();
		}

		statistics.Stop();
	}

	private static async Task InsertVehiclesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		while (true)
		{
			long[] group = work.Take();
			if (group == null)
				return;

			int c = 0;

			while (c < group.Length)
			{
				VehicleDTO[] vehicles = new VehicleDTO[objsPerTran];
				for (int i = 0; i < objsPerTran; i++)
				{
					vehicles[i] = new VehicleDTO(0.0f, 0.0f, vehicleNames[c], i, i);
				}

				long[] tids = await service.InsertVehicles(vehicles);
				for (int i = 0; i < tids.Length; i++)
				{
					group[c++] = tids[i];
				}

				statistics.Inc();
			}
		}
	}

	private static async Task InsertRidesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<Tuple<long[], long[]>> work)
	{
		DateTime dt = DateTime.UtcNow;

		while (true)
		{
			Tuple<long[], long[]> t = work.Take();
			if (t == null)
				return;

			long[] group = t.Item1;
			long[] vehicleGroup = t.Item2;

			int c = 0;
			while (c < group.Length)
			{
				RideDTO[] rides = new RideDTO[objsPerTran];
				for (int i = 0; i < objsPerTran; i++)
				{
					rides[i] = new RideDTO(vehicleGroup[c % vehicleGroup.Length], dt, dt, 1.0);
				}

				long[] tids = await service.InsertRides(rides);
				for (int i = 0; i < tids.Length; i++)
				{
					group[c++] = tids[i];
				}

				statistics.Inc();
			}
		}
	}

	private static async Task UpdateVehiclesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		while (true)
		{
			long[] group = work.Take();
			if (group == null)
				return;

			int c = 0;

			while (c < group.Length)
			{
				long[] vehicleIds = new long[objsPerTran];
				for (int k = 0; k < objsPerTran; k++)
				{
					vehicleIds[k] = group[c++];
				}

				await service.UpdateVehiclePositions(vehicleIds, 3.0, 3.0);
				statistics.Inc();
			}
		}
	}

	private static async Task UpdateRidesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<Tuple<long[], long[]>> work)
	{
		DateTime dt = DateTime.UtcNow;

		while (true)
		{
			Tuple<long[], long[]> t = work.Take();
			if (t == null)
				return;

			long[] group = t.Item1;
			long[] vehicleGroup = t.Item2;

			int c = 0;

			while (c < group.Length)
			{
				RideDTO[] rides = new RideDTO[objsPerTran];
				for (int k = 0; k < objsPerTran; k++)
				{
					rides[k] = new RideDTO(group[c], vehicleGroup[(c + 1) % vehicleGroup.Length], dt, dt, 2.0);
					c++;
				}

				await service.UpdateRides(rides);
				statistics.Inc();
			}
		}
	}

	private static async Task CopyVehiclesPositionWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		while (true)
		{
			long[] group = work.Take();
			if (group == null)
				return;

			int c = 0;

			while (c < group.Length)
			{
				long[] srcVehicleIds = new long[objsPerTran];
				long[] dstVehicleIds = new long[objsPerTran];
				for (int k = 0; k < objsPerTran; k++)
				{
					srcVehicleIds[k] = group[c];
					dstVehicleIds[k] = group[(c + 1) % group.Length];
					c++;
				}

				await service.CopyVehiclePositions(srcVehicleIds, dstVehicleIds);
				statistics.Inc();
			}
		}
	}

	private static async Task DeleteVehiclesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		DateTime dt = DateTime.UtcNow;

		while (true)
		{
			long[] group = work.Take();
			if (group == null)
				return;

			int c = 0;

			while (c < group.Length)
			{
				long[] vehicleIds = new long[objsPerTran];
				for (int k = 0; k < objsPerTran; k++)
				{
					vehicleIds[k] = group[c++];
				}

				await service.DeleteVehicles(vehicleIds);
				statistics.Inc();
			}
		}
	}

	private static async Task DeleteRidesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		DateTime dt = DateTime.UtcNow;

		while (true)
		{
			long[] group = work.Take();
			if (group == null)
				return;

			int c = 0;

			while (c < group.Length)
			{
				long[] rideIds = new long[objsPerTran];
				for (int k = 0; k < objsPerTran; k++)
				{
					rideIds[k] = group[c++];
				}

				await service.DeleteRides(rideIds);
				statistics.Inc();
			}
		}
	}

	private static async Task GetVehiclesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		while (true)
		{
			long[] group = work.Take();
			if (group == null)
				return;

			int c = 0;

			while (c < group.Length)
			{
				long[] vehicleIds = new long[objsPerTran];
				for (int k = 0; k < objsPerTran; k++)
				{
					vehicleIds[k] = group[c++];
				}

				await service.GetVehicles(vehicleIds);
				statistics.Inc();
			}
		}
	}

	private static async Task GetRideVehiclesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		while (true)
		{
			long[] group = work.Take();
			if (group == null)
				return;

			int c = 0;

			while (c < group.Length)
			{
				long[] rideIds = new long[objsPerTran];
				for (int k = 0; k < objsPerTran; k++)
				{
					rideIds[k] = group[c++];
				}

				await service.GetRideVehicle(rideIds);
				statistics.Inc();
			}
		}
	}

	private static async Task GetVehicleRidesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		while (true)
		{
			long[] group = work.Take();
			if (group == null)
				return;

			int c = 0;

			while (c < group.Length)
			{
				long[] vehicleIds = new long[objsPerTran];
				for (int k = 0; k < objsPerTran; k++)
				{
					vehicleIds[k] = group[c++];
				}

				await service.GetVehicleRides(vehicleIds);
				statistics.Inc();
			}
		}
	}
}
