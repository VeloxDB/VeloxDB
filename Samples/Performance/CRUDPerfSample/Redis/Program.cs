namespace Client;

using System;
using System.Collections.Concurrent;
using System.Data;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using StackExchange.Redis;

internal class VehicleDTO
{
	[JsonIgnore]
	public double PositionX { get; set; }

	[JsonIgnore]
	public double PositionY { get; set; }

	public string ModelName { get; set; }
	public int Year { get; set; }

	public int PassengerCapacity { get; set; }

	public VehicleDTO()
	{

	}

	public VehicleDTO(double positionX, double positionY, string modelName, int year, int passengerCapacity)
	{
		this.PositionX = positionX;
		this.PositionY = positionY;
		this.ModelName = modelName;
		this.Year = year;
		this.PassengerCapacity = passengerCapacity;
	}
}

internal class RideDTO
{
    public long Id { get; set; }

    public long VehicleId { get; set; }

    public DateTime StartTime { get; set; }

    public DateTime EndTime { get; set; }

    public double CoveredDistance { get; set; }

	public RideDTO(long id, long vehicleId, DateTime startTime, DateTime endTime, double coveredDistance)
	{
		this.Id = id;
		this.VehicleId = vehicleId;
		this.StartTime = startTime;
		this.EndTime = endTime;
		this.CoveredDistance = coveredDistance;
	}

	public RideDTO(long vehicleId, DateTime startTime, DateTime endTime, double coveredDistance) :
		this(0, vehicleId, startTime, endTime, coveredDistance)
	{
	}

	public RideDTO()
	{
	}
}

internal class SourceDestinationPair
{
    public long SrcVehicleId { get; set; }
	public long DstVehicleId { get; set; }
}

internal class Program
{
	static int vehicleCount = 1024*1024*8;
	const int tranGroupSize = 256;

	static int workerCount = 1;
	static string[] vehicleNames = new string[0];

	static LinuxProcessor processor = new LinuxProcessor();
	private static IDatabase[] redis;

	public static void Main(string[] args)
	{

		System.IO.File.WriteAllText("/tmp/client.pid", Environment.ProcessId.ToString());
		var service = Init(args);

		PrepareData();

		// Warm up
		long[][] vehicleIds = InsertVehicles(service, new Statistics("Insert Vehicles Warmup"), vehicleCount, workerCount, 1);
		DeleteVehicles(service, new Statistics("Delete Vehicles Warmup"), workerCount, 1, vehicleIds);

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

			vehicleIds = InsertVehicles(service, statistics[objsPerTran][0], vehicleCount, workerCount, objsPerTran);
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

	private static IMobilityService Init(string[] args)
	{
		string address = "localhost";
		if (args.Length > 0)
			address = args[0];

		if (args.Length > 1)
			vehicleCount = (int)(vehicleCount * float.Parse(args[1]));

		if (args.Length > 2)
			workerCount = int.Parse(args[2]);

		int connCount = 96;

		if (args.Length > 3)
			connCount = int.Parse(args[3]);

		redis = new IDatabase[processor.PhysicalCoreCount];
		for (int i = 0; i < redis.Length; i++)
			redis[i] = ConnectionMultiplexer.Connect(address, (c) => c.SocketManager = SocketManager.ThreadPool).GetDatabase();

		IMobilityService service = new RedisMobilityService();
		return service;
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
		}

		statistics.Stop();
	}

	private static async Task CopyVehiclesPositionWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		int iteration = 0;
		IDatabase redis = null;
		while (true)
		{
			redis = GetRedis(redis, iteration);
			iteration++;

			long[] group = work.Take();
			if (group == null)
				return;

			int c = 0;

			while (c < group.Length)
			{
				SourceDestinationPair[] pairs = new SourceDestinationPair[objsPerTran];
				for (int k = 0; k < objsPerTran; k++)
				{
					pairs[k] = new SourceDestinationPair();
					pairs[k].SrcVehicleId = group[c];
					pairs[k].DstVehicleId = group[(c + 1) % group.Length];
					c++;
				}

				await service.CopyVehiclePositions(redis, pairs);
				statistics.Inc();
			}
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static IDatabase GetRedis(IDatabase redis, int iteration)
	{
		if (redis != null && (iteration & 64) != 0)
			return redis;

		return Program.redis[processor.GetExecutingPhysicalCore()];
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
		}
		statistics.Stop();

		return ids;
	}



	private static async Task InsertVehiclesWorker(IMobilityService service, Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		int iteration = 0;
		IDatabase redis = null;
		while (true)
		{
			redis = GetRedis(redis, iteration);
			iteration++;

			long[] group = work.Take();
			if (group == null)
				return;

			int c = 0;

			VehicleDTO[] vehicles = new VehicleDTO[objsPerTran];

			for (int i = 0; i < objsPerTran; i++)
			{
				vehicles[i] = new VehicleDTO();
			}

			while (c < group.Length)
			{
				for (int i = 0; i < objsPerTran; i++)
				{
					vehicles[i].PositionX = 0.0f;
					vehicles[i].PositionY = 0.0f;
					vehicles[i].ModelName = vehicleNames[c];
					vehicles[i].Year = i;
					vehicles[i].PassengerCapacity = i;
				}

				long[] tids = await service.InsertVehicles(redis, vehicles);
				for (int i = 0; i < tids.Length; i++)
				{
					group[c++] = tids[i];
				}
				statistics.Inc();
			}
		}
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
		}

		statistics.Stop();
	}


	private static async Task UpdateVehiclesWorker(IMobilityService service, Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		int iteration = 0;
		IDatabase redis = null;
		while (true)
		{
			redis = GetRedis(redis, iteration);
			iteration++;

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

				await service.UpdateVehiclePositions(redis, vehicleIds, 3.0, 3.0);
				statistics.Inc();
			}
		}
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
		}

		statistics.Stop();

		return ids;
	}

	private static async Task InsertRidesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<Tuple<long[], long[]>> work)
	{
		DateTime dt = DateTime.UtcNow;

		int iteration = 0;
		IDatabase redis = null;
		while (true)
		{
			redis = GetRedis(redis, iteration);
			iteration++;

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

				long[] tids = await service.InsertRides(redis, rides);
				for (int i = 0; i < tids.Length; i++)
				{
					group[c++] = tids[i];
				}

				statistics.Inc();
			}
		}
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
		}

		statistics.Stop();
	}

	private static async Task UpdateRidesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<Tuple<long[], long[]>> work)
	{
		DateTime dt = DateTime.UtcNow;

		int iteration = 0;
		IDatabase redis = null;
		while (true)
		{
			redis = GetRedis(redis, iteration);
			iteration++;

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

				await service.UpdateRides(redis, rides);
				statistics.Inc();
			}
		}
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
		}

		statistics.Stop();
	}

	private static async Task GetVehiclesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		int iteration = 0;
		IDatabase redis = null;
		while (true)
		{
			redis = GetRedis(redis, iteration);
			iteration++;

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

				await service.GetVehicles(redis, vehicleIds);
				statistics.Inc();
			}
		}
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
		}

		statistics.Stop();
	}

	private static async Task GetRideVehiclesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		int iteration = 0;
		IDatabase redis = null;
		while (true)
		{
			redis = GetRedis(redis, iteration);
			iteration++;

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

				await service.GetRideVehicle(redis, rideIds);
				statistics.Inc();
			}
		}
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
		}

		statistics.Stop();
	}

	private static async Task GetVehicleRidesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		int iteration = 0;
		IDatabase redis = null;
		while (true)
		{
			redis = GetRedis(redis, iteration);
			iteration++;

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

				await service.GetVehicleRides(redis, vehicleIds);
				statistics.Inc();
			}
		}
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
		}

		statistics.Stop();
	}

	private static async Task DeleteVehiclesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		DateTime dt = DateTime.UtcNow;

		int iteration = 0;
		IDatabase redis = null;
		while (true)
		{
			redis = GetRedis(redis, iteration);
			iteration++;

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

				await service.DeleteVehicles(redis, vehicleIds);
				statistics.Inc();
			}
		}
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
		}

		statistics.Stop();
	}

	private static async Task DeleteRidesWorker(IMobilityService service,
		Statistics statistics, int objsPerTran, BlockingCollection<long[]> work)
	{
		DateTime dt = DateTime.UtcNow;

		int iteration = 0;
		IDatabase redis = null;
		while (true)
		{
			redis = GetRedis(redis, iteration);
			iteration++;

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

				await service.DeleteRides(redis, rideIds);
				statistics.Inc();
			}
		}
	}


}