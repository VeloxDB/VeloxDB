using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using StackExchange.Redis;

namespace Client;

internal class RedisMobilityService : IMobilityService
{
	private readonly IDatabase redis;
	private long maxId = 0;

	private const long VEHICLES = 0 << 62;
	private const long RIDES = 1 << 62;
	private const long VEHICLE_RIDES = 2 << 62;

	private const int MODEL_NAME = 0;
	private const int YEAR = 1;
	private const int PASSENGER_CAPACITY = 2;
	private const int POSITION_X = 3;
	private const int POSITION_Y = 4;
	private const int VEHICLE_ID = 5;
	private const int START_TIME = 6;
	private const int END_TIME = 7;
	private const int COVERED_DISTANCE = 8;

	public RedisMobilityService(IConnectionMultiplexer redis)
	{
		this.redis = redis.GetDatabase();
	}

	public async Task<long[]> InsertVehicles(VehicleDTO[] vehicleDTOs)
	{
		var vehicleIds = new long[vehicleDTOs.Length];
		IBatch batch = redis.CreateBatch();
		Task[] tasks = new Task[vehicleDTOs.Length];

		for (int i = 0; i < vehicleDTOs.Length; i++)
		{
			vehicleIds[i] = GenerateId() | VEHICLES;
			VehicleDTO vehicle = vehicleDTOs[i];

			RedisKey key = new RedisKey((RedisValue)vehicleIds[i]);
			tasks[i] = batch.HashSetAsync(key, new HashEntry[]
			{
				new HashEntry(MODEL_NAME, vehicle.ModelName),
				new HashEntry(YEAR, vehicle.Year),
				new HashEntry(PASSENGER_CAPACITY, vehicle.PassengerCapacity),
				new HashEntry(POSITION_X, vehicle.PositionX),
				new HashEntry(POSITION_Y, vehicle.PositionY)
			});
		}

		batch.Execute();
		await Task.WhenAll(tasks);
		return vehicleIds;
	}

	public async Task UpdateVehiclePositions(long[] vehicleIds, double positionX, double positionY)
	{
		IBatch batch = redis.CreateBatch();
		Task[] tasks = new Task[vehicleIds.Length];
		for (var i = 0; i < vehicleIds.Length; i++)
		{
			RedisKey id = new RedisKey((RedisValue)vehicleIds[i]);
			tasks[i] = batch.HashSetAsync(id, [
				new(POSITION_X, positionX),
				new(POSITION_Y, positionY)
			]);
		}

		await Task.WhenAll(tasks);
	}

	public async Task CopyVehiclePositions(SourceDestinationPair[] pairs)
	{
		Task<RedisValue[]>[] tasks = new Task<RedisValue[]>[pairs.Length];
		Task[] setTasks = new Task[pairs.Length];
		IBatch batch = redis.CreateBatch();
		for (int i = 0; i < pairs.Length; i++)
		{
			RedisKey srcVehicleId = new RedisKey((RedisValue)pairs[i].SrcVehicleId);
			tasks[i] = batch.HashGetAsync(srcVehicleId, [POSITION_X, POSITION_Y]);
		}

		batch.Execute();

		for (int i = 0; i < pairs.Length; i++)
		{
			RedisValue[] position = await tasks[i];
			RedisKey dstVehicleId = new RedisKey((RedisValue)pairs[i].DstVehicleId);
			setTasks[i] = batch.HashSetAsync(dstVehicleId, [
				new(POSITION_X, position[0]),
				new(POSITION_Y, position[1])
			]);
		}

		batch.Execute();
		await Task.WhenAll(setTasks);
	}

	public async Task DeleteVehicles(long[] vehicleIds)
	{
		IBatch batch = redis.CreateBatch();

		Task<RedisValue[]>[] rideIdsTasks = new Task<RedisValue[]>[vehicleIds.Length];

		for (int i = 0; i < vehicleIds.Length; i++)
		{
			RedisValue ridesId = vehicleIds[i] | VEHICLE_RIDES;
			rideIdsTasks[i] = batch.SetMembersAsync(new RedisKey(ridesId));
		}

		batch.Execute();

		List<RedisKey> keys = new List<RedisKey>(vehicleIds.Length * 3);

		for (int i = 0; i < rideIdsTasks.Length; i++)
		{
			RedisValue[] rideIds = await rideIdsTasks[i];
			foreach (var rideId in rideIds)
			{
				keys.Add(new RedisKey(rideId));
			}
		}

		for (var i = 0; i < vehicleIds.Length; i++)
		{
			long id = vehicleIds[i];
			keys.Add(new RedisKey((RedisValue)id));
			keys.Add(new RedisKey((RedisValue)(id | VEHICLE_RIDES)));
		}

		await redis.KeyDeleteAsync(keys.ToArray());
	}

	public async Task<long[]> InsertRides(RideDTO[] rideDTOs)
	{
		var rideIds = new long[rideDTOs.Length];
		IBatch batch = redis.CreateBatch();
		Task[] tasks = new Task[rideDTOs.Length * 2];

		for (int i = 0; i < rideDTOs.Length; i++)
		{
			rideIds[i] = GenerateId() | RIDES;
			RideDTO ride = rideDTOs[i];
			ride.Id = rideIds[i];

			tasks[2 * i] = batch.HashSetAsync(new RedisKey((RedisValue)rideIds[i]), RideToHashEntries(ride));
			tasks[2 * i + 1] = batch.SetAddAsync(new RedisKey((RedisValue)(rideDTOs[i].VehicleId | VEHICLE_RIDES)), rideIds[i]);
		}

		batch.Execute();
		await Task.WhenAll(tasks);
		return rideIds;
	}

	private static HashEntry[] RideToHashEntries(RideDTO ride)
	{
		return [
			new HashEntry(VEHICLE_ID, ride.VehicleId),
			new HashEntry(START_TIME, ride.StartTime.Ticks),
			new HashEntry(END_TIME, ride.EndTime.Ticks),
			new HashEntry(COVERED_DISTANCE, ride.CoveredDistance)
		];
	}

	public async Task UpdateRides(RideDTO[] rideDTOs)
	{
		IBatch batch = redis.CreateBatch();
		Task[] tasks = new Task[rideDTOs.Length];
		for (var i = 0; i < rideDTOs.Length; i++)
		{
			RideDTO ride = rideDTOs[i];

			tasks[i] = batch.HashSetAsync(new RedisKey((RedisValue)ride.Id), RideToHashEntries(ride));
		}

		batch.Execute();
		await Task.WhenAll(tasks);
	}

	public async Task DeleteRides(long[] rideIds)
	{
		IBatch batch = redis.CreateBatch();
		Dictionary<long, Task<RedisValue>> rideData = [];

		foreach (var rideId in rideIds)
		{
			rideData[rideId] = batch.HashGetAsync(new RedisKey((RedisValue)rideId), VEHICLE_ID);
		}

		batch.Execute();

		var deleteTasks = new List<Task>(rideIds.Length * 2);

		foreach (long rideId in rideIds)
		{
			RedisValue vehicleIdValue = await rideData[rideId];

			if (vehicleIdValue.HasValue)
			{
				long vehicleId = (long)vehicleIdValue;

				deleteTasks.Add(batch.SetRemoveAsync(new RedisKey((RedisValue)(vehicleId|VEHICLE_RIDES)), rideId));
				deleteTasks.Add(batch.KeyDeleteAsync(new RedisKey((RedisValue)rideId)));
			}
		}

		batch.Execute();

		await Task.WhenAll(deleteTasks);
	}
	public async Task<VehicleDTO[]> GetVehicles(long[] vehicleIds)
	{
		IBatch batch = redis.CreateBatch();
		Task<HashEntry[]>[] tasks = new Task<HashEntry[]>[vehicleIds.Length];
		VehicleDTO[] result = new VehicleDTO[vehicleIds.Length];

		for (int i = 0; i < vehicleIds.Length; i++)
		{
			long id = vehicleIds[i];
			tasks[i] = batch.HashGetAllAsync(new RedisKey((RedisValue)id));
		}

		batch.Execute();

		for(var i = 0; i < tasks.Length; i++)
		{
			HashEntry[] entries = await tasks[i];

			VehicleDTO vehicle = new VehicleDTO();

			foreach (var entry in entries)
			{
				if (entry.Name == POSITION_X)
				{
					vehicle.PositionX = (double)entry.Value;
				}
				else if (entry.Name == POSITION_Y)
				{
					vehicle.PositionY = (double)entry.Value;
				}
				else if (entry.Name == MODEL_NAME)
				{
					vehicle.ModelName = (string)entry.Value;
				}
				else if (entry.Name == YEAR)
				{
					vehicle.Year = (int)entry.Value;
				}
				else if (entry.Name == PASSENGER_CAPACITY)
				{
					vehicle.PassengerCapacity = (int)entry.Value;
				}
			}

			result[i] = vehicle;
		}

		return result;
	}

	public async Task<VehicleDTO[]> GetRideVehicle(long[] rideIds)
	{
		IBatch batch = redis.CreateBatch();
		Task<RedisValue>[] tasks = new Task<RedisValue>[rideIds.Length];
		long[] vehicleIds = new long[rideIds.Length];

		for (var i = 0; i < rideIds.Length; i++)
		{
			var rideId = rideIds[i];
			tasks[i] = batch.HashGetAsync(new RedisKey((RedisValue)rideId), VEHICLE_ID);
		}

		batch.Execute();

		for (int i = 0; i < tasks.Length; i++)
		{
			RedisValue value = await tasks[i];
			if (!value.HasValue)
			{
				throw new ArgumentException($"Unknown ride with id {rideIds[i]}");
			}

			vehicleIds[i] = (long)value;
		}

		return await GetVehicles(vehicleIds);
	}

	public async Task<RideDTO[][]> GetVehicleRides(long[] vehicleIds)
	{
		IBatch batch = redis.CreateBatch();
		Dictionary<long, Task<RedisValue[]>> rideIdsTasks = new Dictionary<long, Task<RedisValue[]>>();
		Dictionary<long, Dictionary<RedisValue, Task<HashEntry[]>>> rideTasks = new Dictionary<long, Dictionary<RedisValue, Task<HashEntry[]>>>();

		foreach (long vehicleId in vehicleIds)
		{
			rideIdsTasks[vehicleId] = batch.SetMembersAsync(new RedisKey((RedisValue)(vehicleId | VEHICLE_RIDES)));
		}

		batch.Execute(); // Execute to get the ride IDs

		var result = new RideDTO[vehicleIds.Length][];

		for (int i = 0; i < vehicleIds.Length; i++)
		{
			long vehicleId = vehicleIds[i];
			RedisValue[] rideIds = await rideIdsTasks[vehicleId];

			rideTasks[vehicleId] = new Dictionary<RedisValue, Task<HashEntry[]>>();

			foreach (RedisValue rideId in rideIds)
			{
				rideTasks[vehicleId][rideId] = batch.HashGetAllAsync(new RedisKey((RedisValue)rideId));
			}
		}

		batch.Execute();

		for (int i = 0; i < vehicleIds.Length; i++)
		{
			long vehicleId = vehicleIds[i];
			RedisValue[] rideIds = await rideIdsTasks[vehicleId];
			RideDTO[] rides = new RideDTO[rideIds.Length];

			for (var j = 0; j < rideIds.Length; j++)
			{
				var rideId = rideIds[j];
				HashEntry[] rideEntries = await rideTasks[vehicleId][rideId];

				if (rideEntries.Length > 0)
				{
					RideDTO rideDTO = new RideDTO
					{
						Id = (long)rideId,
						VehicleId = vehicleId
					};

					foreach(HashEntry entry in rideEntries)
					{
						if (entry.Name == START_TIME)
						{
							rideDTO.StartTime = new DateTime((long)entry.Value);
						}
						else if (entry.Name == END_TIME)
						{
							rideDTO.EndTime = new DateTime((long)entry.Value);
						}
						else if (entry.Name == COVERED_DISTANCE)
						{
							rideDTO.CoveredDistance = (double)entry.Value;
						}
					}
				}
			}
			result[i] = rides;
		}

		return result;
	}

	private long GenerateId()
	{
		return Interlocked.Increment(ref maxId);
	}
}