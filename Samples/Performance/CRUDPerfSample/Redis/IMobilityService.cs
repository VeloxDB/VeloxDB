using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Client;
internal interface IMobilityService
{
    Task<long[]> InsertVehicles(VehicleDTO[] vehicleDTOs);

    Task UpdateVehiclePositions(long[] vehicleIds, double positionX, double positionY);

    Task CopyVehiclePositions(SourceDestinationPair[] pairs);

    Task DeleteVehicles(long[] vehicleIds);

    Task<long[]> InsertRides(RideDTO[] rideDTOs);

    Task UpdateRides(RideDTO[] rideDTOs);

    Task DeleteRides(long[] rideIds);

    Task<VehicleDTO[]> GetVehicles(long[] vehicleIds);

    Task<VehicleDTO[]> GetRideVehicle(long[] rideIds);

    Task<RideDTO[][]> GetVehicleRides(long[] vehicleIds);
}

internal class RedisMobilityService : IMobilityService
{
    private readonly IDatabase redis;
    private long maxId = 0;


    public RedisMobilityService(IConnectionMultiplexer redis)
    {
        this.redis = redis.GetDatabase();
    }

    public async Task<long[]> InsertVehicles(VehicleDTO[] vehicleDTOs)
    {
        var vehicleIds = new long[vehicleDTOs.Length];
        for (int i = 0; i < vehicleDTOs.Length; i++)
        {
            vehicleIds[i] = GenerateId();
            var json = JsonSerializer.Serialize(vehicleDTOs[i]);
            await redis.HashSetAsync($"vehicle:{vehicleIds[i]}", [new("data", json),
                                                                  new("PositionX", vehicleDTOs[i].PositionX),
                                                                  new("PositionY", vehicleDTOs[i].PositionY)]);
        }
        return vehicleIds;
    }

    public async Task UpdateVehiclePositions(long[] vehicleIds, double positionX, double positionY)
    {
        foreach (var id in vehicleIds)
        {
            await redis.HashSetAsync($"vehicle:{id}", [
                new("PositionX", positionX),
                new("PositionY", positionY)
            ]);
        }
    }

    public async Task CopyVehiclePositions(SourceDestinationPair[] pairs)
    {
        foreach (var pair in pairs)
        {
            RedisValue[] position = await redis.HashGetAsync($"vehicle:{pair.SrcVehicleId}", ["PositionX", "PositionY"]);
            await redis.HashSetAsync($"vehicle:{pair.DstVehicleId}", [new("PositionX", position[0]), new("PositionY", position[1])]);
        }
    }

    public async Task DeleteVehicles(long[] vehicleIds)
    {
		IBatch batch = redis.CreateBatch();

		Task<RedisValue[]>[] rideIdsTasks = new Task<RedisValue[]>[vehicleIds.Length];

		for (int i = 0; i < vehicleIds.Length; i++)
        {
			long vehicleId = vehicleIds[i];
			rideIdsTasks[i] = batch.SetMembersAsync($"vehicle:{vehicleId}:rides");
        }

		batch.Execute();

		List<RedisKey> keys = new List<RedisKey>(vehicleIds.Length*2);

		for (int i = 0; i < rideIdsTasks.Length; i++)
		{
			RedisValue[] rideIds = await rideIdsTasks[i];
			foreach (var rideId in rideIds)
			{
				keys.Add(new RedisKey($"ride:{rideId}"));
			}
		}

        for (var i = 0; i < vehicleIds.Length; i++)
        {
            long id = vehicleIds[i];
            keys.Add(new RedisKey($"vehicle:{id}"));
        }

        await redis.KeyDeleteAsync(keys.ToArray());
    }

    public async Task<long[]> InsertRides(RideDTO[] rideDTOs)
    {
        var rideIds = new long[rideDTOs.Length];
        for (int i = 0; i < rideDTOs.Length; i++)
        {
            rideIds[i] = GenerateId();
            rideDTOs[i].Id = rideIds[i];
            var json = JsonSerializer.Serialize(rideDTOs[i]);
            await redis.HashSetAsync($"ride:{rideIds[i]}", "data", json);
            await redis.SetAddAsync($"vehicle:{rideDTOs[i].VehicleId}:rides", rideIds[i]);
        }
        return rideIds;
    }

    public async Task UpdateRides(RideDTO[] rideDTOs)
    {
        IBatch batch = redis.CreateBatch();
        Task[] tasks = new Task[rideDTOs.Length];
        for (var i = 0; i < rideDTOs.Length; i++)
        {
            RideDTO ride = rideDTOs[i];
            string json = JsonSerializer.Serialize(ride);
            tasks[i] = batch.HashSetAsync($"ride:{ride.Id}", "data", json);
        }

        batch.Execute();
        await Task.WhenAll(tasks);
    }

    public async Task DeleteRides(long[] rideIds)
    {
        IBatch batch = redis.CreateBatch();
        Dictionary<long, Task<RedisValue>> rideJsons = [];

        foreach (var rideId in rideIds)
        {
            rideJsons[rideId] = batch.HashGetAsync($"ride:{rideId}", "data");
        }

        batch.Execute();

        var deleteTasks = new List<Task>(rideIds.Length * 2);

        foreach (long rideId in rideIds)
        {
            string rideJson = await rideJsons[rideId];

            if (rideJson != null)
            {
                RideDTO ride = JsonSerializer.Deserialize<RideDTO>(rideJson);

                Debug.Assert(ride != null);

                deleteTasks.Add(batch.SetRemoveAsync($"vehicle:{ride.VehicleId}:rides", rideId));
                deleteTasks.Add(batch.KeyDeleteAsync($"ride:{rideId}"));
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
            tasks[i] = batch.HashGetAllAsync($"vehicle:{id}");
        }

        batch.Execute();
        HashEntry[][] response = await Task.WhenAll(tasks);

        for (var i = 0; i < response.Length; i++)
        {
            HashEntry[] entries = response[i];

            double x = 0.0;
            double y = 0.0;
            VehicleDTO vehicle = null;

            foreach (var entry in entries)
            {
                if (entry.Name == "PositionX")
                {
                    x = (double)entry.Value;
                }
                else if (entry.Name == "PositionY")
                {
                    y = (double)entry.Value;
                }
                else if (entry.Name == "data")
                {
                    string json = entry.Value;
                    if (json == null)
                        continue;
                    vehicle = JsonSerializer.Deserialize<VehicleDTO>(json);
                }
            }

            if (vehicle == null)
                continue;

            vehicle.PositionX = x;
            vehicle.PositionY = y;

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
            tasks[i] = batch.HashGetAsync($"ride:{rideId}", "data");
        }

        batch.Execute();

        for (int i = 0; i < tasks.Length; i++)
        {
            string rideJson = await tasks[i];
            if (rideJson == null)
            {
                throw new ArgumentException($"Unknown ride with id {rideIds[i]}");
            }

            RideDTO ride = JsonSerializer.Deserialize<RideDTO>(rideJson);

            if (ride == null)
                throw new ArgumentException($"Invalid ride at {rideIds[i]}");

            vehicleIds[i] = ride.VehicleId;
        }

        return await GetVehicles(vehicleIds);
    }

    public async Task<RideDTO[][]> GetVehicleRides(long[] vehicleIds)
    {
		IBatch batch = redis.CreateBatch();
		Dictionary<long, Task<RedisValue[]>> rideIdsTasks = new Dictionary<long, Task<RedisValue[]>>();
		Dictionary<long, Dictionary<RedisValue, Task<RedisValue>>> rideJsonTasks = new Dictionary<long, Dictionary<RedisValue, Task<RedisValue>>>();


        foreach (long vehicleId in vehicleIds)
        {
            rideIdsTasks[vehicleId] = batch.SetMembersAsync($"vehicle:{vehicleId}:rides");
        }

        batch.Execute(); // Execute to get the ride IDs

        var result = new RideDTO[vehicleIds.Length][];

        for (int i = 0; i < vehicleIds.Length; i++)
        {
            long vehicleId = vehicleIds[i];
			RedisValue[] rideIds = await rideIdsTasks[vehicleId];

            rideJsonTasks[vehicleId] = new Dictionary<RedisValue, Task<RedisValue>>();

            foreach (RedisValue rideId in rideIds)
            {
                rideJsonTasks[vehicleId][rideId] = batch.HashGetAsync($"ride:{rideId}", "data");
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
				string rideJson = await rideJsonTasks[vehicleId][rideId];

                if (rideJson != null)
                {
					RideDTO rideDTO = JsonSerializer.Deserialize<RideDTO>(rideJson);
                    Debug.Assert(rideDTO != null);
                    rides[j] = rideDTO;
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