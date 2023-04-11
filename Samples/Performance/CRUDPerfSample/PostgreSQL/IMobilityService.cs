using Npgsql;

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

class PostgresMobilityService : IMobilityService
{
	NpgsqlDataSource source;

	public PostgresMobilityService(NpgsqlDataSource source)
	{
		this.source = source;
	}

	public async Task CopyVehiclePositions(SourceDestinationPair[] pairs)
	{
		using (NpgsqlConnection connection = await source.OpenConnectionAsync())
		{
			using (NpgsqlCommand command = new NpgsqlCommand("SELECT public.copyvehiclepositions(@p_pairs)", connection))
			{
				command.Parameters.AddWithValue("p_pairs", pairs);
				await command.ExecuteNonQueryAsync();
			}
		}
	}

	public async Task DeleteRides(long[] rideIds)
	{
		using (NpgsqlConnection connection = await source.OpenConnectionAsync())
		{
			using (NpgsqlCommand command = new NpgsqlCommand("SELECT public.deleterides(@p_rideIds)", connection))
			{
				command.Parameters.AddWithValue("p_rideIds", rideIds);
				await command.ExecuteNonQueryAsync();
			}
		}
	}

	public async Task DeleteVehicles(long[] vehicleIds)
	{
		using (NpgsqlConnection connection = await source.OpenConnectionAsync())
		{
			using (NpgsqlCommand command = new NpgsqlCommand("SELECT public.deletevehicles(@p_vehicleIds)", connection))
			{
				command.Parameters.AddWithValue("p_vehicleIds", vehicleIds);
				await command.ExecuteNonQueryAsync();
			}
		}
	}

	public async Task<VehicleDTO[]> GetRideVehicle(long[] rideIds)
	{
		using (NpgsqlConnection connection = await source.OpenConnectionAsync())
		{
			using (NpgsqlCommand command = new NpgsqlCommand("SELECT * From public.getridevehicles(@p_rideIds)", connection))
			{
				command.Parameters.AddWithValue("p_rideIds", rideIds);
				using (NpgsqlDataReader reader = await command.ExecuteReaderAsync())
				{
					List<VehicleDTO> result = new List<VehicleDTO>();
					while (await reader.ReadAsync())
					{
						result.Add(new VehicleDTO(reader.GetDouble(1), reader.GetDouble(2), reader.GetString(3), reader.GetInt32(4), reader.GetInt32(5)));
					}
					return result.ToArray();
				}
			}
		}
	}

	public async Task<RideDTO[][]> GetVehicleRides(long[] vehicleIds)
	{
		using (NpgsqlConnection connection = await source.OpenConnectionAsync())
		{
			using (NpgsqlCommand command = new NpgsqlCommand("SELECT * from public.getvehiclerides(@p_vehicleIds);", connection))
			{
				command.Parameters.AddWithValue("p_vehicleIds", vehicleIds);
				using (NpgsqlDataReader reader = await command.ExecuteReaderAsync())
				{
					List<RideDTO[]> result = new List<RideDTO[]>();
					List<RideDTO> vehicleRides = new List<RideDTO>();
					while (await reader.ReadAsync())
					{
						var ride = new RideDTO(reader.GetInt64(0), reader.GetInt64(1), reader.GetDateTime(2), reader.GetDateTime(3), reader.GetDouble(4));
						if(vehicleRides.Count > 0 && vehicleRides[0].VehicleId != ride.VehicleId)
						{
							result.Add(vehicleRides.ToArray());
							vehicleRides.Clear();
						}

						vehicleRides.Add(ride);
					}

					if(vehicleRides.Count > 0)
						result.Add(vehicleRides.ToArray());

					return result.ToArray();
				}
			}
		}
	}

	public async Task<VehicleDTO[]> GetVehicles(long[] vehicleIds)
	{
		using (NpgsqlConnection connection = await source.OpenConnectionAsync())
		{
			using (NpgsqlCommand command = new NpgsqlCommand("SELECT PositionX, PositionY, ModelName, Year, PassengerCapacity FROM VEHICLES WHERE Id = Any(@p_vehicleIds);", connection))
			{
				command.Parameters.AddWithValue("p_vehicleIds", vehicleIds);
				using (NpgsqlDataReader reader = await command.ExecuteReaderAsync())
				{

					List<VehicleDTO> result = new List<VehicleDTO>();
					while (await reader.ReadAsync())
					{
						VehicleDTO v = new VehicleDTO(reader.GetDouble(0), reader.GetDouble(1), reader.GetString(2), reader.GetInt32(3), reader.GetInt32(4));
						result.Add(v);
					}
					return result.ToArray();
				}
			}
		}
	}

	public async Task<long[]> InsertRides(RideDTO[] rideDTOs)
	{
		using (NpgsqlConnection connection = await source.OpenConnectionAsync())
		{
			using (NpgsqlCommand command = new NpgsqlCommand("SELECT public.insertrides(@p_rideDTOs)", connection))
			{
				command.Parameters.AddWithValue("p_rideDTOs", rideDTOs);
				return (long[])await command.ExecuteScalarAsync();
			}
		}
	}

	public async Task<long[]> InsertVehicles(VehicleDTO[] vehicles)
	{
		using (NpgsqlConnection connection = await source.OpenConnectionAsync())
		{
			using (NpgsqlCommand command = new NpgsqlCommand("SELECT public.insertvehicles(@p_vehicleDTOs)", connection))
			{
				command.Parameters.AddWithValue("p_vehicleDTOs", vehicles);

				return (long[])await command.ExecuteScalarAsync();
			}
		}
	}

	public async Task UpdateRides(RideDTO[] rideDTOs)
	{
		using (NpgsqlConnection connection = await source.OpenConnectionAsync())
		{
			using (NpgsqlCommand command = new NpgsqlCommand("SELECT public.updaterides(@p_rideDTOs)", connection))
			{
				command.Parameters.AddWithValue("p_rideDTOs", rideDTOs);
				await command.ExecuteNonQueryAsync();
			}
		}
	}

	public async Task UpdateVehiclePositions(long[] vehicleIds, double positionX, double positionY)
	{
		using (NpgsqlConnection connection = await source.OpenConnectionAsync())
		{
			using (NpgsqlCommand command = new NpgsqlCommand("SELECT public.updatevehiclepositions(@p_vehicle_ids, @p_position_x, @p_position_y)", connection))
			{
				command.Parameters.AddWithValue("p_vehicle_ids", vehicleIds);
				command.Parameters.AddWithValue("p_position_x", positionX);
				command.Parameters.AddWithValue("p_position_y", positionY);

				await command.ExecuteNonQueryAsync();
			}
		}

	}
}