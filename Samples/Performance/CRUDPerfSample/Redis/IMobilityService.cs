using System.Threading.Tasks;

namespace Client;
internal interface IMobilityService
{
    Task<long[]> InsertVehicles(IDatabase redis, VehicleDTO[] vehicleDTOs);

    Task UpdateVehiclePositions(IDatabase redis, long[] vehicleIds, double positionX, double positionY);

    Task CopyVehiclePositions(IDatabase redis, SourceDestinationPair[] pairs);

    Task DeleteVehicles(IDatabase redis, long[] vehicleIds);

    Task<long[]> InsertRides(IDatabase redis, RideDTO[] rideDTOs);

    Task UpdateRides(IDatabase redis, RideDTO[] rideDTOs);

    Task DeleteRides(IDatabase redis, long[] rideIds);

    Task<VehicleDTO[]> GetVehicles(IDatabase redis, long[] vehicleIds);

    Task<VehicleDTO[]> GetRideVehicle(IDatabase redis, long[] rideIds);

    Task<RideDTO[][]> GetVehicleRides(IDatabase redis, long[] vehicleIds);
}
