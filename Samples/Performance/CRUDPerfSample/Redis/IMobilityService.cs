using System.Threading.Tasks;

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
