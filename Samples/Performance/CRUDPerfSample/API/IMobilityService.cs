using System;
using Velox.Protocol;

namespace API;

[DbAPI(Name = "MobilityService")]
public interface IMobilityService
{
	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	Task<long[]> InsertVehicles(VehicleDTO[] vehicleDTOs);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	Task UpdateVehiclePositions(long[] vehicleIds, double positionX, double positionY);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	Task CopyVehiclePositions(long[] srcVehicleIds, long[] dstVehicleIds);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	Task DeleteVehicles(long[] vehicleIds);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	Task<long[]> InsertRides(RideDTO[] rideDTOs);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	Task UpdateRides(RideDTO[] rideDTOs);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None)]
	Task DeleteRides(long[] rideIds);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None, OperationType = DbAPIOperationType.Read)]
	Task<VehicleDTO[]> GetVehicles(long[] vehicleIds);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None, OperationType = DbAPIOperationType.Read)]
	Task<VehicleDTO[]> GetRideVehicle(long[] rideIds);

	[DbAPIOperation(ObjectGraphSupport = DbAPIObjectGraphSupportType.None, OperationType = DbAPIOperationType.Read)]
	Task<RideDTO[][]> GetVehicleRides(long[] vehicleIds);
}
